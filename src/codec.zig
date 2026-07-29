//! Binary codec for Raft messages.
//!
//! Defines the wire format for serializing `Message` structs to/from bytes.
//! Used by the future TCP transport; the `LoopbackTransport` passes Message
//! values directly without encoding. The format is internal to raft-zig and
//! uses little-endian fixed-width fields throughout.

const std = @import("std");

const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");

const Message = types.Message;
const MessageType = types.MessageType;
const Entry = types.Entry;
const EntryType = types.EntryType;
const Snapshot = types.Snapshot;
const ConfState = types.ConfState;
const cloneSnapshot = storage_mod.cloneSnapshot;
const cloneConfState = storage_mod.cloneConfState;

const codec_magic: u32 = 0x52415046; // "RAPF"
const codec_version: u32 = 1;
const header_size: usize = 4 + 4 + 8 + 8 + 8 + 1 + 4; // magic+ver+from+to+req+type+payload_len = 37
const encoded_entry_min_size: usize = 1 + 8 + 8 + 4 + 4 + 4;

// ===========================================================================
// Encoder
// ===========================================================================

/// Encode a Message to an owned byte slice. The caller owns the result.
pub fn encodeMessage(allocator: std.mem.Allocator, msg: Message) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);

    // Scalar fields.
    try buf.append(allocator, @intFromEnum(msg.msg_type));
    try writeU64(allocator, &buf, msg.to);
    try writeU64(allocator, &buf, msg.from);
    try writeU64(allocator, &buf, msg.term);
    try writeU64(allocator, &buf, msg.log_term);
    try writeU64(allocator, &buf, msg.index);
    try writeU64(allocator, &buf, msg.commit);
    try writeU64(allocator, &buf, msg.commit_term);
    try writeU64(allocator, &buf, msg.request_snapshot);
    try buf.append(allocator, if (msg.reject) 1 else 0);
    try writeU64(allocator, &buf, msg.reject_hint);
    var priority_bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &priority_bytes, msg.priority, .little);
    try buf.appendSlice(allocator, &priority_bytes);

    // context bytes.
    try writeBytes(allocator, &buf, msg.context);

    // entries.
    try writeU32(allocator, &buf, try encodedLength(msg.entries.len));
    for (msg.entries) |e| {
        try buf.append(allocator, @intFromEnum(e.entry_type));
        try writeU64(allocator, &buf, e.term);
        try writeU64(allocator, &buf, e.index);
        var checksum_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &checksum_bytes, e.checksum, .little);
        try buf.appendSlice(allocator, &checksum_bytes);
        try writeBytes(allocator, &buf, e.data);
        try writeBytes(allocator, &buf, e.context);
    }

    // snapshot.
    if (msg.snapshot) |snap| {
        try buf.append(allocator, 1);
        try writeU64(allocator, &buf, snap.metadata.index);
        try writeU64(allocator, &buf, snap.metadata.term);
        try writeConfState(allocator, &buf, snap.metadata.conf_state);
        try writeBytes(allocator, &buf, snap.membership);
        try writeBytes(allocator, &buf, snap.data);
    } else {
        try buf.append(allocator, 0);
    }

    return buf.toOwnedSlice(allocator);
}

/// Encode a Message with an RPC frame header (magic + version + routing).
pub fn encodeFramed(
    allocator: std.mem.Allocator,
    msg: Message,
    from_node: u64,
    to_node: u64,
) ![]u8 {
    const payload = try encodeMessage(allocator, msg);
    defer allocator.free(payload);

    var frame: std.ArrayList(u8) = .empty;
    errdefer frame.deinit(allocator);

    // Magic + version.
    var magic_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &magic_bytes, codec_magic, .little);
    try frame.appendSlice(allocator, &magic_bytes);
    var ver_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &ver_bytes, codec_version, .little);
    try frame.appendSlice(allocator, &ver_bytes);

    // Routing: from_node, to_node, request_id (0).
    try writeU64(allocator, &frame, from_node);
    try writeU64(allocator, &frame, to_node);
    try writeU64(allocator, &frame, 0); // request_id

    // Message type + payload size.
    try frame.append(allocator, @intFromEnum(msg.msg_type));
    try writeU32(allocator, &frame, try encodedLength(payload.len));

    // Payload.
    try frame.appendSlice(allocator, payload);

    return frame.toOwnedSlice(allocator);
}

// ===========================================================================
// Decoder
// ===========================================================================

pub const DecodeError = error{
    InvalidMagic,
    InvalidVersion,
    InvalidMessageType,
    InvalidEntryType,
    MessageTypeMismatch,
    TrailingData,
    TruncatedMessage,
    OutOfMemory,
};

/// Decode a Message from bytes. The caller owns the returned Message and must
/// call `deinit`.
pub fn decodeMessage(allocator: std.mem.Allocator, data: []const u8) !Message {
    var pos: usize = 0;
    const message = try decodeMessageAt(allocator, data, &pos);
    if (pos != data.len) {
        var owned = message;
        owned.deinit(allocator);
        return error.TrailingData;
    }
    return message;
}

fn decodeMessageAt(allocator: std.mem.Allocator, data: []const u8, pos: *usize) !Message {
    var decoder = Decoder{ .data = data, .pos = pos.* };

    const msg_type = checkedEnum(MessageType, try decoder.readByte()) orelse return error.InvalidMessageType;
    const to = try decoder.readInt(u64);
    const from = try decoder.readInt(u64);
    const term = try decoder.readInt(u64);
    const log_term = try decoder.readInt(u64);
    const index = try decoder.readInt(u64);
    const commit = try decoder.readInt(u64);
    const commit_term = try decoder.readInt(u64);
    const request_snapshot = try decoder.readInt(u64);
    const reject = try decoder.readByte() != 0;
    const reject_hint = try decoder.readInt(u64);
    const priority = try decoder.readInt(i64);

    const context = try decoder.readBytes(allocator);
    errdefer if (context.len > 0) allocator.free(context);

    const num_entries = try decoder.readInt(u32);
    const remaining = decoder.data.len - decoder.pos;
    if (remaining == 0 or num_entries > (remaining - 1) / encoded_entry_min_size) {
        return error.TruncatedMessage;
    }
    var entries = try allocator.alloc(Entry, num_entries);
    var actual_entries: usize = 0;
    errdefer {
        for (entries[0..actual_entries]) |*e| e.deinit(allocator);
        allocator.free(entries);
    }
    for (0..num_entries) |_| {
        entries[actual_entries] = try decodeEntry(allocator, &decoder);
        actual_entries += 1;
    }

    var snapshot: ?Snapshot = null;
    const has_snap = try decoder.readByte() != 0;
    if (has_snap) {
        const snap_index = try decoder.readInt(u64);
        const snap_term = try decoder.readInt(u64);
        var snap_conf = try decoder.readConfState(allocator);
        errdefer snap_conf.deinit(allocator);
        const snap_membership = try decoder.readBytes(allocator);
        errdefer if (snap_membership.len > 0) allocator.free(snap_membership);
        const snap_data = try decoder.readBytes(allocator);
        errdefer if (snap_data.len > 0) allocator.free(snap_data);
        snapshot = .{
            .membership = snap_membership,
            .data = snap_data,
            .metadata = .{
                .index = snap_index,
                .term = snap_term,
                .conf_state = snap_conf,
            },
        };
    }

    pos.* = decoder.pos;
    return .{
        .msg_type = msg_type,
        .to = to,
        .from = from,
        .term = term,
        .log_term = log_term,
        .index = index,
        .commit = commit,
        .commit_term = commit_term,
        .request_snapshot = request_snapshot,
        .reject = reject,
        .reject_hint = reject_hint,
        .priority = priority,
        .context = context,
        .entries = entries,
        .snapshot = snapshot,
    };
}

fn decodeEntry(allocator: std.mem.Allocator, decoder: *Decoder) !Entry {
    var entry = Entry{
        .entry_type = checkedEnum(EntryType, try decoder.readByte()) orelse return error.InvalidEntryType,
        .term = try decoder.readInt(u64),
        .index = try decoder.readInt(u64),
        .checksum = try decoder.readInt(u32),
    };
    errdefer entry.deinit(allocator);

    const data = try decoder.readBytes(allocator);
    entry.adoptData(allocator, data) catch |err| {
        allocator.free(data);
        return err;
    };
    entry.context = try decoder.readBytes(allocator);
    return entry;
}

/// Decode a framed message (with RPC header). Returns the decoded Message
/// and bytes consumed. The caller owns the Message.
pub const FramedDecodeResult = struct {
    message: Message,
    bytes_consumed: usize,
};

pub fn decodeFramed(allocator: std.mem.Allocator, data: []const u8) !FramedDecodeResult {
    var decoder = Decoder{ .data = data };
    const magic = try decoder.readInt(u32);
    if (magic != codec_magic) return error.InvalidMagic;
    if (try decoder.readInt(u32) != codec_version) return error.InvalidVersion;
    _ = try decoder.readInt(u64); // from_node
    _ = try decoder.readInt(u64); // to_node
    _ = try decoder.readInt(u64); // request_id
    const header_type = checkedEnum(MessageType, try decoder.readByte()) orelse return error.InvalidMessageType;
    const payload_len = try decoder.readInt(u32);
    const payload = try decoder.take(payload_len);
    var msg = try decodeMessage(allocator, payload);
    errdefer msg.deinit(allocator);
    if (msg.msg_type != header_type) return error.MessageTypeMismatch;
    return .{
        .message = msg,
        .bytes_consumed = decoder.pos,
    };
}

// ===========================================================================
// Primitive read/write helpers
// ===========================================================================

fn writeU64(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), val: u64) !void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, val, .little);
    try buf.appendSlice(allocator, &bytes);
}

fn writeU32(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), val: u32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, val, .little);
    try buf.appendSlice(allocator, &bytes);
}

fn writeBytes(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), bytes: []const u8) !void {
    try writeU32(allocator, buf, try encodedLength(bytes.len));
    try buf.appendSlice(allocator, bytes);
}

fn writeConfState(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), cs: ConfState) !void {
    try writeU64Slice(allocator, buf, cs.voters);
    try writeU64Slice(allocator, buf, cs.learners);
    try writeU64Slice(allocator, buf, cs.voters_outgoing);
    try writeU64Slice(allocator, buf, cs.learners_next);
    try buf.append(allocator, if (cs.auto_leave) 1 else 0);
}

fn writeU64Slice(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), slice: []const u64) !void {
    try writeU32(allocator, buf, try encodedLength(slice.len));
    for (slice) |v| try writeU64(allocator, buf, v);
}

fn encodedLength(len: usize) !u32 {
    return std.math.cast(u32, len) orelse error.MessageTooLarge;
}

const Decoder = struct {
    data: []const u8,
    pos: usize = 0,

    fn take(self: *Decoder, len: usize) ![]const u8 {
        const end = std.math.add(usize, self.pos, len) catch return error.TruncatedMessage;
        if (end > self.data.len) return error.TruncatedMessage;
        const result = self.data[self.pos..end];
        self.pos = end;
        return result;
    }

    fn readByte(self: *Decoder) !u8 {
        return (try self.take(1))[0];
    }

    fn readInt(self: *Decoder, comptime T: type) !T {
        const size = @divExact(@bitSizeOf(T), 8);
        return std.mem.readInt(T, (try self.take(size))[0..size], .little);
    }

    fn readBytes(self: *Decoder, allocator: std.mem.Allocator) ![]u8 {
        const bytes = try self.take(try self.readInt(u32));
        return if (bytes.len == 0) &.{} else allocator.dupe(u8, bytes);
    }

    fn readConfState(self: *Decoder, allocator: std.mem.Allocator) !ConfState {
        const voters = try self.readU64Slice(allocator);
        errdefer allocator.free(voters);
        const learners = try self.readU64Slice(allocator);
        errdefer allocator.free(learners);
        const voters_outgoing = try self.readU64Slice(allocator);
        errdefer allocator.free(voters_outgoing);
        const learners_next = try self.readU64Slice(allocator);
        errdefer allocator.free(learners_next);
        return .{
            .voters = voters,
            .learners = learners,
            .voters_outgoing = voters_outgoing,
            .learners_next = learners_next,
            .auto_leave = try self.readByte() != 0,
        };
    }

    fn readU64Slice(self: *Decoder, allocator: std.mem.Allocator) ![]u64 {
        const count: usize = @intCast(try self.readInt(u32));
        const byte_len = std.math.mul(usize, count, @sizeOf(u64)) catch return error.TruncatedMessage;
        const bytes = try self.take(byte_len);
        const result = try allocator.alloc(u64, count);
        for (result, 0..) |*value, i| {
            value.* = std.mem.readInt(u64, bytes[i * 8 ..][0..8], .little);
        }
        return result;
    }
};

fn checkedEnum(comptime T: type, value: std.meta.Tag(T)) ?T {
    inline for (std.meta.fields(T)) |field| {
        if (field.value == value) return @enumFromInt(value);
    }
    return null;
}

// ===========================================================================
// Tests
// ===========================================================================

// KCOV_EXCL_START
test "codec: message round-trip with entries and data" {
    const allocator = std.testing.allocator;

    var entries = try allocator.alloc(Entry, 2);
    entries[0] = .{
        .entry_type = .normal,
        .term = 1,
        .index = 5,
        .data = try allocator.dupe(u8, "hello"),
    };
    entries[1] = .{
        .entry_type = .conf_change_v2,
        .term = 2,
        .index = 6,
        .context = try allocator.dupe(u8, "ctx"),
    };

    var original = Message{
        .msg_type = .append,
        .to = 2,
        .from = 1,
        .term = 3,
        .log_term = 2,
        .index = 4,
        .commit = 5,
        .entries = entries,
        .context = try allocator.dupe(u8, "routing"),
    };
    defer original.deinit(allocator);

    const bytes = try encodeMessage(allocator, original);
    defer allocator.free(bytes);

    var decoded = try decodeMessage(allocator, bytes);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(original.msg_type, decoded.msg_type);
    try std.testing.expectEqual(original.to, decoded.to);
    try std.testing.expectEqual(original.from, decoded.from);
    try std.testing.expectEqual(original.term, decoded.term);
    try std.testing.expectEqual(original.log_term, decoded.log_term);
    try std.testing.expectEqual(original.index, decoded.index);
    try std.testing.expectEqual(original.commit, decoded.commit);
    try std.testing.expectEqualStrings("routing", decoded.context);
    try std.testing.expectEqual(@as(usize, 2), decoded.entries.len);
    try std.testing.expectEqualStrings("hello", decoded.entries[0].data);
    try std.testing.expectEqualStrings("ctx", decoded.entries[1].context);

    var shared = try storage_mod.shareEntry(allocator, decoded.entries[0]);
    defer shared.deinit(allocator);
    try std.testing.expectEqual(@intFromPtr(decoded.entries[0].data.ptr), @intFromPtr(shared.data.ptr));
    decoded.deinit(allocator);
    try std.testing.expectEqualStrings("hello", shared.data);
}

test "codec: message with snapshot round-trips" {
    const allocator = std.testing.allocator;
    const voters = try allocator.dupe(u64, &.{ 1, 2, 3 });

    var original = Message{
        .msg_type = .snapshot,
        .to = 3,
        .from = 1,
        .term = 5,
        .snapshot = .{
            .membership = try allocator.dupe(u8, "RCLS membership"),
            .data = try allocator.dupe(u8, "snap"),
            .metadata = .{
                .index = 100,
                .term = 4,
                .conf_state = .{ .voters = voters },
            },
        },
    };
    defer original.deinit(allocator);

    const bytes = try encodeMessage(allocator, original);
    defer allocator.free(bytes);

    var decoded = try decodeMessage(allocator, bytes);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 3), decoded.to);
    try std.testing.expect(decoded.snapshot != null);
    try std.testing.expectEqual(@as(u64, 100), decoded.snapshot.?.metadata.index);
    try std.testing.expectEqual(@as(u64, 4), decoded.snapshot.?.metadata.term);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, decoded.snapshot.?.metadata.conf_state.voters);
    try std.testing.expectEqualStrings("RCLS membership", decoded.snapshot.?.membership);
    try std.testing.expectEqualStrings("snap", decoded.snapshot.?.data);
}

test "codec: framed encode/decode round-trip" {
    const allocator = std.testing.allocator;
    const original = Message{
        .msg_type = .request_vote,
        .to = 2,
        .from = 1,
        .term = 5,
        .index = 10,
        .log_term = 3,
    };

    const framed = try encodeFramed(allocator, original, 1, 2);
    defer allocator.free(framed);

    var result = try decodeFramed(allocator, framed);
    defer result.message.deinit(allocator);

    try std.testing.expectEqual(original.msg_type, result.message.msg_type);
    try std.testing.expectEqual(original.to, result.message.to);
    try std.testing.expectEqual(original.term, result.message.term);
    try std.testing.expectEqual(framed.len, result.bytes_consumed);
}

test "codec: decodeFramed rejects bad magic" {
    const allocator = std.testing.allocator;
    const bad = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectError(error.InvalidMagic, decodeFramed(allocator, &bad));
}

test "codec: malformed messages return errors" {
    const allocator = std.testing.allocator;
    const original = Message{ .msg_type = .append, .to = 2, .from = 1 };
    const encoded = try encodeMessage(allocator, original);
    defer allocator.free(encoded);

    var invalid_type = try allocator.dupe(u8, encoded);
    defer allocator.free(invalid_type);
    invalid_type[0] = 0xff;
    try std.testing.expectError(error.InvalidMessageType, decodeMessage(allocator, invalid_type));

    for (0..encoded.len) |prefix_len| {
        try std.testing.expectError(error.TruncatedMessage, decodeMessage(allocator, encoded[0..prefix_len]));
    }

    const trailing = try std.mem.concat(allocator, u8, &.{ encoded, "x" });
    defer allocator.free(trailing);
    try std.testing.expectError(error.TrailingData, decodeMessage(allocator, trailing));
}

test "codec: framed decoder respects declared payload" {
    const allocator = std.testing.allocator;
    const original = Message{ .msg_type = .append, .to = 2, .from = 1 };
    var framed = try encodeFramed(allocator, original, 1, 2);
    defer allocator.free(framed);

    const payload_len = std.mem.readInt(u32, framed[33..37], .little);
    std.mem.writeInt(u32, framed[33..37], payload_len - 1, .little);
    try std.testing.expectError(error.TruncatedMessage, decodeFramed(allocator, framed));
}

test "codec: framed decoder validates version and message type" {
    const allocator = std.testing.allocator;
    const original = Message{ .msg_type = .append, .to = 2, .from = 1 };

    var invalid_version = try encodeFramed(allocator, original, 1, 2);
    defer allocator.free(invalid_version);
    std.mem.writeInt(u32, invalid_version[4..8], codec_version + 1, .little);
    try std.testing.expectError(error.InvalidVersion, decodeFramed(allocator, invalid_version));

    var mismatched_type = try encodeFramed(allocator, original, 1, 2);
    defer allocator.free(mismatched_type);
    mismatched_type[32] = @intFromEnum(MessageType.heartbeat);
    try std.testing.expectError(error.MessageTypeMismatch, decodeFramed(allocator, mismatched_type));
}

test "codec: entry count is bounded by the remaining payload" {
    const allocator = std.testing.allocator;
    const original = Message{ .msg_type = .append };
    var encoded = try encodeMessage(allocator, original);
    defer allocator.free(encoded);

    const entry_count_offset = 1 + 8 * 8 + 1 + 8 + 8 + 4;
    std.mem.writeInt(u32, encoded[entry_count_offset..][0..4], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.TruncatedMessage, decodeMessage(allocator, encoded));
}

test "codec: rich message allocation failures clean up" {
    const allocator = std.testing.allocator;
    var entries = [_]Entry{
        .{
            .entry_type = .normal,
            .term = 8,
            .index = 12,
            .checksum = 0x12345678,
            .data = @constCast("entry-data"),
            .context = @constCast("entry-context"),
        },
        .{
            .entry_type = .conf_change_v2,
            .term = 8,
            .index = 13,
            .data = @constCast("configuration"),
            .context = @constCast("change-context"),
        },
    };
    const message = Message{
        .msg_type = .snapshot,
        .to = 9,
        .from = 4,
        .term = 8,
        .log_term = 7,
        .index = 13,
        .commit = 12,
        .commit_term = 8,
        .request_snapshot = 11,
        .reject = true,
        .reject_hint = 10,
        .priority = -3,
        .context = @constCast("message-context"),
        .entries = &entries,
        .snapshot = .{
            .membership = @constCast("membership-data"),
            .data = @constCast("snapshot-data"),
            .metadata = .{
                .index = 11,
                .term = 7,
                .conf_state = .{
                    .voters = @constCast(&[_]u64{ 1, 2, 3 }),
                    .learners = @constCast(&[_]u64{4}),
                    .voters_outgoing = @constCast(&[_]u64{ 1, 2 }),
                    .learners_next = @constCast(&[_]u64{5}),
                    .auto_leave = true,
                },
            },
        },
    };
    const encoded = try encodeMessage(allocator, message);
    defer allocator.free(encoded);
    const framed = try encodeFramed(allocator, message, message.from, message.to);
    defer allocator.free(framed);

    const Check = struct {
        fn encode(failing_allocator: std.mem.Allocator, msg: Message) !void {
            const result = try encodeMessage(failing_allocator, msg);
            defer failing_allocator.free(result);
        }

        fn encodeFrame(failing_allocator: std.mem.Allocator, msg: Message) !void {
            const result = try encodeFramed(failing_allocator, msg, msg.from, msg.to);
            defer failing_allocator.free(result);
        }

        fn decode(failing_allocator: std.mem.Allocator, bytes: []const u8) !void {
            var result = try decodeMessage(failing_allocator, bytes);
            defer result.deinit(failing_allocator);
            try std.testing.expectEqual(@as(usize, 2), result.entries.len);
            try std.testing.expect(result.snapshot != null);
        }

        fn decodeFrame(failing_allocator: std.mem.Allocator, bytes: []const u8) !void {
            var result = try decodeFramed(failing_allocator, bytes);
            defer result.message.deinit(failing_allocator);
            try std.testing.expectEqual(bytes.len, result.bytes_consumed);
            try std.testing.expect(result.message.snapshot != null);
        }
    };

    try std.testing.checkAllAllocationFailures(allocator, Check.encode, .{message});
    try std.testing.checkAllAllocationFailures(allocator, Check.encodeFrame, .{message});
    try std.testing.checkAllAllocationFailures(allocator, Check.decode, .{encoded});
    try std.testing.checkAllAllocationFailures(allocator, Check.decodeFrame, .{framed});
}

test "fuzz: codec decoders" {
    try std.testing.fuzz({}, fuzzCodec, .{ .corpus = &.{
        "",
        "RAPF",
        "\xff\xff\xff\xff",
    } });
}

fn fuzzCodec(_: void, smith: *std.testing.Smith) !void {
    const allocator = std.testing.allocator;
    var input_buffer: [4096]u8 = undefined;
    const input_len = smith.valueRangeAtMost(u16, 0, input_buffer.len);
    const input = input_buffer[0..input_len];
    smith.bytes(input);

    if (decodeMessage(allocator, input)) |decoded_value| {
        var decoded = decoded_value;
        defer decoded.deinit(allocator);
        const canonical = try encodeMessage(allocator, decoded);
        defer allocator.free(canonical);
        var round_trip = try decodeMessage(allocator, canonical);
        defer round_trip.deinit(allocator);
        const encoded_again = try encodeMessage(allocator, round_trip);
        defer allocator.free(encoded_again);
        try std.testing.expectEqualSlices(u8, canonical, encoded_again);
    } else |_| {}

    if (decodeFramed(allocator, input)) |decoded_value| {
        var decoded = decoded_value;
        defer decoded.message.deinit(allocator);
        try std.testing.expect(decoded.bytes_consumed <= input.len);
        const canonical = try encodeFramed(
            allocator,
            decoded.message,
            decoded.message.from,
            decoded.message.to,
        );
        defer allocator.free(canonical);
        var round_trip = try decodeFramed(allocator, canonical);
        defer round_trip.message.deinit(allocator);
        try std.testing.expectEqual(canonical.len, round_trip.bytes_consumed);
    } else |_| {}

    const split = input.len / 2;
    var entries = [_]Entry{.{
        .entry_type = smith.value(EntryType),
        .term = smith.value(u64),
        .index = smith.value(u64),
        .checksum = smith.value(u32),
        .data = @constCast(input[0..split]),
        .context = @constCast(input[split..]),
    }};
    const structured = Message{
        .msg_type = smith.value(MessageType),
        .to = smith.value(u64),
        .from = smith.value(u64),
        .term = smith.value(u64),
        .entries = if (smith.value(bool)) entries[0..] else &.{},
        .context = @constCast(input),
        .snapshot = if (smith.value(bool)) .{
            .membership = @constCast(input[0..split]),
            .data = @constCast(input[split..]),
        } else null,
    };
    const canonical = try encodeMessage(allocator, structured);
    defer allocator.free(canonical);
    var round_trip = try decodeMessage(allocator, canonical);
    defer round_trip.deinit(allocator);
    const encoded_again = try encodeMessage(allocator, round_trip);
    defer allocator.free(encoded_again);
    try std.testing.expectEqualSlices(u8, canonical, encoded_again);
}
// KCOV_EXCL_STOP
