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
const cloneEntry = storage_mod.cloneEntry;
const cloneSnapshot = storage_mod.cloneSnapshot;
const cloneConfState = storage_mod.cloneConfState;

const codec_magic: u32 = 0x52415046; // "RAPF"
const codec_version: u32 = 1;
const header_size: usize = 4 + 4 + 8 + 8 + 8 + 1 + 4; // magic+ver+from+to+req+type+payload_len = 37

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
    try writeU32(allocator, &buf, @intCast(msg.entries.len));
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
    try writeU32(allocator, &frame, @intCast(payload.len));

    // Payload.
    try frame.appendSlice(allocator, payload);

    return frame.toOwnedSlice(allocator);
}

// ===========================================================================
// Decoder
// ===========================================================================

pub const DecodeError = error{
    InvalidMagic,
    TruncatedMessage,
    OutOfMemory,
};

/// Decode a Message from bytes. The caller owns the returned Message and must
/// call `deinit`.
pub fn decodeMessage(allocator: std.mem.Allocator, data: []const u8) !Message {
    var pos: usize = 0;
    return decodeMessageAt(allocator, data, &pos);
}

fn decodeMessageAt(allocator: std.mem.Allocator, data: []const u8, pos: *usize) !Message {
    if (data.len < pos.* + 1 + 8 * 9 + 1 + 8 + 8) return error.TruncatedMessage;
    var p = pos.*;

    const msg_type: MessageType = @enumFromInt(data[p]);
    p += 1;
    const to = readU64(data, &p);
    const from = readU64(data, &p);
    const term = readU64(data, &p);
    const log_term = readU64(data, &p);
    const index = readU64(data, &p);
    const commit = readU64(data, &p);
    const commit_term = readU64(data, &p);
    const request_snapshot = readU64(data, &p);
    const reject = data[p] != 0;
    p += 1;
    const reject_hint = readU64(data, &p);
    const priority = std.mem.readInt(i64, data[p..][0..8], .little);
    p += 8;

    const context = try readBytes(allocator, data, &p);

    const num_entries = readU32(data, &p);
    var entries = try allocator.alloc(Entry, num_entries);
    var actual_entries: usize = 0;
    errdefer {
        for (entries[0..actual_entries]) |*e| e.deinit(allocator);
        allocator.free(entries);
    }
    for (0..num_entries) |_| {
        if (data.len < p + 1 + 8 + 8 + 4) return error.TruncatedMessage;
        const entry_type: EntryType = @enumFromInt(data[p]);
        p += 1;
        const e_term = readU64(data, &p);
        const e_index = readU64(data, &p);
        const checksum = std.mem.readInt(u32, data[p..][0..4], .little);
        p += 4;
        const e_data = try readBytes(allocator, data, &p);
        const e_ctx = try readBytes(allocator, data, &p);
        entries[actual_entries] = .{
            .entry_type = entry_type,
            .term = e_term,
            .index = e_index,
            .checksum = checksum,
            .data = e_data,
            .context = e_ctx,
        };
        actual_entries += 1;
    }

    var snapshot: ?Snapshot = null;
    if (p < data.len) {
        const has_snap = data[p] != 0;
        p += 1;
        if (has_snap) {
            const snap_index = readU64(data, &p);
            const snap_term = readU64(data, &p);
            const snap_conf = try readConfState(allocator, data, &p);
            const snap_data = try readBytes(allocator, data, &p);
            snapshot = .{
                .data = snap_data,
                .metadata = .{
                    .index = snap_index,
                    .term = snap_term,
                    .conf_state = snap_conf,
                },
            };
        }
    }

    pos.* = p;
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

/// Decode a framed message (with RPC header). Returns the decoded Message
/// and bytes consumed. The caller owns the Message.
pub const FramedDecodeResult = struct {
    message: Message,
    bytes_consumed: usize,
};

pub fn decodeFramed(allocator: std.mem.Allocator, data: []const u8) !FramedDecodeResult {
    if (data.len < header_size) return error.TruncatedMessage;

    var pos: usize = 0;
    const magic = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (magic != codec_magic) return error.InvalidMagic;
    pos += 4; // version

    const from_node = readU64(data, &pos);
    _ = from_node;
    const to_node = readU64(data, &pos);
    _ = to_node;
    pos += 8; // request_id
    pos += 1; // msg_type
    const payload_len = readU32(data, &pos);

    if (data.len < pos + payload_len) return error.TruncatedMessage;

    var msg_pos: usize = pos;
    const msg = try decodeMessageAt(allocator, data, &msg_pos);
    return .{
        .message = msg,
        .bytes_consumed = pos + payload_len,
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
    try writeU32(allocator, buf, @intCast(bytes.len));
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
    try writeU32(allocator, buf, @intCast(slice.len));
    for (slice) |v| try writeU64(allocator, buf, v);
}

fn readU64(data: []const u8, pos: *usize) u64 {
    const val = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return val;
}

fn readU32(data: []const u8, pos: *usize) u32 {
    const val = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return val;
}

fn readBytes(allocator: std.mem.Allocator, data: []const u8, pos: *usize) ![]u8 {
    const len = readU32(data, pos);
    if (data.len < pos.* + len) return error.TruncatedMessage;
    if (len == 0) {
        pos.* += 0;
        return &.{};
    }
    const out = try allocator.dupe(u8, data[pos.* .. pos.* + len]);
    pos.* += len;
    return out;
}

fn readConfState(allocator: std.mem.Allocator, data: []const u8, pos: *usize) !ConfState {
    const voters = try readU64SliceOwned(allocator, data, pos);
    errdefer allocator.free(voters);
    const learners = try readU64SliceOwned(allocator, data, pos);
    errdefer allocator.free(learners);
    const voters_outgoing = try readU64SliceOwned(allocator, data, pos);
    errdefer allocator.free(voters_outgoing);
    const learners_next = try readU64SliceOwned(allocator, data, pos);
    errdefer allocator.free(learners_next);
    const auto_leave = if (pos.* < data.len) blk: {
        const v = data[pos.*] != 0;
        pos.* += 1;
        break :blk v;
    } else false;
    return .{
        .voters = voters,
        .learners = learners,
        .voters_outgoing = voters_outgoing,
        .learners_next = learners_next,
        .auto_leave = auto_leave,
    };
}

fn readU64SliceOwned(allocator: std.mem.Allocator, data: []const u8, pos: *usize) ![]u64 {
    const len = readU32(data, pos);
    const count: usize = @intCast(len);
    if (data.len < pos.* + count * 8) return error.TruncatedMessage;
    const out = try allocator.alloc(u64, count);
    for (0..count) |i| {
        out[i] = std.mem.readInt(u64, data[pos.* + i * 8 ..][0..8], .little);
    }
    pos.* += count * 8;
    return out;
}

// ===========================================================================
// Tests
// ===========================================================================

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
