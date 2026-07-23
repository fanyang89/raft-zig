//! Utility helpers for entry sizing and slicing.
//!
//! Ports `include/raftpp/core/util.h` and `lib/core/util.cc`. The size metric
//! is an approximation: raftpp uses serialized Cap'n Proto bytes; we use
//! `data.len + context.len + entry_message_overhead` to keep the same
//! threshold-based truncation semantics for `LimitSize`.

const std = @import("std");

const types = @import("types.zig");

const Entry = types.Entry;
const EntryType = types.EntryType;
const Message = types.Message;
const Snapshot = types.Snapshot;
const ConfChangeV2 = types.ConfChangeV2;
const ConfChangeSingle = types.ConfChangeSingle;
const ConfChangeType = types.ConfChangeType;
const ConfChangeTransition = types.ConfChangeTransition;

/// Fixed overhead added to data/context length to approximate the serialized
/// size of an Entry (header + index/term/type fields). Matches raftpp's
/// `kEntryMessageOverhead` constant.
pub const entry_message_overhead: usize = 12;

pub const IndexTerm = struct {
    index: u64,
    term: u64,

    pub fn fromSnapshot(snapshot: Snapshot) IndexTerm {
        return .{
            .index = snapshot.metadata.index,
            .term = snapshot.metadata.term,
        };
    }
};

pub fn entryApproximateSize(ent: Entry) usize {
    return ent.data.len + ent.context.len + entry_message_overhead;
}

/// Truncate `entries` so the total approximate size stays at or below `max`.
/// Always keeps at least one entry to make progress, matching raftpp.
pub fn limitSize(entries: *[]Entry, max: ?u64) void {
    if (entries.len <= 1) return;
    const cap = max orelse return;
    if (cap == std.math.maxInt(u64)) return;

    var current_total: usize = 0;
    var keep_count: usize = 0;

    for (entries.*, 0..) |entry, i| {
        const entry_size = entryApproximateSize(entry);
        if (i == 0) {
            current_total += entry_size;
            keep_count = 1;
            continue;
        }
        if (@as(u64, current_total) + @as(u64, entry_size) > cap) break;
        current_total += entry_size;
        keep_count += 1;
    }

    entries.len = keep_count;
}

/// True when entries pick up exactly where the message's last entry ends.
pub fn isContinuousEntries(message: Message, entries: []const Entry) bool {
    if (message.entries.len > 0 and entries.len > 0) {
        const expected_next_idx = message.entries[message.entries.len - 1].index + 1;
        return expected_next_idx == entries[0].index;
    }
    return true;
}

test "limitSize keeps first entry even when max is zero" {
    var entries = [_]Entry{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
        .{ .index = 3, .term = 1 },
    };
    var slice: []Entry = &entries;
    limitSize(&slice, 0);
    try std.testing.expectEqual(@as(usize, 1), slice.len);
}

test "limitSize truncates based on approximate size" {
    var entries = [_]Entry{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
        .{ .index = 3, .term = 1 },
    };
    var slice: []Entry = &entries;
    // Each entry is entry_message_overhead bytes (no data/context); cap of
    // 2*overhead keeps the first two.
    limitSize(&slice, 2 * entry_message_overhead);
    try std.testing.expectEqual(@as(usize, 2), slice.len);

    slice = &entries;
    limitSize(&slice, 3 * entry_message_overhead);
    try std.testing.expectEqual(@as(usize, 3), slice.len);

    slice = &entries;
    limitSize(&slice, null);
    try std.testing.expectEqual(@as(usize, 3), slice.len);
}

test "indexTerm from snapshot" {
    const snap = Snapshot{ .metadata = .{ .index = 42, .term = 7 } };
    const it = IndexTerm.fromSnapshot(snap);
    try std.testing.expectEqual(@as(u64, 42), it.index);
    try std.testing.expectEqual(@as(u64, 7), it.term);
}

test "isContinuousEntries detects gap" {
    var msg_entries = [_]Entry{.{ .index = 5, .term = 1 }};
    const msg = Message{ .entries = &msg_entries };
    const cont = [_]Entry{.{ .index = 6, .term = 1 }};
    const gap = [_]Entry{.{ .index = 7, .term = 1 }};
    try std.testing.expect(isContinuousEntries(msg, &cont));
    try std.testing.expect(!isContinuousEntries(msg, &gap));
}

// ===========================================================================
// Entry checksum (CRC32C, matches raftpp's wal::CRC32C)
// ===========================================================================

const Crc32Iscsi = std.hash.crc.Crc32Iscsi;

/// Compute the CRC32C checksum of an entry's wire-identifying fields:
/// entry_type, context, then data. Matches `ComputeEntryChecksum` in
/// `include/raftpp/raftor/entry_checksum.h`.
pub fn computeEntryChecksum(entry: Entry) u32 {
    var crc = Crc32Iscsi.init();
    const entry_type_word: u32 = @intFromEnum(entry.entry_type);
    crc.update(std.mem.asBytes(&entry_type_word));
    crc.update(entry.context);
    crc.update(entry.data);
    return crc.final();
}

/// Empty normal entries (no data, no context) are exempt from checksumming
/// because raftpp historically allows them through without verification.
pub fn isChecksumExemptEntry(entry: Entry) bool {
    return entry.entry_type == .normal and entry.data.len == 0 and entry.context.len == 0;
}

/// Set `entry.checksum` if it isn't exempt. Mirrors `SetEntryChecksum`.
pub fn setEntryChecksum(entry: *Entry) void {
    if (isChecksumExemptEntry(entry.*)) return;
    entry.checksum = computeEntryChecksum(entry.*);
}

test "computeEntryChecksum is stable and order-sensitive" {
    const allocator = std.testing.allocator;
    const hello = try allocator.dupe(u8, "hello");
    defer allocator.free(hello);
    const world = try allocator.dupe(u8, "world");
    defer allocator.free(world);
    const ctx = try allocator.dupe(u8, "ctx");
    defer allocator.free(ctx);

    const e1 = Entry{ .entry_type = .normal, .data = hello, .context = ctx };
    const e2 = Entry{ .entry_type = .normal, .data = hello, .context = ctx };
    try std.testing.expectEqual(computeEntryChecksum(e1), computeEntryChecksum(e2));

    const e2b = Entry{ .entry_type = .normal, .data = world, .context = ctx };
    try std.testing.expect(computeEntryChecksum(e1) != computeEntryChecksum(e2b));

    const e3 = Entry{ .entry_type = .conf_change_v2, .data = hello, .context = ctx };
    try std.testing.expect(computeEntryChecksum(e1) != computeEntryChecksum(e3));
}

test "setEntryChecksum skips exempt entries" {
    var empty = Entry{};
    setEntryChecksum(&empty);
    try std.testing.expectEqual(@as(u32, 0), empty.checksum);

    const allocator = std.testing.allocator;
    const x = try allocator.dupe(u8, "x");
    defer allocator.free(x);
    var populated = Entry{ .data = x };
    setEntryChecksum(&populated);
    try std.testing.expect(populated.checksum != 0);
}

// ===========================================================================
// ConfChangeV2 binary codec
// ===========================================================================
//
// Wire format (all integers little-endian):
//   magic           : 4 bytes  "RCC2"
//   version         : 1 byte   currently 1
//   transition      : 1 byte   ConfChangeTransition value
//   num_changes     : 2 bytes  u16
//   changes[]       : num_changes * 9 bytes
//       change_type : 1 byte   ConfChangeType value
//       node_id     : 8 bytes  u64
//   context_len     : 4 bytes  u32
//   context         : context_len bytes
//
// This is internal to raft-zig; encoding and decoding are both in this file.
// Swap for a standard schema (protobuf, capnp) when wire compatibility with
// other raft implementations is required.

const cc_magic = [_]u8{ 'R', 'C', 'C', '2' };
const cc_version: u8 = 1;

pub fn encodeConfChangeV2(allocator: std.mem.Allocator, cc: ConfChangeV2) ![]u8 {
    const change_count = std.math.cast(u16, cc.changes.len) orelse return error.ConfChangeError;
    const context_len = std.math.cast(u32, cc.context.len) orelse return error.ConfChangeError;
    const changes_bytes = std.math.mul(usize, cc.changes.len, 9) catch return error.ConfChangeError;
    const total = std.math.add(usize, 12 + changes_bytes, cc.context.len) catch return error.ConfChangeError;
    var out = try allocator.alloc(u8, total);
    var pos: usize = 0;

    @memcpy(out[pos..][0..4], &cc_magic);
    pos += 4;
    out[pos] = cc_version;
    pos += 1;
    out[pos] = @intFromEnum(cc.transition);
    pos += 1;
    std.mem.writeInt(u16, out[pos..][0..2], change_count, .little);
    pos += 2;
    for (cc.changes) |c| {
        out[pos] = @intFromEnum(c.change_type);
        pos += 1;
        std.mem.writeInt(u64, out[pos..][0..8], c.node_id, .little);
        pos += 8;
    }
    std.mem.writeInt(u32, out[pos..][0..4], context_len, .little);
    pos += 4;
    @memcpy(out[pos..][0..cc.context.len], cc.context);
    return out;
}

pub fn decodeConfChangeV2(allocator: std.mem.Allocator, bytes: []const u8) !ConfChangeV2 {
    if (bytes.len < 4 + 1 + 1 + 2 + 4) return error.ConfChangeParseError;
    if (!std.mem.eql(u8, bytes[0..4], &cc_magic)) return error.ConfChangeParseError;
    if (bytes[4] != cc_version) return error.ConfChangeParseError;

    var pos: usize = 5;
    const transition = checkedEnum(ConfChangeTransition, bytes[pos]) orelse return error.ConfChangeParseError;
    pos += 1;
    const num_changes = std.mem.readInt(u16, bytes[pos..][0..2], .little);
    pos += 2;

    const changes_bytes = std.math.mul(usize, num_changes, 9) catch return error.ConfChangeParseError;
    const expect_after_changes = std.math.add(usize, pos, changes_bytes + 4) catch return error.ConfChangeParseError;
    if (bytes.len < expect_after_changes) return error.ConfChangeParseError;

    var changes = try allocator.alloc(ConfChangeSingle, num_changes);
    errdefer allocator.free(changes);
    for (0..num_changes) |i| {
        changes[i] = .{
            .change_type = checkedEnum(ConfChangeType, bytes[pos]) orelse return error.ConfChangeParseError,
            .node_id = std.mem.readInt(u64, bytes[pos + 1 ..][0..8], .little),
        };
        pos += 9;
    }

    const context_len = std.mem.readInt(u32, bytes[pos..][0..4], .little);
    pos += 4;
    const end = std.math.add(usize, pos, context_len) catch return error.ConfChangeParseError;
    if (bytes.len != end) return error.ConfChangeParseError;
    const context = try allocator.dupe(u8, bytes[pos..end]);

    return ConfChangeV2{
        .transition = transition,
        .changes = changes,
        .context = context,
    };
}

fn checkedEnum(comptime T: type, value: std.meta.Tag(T)) ?T {
    inline for (std.meta.fields(T)) |field| {
        if (field.value == value) return @enumFromInt(value);
    }
    return null;
}

test "encodeConfChangeV2 round-trips through decodeConfChangeV2" {
    const allocator = std.testing.allocator;

    var changes = [_]ConfChangeSingle{
        .{ .change_type = .add_node, .node_id = 1 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
    };
    const ctx = try allocator.dupe(u8, "hi");
    defer allocator.free(ctx);
    const original = ConfChangeV2{
        .transition = .auto_,
        .changes = &changes,
        .context = ctx,
    };

    const bytes = try encodeConfChangeV2(allocator, original);
    defer allocator.free(bytes);

    var decoded = try decodeConfChangeV2(allocator, bytes);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(original.transition, decoded.transition);
    try std.testing.expectEqual(original.changes.len, decoded.changes.len);
    for (original.changes, decoded.changes) |a, b| {
        try std.testing.expectEqual(a.change_type, b.change_type);
        try std.testing.expectEqual(a.node_id, b.node_id);
    }
    try std.testing.expectEqualStrings(original.context, decoded.context);
}

test "decodeConfChangeV2 rejects bad magic" {
    const allocator = std.testing.allocator;
    const bad = [_]u8{ 'B', 'A', 'D', 'X', 1, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectError(error.ConfChangeParseError, decodeConfChangeV2(allocator, &bad));
}

test "decodeConfChangeV2 rejects invalid enums and trailing data" {
    const allocator = std.testing.allocator;
    const encoded = try encodeConfChangeV2(allocator, .{});
    defer allocator.free(encoded);

    var invalid_transition = try allocator.dupe(u8, encoded);
    defer allocator.free(invalid_transition);
    invalid_transition[5] = 0xff;
    try std.testing.expectError(error.ConfChangeParseError, decodeConfChangeV2(allocator, invalid_transition));

    const trailing = try std.mem.concat(allocator, u8, &.{ encoded, "x" });
    defer allocator.free(trailing);
    try std.testing.expectError(error.ConfChangeParseError, decodeConfChangeV2(allocator, trailing));
}

test "fuzz: ConfChangeV2 codec" {
    try std.testing.fuzz({}, fuzzConfChangeV2, .{ .corpus = &.{
        "",
        "RCC2",
        "RCC2\x01\xff\x00\x00\x00\x00\x00\x00",
    } });
}

fn fuzzConfChangeV2(_: void, smith: *std.testing.Smith) !void {
    const allocator = std.testing.allocator;
    var input_buffer: [2048]u8 = undefined;
    const input_len = smith.valueRangeAtMost(u16, 0, input_buffer.len);
    const input = input_buffer[0..input_len];
    smith.bytes(input);

    if (decodeConfChangeV2(allocator, input)) |decoded_value| {
        var decoded = decoded_value;
        defer decoded.deinit(allocator);
        const canonical = try encodeConfChangeV2(allocator, decoded);
        defer allocator.free(canonical);
        var round_trip = try decodeConfChangeV2(allocator, canonical);
        defer round_trip.deinit(allocator);
        try expectConfChangeEqual(decoded, round_trip);
    } else |_| {}

    var changes: [8]ConfChangeSingle = undefined;
    const change_count = smith.valueRangeAtMost(u8, 0, changes.len);
    for (changes[0..change_count]) |*change| {
        change.* = .{
            .change_type = smith.value(ConfChangeType),
            .node_id = smith.value(u64),
        };
    }
    var context_buffer: [64]u8 = undefined;
    const context_len = smith.valueRangeAtMost(u8, 0, context_buffer.len);
    const context = context_buffer[0..context_len];
    smith.bytes(context);
    const generated = ConfChangeV2{
        .transition = smith.value(ConfChangeTransition),
        .changes = changes[0..change_count],
        .context = context,
    };
    const encoded = try encodeConfChangeV2(allocator, generated);
    defer allocator.free(encoded);
    var decoded = try decodeConfChangeV2(allocator, encoded);
    defer decoded.deinit(allocator);
    try expectConfChangeEqual(generated, decoded);
}

fn expectConfChangeEqual(expected: ConfChangeV2, actual: ConfChangeV2) !void {
    try std.testing.expectEqual(expected.transition, actual.transition);
    try std.testing.expectEqual(expected.changes.len, actual.changes.len);
    for (expected.changes, actual.changes) |expected_change, actual_change| {
        try std.testing.expectEqual(expected_change.change_type, actual_change.change_type);
        try std.testing.expectEqual(expected_change.node_id, actual_change.node_id);
    }
    try std.testing.expectEqualSlices(u8, expected.context, actual.context);
}
