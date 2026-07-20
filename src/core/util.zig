//! Utility helpers for entry sizing and slicing.
//!
//! Ports `include/raftpp/core/util.h` and `lib/core/util.cc`. The size metric
//! is an approximation: raftpp uses serialized Cap'n Proto bytes; we use
//! `data.len + context.len + entry_message_overhead` to keep the same
//! threshold-based truncation semantics for `LimitSize`.

const std = @import("std");

const types = @import("types.zig");

const Entry = types.Entry;
const Message = types.Message;
const Snapshot = types.Snapshot;

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
