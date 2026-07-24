//! MemoryStorage test suite.
//!
//! These cases describe the storage contract external callers depend on:
//! index/term lookups across compaction boundaries, entry slicing with size
//! caps, append/truncate semantics, snapshot application, and the
//! unavailable-snapshot trigger.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

fn makeEntries(start_idx: u64, count: usize) ![]raft.Entry {
    const out = try allocator.alloc(raft.Entry, count);
    for (out, 0..) |*e, i| e.* = .{ .index = start_idx + @as(u64, @intCast(i)), .term = start_idx + @as(u64, @intCast(i)) };
    return out;
}

fn freeEntries(ents: []raft.Entry) void {
    for (ents) |*e| e.deinit(allocator);
    allocator.free(ents);
}

fn expectEntriesEqual(actual: []const raft.Entry, expected: []const raft.Entry) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (actual, expected) |a, e| {
        try std.testing.expectEqual(e.index, a.index);
        try std.testing.expectEqual(e.term, a.term);
    }
}

test "storage: term reports compacted and unavailable" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    const ents = try makeEntries(3, 3);
    defer freeEntries(ents);
    try storage.setEntries(allocator, ents);

    try std.testing.expectError(error.Compacted, storage.term(2));
    try std.testing.expectEqual(@as(u64, 3), try storage.term(3));
    try std.testing.expectEqual(@as(u64, 4), try storage.term(4));
    try std.testing.expectEqual(@as(u64, 5), try storage.term(5));
    try std.testing.expectError(error.Unavailable, storage.term(6));
}

test "storage: entries slice and limit behavior" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    const ents = try makeEntries(3, 4);
    defer freeEntries(ents);
    try storage.setEntries(allocator, ents);

    // compacted: low below first index.
    try std.testing.expectError(error.Compacted, storage.entries(allocator, 2, 6, null, .{ .empty = .{ .can_async = false } }));

    const cases = [_]struct { lo: u64, hi: u64, max: ?u64, want: []const struct { idx: u64, term: u64 } }{
        .{ .lo = 3, .hi = 4, .max = null, .want = &.{.{ .idx = 3, .term = 3 }} },
        .{ .lo = 4, .hi = 5, .max = null, .want = &.{.{ .idx = 4, .term = 4 }} },
        .{ .lo = 4, .hi = 6, .max = null, .want = &.{ .{ .idx = 4, .term = 4 }, .{ .idx = 5, .term = 5 } } },
        .{ .lo = 4, .hi = 7, .max = null, .want = &.{ .{ .idx = 4, .term = 4 }, .{ .idx = 5, .term = 5 }, .{ .idx = 6, .term = 6 } } },
        // Even with max=0 the first entry is returned.
        .{ .lo = 4, .hi = 7, .max = 0, .want = &.{.{ .idx = 4, .term = 4 }} },
        // Cap of 2*overhead keeps two entries (each entry has no payload).
        .{ .lo = 4, .hi = 7, .max = 2 * raft.entry_message_overhead, .want = &.{ .{ .idx = 4, .term = 4 }, .{ .idx = 5, .term = 5 } } },
        .{ .lo = 4, .hi = 7, .max = 3 * raft.entry_message_overhead, .want = &.{ .{ .idx = 4, .term = 4 }, .{ .idx = 5, .term = 5 }, .{ .idx = 6, .term = 6 } } },
    };
    for (cases) |c| {
        const got = try storage.entries(allocator, c.lo, c.hi, c.max, .{ .empty = .{ .can_async = false } });
        defer freeEntries(got);
        try std.testing.expectEqual(c.want.len, got.len);
        for (got, c.want) |g, w| {
            try std.testing.expectEqual(w.idx, g.index);
            try std.testing.expectEqual(w.term, g.term);
        }
    }
}

test "storage: first and last index track append" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    const ents = try makeEntries(3, 3);
    defer freeEntries(ents);
    try storage.setEntries(allocator, ents);

    try std.testing.expectEqual(@as(u64, 3), try storage.firstIndex());
    try std.testing.expectEqual(@as(u64, 5), try storage.lastIndex());

    var more = [_]raft.Entry{.{ .index = 6, .term = 5 }};
    try storage.append(allocator, &more);
    try std.testing.expectEqual(@as(u64, 6), try storage.lastIndex());
}

test "storage: compact shifts first index" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    const ents = try makeEntries(3, 3);
    defer freeEntries(ents);
    try storage.setEntries(allocator, ents);

    try std.testing.expectEqual(@as(u64, 3), try storage.firstIndex());
    try storage.compact(allocator, 4);
    try std.testing.expectEqual(@as(u64, 4), try storage.firstIndex());

    // No-op when compacting already-compacted range.
    try storage.compact(allocator, 3);
    try std.testing.expectEqual(@as(u64, 4), try storage.firstIndex());

    try storage.compact(allocator, 5);
    try std.testing.expectEqual(@as(u64, 5), try storage.firstIndex());

    // Reject compact far past last index.
    try std.testing.expectError(error.Fatal, storage.compact(allocator, 100));
}

test "storage: append overwrites, truncates, and rejects gaps" {
    const specs = [_]struct {
        append: []const raft.Entry,
        want: []const raft.Entry,
        want_error: ?anyerror,
    }{
        .{
            .append = &.{ .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 4 }, .{ .index = 5, .term = 5 } },
            .want = &.{ .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 4 }, .{ .index = 5, .term = 5 } },
            .want_error = null,
        },
        .{
            .append = &.{ .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 6 }, .{ .index = 5, .term = 6 } },
            .want = &.{ .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 6 }, .{ .index = 5, .term = 6 } },
            .want_error = null,
        },
        .{
            .append = &.{ .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 4 }, .{ .index = 5, .term = 5 }, .{ .index = 6, .term = 5 } },
            .want = &.{ .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 4 }, .{ .index = 5, .term = 5 }, .{ .index = 6, .term = 5 } },
            .want_error = null,
        },
        .{
            // Gap; the index before firstIndex is rejected.
            .append = &.{ .{ .index = 2, .term = 3 }, .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 5 } },
            .want = &.{},
            .want_error = error.Fatal,
        },
        .{
            .append = &.{.{ .index = 4, .term = 5 }},
            .want = &.{ .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 5 } },
            .want_error = null,
        },
        .{
            .append = &.{.{ .index = 6, .term = 6 }},
            .want = &.{ .{ .index = 3, .term = 3 }, .{ .index = 4, .term = 4 }, .{ .index = 5, .term = 5 }, .{ .index = 6, .term = 6 } },
            .want_error = null,
        },
    };

    for (specs) |s| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);

        const initial = try makeEntries(3, 3);
        defer freeEntries(initial);
        try storage.setEntries(allocator, initial);

        if (s.want_error) |err| {
            try std.testing.expectError(err, storage.mayAppend(allocator, s.append));
        } else {
            try storage.append(allocator, s.append);
            const got = try storage.allEntries(allocator);
            defer freeEntries(got);
            try expectEntriesEqual(got, s.want);
        }
    }
}

test "storage: apply snapshot and reject stale" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    {
        var snap = raft.Snapshot{
            .metadata = .{
                .index = 4,
                .term = 4,
                .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }) },
            },
        };
        defer snap.deinit(allocator);
        try storage.applySnapshot(allocator, snap);
    }

    {
        var stale = raft.Snapshot{
            .metadata = .{
                .index = 3,
                .term = 3,
                .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }) },
            },
        };
        defer stale.deinit(allocator);
        try std.testing.expectError(error.SnapshotOutOfDate, storage.applySnapshot(allocator, stale));
    }
}

test "storage: log temporarily unavailable surfaces context" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    const ents = try makeEntries(3, 3);
    defer freeEntries(ents);
    try storage.setEntries(allocator, ents);

    storage.triggerLogUnavailable(true);
    const ctx = raft.GetEntriesContext{ .send_append = .{ .to = 2, .term = 5, .aggressively = false } };
    try std.testing.expectError(error.LogTemporarilyUnavailable, storage.entries(allocator, 3, 5, null, ctx));

    const taken = storage.takeGetEntriesContext().?;
    try std.testing.expectEqual(@as(u64, 2), taken.send_append.to);
    try std.testing.expectEqual(@as(u64, 5), taken.send_append.term);
    try std.testing.expect(storage.takeGetEntriesContext() == null);
}

test "storage: writable storage vtable dispatches to methods" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    const ents = try makeEntries(3, 3);
    defer freeEntries(ents);
    try storage.setEntries(allocator, ents);

    const writable = storage.asWritableStorage();
    try std.testing.expectEqual(@as(u64, 5), try writable.lastIndex());

    const extra = [_]raft.Entry{.{ .index = 6, .term = 5 }};
    try writable.append(allocator, &extra);
    try std.testing.expectEqual(@as(u64, 6), try writable.lastIndex());

    const read_iface = storage.asStorage();
    try std.testing.expectEqual(@as(u64, 6), try read_iface.lastIndex());
}
