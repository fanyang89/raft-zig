//! RaftLog integration suite, ported from `ref/raftpp/tests/log_test.cc`.
//!
//! Each test case constructs a fresh MemoryStorage + RaftLog and exercises one
//! aspect of the log contract: append/commit/slice/term/restore/maybeAppend/
//! maybePersist/etc. Heavy parameterized matrices are kept as inline tables.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

const Entry = raft.Entry;
const Snapshot = raft.Snapshot;

fn newEntry(index: u64, term: u64) Entry {
    return .{ .index = index, .term = term };
}

fn newEntries(specs: []const [2]u64) ![]Entry {
    // Heap-allocated because callers pass the result to RaftLog methods that
    // inspect each element. Stack-allocation would dangle on return.
    // Entries produced here own no buffers, so callers free with `allocator.free`.
    const out = try allocator.alloc(Entry, specs.len);
    for (specs, 0..) |s, i| out[i] = newEntry(s[0], s[1]);
    return out;
}

fn cloneSlice(src: []const Entry) ![]Entry {
    const out = try allocator.alloc(Entry, src.len);
    for (src, 0..) |e, i| {
        out[i] = .{ .index = e.index, .term = e.term };
    }
    return out;
}

fn freeOwned(ents: []Entry) void {
    for (ents) |*e| e.deinit(allocator);
    allocator.free(ents);
}

fn expectIndices(actual: []const Entry, expected_specs: []const [2]u64) !void {
    try std.testing.expectEqual(expected_specs.len, actual.len);
    for (actual, expected_specs) |a, exp| {
        try std.testing.expectEqual(exp[0], a.index);
        try std.testing.expectEqual(exp[1], a.term);
    }
}

test "raft_log: find conflict" {
    const previous: [3]Entry = .{ newEntry(1, 1), newEntry(2, 2), newEntry(3, 3) };

    const Case = struct {
        ents: []const [2]u64,
        want: u64,
    };
    const cases = [_]Case{
        .{ .ents = &.{}, .want = 0 },
        .{ .ents = &.{ .{ 1, 1 }, .{ 2, 2 }, .{ 3, 3 } }, .want = 0 },
        .{ .ents = &.{ .{ 2, 2 }, .{ 3, 3 } }, .want = 0 },
        .{ .ents = &.{.{ 3, 3 }}, .want = 0 },
        // new entries past the end
        .{ .ents = &.{ .{ 1, 1 }, .{ 2, 2 }, .{ 3, 3 }, .{ 4, 4 }, .{ 5, 4 } }, .want = 4 },
        .{ .ents = &.{ .{ 2, 2 }, .{ 3, 3 }, .{ 4, 4 }, .{ 5, 4 } }, .want = 4 },
        .{ .ents = &.{ .{ 3, 3 }, .{ 4, 4 }, .{ 5, 4 } }, .want = 4 },
        .{ .ents = &.{ .{ 4, 4 }, .{ 5, 4 } }, .want = 4 },
        // conflicts
        .{ .ents = &.{ .{ 1, 4 }, .{ 2, 4 } }, .want = 1 },
        .{ .ents = &.{ .{ 2, 1 }, .{ 3, 4 }, .{ 4, 4 } }, .want = 2 },
        .{ .ents = &.{ .{ 3, 1 }, .{ 4, 2 }, .{ 5, 4 }, .{ 6, 4 } }, .want = 3 },
    };

    for (cases) |c| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();

        _ = try rl.append(&previous);
        const ents = try newEntries(c.ents);
        defer allocator.free(ents);
        try std.testing.expectEqual(c.want, try rl.findConflict(ents));
    }
}

test "raft_log: is up to date" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer rl.deinit();

    const previous: [3]Entry = .{ newEntry(1, 1), newEntry(2, 2), newEntry(3, 3) };
    _ = try rl.append(&previous);

    const last_index = rl.lastIndex();

    const Case = struct { idx: u64, term: u64, want: bool };
    const cases = [_]Case{
        .{ .idx = last_index - 1, .term = 4, .want = true },
        .{ .idx = last_index, .term = 4, .want = true },
        .{ .idx = last_index + 1, .term = 4, .want = true },
        .{ .idx = last_index - 1, .term = 2, .want = false },
        .{ .idx = last_index, .term = 2, .want = false },
        .{ .idx = last_index + 1, .term = 2, .want = false },
        .{ .idx = last_index - 1, .term = 3, .want = false },
        .{ .idx = last_index, .term = 3, .want = true },
        .{ .idx = last_index + 1, .term = 3, .want = true },
    };
    for (cases) |c| {
        try std.testing.expectEqual(c.want, try rl.isUpToDate(c.idx, c.term));
    }
}

test "raft_log: append appends, overwrites, and replaces" {
    const Case = struct {
        append: []const [2]u64,
        want_index: u64,
        want_entries: []const [2]u64,
    };
    const cases = [_]Case{
        .{ .append = &.{}, .want_index = 2, .want_entries = &.{ .{ 1, 1 }, .{ 2, 2 } } },
        .{ .append = &.{.{ 3, 2 }}, .want_index = 3, .want_entries = &.{ .{ 1, 1 }, .{ 2, 2 }, .{ 3, 2 } } },
        .{ .append = &.{.{ 1, 2 }}, .want_index = 1, .want_entries = &.{.{ 1, 2 }} },
        .{ .append = &.{ .{ 2, 3 }, .{ 3, 3 } }, .want_index = 3, .want_entries = &.{ .{ 1, 1 }, .{ 2, 3 }, .{ 3, 3 } } },
    };

    for (cases) |c| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);

        const initial: [2]Entry = .{ newEntry(1, 1), newEntry(2, 2) };
        try storage.mayAppend(allocator, &initial);

        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();

        const ents_to_append = try newEntries(c.append);
        defer allocator.free(ents_to_append);
        const got_index = try rl.append(ents_to_append);
        try std.testing.expectEqual(c.want_index, got_index);

        const ents = try rl.getEntries(1, null, raft.GetEntriesContext.empty_(false));
        defer freeOwned(ents);
        try expectIndices(ents, c.want_entries);
    }
}

test "raft_log: compaction side effects" {
    const last_index: u64 = 1000;
    const unstable_index: u64 = 750;
    const last_term: u64 = last_index;

    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    var i: u64 = 1;
    while (i <= unstable_index) : (i += 1) {
        var batch = [_]Entry{newEntry(i, i)};
        try storage.mayAppend(allocator, &batch);
    }

    var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer rl.deinit();

    while (i <= last_index) : (i += 1) {
        var batch = [_]Entry{newEntry(i, i)};
        _ = try rl.append(&batch);
    }

    try std.testing.expect(try rl.maybeCommit(last_index, last_term));

    const offset: u64 = 500;
    try storage.compact(allocator, offset);
    try std.testing.expectEqual(last_index, rl.lastIndex());

    var j: u64 = offset;
    while (j < rl.lastIndex()) : (j += 1) {
        try std.testing.expectEqual(j, try rl.term(j));
        try std.testing.expect(try rl.matchTerm(j, j));
    }

    try std.testing.expectEqual(last_index - unstable_index, @as(u64, @intCast(rl.unstable.entries.items.len)));
    try std.testing.expectEqual(unstable_index + 1, rl.unstable.entries.items[0].index);

    const prev = rl.lastIndex();
    var more = [_]Entry{newEntry(prev + 1, prev + 1)};
    _ = try rl.append(&more);
    try std.testing.expectEqual(prev + 1, rl.lastIndex());
}

test "raft_log: term with unstable snapshot" {
    const storage_snap_idx: u64 = 10064;
    const unstable_snap_idx: u64 = storage_snap_idx + 5;

    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    {
        var snap = Snapshot{ .metadata = .{ .index = storage_snap_idx, .term = 1 } };
        defer snap.deinit(allocator);
        try storage.applySnapshot(allocator, snap);
    }

    var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer rl.deinit();

    var unstable_snap = Snapshot{ .metadata = .{ .index = unstable_snap_idx, .term = 1 } };
    defer unstable_snap.deinit(allocator);
    try rl.restore(unstable_snap);
    try std.testing.expectEqual(unstable_snap_idx, rl.committed);
    try std.testing.expectEqual(storage_snap_idx, rl.persisted);

    const Case = struct { idx: u64, want: u64 };
    const cases = [_]Case{
        .{ .idx = storage_snap_idx, .want = 0 },
        .{ .idx = storage_snap_idx + 1, .want = 0 },
        .{ .idx = unstable_snap_idx - 1, .want = 0 },
        .{ .idx = unstable_snap_idx, .want = 1 },
    };
    for (cases) |c| {
        try std.testing.expectEqual(c.want, try rl.term(c.idx));
    }
}

test "raft_log: term across offset and entries" {
    const offset: u64 = 100;
    const num: u64 = 100;

    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    {
        var snap = Snapshot{ .metadata = .{ .index = offset, .term = 1 } };
        defer snap.deinit(allocator);
        try storage.applySnapshot(allocator, snap);
    }

    var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer rl.deinit();
    var i: u64 = 1;
    while (i < num) : (i += 1) {
        var batch = [_]Entry{newEntry(offset + i, i)};
        _ = try rl.append(&batch);
    }

    const Case = struct { idx: u64, want: u64 };
    const cases = [_]Case{
        .{ .idx = offset - 1, .want = 0 },
        .{ .idx = offset, .want = 1 },
        .{ .idx = offset + num / 2, .want = num / 2 },
        .{ .idx = offset + num - 1, .want = num - 1 },
        .{ .idx = offset + num, .want = 0 },
    };
    for (cases) |c| {
        try std.testing.expectEqual(c.want, try rl.term(c.idx));
    }
}

test "raft_log: log restore from snapshot" {
    const index: u64 = 1000;
    const term: u64 = 1000;
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    {
        var snap = Snapshot{ .metadata = .{ .index = index, .term = term } };
        defer snap.deinit(allocator);
        try storage.applySnapshot(allocator, snap);
    }

    var init_ents: [2]Entry = .{ newEntry(index + 1, term), newEntry(index + 2, term + 1) };
    try storage.mayAppend(allocator, &init_ents);

    var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer rl.deinit();

    const got = try rl.allEntries();
    defer freeOwned(got);
    try expectIndices(got, &.{ .{ index + 1, term }, .{ index + 2, term + 1 } });
    try std.testing.expectEqual(index + 1, rl.firstIndex());
    try std.testing.expectEqual(index, rl.committed);
    try std.testing.expectEqual(index + 2, rl.persisted);
    try std.testing.expectEqual(index + 3, rl.unstable.offset);

    try std.testing.expectEqual(term, try rl.term(index));
    try std.testing.expectEqual(term, try rl.term(index + 1));
    try std.testing.expectEqual(term + 1, try rl.term(index + 2));
}

test "raft_log: maybe persist with snapshot" {
    const snap_index: u64 = 5;
    const snap_term: u64 = 2;

    const Case = struct {
        stable_index: u64,
        stable_term: u64,
        want_persist: u64,
    };
    const cases = [_]Case{
        .{ .stable_index = snap_index + 1, .stable_term = snap_term, .want_persist = snap_index },
        .{ .stable_index = snap_index, .stable_term = snap_term, .want_persist = snap_index },
        .{ .stable_index = snap_index - 1, .stable_term = snap_term, .want_persist = snap_index },
        .{ .stable_index = snap_index + 1, .stable_term = snap_term + 1, .want_persist = snap_index },
        .{ .stable_index = snap_index, .stable_term = snap_term + 1, .want_persist = snap_index },
        .{ .stable_index = snap_index - 1, .stable_term = snap_term + 1, .want_persist = snap_index },
    };

    for (cases) |c| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        {
            var snap = Snapshot{ .metadata = .{ .index = snap_index, .term = snap_term } };
            defer snap.deinit(allocator);
            try storage.applySnapshot(allocator, snap);
        }

        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();
        try std.testing.expectEqual(snap_index, rl.persisted);
        _ = try rl.append(&[_]Entry{});

        const unstable = rl.unstable.entries.items;
        if (unstable.len > 0) {
            const tail = unstable[unstable.len - 1];
            rl.stableEntries(tail.index, tail.term);
            try storage.mayAppend(allocator, unstable);
        }

        const changed = rl.persisted != c.want_persist;
        try std.testing.expectEqual(changed, try rl.maybePersist(c.stable_index, c.stable_term));
        try std.testing.expectEqual(c.want_persist, rl.persisted);
    }

    // restore-and-append subcase.
    {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();

        var snap = Snapshot{ .metadata = .{ .index = 100, .term = 1 } };
        defer snap.deinit(allocator);
        try rl.restore(snap);
        try std.testing.expectEqual(@as(u64, 101), rl.unstable.offset);

        var e1 = [_]Entry{newEntry(101, 1)};
        _ = try rl.append(&e1);
        try std.testing.expectEqual(@as(u64, 1), try rl.term(101));
        try std.testing.expect(!try rl.maybePersist(101, 1));

        var e2 = [_]Entry{newEntry(102, 1)};
        _ = try rl.append(&e2);
        try std.testing.expect(!try rl.maybePersist(102, 1));
    }
}

test "raft_log: unstable entries flow through offset" {
    // All entries in storage already.
    {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        const initial: [2]Entry = .{ newEntry(1, 1), newEntry(2, 2) };
        try storage.append(allocator, &initial);

        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();

        const ents = rl.unstable.entries.items;
        try std.testing.expectEqual(@as(usize, 0), ents.len);
        try std.testing.expectEqual(@as(u64, 3), rl.unstable.offset);
    }
    // All entries are unstable.
    {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();

        const previous: [2]Entry = .{ newEntry(1, 1), newEntry(2, 2) };
        _ = try rl.append(&previous);

        try std.testing.expectEqual(@as(usize, 2), rl.unstable.entries.items.len);
        rl.stableEntries(2, 2);
        try std.testing.expectEqual(@as(u64, 3), rl.unstable.offset);
    }
}

test "raft_log: has next entries / next entries (full matrix)" {
    const Case = struct {
        applied: u64,
        persisted: u64,
        committed: u64,
        want_range: ?[2]usize, // [start, end) into ents[]
    };
    const ents: [4]Entry = .{ newEntry(4, 1), newEntry(5, 1), newEntry(6, 1), newEntry(7, 1) };
    const cases = [_]Case{
        .{ .applied = 0, .persisted = 3, .committed = 3, .want_range = null },
        .{ .applied = 0, .persisted = 3, .committed = 4, .want_range = null },
        .{ .applied = 0, .persisted = 4, .committed = 6, .want_range = .{ 0, 1 } },
        .{ .applied = 0, .persisted = 6, .committed = 4, .want_range = .{ 0, 1 } },
        .{ .applied = 0, .persisted = 5, .committed = 5, .want_range = .{ 0, 2 } },
        .{ .applied = 0, .persisted = 5, .committed = 7, .want_range = .{ 0, 2 } },
        .{ .applied = 0, .persisted = 7, .committed = 5, .want_range = .{ 0, 2 } },
        .{ .applied = 3, .persisted = 4, .committed = 3, .want_range = null },
        .{ .applied = 3, .persisted = 5, .committed = 5, .want_range = .{ 0, 2 } },
        .{ .applied = 3, .persisted = 6, .committed = 7, .want_range = .{ 0, 3 } },
        .{ .applied = 3, .persisted = 7, .committed = 6, .want_range = .{ 0, 3 } },
        .{ .applied = 4, .persisted = 5, .committed = 5, .want_range = .{ 1, 2 } },
        .{ .applied = 4, .persisted = 5, .committed = 7, .want_range = .{ 1, 2 } },
        .{ .applied = 4, .persisted = 7, .committed = 5, .want_range = .{ 1, 2 } },
        .{ .applied = 4, .persisted = 7, .committed = 7, .want_range = .{ 1, 4 } },
        .{ .applied = 5, .persisted = 5, .committed = 5, .want_range = null },
        .{ .applied = 5, .persisted = 7, .committed = 7, .want_range = .{ 2, 4 } },
        .{ .applied = 7, .persisted = 7, .committed = 7, .want_range = null },
    };

    for (cases) |c| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        {
            var snap = Snapshot{ .metadata = .{ .index = 3, .term = 1 } };
            defer snap.deinit(allocator);
            try storage.applySnapshot(allocator, snap);
        }
        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();
        _ = try rl.append(&ents);

        const unstable = rl.unstable.entries.items;
        if (unstable.len > 0) {
            const tail = unstable[unstable.len - 1];
            try storage.append(allocator, unstable);
            rl.stableEntries(tail.index, tail.term);
        }

        _ = try rl.maybePersist(c.persisted, 1);
        try std.testing.expectEqual(c.persisted, rl.persisted);
        _ = try rl.maybeCommit(c.committed, 1);
        try std.testing.expectEqual(c.committed, rl.committed);
        rl.appliedTo(c.applied);

        try std.testing.expectEqual(c.want_range != null, rl.hasNextEntries());
        const next = try rl.nextEntries(null);
        if (c.want_range) |r| {
            const got = next orelse return error.TestUnexpectedNull;
            defer freeOwned(got);
            try std.testing.expectEqual(r[1] - r[0], got.len);
            var k: usize = 0;
            while (k < got.len) : (k += 1) {
                try std.testing.expectEqual(ents[r[0] + k].index, got[k].index);
            }
        } else {
            try std.testing.expect(next == null);
        }
    }
}

test "raft_log: slice bounds, compaction, and limit" {
    const offset: u64 = 100;
    const num: u64 = 100;
    const last = offset + num;
    const half = offset + num / 2;
    const half_size = raft.entryApproximateSize(newEntry(half, half));

    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    {
        var snap = Snapshot{ .metadata = .{ .index = offset, .term = 0 } };
        defer snap.deinit(allocator);
        try storage.applySnapshot(allocator, snap);
    }
    var i: u64 = 1;
    while (i < num / 2) : (i += 1) {
        var batch = [_]Entry{newEntry(offset + i, offset + i)};
        try storage.append(allocator, &batch);
    }

    var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer rl.deinit();
    while (i < num) : (i += 1) {
        var batch = [_]Entry{newEntry(offset + i, offset + i)};
        _ = try rl.append(&batch);
    }

    const Case = struct {
        from: u64,
        to: u64,
        limit: ?u64,
        want_from: u64,
        want_to: u64,
        want_fatal: bool,
        want_compacted: bool,
    };
    const cases = [_]Case{
        .{ .from = offset - 1, .to = offset + 1, .limit = std.math.maxInt(u64), .want_from = 0, .want_to = 0, .want_fatal = false, .want_compacted = true },
        .{ .from = offset, .to = offset + 1, .limit = std.math.maxInt(u64), .want_from = 0, .want_to = 0, .want_fatal = false, .want_compacted = true },
        .{ .from = half - 1, .to = half + 1, .limit = std.math.maxInt(u64), .want_from = half - 1, .want_to = half + 1, .want_fatal = false, .want_compacted = false },
        .{ .from = half, .to = half + 1, .limit = std.math.maxInt(u64), .want_from = half, .want_to = half + 1, .want_fatal = false, .want_compacted = false },
        .{ .from = last - 1, .to = last, .limit = std.math.maxInt(u64), .want_from = last - 1, .want_to = last, .want_fatal = false, .want_compacted = false },
        .{ .from = last, .to = last + 1, .limit = std.math.maxInt(u64), .want_from = 0, .want_to = 0, .want_fatal = true, .want_compacted = false },
        // limit = 0 still keeps 1 entry
        .{ .from = half - 1, .to = half + 1, .limit = 0, .want_from = half - 1, .want_to = half, .want_fatal = false, .want_compacted = false },
        .{ .from = half - 1, .to = half + 1, .limit = half_size + 1, .want_from = half - 1, .want_to = half, .want_fatal = false, .want_compacted = false },
        .{ .from = half - 2, .to = half + 1, .limit = half_size + 1, .want_from = half - 2, .want_to = half - 1, .want_fatal = false, .want_compacted = false },
        .{ .from = half - 1, .to = half + 1, .limit = half_size * 2, .want_from = half - 1, .want_to = half + 1, .want_fatal = false, .want_compacted = false },
    };

    for (cases) |c| {
        const got = rl.slice(c.from, c.to, c.limit, raft.GetEntriesContext.empty_(false)) catch |e| {
            if (c.want_fatal) try std.testing.expectEqual(error.Fatal, e);
            if (c.want_compacted) try std.testing.expectEqual(error.Compacted, e);
            continue;
        };
        defer freeOwned(got);
        try std.testing.expectEqual(c.want_to - c.want_from, got.len);
        if (got.len > 0) {
            try std.testing.expectEqual(c.want_from, got[0].index);
            try std.testing.expectEqual(c.want_to - 1, got[got.len - 1].index);
        }
    }
}

test "raft_log: maybe append full matrix" {
    const last_index: u64 = 3;
    const last_term: u64 = 3;
    const initial_commit: u64 = 1;
    const initial_persist: u64 = 3;

    const previous: [3]Entry = .{ newEntry(1, 1), newEntry(2, 2), newEntry(3, 3) };

    const Case = struct {
        log_term: u64,
        index: u64,
        committed: u64,
        ents: []const [2]u64,
        want_last_index: ?u64,
        want_commit: u64,
        want_persist: u64,
        want_fatal: bool,
    };
    const cases = [_]Case{
        .{ .log_term = last_term - 1, .index = last_index, .committed = last_index, .ents = &.{.{ last_index + 1, 4 }}, .want_last_index = null, .want_commit = initial_commit, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index + 1, .committed = last_index, .ents = &.{.{ last_index + 2, 4 }}, .want_last_index = null, .want_commit = initial_commit, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index, .committed = last_index, .ents = &.{}, .want_last_index = last_index, .want_commit = last_index, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index, .committed = last_index + 1, .ents = &.{}, .want_last_index = last_index, .want_commit = last_index, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index, .committed = last_index - 1, .ents = &.{}, .want_last_index = last_index, .want_commit = last_index - 1, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index, .committed = 0, .ents = &.{}, .want_last_index = last_index, .want_commit = initial_commit, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = 0, .index = 0, .committed = last_index, .ents = &.{}, .want_last_index = @as(u64, 0), .want_commit = initial_commit, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index, .committed = last_index, .ents = &.{.{ last_index + 1, 4 }}, .want_last_index = last_index + 1, .want_commit = last_index, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index, .committed = last_index + 1, .ents = &.{.{ last_index + 1, 4 }}, .want_last_index = last_index + 1, .want_commit = last_index + 1, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index, .committed = last_index + 2, .ents = &.{.{ last_index + 1, 4 }}, .want_last_index = last_index + 1, .want_commit = last_index + 1, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term, .index = last_index, .committed = last_index + 2, .ents = &.{ .{ last_index + 1, 4 }, .{ last_index + 2, 4 } }, .want_last_index = last_index + 2, .want_commit = last_index + 2, .want_persist = initial_persist, .want_fatal = false },
        .{ .log_term = last_term - 1, .index = last_index - 1, .committed = last_index, .ents = &.{.{ last_index, 4 }}, .want_last_index = last_index, .want_commit = last_index, .want_persist = @min(last_index - 1, initial_persist), .want_fatal = false },
        .{ .log_term = last_term - 2, .index = last_index - 2, .committed = last_index, .ents = &.{.{ last_index - 1, 4 }}, .want_last_index = last_index - 1, .want_commit = last_index - 1, .want_persist = @min(last_index - 2, initial_persist), .want_fatal = false },
        .{ .log_term = last_term - 3, .index = last_index - 3, .committed = last_index, .ents = &.{.{ last_index - 2, 4 }}, .want_last_index = 0, .want_commit = 0, .want_persist = 0, .want_fatal = true },
        .{ .log_term = last_term - 2, .index = last_index - 2, .committed = last_index, .ents = &.{ .{ last_index - 1, 4 }, .{ last_index, 4 } }, .want_last_index = last_index, .want_commit = last_index, .want_persist = @min(last_index - 2, initial_persist), .want_fatal = false },
    };

    for (cases) |c| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();
        _ = try rl.append(&previous);
        rl.committed = initial_commit;
        rl.persisted = initial_persist;

        const ents = try newEntries(c.ents);
        defer allocator.free(ents);
        const result = rl.maybeAppend(c.index, c.log_term, c.committed, ents) catch |e| {
            try std.testing.expectEqual(error.Fatal, e);
            try std.testing.expect(c.want_fatal);
            continue;
        };
        if (c.want_fatal) {
            return error.TestExpectedFatal;
        }
        try std.testing.expectEqual(c.want_last_index != null, result.term_matched);
        if (c.want_last_index) |w| {
            try std.testing.expectEqual(w, result.last_index);
        }
        try std.testing.expectEqual(c.want_commit, rl.committed);
        try std.testing.expectEqual(c.want_persist, rl.persisted);
    }
}

test "raft_log: commit to" {
    const Case = struct { commit: u64, want: u64, want_fatal: bool };
    const cases = [_]Case{
        .{ .commit = 3, .want = 3, .want_fatal = false },
        .{ .commit = 1, .want = 2, .want_fatal = false }, // never decrease
        .{ .commit = 4, .want = 0, .want_fatal = true },
    };

    for (cases) |c| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();
        const previous: [3]Entry = .{ newEntry(1, 1), newEntry(2, 2), newEntry(3, 3) };
        _ = try rl.append(&previous);
        rl.committed = 2;

        rl.commitTo(c.commit) catch |e| {
            try std.testing.expectEqual(error.Fatal, e);
            try std.testing.expect(c.want_fatal);
            continue;
        };
        try std.testing.expect(!c.want_fatal);
        try std.testing.expectEqual(c.want, rl.committed);
    }
}

test "raft_log: compaction reduces visible entries" {
    const count: u64 = 1000;
    const Case = struct {
        compact: []const u64,
        want_left: []const usize,
        want_fatal: bool,
    };
    const cases = [_]Case{
        .{ .compact = &.{1001}, .want_left = &.{0}, .want_fatal = true },
        .{ .compact = &.{ 300, 500, 800, 900 }, .want_left = &.{ 700, 500, 200, 100 }, .want_fatal = false },
        .{ .compact = &.{ 300, 299 }, .want_left = &.{ 700, 700 }, .want_fatal = false },
    };

    for (cases) |c| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        var i: u64 = 1;
        while (i < count) : (i += 1) {
            var batch = [_]Entry{newEntry(i, 0)};
            try storage.append(allocator, &batch);
        }
        var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
        defer rl.deinit();
        _ = try rl.maybeCommit(count - 1, 0);
        rl.appliedTo(rl.committed);

        for (c.compact, 0..) |idx, ci| {
            storage.compact(allocator, idx) catch |e| {
                try std.testing.expectEqual(error.Fatal, e);
                try std.testing.expect(c.want_fatal);
                break;
            };
            const got = try rl.allEntries();
            defer freeOwned(got);
            try std.testing.expectEqual(c.want_left[ci], got.len);
        }
    }
}

test "raft_log: is out of bounds" {
    const offset: u64 = 100;
    const num: u64 = 100;
    const first = offset + 1;

    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    {
        var snap = Snapshot{ .metadata = .{ .index = offset, .term = 0 } };
        defer snap.deinit(allocator);
        try storage.applySnapshot(allocator, snap);
    }

    var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer rl.deinit();
    var i: u64 = 1;
    while (i <= num) : (i += 1) {
        var batch = [_]Entry{newEntry(i + offset, 0)};
        _ = try rl.append(&batch);
    }

    const Case = struct { lo: u64, hi: u64, want_fatal: bool, want_compacted: bool };
    const cases = [_]Case{
        .{ .lo = first - 2, .hi = first + 1, .want_fatal = false, .want_compacted = true },
        .{ .lo = first - 1, .hi = first + 1, .want_fatal = false, .want_compacted = true },
        .{ .lo = first, .hi = first, .want_fatal = false, .want_compacted = false },
        .{ .lo = first + num / 2, .hi = first + num / 2, .want_fatal = false, .want_compacted = false },
        .{ .lo = first + num - 1, .hi = first + num - 1, .want_fatal = false, .want_compacted = false },
        .{ .lo = first + num, .hi = first + num, .want_fatal = false, .want_compacted = false },
        .{ .lo = first + num, .hi = first + num + 1, .want_fatal = true, .want_compacted = false },
        .{ .lo = first + num + 1, .hi = first + num + 1, .want_fatal = true, .want_compacted = false },
    };
    for (cases) |c| {
        rl.mustCheckOutOfBounds(c.lo, c.hi) catch |e| {
            if (c.want_compacted) try std.testing.expectEqual(error.Compacted, e);
            if (c.want_fatal) try std.testing.expectEqual(error.Fatal, e);
            continue;
        };
        try std.testing.expect(!c.want_compacted and !c.want_fatal);
    }
}

test "raft_log: restore snapshot then resume appending" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    {
        var snap = Snapshot{ .metadata = .{ .index = 100, .term = 1 } };
        defer snap.deinit(allocator);
        try storage.applySnapshot(allocator, snap);
    }

    var rl = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer rl.deinit();
    try std.testing.expectEqual(@as(u64, 100), rl.committed);
    try std.testing.expectEqual(@as(u64, 100), rl.persisted);

    var snap200 = Snapshot{ .metadata = .{ .index = 200, .term = 1 } };
    defer snap200.deinit(allocator);
    try rl.restore(snap200);
    try std.testing.expectEqual(@as(u64, 200), rl.committed);
    try std.testing.expectEqual(@as(u64, 100), rl.persisted);

    var i: u64 = 201;
    while (i < 210) : (i += 1) {
        var batch = [_]Entry{newEntry(i, 1)};
        _ = try rl.append(&batch);
    }

    {
        var snap_stored = Snapshot{ .metadata = .{ .index = 200, .term = 1 } };
        defer snap_stored.deinit(allocator);
        try storage.applySnapshot(allocator, snap_stored);
    }
    rl.stableSnapshot(200);

    const unstable = rl.unstable.entries.items;
    try storage.append(allocator, unstable);
    if (unstable.len > 0) {
        const tail = unstable[unstable.len - 1];
        rl.stableEntries(tail.index, tail.term);
    }
    try std.testing.expect(try rl.maybePersist(209, 1));
    try std.testing.expectEqual(@as(u64, 209), rl.persisted);

    var snap205 = Snapshot{ .metadata = .{ .index = 205, .term = 1 } };
    defer snap205.deinit(allocator);
    try rl.restore(snap205);
    try std.testing.expectEqual(@as(u64, 205), rl.committed);
    try std.testing.expectEqual(@as(u64, 200), rl.persisted);

    var snap204 = Snapshot{ .metadata = .{ .index = 204, .term = 1 } };
    defer snap204.deinit(allocator);
    try std.testing.expectError(error.Fatal, rl.restore(snap204));
}
