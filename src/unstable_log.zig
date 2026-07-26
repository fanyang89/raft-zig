//! In-memory buffer for entries not yet persisted to Storage.
//!
//! `Unstable` is the write-back buffer sitting in front of `Storage`. It
//! tracks entries with indices `>= offset` plus an optional pending snapshot.
//! RaftLog delegates term/first/last queries to it before falling through to
//! the durable backend.

const std = @import("std");

const types = @import("core/types.zig");
const util = @import("core/util.zig");
const storage_mod = @import("storage.zig");

const Entry = types.Entry;
const Snapshot = types.Snapshot;
const shareEntry = storage_mod.shareEntry;
const cloneSnapshot = storage_mod.cloneSnapshot;
const entryApproximateSize = util.entryApproximateSize;

const log = std.log.scoped(.raft_zig_unstable);

pub const Unstable = struct {
    snapshot: ?Snapshot,
    entries: std.ArrayList(Entry),
    entries_size: usize,
    offset: u64,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, offset: u64) Unstable {
        return .{
            .snapshot = null,
            .entries = .empty,
            .entries_size = 0,
            .offset = offset,
            .allocator = allocator,
        };
    }

    /// Construct from existing entries and optional snapshot. Ownership of the
    /// passed slices transfers to the Unstable; the caller must not free them.
    pub fn initWith(
        allocator: std.mem.Allocator,
        entries: std.ArrayList(Entry),
        entries_size: usize,
        offset: u64,
        snapshot: ?Snapshot,
    ) Unstable {
        return .{
            .snapshot = snapshot,
            .entries = entries,
            .entries_size = entries_size,
            .offset = offset,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Unstable) void {
        for (self.entries.items) |*e| e.deinit(self.allocator);
        self.entries.deinit(self.allocator);
        if (self.snapshot) |*s| s.deinit(self.allocator);
        self.* = undefined;
    }

    /// First index this buffer would report once the snapshot is applied.
    /// `null` when there is no snapshot (callers fall back to Storage).
    pub fn maybeFirstIndex(self: Unstable) ?u64 {
        if (self.snapshot) |s| return s.metadata.index + 1;
        return null;
    }

    pub fn maybeLastIndex(self: Unstable) ?u64 {
        if (self.entries.items.len == 0) {
            if (self.snapshot) |s| return s.metadata.index;
            return null;
        }
        return self.offset + @as(u64, @intCast(self.entries.items.len - 1));
    }

    /// Term lookup across the snapshot/entries boundary.
    pub fn maybeTerm(self: Unstable, idx: u64) ?u64 {
        if (idx < self.offset) {
            const s = self.snapshot orelse return null;
            if (idx == s.metadata.index) return s.metadata.term;
            return null;
        }

        const last = self.maybeLastIndex() orelse return null;
        if (idx > last) return null;
        const i: usize = @intCast(idx - self.offset);
        return self.entries.items[i].term;
    }

    /// Mark the current tail entries as persisted. The contract asserts that
    /// the caller-supplied `(index, term)` exactly matches the in-memory tail;
    /// mismatches panic.
    pub fn stableEntries(self: *Unstable, index: u64, term: u64) void {
        std.debug.assert(self.snapshot == null);
        if (self.entries.items.len == 0) {
            @panic("unstable.slice is empty, expect its last one's index and term");
        }
        const tail = &self.entries.items[self.entries.items.len - 1];
        if (tail.index != index or tail.term != term) {
            log.warn(
                "unstable.slice tail has different index {} and term {}, expect {} {}",
                .{ tail.index, tail.term, index, term },
            );
            @panic("unstable.slice tail index/term mismatch");
        }
        self.offset = tail.index + 1;
        for (self.entries.items) |*e| e.deinit(self.allocator);
        self.entries.clearRetainingCapacity();
        self.entries_size = 0;
    }

    pub fn restore(self: *Unstable, snapshot: Snapshot) !void {
        var restored_snapshot = try cloneSnapshot(self.allocator, snapshot);
        errdefer restored_snapshot.deinit(self.allocator);

        for (self.entries.items) |*e| e.deinit(self.allocator);
        self.entries.clearRetainingCapacity();
        self.entries_size = 0;
        self.offset = snapshot.metadata.index + 1;
        if (self.snapshot) |*old| old.deinit(self.allocator);
        self.snapshot = restored_snapshot;
    }

    /// Append (or replace) entries starting at `ents[0].index`. Three paths:
    ///   * contiguous append — push to the back.
    ///   * full overwrite — drop existing entries, reset offset.
    ///   * truncate-and-append — drop entries past the new head, then append.
    pub fn truncateAndAppend(self: *Unstable, ents: []const Entry) void {
        self.prepareAppend(ents);
        for (ents) |ent| {
            self.entries.appendAssumeCapacity(shareEntry(self.allocator, ent) catch @panic("OOM in shareEntry"));
            self.entries_size += entryApproximateSize(ent);
        }
    }

    pub fn truncateAndAppendOwned(self: *Unstable, ents: []Entry) void {
        self.prepareAppend(ents);
        for (ents) |*ent| {
            self.entries.appendAssumeCapacity(ent.*);
            self.entries_size += entryApproximateSize(ent.*);
            ent.reset();
        }
    }

    fn prepareAppend(self: *Unstable, ents: []const Entry) void {
        std.debug.assert(ents.len > 0);
        const after = ents[0].index;
        if (after == self.offset + @as(u64, @intCast(self.entries.items.len))) {
            // contiguous append
        } else if (after <= self.offset) {
            self.offset = after;
            for (self.entries.items) |*e| e.deinit(self.allocator);
            self.entries.clearRetainingCapacity();
            self.entries_size = 0;
        } else {
            const keep_count: usize = @intCast(after - self.offset);
            self.mustCheckOutOfBounds(self.offset, after);
            var i: usize = keep_count;
            while (i < self.entries.items.len) : (i += 1) {
                self.entries_size -= entryApproximateSize(self.entries.items[i]);
            }
            i = keep_count;
            while (i < self.entries.items.len) : (i += 1) self.entries.items[i].deinit(self.allocator);
            self.entries.shrinkRetainingCapacity(keep_count);
        }

        self.entries.ensureUnusedCapacity(self.allocator, ents.len) catch @panic("OOM in truncateAndAppend");
    }

    /// Mark the in-memory snapshot as persisted. Panics on missing snapshot or
    /// index mismatch — both represent caller contract violations.
    pub fn stableSnapshot(self: *Unstable, index: u64) void {
        if (self.snapshot) |s| {
            if (s.metadata.index != index) {
                log.warn(
                    "unstable.snap has different index {}, expect {}",
                    .{ s.metadata.index, index },
                );
                @panic("unstable.snap index mismatch");
            }
            var mut = s;
            mut.deinit(self.allocator);
            self.snapshot = null;
        } else {
            @panic("unstable.snap is none, expected a snapshot");
        }
    }

    pub fn mustCheckOutOfBounds(self: Unstable, lo: u64, hi: u64) void {
        std.debug.assert(lo <= hi);
        const upper = self.offset + @as(u64, @intCast(self.entries.items.len));
        std.debug.assert(self.offset <= lo and hi <= upper);
    }

    /// Borrowed view into `entries.items[lo-offset .. hi-offset]`. Asserts
    /// in-bounds; callers clone entries if they must outlive the next mutate.
    pub fn slice(self: *const Unstable, lo: u64, hi: u64) []const Entry {
        self.mustCheckOutOfBounds(lo, hi);
        const start: usize = @intCast(lo - self.offset);
        const end: usize = @intCast(hi - self.offset);
        return self.entries.items[start..end];
    }
};

test "unstable maybeFirstIndex with and without snapshot" {
    const allocator = std.testing.allocator;

    // entries, no snapshot → null
    var with_entries_no_snap = Unstable.init(allocator, 5);
    defer with_entries_no_snap.deinit();
    var e1 = [_]Entry{.{ .index = 5, .term = 1 }};
    try with_entries_no_snap.entries.appendSlice(allocator, &e1);
    with_entries_no_snap.entries_size = entryApproximateSize(e1[0]);
    try std.testing.expect(with_entries_no_snap.maybeFirstIndex() == null);

    // empty entries, no snapshot → null
    var empty_no_snap = Unstable.init(allocator, 0);
    defer empty_no_snap.deinit();
    try std.testing.expect(empty_no_snap.maybeFirstIndex() == null);

    // entries + snapshot → snapshot.index+1
    var e3 = [_]Entry{.{ .index = 5, .term = 1 }};
    var with_entries_and_snap = Unstable.initWith(
        allocator,
        .empty,
        0,
        5,
        .{ .metadata = .{ .index = 4, .term = 1 } },
    );
    defer with_entries_and_snap.deinit();
    try with_entries_and_snap.entries.appendSlice(allocator, &e3);
    try std.testing.expectEqual(@as(u64, 5), with_entries_and_snap.maybeFirstIndex().?);

    // empty entries + snapshot → snapshot.index+1
    var empty_with_snap = Unstable.initWith(
        allocator,
        .empty,
        0,
        5,
        .{ .metadata = .{ .index = 4, .term = 1 } },
    );
    defer empty_with_snap.deinit();
    try std.testing.expectEqual(@as(u64, 5), empty_with_snap.maybeFirstIndex().?);
}

test "unstable maybeLastIndex falls back to snapshot" {
    const allocator = std.testing.allocator;

    var e1 = [_]Entry{.{ .index = 5, .term = 1 }};
    var with_entries_no_snap = Unstable.initWith(allocator, .empty, 0, 5, null);
    defer with_entries_no_snap.deinit();
    try with_entries_no_snap.entries.appendSlice(allocator, &e1);
    try std.testing.expectEqual(@as(u64, 5), with_entries_no_snap.maybeLastIndex().?);

    var empty_no_snap = Unstable.initWith(
        allocator,
        .empty,
        0,
        5,
        .{ .metadata = .{ .index = 4, .term = 1 } },
    );
    defer empty_no_snap.deinit();
    try empty_no_snap.entries.appendSlice(allocator, &e1);
    try std.testing.expectEqual(@as(u64, 5), empty_no_snap.maybeLastIndex().?);

    var with_entries_and_snap = Unstable.initWith(
        allocator,
        .empty,
        0,
        5,
        .{ .metadata = .{ .index = 4, .term = 1 } },
    );
    defer with_entries_and_snap.deinit();
    try std.testing.expectEqual(@as(u64, 4), with_entries_and_snap.maybeLastIndex().?);

    var empty_with_snap = Unstable.init(allocator, 0);
    defer empty_with_snap.deinit();
    try std.testing.expect(empty_with_snap.maybeLastIndex() == null);
}

test "unstable maybeTerm across entries/snapshot boundary" {
    const allocator = std.testing.allocator;

    const Case = struct {
        has_entry: bool,
        offset: u64,
        has_snapshot: bool,
        index: u64,
        want_ok: bool,
        want_term: u64,
    };
    const cases = [_]Case{
        .{ .has_entry = true, .offset = 5, .has_snapshot = false, .index = 5, .want_ok = true, .want_term = 1 },
        .{ .has_entry = true, .offset = 5, .has_snapshot = false, .index = 6, .want_ok = false, .want_term = 0 },
        .{ .has_entry = true, .offset = 5, .has_snapshot = false, .index = 4, .want_ok = false, .want_term = 0 },
        .{ .has_entry = true, .offset = 5, .has_snapshot = true, .index = 5, .want_ok = true, .want_term = 1 },
        .{ .has_entry = true, .offset = 5, .has_snapshot = true, .index = 6, .want_ok = false, .want_term = 0 },
        .{ .has_entry = true, .offset = 5, .has_snapshot = true, .index = 4, .want_ok = true, .want_term = 1 },
        .{ .has_entry = true, .offset = 5, .has_snapshot = true, .index = 3, .want_ok = false, .want_term = 0 },
        .{ .has_entry = false, .offset = 5, .has_snapshot = true, .index = 5, .want_ok = false, .want_term = 0 },
        .{ .has_entry = false, .offset = 5, .has_snapshot = true, .index = 4, .want_ok = true, .want_term = 1 },
        .{ .has_entry = false, .offset = 0, .has_snapshot = false, .index = 5, .want_ok = false, .want_term = 0 },
    };

    for (cases) |c| {
        var uns: Unstable = blk: {
            var entries: std.ArrayList(Entry) = .empty;
            var entries_size: usize = 0;
            if (c.has_entry) {
                try entries.append(allocator, .{ .index = 5, .term = 1 });
                entries_size = entryApproximateSize(.{ .index = 5, .term = 1 });
            }
            const snap: ?Snapshot = if (c.has_snapshot) .{ .metadata = .{ .index = 4, .term = 1 } } else null;
            break :blk Unstable.initWith(allocator, entries, entries_size, c.offset, snap);
        };
        defer uns.deinit();

        if (c.want_ok) {
            try std.testing.expectEqual(c.want_term, uns.maybeTerm(c.index).?);
        } else {
            try std.testing.expect(uns.maybeTerm(c.index) == null);
        }
    }
}

test "unstable restore resets entries and clones snapshot" {
    const allocator = std.testing.allocator;

    var entries: std.ArrayList(Entry) = .empty;
    try entries.append(allocator, .{ .index = 5, .term = 1 });
    var uns = Unstable.initWith(
        allocator,
        entries,
        entryApproximateSize(.{ .index = 5, .term = 1 }),
        5,
        .{ .metadata = .{ .index = 4, .term = 1 } },
    );
    defer uns.deinit();

    const s = Snapshot{ .metadata = .{ .index = 6, .term = 2 } };
    try uns.restore(s);

    try std.testing.expectEqual(@as(u64, 7), uns.offset);
    try std.testing.expectEqual(@as(usize, 0), uns.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), uns.entries_size);
    try std.testing.expect(uns.snapshot != null);
    try std.testing.expectEqual(@as(u64, 6), uns.snapshot.?.metadata.index);
    try std.testing.expectEqual(@as(u64, 2), uns.snapshot.?.metadata.term);
}

test "unstable stable snapshot and entries advances offset" {
    const allocator = std.testing.allocator;

    var entries: std.ArrayList(Entry) = .empty;
    try entries.append(allocator, .{ .index = 5, .term = 1 });
    try entries.append(allocator, .{ .index = 5, .term = 2 });
    try entries.append(allocator, .{ .index = 6, .term = 3 });
    var entries_size: usize = 0;
    for (entries.items) |e| entries_size += entryApproximateSize(e);

    var uns = Unstable.initWith(
        allocator,
        entries,
        entries_size,
        5,
        .{ .metadata = .{ .index = 4, .term = 1 } },
    );
    defer uns.deinit();

    uns.stableSnapshot(4);
    uns.stableEntries(6, 3);

    try std.testing.expectEqual(@as(usize, 0), uns.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), uns.entries_size);
    try std.testing.expectEqual(@as(u64, 7), uns.offset);
}

test "unstable truncateAndAppend handles append/replace/truncate" {
    const allocator = std.testing.allocator;

    const Spec = struct {
        entries: []const Entry,
        offset: u64,
        to_append: []const Entry,
        want_offset: u64,
        want_entries: []const Entry,
    };

    const case1_entries = [_]Entry{.{ .index = 5, .term = 1 }};
    const case1_append = [_]Entry{ .{ .index = 6, .term = 1 }, .{ .index = 7, .term = 1 } };
    const case1_want = [_]Entry{ .{ .index = 5, .term = 1 }, .{ .index = 6, .term = 1 }, .{ .index = 7, .term = 1 } };

    const case2_entries = [_]Entry{.{ .index = 5, .term = 1 }};
    const case2_append = [_]Entry{ .{ .index = 5, .term = 2 }, .{ .index = 6, .term = 2 } };
    const case2_want = [_]Entry{ .{ .index = 5, .term = 2 }, .{ .index = 6, .term = 2 } };

    const case3_entries = [_]Entry{.{ .index = 5, .term = 1 }};
    const case3_append = [_]Entry{ .{ .index = 4, .term = 2 }, .{ .index = 5, .term = 2 }, .{ .index = 6, .term = 2 } };
    const case3_want = [_]Entry{ .{ .index = 4, .term = 2 }, .{ .index = 5, .term = 2 }, .{ .index = 6, .term = 2 } };

    const case4_entries = [_]Entry{ .{ .index = 5, .term = 1 }, .{ .index = 6, .term = 1 }, .{ .index = 7, .term = 1 } };
    const case4_append = [_]Entry{.{ .index = 6, .term = 2 }};
    const case4_want = [_]Entry{ .{ .index = 5, .term = 1 }, .{ .index = 6, .term = 2 } };

    const case5_entries = [_]Entry{ .{ .index = 5, .term = 1 }, .{ .index = 6, .term = 1 }, .{ .index = 7, .term = 1 } };
    const case5_append = [_]Entry{ .{ .index = 7, .term = 2 }, .{ .index = 8, .term = 2 } };
    const case5_want = [_]Entry{ .{ .index = 5, .term = 1 }, .{ .index = 6, .term = 1 }, .{ .index = 7, .term = 2 }, .{ .index = 8, .term = 2 } };

    const cases = [_]Spec{
        .{ .entries = &case1_entries, .offset = 5, .to_append = &case1_append, .want_offset = 5, .want_entries = &case1_want },
        .{ .entries = &case2_entries, .offset = 5, .to_append = &case2_append, .want_offset = 5, .want_entries = &case2_want },
        .{ .entries = &case3_entries, .offset = 5, .to_append = &case3_append, .want_offset = 4, .want_entries = &case3_want },
        .{ .entries = &case4_entries, .offset = 5, .to_append = &case4_append, .want_offset = 5, .want_entries = &case4_want },
        .{ .entries = &case5_entries, .offset = 5, .to_append = &case5_append, .want_offset = 5, .want_entries = &case5_want },
    };

    for (cases) |c| {
        var entries: std.ArrayList(Entry) = .empty;
        try entries.appendSlice(allocator, c.entries);
        var entries_size: usize = 0;
        for (c.entries) |e| entries_size += entryApproximateSize(e);

        var uns = Unstable.initWith(allocator, entries, entries_size, c.offset, null);
        defer uns.deinit();

        uns.truncateAndAppend(c.to_append);

        try std.testing.expectEqual(c.want_offset, uns.offset);
        try std.testing.expectEqual(c.want_entries.len, uns.entries.items.len);
        for (uns.entries.items, c.want_entries) |got, want| {
            try std.testing.expectEqual(want.index, got.index);
            try std.testing.expectEqual(want.term, got.term);
        }
        var want_size: usize = 0;
        for (c.want_entries) |e| want_size += entryApproximateSize(e);
        try std.testing.expectEqual(want_size, uns.entries_size);
    }
}
