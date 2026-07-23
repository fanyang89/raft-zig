//! Combined log view over an Unstable buffer and a durable Storage.
//!
//! Ports `include/raftpp/core/raft_log.h` and `lib/core/raft_log.cc`. RaftLog
//! is the consensus layer's log abstraction: it stitches together an in-memory
//! `Unstable` buffer (uncommitted entries + pending snapshot) with a pluggable
//! read-only `Storage` backend (MemoryStorage, WAL, custom).
//!
//! All entries returned from this module are freshly allocated clones; the
//! caller owns them and must deinit each entry plus the backing slice.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const util = @import("core/util.zig");
const storage_mod = @import("storage.zig");
const unstable_mod = @import("unstable_log.zig");

const Error = error_model.Error;
const errorName = error_model.name;
const Entry = types.Entry;
const Snapshot = types.Snapshot;
const Storage = storage_mod.Storage;
const GetEntriesContext = storage_mod.GetEntriesContext;
const Unstable = unstable_mod.Unstable;
const cloneEntry = storage_mod.cloneEntry;
const cloneSnapshot = storage_mod.cloneSnapshot;
const limitSize = util.limitSize;
const entryApproximateSize = util.entryApproximateSize;

const log = std.log.scoped(.raft_zig_log);

/// Result of `RaftLog.maybeAppend`: tells the caller whether the prev-entry
/// matched, where the first conflict was, and the new last index.
pub const MaybeAppendResult = struct {
    term_matched: bool,
    conflict_index: u64,
    last_index: u64,
};

pub const FindConflictByTermResult = struct {
    index: u64,
    term: ?u64,
};

pub const CommitInfo = struct {
    index: u64,
    term: u64,
};

pub const RaftLog = struct {
    store: Storage,
    unstable: Unstable,
    committed: u64,
    persisted: u64,
    applied: u64,
    max_apply_unpersisted_log_limit: u64,
    allocator: std.mem.Allocator,

    /// Construct from a Storage and the unpersisted-log apply limit from
    /// raftpp's `Config`. The full `Config` struct is ported with the raft
    /// state machine in a later phase.
    pub fn init(
        allocator: std.mem.Allocator,
        store: Storage,
        max_apply_unpersisted_log_limit: u64,
    ) Error!RaftLog {
        const last_index = try store.lastIndex();
        const first_index = try store.firstIndex();
        return .{
            .store = store,
            .unstable = Unstable.init(allocator, last_index + 1),
            .committed = first_index -% 1,
            .persisted = last_index,
            .applied = first_index -% 1,
            .max_apply_unpersisted_log_limit = max_apply_unpersisted_log_limit,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RaftLog) void {
        self.unstable.deinit();
        self.* = undefined;
    }

    pub fn getInitialState(self: *const RaftLog) Error!storage_mod.RaftState {
        return self.store.initialState(self.allocator);
    }

    /// Term lookup. Returns 0 when `idx` is in the dummy-range or beyond
    /// lastIndex. Storage `Compacted`/`Unavailable` errors propagate.
    pub fn term(self: *const RaftLog, idx: u64) Error!u64 {
        const dummy_idx = self.firstIndex() -% 1;
        if (idx < dummy_idx or idx > self.lastIndex()) return 0;

        if (self.unstable.maybeTerm(idx)) |t| return t;
        return self.store.term(idx);
    }

    pub fn lastTerm(self: *const RaftLog) Error!u64 {
        return self.term(self.lastIndex()) catch |e| switch (e) {
            error.Compacted => 0,
            else => return e,
        };
    }

    pub fn lastIndex(self: *const RaftLog) u64 {
        if (self.unstable.maybeLastIndex()) |i| return i;
        return self.store.lastIndex() catch |e| {
            log.warn("store.lastIndex failed: {s}", .{@errorName(e)});
            return 0;
        };
    }

    pub fn firstIndex(self: *const RaftLog) u64 {
        if (self.unstable.maybeFirstIndex()) |i| return i;
        return self.store.firstIndex() catch |e| {
            log.warn("store.firstIndex failed: {s}", .{@errorName(e)});
            return 0;
        };
    }

    /// Walk a batch of entries and return the first index whose term differs
    /// from the log (or 0 when everything matches).
    pub fn findConflict(self: *const RaftLog, ents: []const Entry) Error!u64 {
        for (ents) |e| {
            if (!try self.matchTerm(e.index, e.term)) {
                if (e.index <= self.lastIndex()) {
                    const existing = self.term(e.index) catch 0;
                    log.info(
                        "found conflict at index({}), existing_term={}, conflicting_term={}",
                        .{ e.index, existing, e.term },
                    );
                }
                return e.index;
            }
        }
        return 0;
    }

    pub fn matchTerm(self: *const RaftLog, idx: u64, term_: u64) Error!bool {
        const t = self.term(idx) catch |e| switch (e) {
            error.Compacted, error.Unavailable => return false,
            else => return e,
        };
        return t == term_;
    }

    /// Bump `persisted` forward when storage independently confirms an entry.
    pub fn maybePersist(self: *RaftLog, index: u64, term_: u64) Error!bool {
        const first_update_index: u64 = blk: {
            if (self.unstable.snapshot) |s| break :blk s.metadata.index;
            break :blk self.unstable.offset;
        };

        if (index > self.persisted and index < first_update_index) {
            if ((try self.store.term(index)) == term_) {
                log.debug("persisted index {}", .{index});
                self.persisted = index;
                return true;
            }
        }
        return false;
    }

    pub fn maybePersistSnapshot(self: *RaftLog, index: u64) bool {
        if (index <= self.persisted) return false;
        std.debug.assert(index <= self.committed);
        std.debug.assert(index < self.unstable.offset);
        log.debug("snapshot persisted index {}", .{index});
        self.persisted = index;
        return true;
    }

    pub fn maybeCommit(self: *RaftLog, max_index: u64, term_: u64) Error!bool {
        if (max_index > self.committed and (try self.term(max_index)) == term_) {
            try self.commitTo(max_index);
            return true;
        }
        return false;
    }

    /// Try to append a leader-supplied batch. Returns the outcome (term
    /// matched? where was the conflict? new last index?). The caller-visible
    /// error is `error.Fatal` when a conflict hits already-committed entries.
    pub fn maybeAppend(
        self: *RaftLog,
        idx: u64,
        term_: u64,
        committed: u64,
        ents: []const Entry,
    ) Error!MaybeAppendResult {
        if (!try self.matchTerm(idx, term_)) {
            log.debug(
                "MaybeAppend failed: idx={}, term={}, last_index={}",
                .{ idx, term_, self.lastIndex() },
            );
            return MaybeAppendResult{ .term_matched = false, .conflict_index = 0, .last_index = 0 };
        }

        const conflict_idx = try self.findConflict(ents);

        if (conflict_idx == 0) {
            // no conflict
        } else if (conflict_idx <= self.committed) {
            return error.Fatal;
        } else {
            const start: usize = @intCast(conflict_idx - (idx + 1));
            const to_append = ents[start..];
            var cloned: std.ArrayList(Entry) = .empty;
            defer {
                for (cloned.items) |*e| e.deinit(self.allocator);
                cloned.deinit(self.allocator);
            }
            try cloned.ensureTotalCapacity(self.allocator, to_append.len);
            for (to_append) |e| {
                try cloned.append(self.allocator, try cloneEntry(self.allocator, e));
            }
            _ = try self.append(cloned.items);

            // Overwriting entries rewinds the persistence cursor.
            self.persisted = @min(self.persisted, conflict_idx - 1);
        }

        const last_new_idx = idx + @as(u64, @intCast(ents.len));
        try self.commitTo(@min(committed, last_new_idx));
        return MaybeAppendResult{
            .term_matched = true,
            .conflict_index = conflict_idx,
            .last_index = last_new_idx,
        };
    }

    /// Append entries to the unstable buffer. Returns the new lastIndex.
    pub fn append(self: *RaftLog, ents: []const Entry) Error!u64 {
        if (ents.len == 0) return self.lastIndex();

        const after = ents[0].index -% 1;
        if (after < self.committed) {
            log.warn(
                "after {} is out of range [committed {}], resetting committed",
                .{ after, self.committed },
            );
            self.committed = after;
        }
        self.unstable.truncateAndAppend(ents);
        return self.lastIndex();
    }

    pub fn commitTo(self: *RaftLog, to_commit: u64) Error!void {
        if (self.committed >= to_commit) return;
        if (self.lastIndex() < to_commit) {
            return error.Fatal;
        }
        self.committed = to_commit;
    }

    pub fn mustCheckOutOfBounds(self: *const RaftLog, low: u64, high: u64) Error!void {
        if (low > high) {
            return error.Fatal;
        }

        const first_index = self.firstIndex();
        if (low < first_index) return error.Compacted;

        const length = self.lastIndex() + 1 - first_index;
        if (high > first_index + length) {
            return error.Fatal;
        }
    }

    /// Read a cloned slice `[low, high)` from storage + unstable, truncated to
    /// `max_size` approximate bytes. Caller owns the returned entries.
    pub fn slice(
        self: *const RaftLog,
        low: u64,
        high: u64,
        max_size: ?u64,
        context: GetEntriesContext,
    ) Error![]Entry {
        try self.mustCheckOutOfBounds(low, high);
        if (low == high) return self.allocator.alloc(Entry, 0);

        var result: std.ArrayList(Entry) = .empty;
        errdefer {
            for (result.items) |*e| e.deinit(self.allocator);
            result.deinit(self.allocator);
        }

        const unstable_offset = self.unstable.offset;

        if (low < unstable_offset) {
            const unstable_high = @min(high, unstable_offset);
            const ents = self.store.entries(self.allocator, low, unstable_high, max_size, context) catch |e| switch (e) {
                error.Compacted, error.LogTemporarilyUnavailable => return e,
                error.Unavailable => @panic("entries[lo:hi] unavailable from storage"),
                else => return e,
            };
            defer {
                for (ents) |*e| e.deinit(self.allocator);
                self.allocator.free(ents);
            }
            try result.ensureUnusedCapacity(self.allocator, ents.len);
            for (ents) |e| {
                try result.append(self.allocator, try cloneEntry(self.allocator, e));
            }
            if (ents.len < unstable_high - low) {
                return result.toOwnedSlice(self.allocator);
            }
        }

        if (high > unstable_offset) {
            const lo = @max(low, unstable_offset);
            const unstable_ents = self.unstable.slice(lo, high);
            try result.ensureUnusedCapacity(self.allocator, unstable_ents.len);
            for (unstable_ents) |e| {
                try result.append(self.allocator, try cloneEntry(self.allocator, e));
            }
        }

        var view: []Entry = result.items;
        limitSize(&view, max_size);
        if (view.len < result.items.len) {
            var i = view.len;
            while (i < result.items.len) : (i += 1) result.items[i].deinit(self.allocator);
            result.shrinkRetainingCapacity(view.len);
        }

        return result.toOwnedSlice(self.allocator);
    }

    pub fn getEntries(
        self: *const RaftLog,
        idx: u64,
        max_size: ?u64,
        context: GetEntriesContext,
    ) Error![]Entry {
        const last = self.lastIndex();
        if (idx > last) return self.allocator.alloc(Entry, 0);
        return self.slice(idx, last + 1, max_size, context);
    }

    pub fn allEntries(self: *RaftLog) Error![]Entry {
        const first = self.firstIndex();
        const got = self.getEntries(first, null, GetEntriesContext.empty_(false)) catch |e| switch (e) {
            error.Compacted => return self.allEntries(),
            else => return e,
        };
        return got;
    }

    pub fn appliedTo(self: *RaftLog, idx: u64) void {
        if (idx == 0) return;
        std.debug.assert(idx <= self.committed and idx >= self.applied);
        self.appliedToUnchecked(idx);
    }

    pub fn appliedToUnchecked(self: *RaftLog, idx: u64) void {
        self.applied = idx;
    }

    /// Walk backwards from `index` until we hit an entry with term <= the
    /// requested term. Returns the resulting (index, term_or_null).
    pub fn findConflictByTerm(
        self: *const RaftLog,
        index: u64,
        term_: u64,
    ) Error!FindConflictByTermResult {
        var conflict_index = index;
        if (index > self.lastIndex()) {
            log.debug(
                "index({}) is out of range [0, last_index({})] in find_conflict_by_term",
                .{ index, self.lastIndex() },
            );
            return .{ .index = index, .term = null };
        }
        while (true) {
            const t = try self.term(conflict_index);
            if (t > term_) {
                conflict_index -%= 1;
            } else {
                return .{ .index = conflict_index, .term = t };
            }
        }
    }

    pub fn getSnapshot(
        self: *RaftLog,
        request_index: u64,
        to: u64,
    ) Error!Snapshot {
        if (self.unstable.snapshot) |s| {
            if (s.metadata.index >= request_index) {
                return cloneSnapshot(self.allocator, s);
            }
        }
        return self.store.getSnapshot(self.allocator, request_index, to);
    }

    pub fn commitInfo(self: *const RaftLog) Error!CommitInfo {
        const t = try self.term(self.committed);
        return .{ .index = self.committed, .term = t };
    }

    pub fn isUpToDate(self: *const RaftLog, last_index: u64, term_: u64) Error!bool {
        const lt = try self.lastTerm();
        return term_ > lt or (term_ == lt and last_index >= self.lastIndex());
    }

    pub fn restore(self: *RaftLog, snapshot: Snapshot) Error!void {
        const index = snapshot.metadata.index;
        if (index < self.committed) {
            return error.Fatal;
        }
        if (self.persisted > self.committed) {
            self.persisted = self.committed;
        }
        self.committed = index;
        self.unstable.restore(snapshot);
    }

    pub fn appliedIndexUpperBound(self: *const RaftLog) u64 {
        return @min(self.committed, self.persisted +% self.max_apply_unpersisted_log_limit);
    }

    pub fn hasNextEntriesSince(self: *const RaftLog, since_idx: u64) bool {
        const offset = @max(since_idx + 1, self.firstIndex());
        const high = self.appliedIndexUpperBound() + 1;
        return high > offset;
    }

    pub fn hasNextEntries(self: *const RaftLog) bool {
        return self.hasNextEntriesSince(self.applied);
    }

    pub fn nextEntriesSince(
        self: *RaftLog,
        since_idx: u64,
        max_size: ?u64,
    ) Error!?[]Entry {
        const offset = @max(since_idx + 1, self.firstIndex());
        const high = self.appliedIndexUpperBound() + 1;
        if (high > offset) {
            const ctx = GetEntriesContext{ .gen_ready = {} };
            return try self.slice(offset, high, max_size, ctx);
        }
        return null;
    }

    pub fn nextEntries(self: *RaftLog, max_size: ?u64) Error!?[]Entry {
        return self.nextEntriesSince(self.applied, max_size);
    }

    pub fn stableSnapshot(self: *RaftLog, index: u64) void {
        self.unstable.stableSnapshot(index);
    }

    pub fn stableEntries(self: *RaftLog, index: u64, term_: u64) void {
        self.unstable.stableEntries(index, term_);
    }

    /// Scan `[low, high)` in pages of `page_size` bytes. `Scanner.scan` is
    /// called with each non-empty page and may return `false` to stop early.
    /// Empty page from storage is `error.ZeroEntriesInSlice`.
    pub fn scan(
        self: *RaftLog,
        low: u64,
        high: u64,
        page_size: u64,
        context: GetEntriesContext,
        comptime Scanner: type,
        scanner: *Scanner,
    ) Error!void {
        var lo = low;
        while (lo < high) {
            const got = try self.slice(lo, high, page_size, context);
            if (got.len == 0) {
                return error.ZeroEntriesInSlice;
            }
            lo += got.len;
            const want_more = try scanner.scan(got);
            for (got) |*e| e.deinit(self.allocator);
            self.allocator.free(got);
            if (!want_more) return;
        }
    }
};

test "raft log term returns 0 for out-of-range and dummy index" {
    const allocator = std.testing.allocator;
    var storage = @import("memory_storage.zig").MemoryStorage.init();
    defer storage.deinit(allocator);
    var raw = [_]Entry{ .{ .index = 1, .term = 1 }, .{ .index = 2, .term = 2 } };
    try storage.setEntries(allocator, &raw);

    var raft_log = try RaftLog.init(allocator, storage.asStorage(), 0);
    defer raft_log.deinit();

    try std.testing.expectEqual(@as(u64, 0), try raft_log.term(0));
    try std.testing.expectEqual(@as(u64, 1), try raft_log.term(1));
    try std.testing.expectEqual(@as(u64, 2), try raft_log.term(2));
    try std.testing.expectEqual(@as(u64, 0), try raft_log.term(3));
}

test "raft log append and lastIndex" {
    const allocator = std.testing.allocator;
    var storage = @import("memory_storage.zig").MemoryStorage.init();
    defer storage.deinit(allocator);

    var raft_log = try RaftLog.init(allocator, storage.asStorage(), 0);
    defer raft_log.deinit();

    var ents = [_]Entry{ .{ .index = 1, .term = 1 }, .{ .index = 2, .term = 2 } };
    const last = try raft_log.append(&ents);
    try std.testing.expectEqual(@as(u64, 2), last);
    try std.testing.expectEqual(@as(u64, 2), raft_log.lastIndex());
}

test "raft log commitTo and maybeCommit" {
    const allocator = std.testing.allocator;
    var storage = @import("memory_storage.zig").MemoryStorage.init();
    defer storage.deinit(allocator);

    var raft_log = try RaftLog.init(allocator, storage.asStorage(), 0);
    defer raft_log.deinit();

    var ents = [_]Entry{ .{ .index = 1, .term = 1 }, .{ .index = 2, .term = 2 }, .{ .index = 3, .term = 3 } };
    _ = try raft_log.append(&ents);

    try std.testing.expect(try raft_log.maybeCommit(2, 2));
    try std.testing.expectEqual(@as(u64, 2), raft_log.committed);

    // Wrong term: no-op.
    try std.testing.expect(!try raft_log.maybeCommit(3, 2));
    try std.testing.expectEqual(@as(u64, 2), raft_log.committed);

    // Out-of-range commit returns error.Fatal.
    try std.testing.expectError(error.Fatal, raft_log.commitTo(99));
}

test "raft log slice respects bounds and limit" {
    const allocator = std.testing.allocator;
    var storage = @import("memory_storage.zig").MemoryStorage.init();
    defer storage.deinit(allocator);

    var raft_log = try RaftLog.init(allocator, storage.asStorage(), 0);
    defer raft_log.deinit();

    var ents = [_]Entry{ .{ .index = 1, .term = 1 }, .{ .index = 2, .term = 2 }, .{ .index = 3, .term = 3 } };
    _ = try raft_log.append(&ents);

    const got = try raft_log.slice(1, 4, null, GetEntriesContext.empty_(false));
    defer {
        for (got) |*e| e.deinit(allocator);
        allocator.free(got);
    }
    try std.testing.expectEqual(@as(usize, 3), got.len);
    try std.testing.expectEqual(@as(u64, 1), got[0].index);
    try std.testing.expectEqual(@as(u64, 3), got[2].index);

    try std.testing.expectError(error.Fatal, raft_log.slice(1, 99, null, GetEntriesContext.empty_(false)));
}

test "raft log scan walks pages and supports early exit" {
    const allocator = std.testing.allocator;
    var storage = @import("memory_storage.zig").MemoryStorage.init();
    defer storage.deinit(allocator);

    var raft_log = try RaftLog.init(allocator, storage.asStorage(), 0);
    defer raft_log.deinit();

    var ents = [_]Entry{ .{ .index = 1, .term = 1 }, .{ .index = 2, .term = 2 }, .{ .index = 3, .term = 3 } };
    _ = try raft_log.append(&ents);

    const Collector = struct {
        sum: u64,
        max_iters: u32,
        iters: u32 = 0,
        pub fn scan(self: *@This(), entries: []const Entry) Error!bool {
            self.iters += 1;
            for (entries) |e| self.sum += e.index;
            return self.iters < self.max_iters;
        }
    };

    var c = Collector{ .sum = 0, .max_iters = 1 };
    try raft_log.scan(1, 4, 0, GetEntriesContext.empty_(false), Collector, &c);
    try std.testing.expectEqual(@as(u32, 1), c.iters);
    try std.testing.expectEqual(@as(u64, 1), c.sum);

    var full = Collector{ .sum = 0, .max_iters = 100 };
    try raft_log.scan(1, 4, 0, GetEntriesContext.empty_(false), Collector, &full);
    try std.testing.expectEqual(@as(u64, 1 + 2 + 3), full.sum);
}
