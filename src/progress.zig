//! Per-follower replication progress + ProgressMap.
//!
//! A `Progress` tracks how closely a follower replicates the leader's log:
//!   * `matched` — highest index confirmed received.
//!   * `next_idx` — next entry to send.
//!   * `state` — Probe / Replicate / Snapshot, with state-specific flow control.
//!   * `inflights` — bounded ring of in-flight AppendEntries.
//!
//! `ProgressMap` is a `Map<u64, Progress>` that also implements the
//! `AckedIndexer` vtable so quorum math can read `matched` directly.

const std = @import("std");

const inflights_mod = @import("inflights.zig");
const ack_indexer_mod = @import("ack_indexer.zig");
const primitives = @import("core/primitives.zig");

const Inflights = inflights_mod.Inflights;
const Index = ack_indexer_mod.Index;
const AckedIndexer = ack_indexer_mod.AckedIndexer;
const invalid_index = primitives.invalid_index;

const log = std.log.scoped(.raft_zig_progress);

pub const ProgressState = enum(u8) { probe, replicate, snapshot };

pub fn progressStateName(s: ProgressState) []const u8 {
    return switch (s) {
        .probe => "StateProbe",
        .replicate => "StateReplicate",
        .snapshot => "StateSnapshot",
    };
}

pub const Progress = struct {
    matched: u64 = 0,
    next_idx: u64,
    state: ProgressState = .probe,
    paused: bool = false,
    pending_snapshot: u64 = 0,
    pending_request_snapshot: u64 = 0,
    recent_active: bool = false,
    inflights: Inflights,
    commit_group_id: u64 = 0,
    committed_index: u64 = 0,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, next_idx: u64, max_inflight: usize) Progress {
        return .{
            .next_idx = next_idx,
            .inflights = Inflights.init(max_inflight),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Progress) void {
        self.inflights.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn reset(self: *Progress, next_idx: u64) void {
        self.matched = 0;
        self.next_idx = next_idx;
        self.state = .probe;
        self.paused = false;
        self.pending_snapshot = 0;
        self.pending_request_snapshot = 0;
        self.recent_active = false;
        self.inflights.reset(self.allocator);
    }

    pub fn becomeProbe(self: *Progress) void {
        if (self.state == .snapshot) {
            const pending_snapshot = self.pending_snapshot;
            self.resetState(.probe);
            self.next_idx = @max(self.matched + 1, pending_snapshot + 1);
        } else {
            self.resetState(.probe);
            self.next_idx = self.matched + 1;
        }
    }

    pub fn becomeReplicate(self: *Progress) void {
        self.resetState(.replicate);
        self.next_idx = self.matched + 1;
    }

    pub fn becomeSnapshot(self: *Progress, snapshot_idx: u64) void {
        self.resetState(.snapshot);
        self.pending_snapshot = snapshot_idx;
    }

    pub fn isSnapshotCaughtUp(self: Progress) bool {
        return self.state == .snapshot and self.matched >= self.pending_snapshot;
    }

    pub fn resume_(self: *Progress) void {
        self.paused = false;
    }

    pub fn pause(self: *Progress) void {
        self.paused = true;
    }

    pub fn updateCommitted(self: *Progress, committed_index: u64) void {
        if (committed_index > self.committed_index) {
            self.committed_index = committed_index;
        }
    }

    /// Bump matched/next to at least `n`. Returns true iff matched advanced.
    pub fn maybeUpdate(self: *Progress, n: u64) bool {
        const need_update = self.matched < n;
        if (need_update) {
            self.matched = n;
            self.resume_();
        }
        if (self.next_idx < n + 1) self.next_idx = n + 1;
        return need_update;
    }

    /// State-specific bookkeeping after sending entries with last index `last`.
    pub fn updateState(self: *Progress, last: u64) !void {
        switch (self.state) {
            .probe => self.pause(),
            .replicate => {
                self.optimisticUpdate(last);
                try self.inflights.add(self.allocator, last);
            },
            .snapshot => @panic("updating progress state in snapshot state"),
        }
    }

    pub fn snapshotFailure(self: *Progress) void {
        self.pending_snapshot = 0;
    }

    pub fn optimisticUpdate(self: *Progress, n: u64) void {
        self.next_idx = n + 1;
    }

    pub fn isPaused(self: Progress) bool {
        return switch (self.state) {
            .probe => self.paused,
            .replicate => self.inflights.full(),
            .snapshot => true,
        };
    }

    /// React to a rejection at index `rejected` with the suggested `match_hint`
    /// and optional `request_snapshot`. Returns true if next_idx was adjusted.
    pub fn maybeDecTo(
        self: *Progress,
        rejected: u64,
        match_hint: u64,
        request_snapshot: u64,
    ) bool {
        log.debug(
            "MaybeDecTo: rejected={}, match_hint={}, request_snapshot={}, state={s}, matched={}",
            .{ rejected, match_hint, request_snapshot, progressStateName(self.state), self.matched },
        );

        if (self.state == .replicate) {
            if (rejected < self.matched or (rejected == self.matched and request_snapshot == invalid_index)) {
                return false;
            }
            if (request_snapshot == invalid_index) {
                self.next_idx = self.matched + 1;
            } else {
                self.pending_request_snapshot = request_snapshot;
            }
            return true;
        }

        if ((self.next_idx == 0 or self.next_idx - 1 != rejected) and request_snapshot == invalid_index) {
            return false;
        }

        if (request_snapshot == invalid_index) {
            self.next_idx = @min(rejected, match_hint + 1);
            if (self.next_idx < self.matched + 1) self.next_idx = self.matched + 1;
        } else if (self.pending_request_snapshot == invalid_index) {
            self.pending_request_snapshot = request_snapshot;
        }

        self.resume_();
        return true;
    }

    fn resetState(self: *Progress, state: ProgressState) void {
        self.paused = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.inflights.reset(self.allocator);
    }
};

/// Map of node id → Progress. Implements the AckedIndexer vtable by reading
/// each Progress's `matched`/`commit_group_id`.
pub const ProgressMap = struct {
    map: std.AutoHashMap(u64, Progress),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ProgressMap {
        return .{
            .map = std.AutoHashMap(u64, Progress).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ProgressMap) void {
        var it = self.map.valueIterator();
        while (it.next()) |p| p.deinit();
        self.map.deinit();
        self.* = undefined;
    }

    /// Insert a fresh Progress at `id`. If an entry already exists it is
    /// replaced (and the old Progress is deinit'd).
    pub fn put(self: *ProgressMap, id: u64, p: Progress) !void {
        const gop = try self.map.getOrPut(id);
        if (gop.found_existing) gop.value_ptr.deinit();
        gop.value_ptr.* = p;
    }

    pub fn remove(self: *ProgressMap, id: u64) bool {
        if (self.map.getEntry(id)) |entry| {
            entry.value_ptr.deinit();
            return self.map.remove(id);
        }
        return false;
    }

    pub fn get(self: ProgressMap, id: u64) ?Progress {
        return self.map.get(id);
    }

    pub fn getPtr(self: *ProgressMap, id: u64) ?*Progress {
        return self.map.getPtr(id);
    }

    pub fn contains(self: ProgressMap, id: u64) bool {
        return self.map.contains(id);
    }

    pub fn count(self: ProgressMap) usize {
        return self.map.count();
    }

    pub fn ackedIndex(self: *const ProgressMap, voter: u64) ?Index {
        const p = self.map.get(voter) orelse return null;
        return .{ .index = p.matched, .group_id = p.commit_group_id };
    }

    fn ackedIndexImpl(ctx: *const anyopaque, voter: u64) ?Index {
        const self: *const ProgressMap = @ptrCast(@alignCast(@constCast(ctx)));
        return self.ackedIndex(voter);
    }

    pub const vtable: AckedIndexer.VTable = .{ .acked_index = ackedIndexImpl };

    /// Borrow `self` as a generic indexer. The returned value is invalidated
    /// when `self` is moved or destroyed.
    pub fn indexer(self: *const ProgressMap) AckedIndexer {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

test "progress resume clears paused via maybeUpdate and maybeDecTo" {
    var p = Progress.init(std.testing.allocator, 2, 256);
    defer p.deinit();
    p.paused = true;
    _ = p.maybeDecTo(1, 1, invalid_index);
    try std.testing.expect(!p.paused);

    p.paused = true;
    _ = p.maybeUpdate(2);
    try std.testing.expect(!p.paused);
}

test "progress isPaused by state" {
    const Case = struct { state: ProgressState, paused: bool, want: bool };
    const cases = [_]Case{
        .{ .state = .probe, .paused = false, .want = false },
        .{ .state = .probe, .paused = true, .want = true },
        .{ .state = .replicate, .paused = false, .want = false },
        .{ .state = .replicate, .paused = true, .want = false },
        .{ .state = .snapshot, .paused = false, .want = true },
        .{ .state = .snapshot, .paused = true, .want = true },
    };

    for (cases) |c| {
        var p = Progress.init(std.testing.allocator, 0, 256);
        defer p.deinit();
        p.state = c.state;
        p.paused = c.paused;
        try std.testing.expectEqual(c.want, p.isPaused());
    }
}

test "progress becomeProbe transitions" {
    const Case = struct {
        initial_state: ProgressState,
        matched: u64,
        next_idx: u64,
        pending_snapshot: u64,
        expected_next_idx: u64,
    };
    const cases = [_]Case{
        .{ .initial_state = .replicate, .matched = 1, .next_idx = 5, .pending_snapshot = 0, .expected_next_idx = 2 },
        .{ .initial_state = .snapshot, .matched = 1, .next_idx = 5, .pending_snapshot = 10, .expected_next_idx = 11 },
        .{ .initial_state = .snapshot, .matched = 1, .next_idx = 5, .pending_snapshot = 0, .expected_next_idx = 2 },
    };

    for (cases) |c| {
        var p = Progress.init(std.testing.allocator, c.next_idx, 256);
        defer p.deinit();
        p.state = c.initial_state;
        p.matched = c.matched;
        p.pending_snapshot = c.pending_snapshot;

        p.becomeProbe();
        try std.testing.expectEqual(ProgressState.probe, p.state);
        try std.testing.expectEqual(c.matched, p.matched);
        try std.testing.expectEqual(c.expected_next_idx, p.next_idx);
    }
}

test "progress becomeReplicate and becomeSnapshot" {
    var p = Progress.init(std.testing.allocator, 5, 256);
    defer p.deinit();
    p.state = .probe;
    p.matched = 1;

    p.becomeReplicate();
    try std.testing.expectEqual(ProgressState.replicate, p.state);
    try std.testing.expectEqual(@as(u64, 1), p.matched);
    try std.testing.expectEqual(@as(u64, 2), p.next_idx);

    p.becomeSnapshot(10);
    try std.testing.expectEqual(ProgressState.snapshot, p.state);
    try std.testing.expectEqual(@as(u64, 10), p.pending_snapshot);
}

test "progress maybeUpdate advances matched and next_idx" {
    const prev_matched: u64 = 3;
    const prev_next: u64 = 5;
    const Case = struct { update: u64, want_matched: u64, want_next: u64, want_ok: bool };
    const cases = [_]Case{
        .{ .update = prev_matched - 1, .want_matched = prev_matched, .want_next = prev_next, .want_ok = false },
        .{ .update = prev_matched, .want_matched = prev_matched, .want_next = prev_next, .want_ok = false },
        .{ .update = prev_matched + 1, .want_matched = prev_matched + 1, .want_next = prev_next, .want_ok = true },
        .{ .update = prev_matched + 2, .want_matched = prev_matched + 2, .want_next = prev_next + 1, .want_ok = true },
    };

    for (cases) |c| {
        var p = Progress.init(std.testing.allocator, prev_next, 256);
        defer p.deinit();
        p.matched = prev_matched;

        try std.testing.expectEqual(c.want_ok, p.maybeUpdate(c.update));
        try std.testing.expectEqual(c.want_matched, p.matched);
        try std.testing.expectEqual(c.want_next, p.next_idx);
    }
}

test "progress maybeDecTo handles probe and replicate states" {
    const Case = struct {
        state: ProgressState,
        matched: u64,
        next_idx: u64,
        rejected: u64,
        last: u64,
        want_changed: bool,
        want_next: u64,
    };
    const cases = [_]Case{
        .{ .state = .replicate, .matched = 5, .next_idx = 10, .rejected = 5, .last = 5, .want_changed = false, .want_next = 10 },
        .{ .state = .replicate, .matched = 5, .next_idx = 10, .rejected = 4, .last = 4, .want_changed = false, .want_next = 10 },
        .{ .state = .replicate, .matched = 5, .next_idx = 10, .rejected = 9, .last = 9, .want_changed = true, .want_next = 6 },
        .{ .state = .probe, .matched = 0, .next_idx = 0, .rejected = 0, .last = 0, .want_changed = false, .want_next = 0 },
        .{ .state = .probe, .matched = 0, .next_idx = 10, .rejected = 5, .last = 5, .want_changed = false, .want_next = 10 },
        .{ .state = .probe, .matched = 0, .next_idx = 10, .rejected = 9, .last = 9, .want_changed = true, .want_next = 9 },
        .{ .state = .probe, .matched = 0, .next_idx = 2, .rejected = 1, .last = 1, .want_changed = true, .want_next = 1 },
        .{ .state = .probe, .matched = 0, .next_idx = 1, .rejected = 0, .last = 0, .want_changed = true, .want_next = 1 },
        .{ .state = .probe, .matched = 0, .next_idx = 10, .rejected = 9, .last = 2, .want_changed = true, .want_next = 3 },
        .{ .state = .probe, .matched = 0, .next_idx = 10, .rejected = 9, .last = 0, .want_changed = true, .want_next = 1 },
    };

    for (cases) |c| {
        var p = Progress.init(std.testing.allocator, c.next_idx, 256);
        defer p.deinit();
        p.state = c.state;
        p.matched = c.matched;

        const changed = p.maybeDecTo(c.rejected, c.last, invalid_index);
        try std.testing.expectEqual(c.want_changed, changed);
        try std.testing.expectEqual(c.matched, p.matched);
        try std.testing.expectEqual(c.want_next, p.next_idx);
    }
}

test "progress map ackedIndex vtable dispatch" {
    var pm = ProgressMap.init(std.testing.allocator);
    defer pm.deinit();

    try pm.put(1, blk: {
        var p = Progress.init(std.testing.allocator, 1, 8);
        p.matched = 5;
        p.commit_group_id = 7;
        break :blk p;
    });
    try pm.put(2, blk: {
        var p = Progress.init(std.testing.allocator, 1, 8);
        p.matched = 9;
        break :blk p;
    });

    const idx = pm.indexer();
    const a = idx.ackedIndex(1).?;
    try std.testing.expectEqual(@as(u64, 5), a.index);
    try std.testing.expectEqual(@as(u64, 7), a.group_id);
    const b = idx.ackedIndex(2).?;
    try std.testing.expectEqual(@as(u64, 9), b.index);
    try std.testing.expect(idx.ackedIndex(99) == null);
}
