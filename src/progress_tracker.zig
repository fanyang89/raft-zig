//! Top-level ProgressTracker: owns config + per-peer Progress + vote tally.
//!
//! Tracks replication state for every peer, computes quorum outcomes, and
//! applies configuration changes produced by the ConfChanger.

const std = @import("std");

const progress_mod = @import("progress.zig");
const tracker_conf_mod = @import("tracker_conf.zig");
const joint_conf_mod = @import("joint_conf.zig");
const ack_indexer_mod = @import("ack_indexer.zig");

const Progress = progress_mod.Progress;
const ProgressMap = progress_mod.ProgressMap;
const TrackerConfiguration = tracker_conf_mod.TrackerConfiguration;
const JointConfiguration = joint_conf_mod.JointConfiguration;
const VoteResult = ack_indexer_mod.VoteResult;
const Index = ack_indexer_mod.Index;

pub const MapChangeKind = enum(u8) { add, remove };

pub const MapChangeEntry = struct {
    id: u64,
    kind: MapChangeKind,
};

pub const MapChange = []const MapChangeEntry;

pub const CountVoteResult = struct {
    granted: usize,
    rejected: usize,
    result: VoteResult,
};

pub const ProgressTracker = struct {
    progress: ProgressMap,
    peer_ids: std.ArrayList(u64),
    conf: TrackerConfiguration,
    votes: std.AutoHashMap(u64, bool),
    max_inflight: usize,
    group_commit: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, max_inflight: usize) ProgressTracker {
        return .{
            .progress = ProgressMap.init(allocator),
            .peer_ids = .empty,
            .conf = TrackerConfiguration.init(allocator),
            .votes = std.AutoHashMap(u64, bool).init(allocator),
            .max_inflight = max_inflight,
            .group_commit = false,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ProgressTracker) void {
        self.progress.deinit();
        self.peer_ids.deinit(self.allocator);
        self.conf.deinit();
        self.votes.deinit();
        self.* = undefined;
    }

    /// Vote tally over the current voter set. Each vote in `self.votes` is
    /// counted only if the voter is currently in the configuration.
    pub fn getVoteResult(self: *const ProgressTracker) VoteResult {
        const votes_ptr = &self.votes;
        const Checker = struct {
            votes: *const std.AutoHashMap(u64, bool),
            pub fn check(c: @This(), id: u64) ?bool {
                return c.votes.get(id);
            }
        };
        const checker = Checker{ .votes = votes_ptr };
        return self.conf.voters.getVoteResult(Checker, &checker);
    }

    pub fn countVotes(self: *const ProgressTracker) CountVoteResult {
        var granted: usize = 0;
        var rejected: usize = 0;
        var it = self.votes.iterator();
        while (it.next()) |entry| {
            if (!self.conf.voters.contains(entry.key_ptr.*)) continue;
            if (entry.value_ptr.*) granted += 1 else rejected += 1;
        }
        return .{
            .granted = granted,
            .rejected = rejected,
            .result = self.getVoteResult(),
        };
    }

    /// Install a new configuration and apply the corresponding add/remove
    /// changes to the per-peer Progress map. New peers get a fresh Progress
    /// at `next_idx` and `recent_active = true`.
    pub fn applyConf(
        self: *ProgressTracker,
        conf: TrackerConfiguration,
        changes: MapChange,
        next_idx: u64,
    ) !void {
        var new_conf = try conf.clone();
        errdefer new_conf.deinit();
        var new_peer_ids: std.ArrayList(u64) = .empty;
        errdefer new_peer_ids.deinit(self.allocator);
        var incoming = new_conf.voters.incoming.voters.keyIterator();
        while (incoming.next()) |id| try new_peer_ids.append(self.allocator, id.*);
        var outgoing = new_conf.voters.outgoing.voters.keyIterator();
        while (outgoing.next()) |id| try new_peer_ids.append(self.allocator, id.*);
        var learners = new_conf.learners.keyIterator();
        while (learners.next()) |id| try new_peer_ids.append(self.allocator, id.*);
        var learners_next = new_conf.learners_next.keyIterator();
        while (learners_next.next()) |id| try new_peer_ids.append(self.allocator, id.*);
        std.mem.sort(u64, new_peer_ids.items, {}, std.sort.asc(u64));
        var unique_count: usize = 0;
        for (new_peer_ids.items) |id| {
            if (unique_count == 0 or new_peer_ids.items[unique_count - 1] != id) {
                new_peer_ids.items[unique_count] = id;
                unique_count += 1;
            }
        }
        new_peer_ids.shrinkRetainingCapacity(unique_count);

        var additions: u32 = 0;
        for (changes) |change| {
            if (change.kind == .add and !self.progress.contains(change.id)) {
                additions = std.math.add(u32, additions, 1) catch return error.OutOfMemory;
            }
        }
        try self.progress.ensureUnusedCapacity(additions);

        var old_conf = self.conf;
        var old_peer_ids = self.peer_ids;
        self.conf = new_conf;
        self.peer_ids = new_peer_ids;
        for (changes) |change| {
            switch (change.kind) {
                .add => {
                    var pr = Progress.init(self.allocator, next_idx, self.max_inflight);
                    pr.recent_active = true;
                    self.progress.putAssumeCapacity(change.id, pr);
                },
                .remove => {
                    _ = self.progress.remove(change.id);
                },
            }
        }
        old_conf.deinit();
        old_peer_ids.deinit(self.allocator);
    }

    pub fn resetVotes(self: *ProgressTracker) void {
        self.votes.clearRetainingCapacity();
    }

    pub fn maxCommittedIndex(self: *const ProgressTracker) struct { index: u64, use_group_commit: bool } {
        const r = self.conf.voters.committedIndex(self.group_commit, self.progress.indexer());
        return .{ .index = r.index, .use_group_commit = r.use_group_commit };
    }

    pub fn recordVote(self: *ProgressTracker, id: u64, vote: bool) !void {
        try self.votes.put(id, vote);
    }

    pub fn hasQuorum(self: *const ProgressTracker, potential_quorum: std.AutoHashMap(u64, void)) bool {
        const pq_ptr = &potential_quorum;
        const Checker = struct {
            pq: *const std.AutoHashMap(u64, void),
            pub fn check(c: @This(), id: u64) ?bool {
                return c.pq.contains(id);
            }
        };
        const checker = Checker{ .pq = pq_ptr };
        return self.conf.voters.getVoteResult(Checker, &checker) == .won;
    }

    /// Sweep all Progress entries, marking the caller as recently active and
    /// toggling others off. Returns true if a quorum was active in the
    /// previous sweep window.
    pub fn quorumRecentlyActive(self: *ProgressTracker, perspective_of: u64) !bool {
        var active = std.AutoHashMap(u64, void).init(self.allocator);
        defer active.deinit();

        var it = self.progress.map.iterator();
        while (it.next()) |entry| {
            const id = entry.key_ptr.*;
            const pr = entry.value_ptr;
            if (id == perspective_of) {
                try active.put(id, {});
                pr.recent_active = true;
            } else if (pr.recent_active) {
                try active.put(id, {});
                pr.recent_active = false;
            }
        }
        return self.hasQuorum(active);
    }

    pub fn isSingleton(self: *const ProgressTracker) bool {
        return self.conf.voters.isSingleton();
    }

    pub fn orderedPeerIds(self: *const ProgressTracker) []const u64 {
        return self.peer_ids.items;
    }

    pub fn getPtr(self: *ProgressTracker, id: u64) ?*Progress {
        return self.progress.getPtr(id);
    }

    pub fn at(self: *ProgressTracker, id: u64) *Progress {
        return self.progress.getPtr(id) orelse @panic("ProgressTracker.at: id missing");
    }

    pub fn enableGroupCommit(self: *ProgressTracker, enable: bool) void {
        self.group_commit = enable;
    }

    pub fn groupCommitEnabled(self: *const ProgressTracker) bool {
        return self.group_commit;
    }
};

// KCOV_EXCL_START
test "progress tracker applyConf adds and removes progress entries" {
    const allocator = std.testing.allocator;
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();

    var conf = TrackerConfiguration.init(allocator);
    defer conf.deinit();
    try conf.voters.incoming.add(1);
    try conf.voters.incoming.add(2);

    const changes = [_]MapChangeEntry{
        .{ .id = 1, .kind = .add },
        .{ .id = 2, .kind = .add },
    };
    try tr.applyConf(conf, &changes, 5);
    try std.testing.expectEqual(@as(usize, 2), tr.progress.count());
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, tr.orderedPeerIds());
    try std.testing.expectEqual(@as(u64, 5), tr.at(1).next_idx);
    try std.testing.expect(tr.at(1).recent_active);

    // Remove one voter.
    var conf2 = try tr.conf.clone();
    defer conf2.deinit();
    _ = conf2.voters.incoming.remove(2);
    const changes2 = [_]MapChangeEntry{.{ .id = 2, .kind = .remove }};
    try tr.applyConf(conf2, &changes2, 5);
    try std.testing.expectEqual(@as(usize, 1), tr.progress.count());
    try std.testing.expectEqualSlices(u64, &.{1}, tr.orderedPeerIds());
    try std.testing.expect(tr.progress.contains(1));
}

test "progress tracker vote tally excludes non-voters" {
    const allocator = std.testing.allocator;
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();

    try tr.conf.voters.incoming.add(1);
    try tr.conf.voters.incoming.add(2);
    try tr.conf.voters.incoming.add(3);

    try tr.recordVote(1, true);
    try tr.recordVote(2, false);
    try tr.recordVote(99, true); // not a voter, ignored

    const r = tr.countVotes();
    try std.testing.expectEqual(@as(usize, 1), r.granted);
    try std.testing.expectEqual(@as(usize, 1), r.rejected);
    try std.testing.expectEqual(VoteResult.pending, r.result);
}

test "progress tracker hasQuorum checks intersection" {
    const allocator = std.testing.allocator;
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();
    try tr.conf.voters.incoming.add(1);
    try tr.conf.voters.incoming.add(2);
    try tr.conf.voters.incoming.add(3);

    var full = std.AutoHashMap(u64, void).init(allocator);
    defer full.deinit();
    try full.put(1, {});
    try full.put(2, {});
    try full.put(3, {});
    try std.testing.expect(tr.hasQuorum(full));

    var one = std.AutoHashMap(u64, void).init(allocator);
    defer one.deinit();
    try one.put(1, {});
    try std.testing.expect(!tr.hasQuorum(one));

    var two = std.AutoHashMap(u64, void).init(allocator);
    defer two.deinit();
    try two.put(1, {});
    try two.put(2, {});
    try std.testing.expect(tr.hasQuorum(two));
}

test "progress tracker isSingleton" {
    const allocator = std.testing.allocator;
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();
    // Empty config: outgoing empty + incoming size 0 (not singleton per spec).
    try std.testing.expect(!tr.isSingleton());

    try tr.conf.voters.incoming.add(1);
    try std.testing.expect(tr.isSingleton());

    try tr.conf.voters.incoming.add(2);
    try std.testing.expect(!tr.isSingleton());
}
// KCOV_EXCL_STOP
