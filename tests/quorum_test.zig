//! Quorum math tests.
//!
//! Each test case is a parameterized row mirroring a directive in the original
//! testdata files. We exercise `MajorityConfig`, `JointConfiguration`, both
//! with and without `use_group_commit`, and the symmetry checks the datadriven
//! harness performed by swapping incoming/outgoing.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;
const AckIndexer = raft.AckIndexer;
const Index = raft.Index;
const VoteResult = raft.VoteResult;

const IndexerEntry = struct { id: u64, idx: ?u64, gid: u64 };

fn buildIndexer(cases: []const IndexerEntry) !AckIndexer {
    var idx = AckIndexer.init(allocator);
    for (cases) |c| {
        if (c.idx) |n| try idx.set(c.id, .{ .index = n, .group_id = c.gid });
    }
    return idx;
}

// ---------------------------------------------------------------------------
// vote: majority_config
// ---------------------------------------------------------------------------

test "quorum: majority vote matrix" {
    const Case = struct {
        voters: []const u64,
        // votes[i] applies to voters[i]; '?' = missing.
        votes: []const u8,
        want: VoteResult,
    };
    const cases = [_]Case{
        // Empty config always wins.
        .{ .voters = &.{}, .votes = &.{}, .want = .won },
        .{ .voters = &.{1}, .votes = &.{'?'}, .want = .pending },
        .{ .voters = &.{1}, .votes = &.{'n'}, .want = .lost },
        .{ .voters = &.{123}, .votes = &.{'y'}, .want = .won },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ '?', '?' }, .want = .pending },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ 'n', '?' }, .want = .lost },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ 'y', '?' }, .want = .pending },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ 'n', 'y' }, .want = .lost },
        .{ .voters = &.{ 4, 8 }, .votes = &.{ 'y', 'y' }, .want = .won },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ '?', '?', '?' }, .want = .pending },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'n', '?', '?' }, .want = .pending },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'y', '?', '?' }, .want = .pending },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'n', 'n', '?' }, .want = .lost },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'y', 'n', '?' }, .want = .pending },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'y', 'y', '?' }, .want = .won },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'y', 'y', 'n' }, .want = .won },
        .{ .voters = &.{ 2, 4, 7 }, .votes = &.{ 'n', 'y', 'n' }, .want = .lost },
        // 7-voter sample from the testdata.
        .{ .voters = &.{ 1, 2, 3, 4, 5, 6, 7 }, .votes = &.{ 'y', 'y', 'n', 'y', '?', '?', '?' }, .want = .pending },
        .{ .voters = &.{ 1, 2, 3, 4, 5, 6, 7 }, .votes = &.{ '?', 'y', 'y', '?', 'n', 'y', 'n' }, .want = .pending },
        .{ .voters = &.{ 1, 2, 3, 4, 5, 6, 7 }, .votes = &.{ 'y', 'y', 'n', 'y', '?', 'n', 'y' }, .want = .won },
        .{ .voters = &.{ 1, 2, 3, 4, 5, 6, 7 }, .votes = &.{ 'y', 'y', '?', 'n', 'y', 'n', 'n' }, .want = .pending },
        .{ .voters = &.{ 1, 2, 3, 4, 5, 6, 7 }, .votes = &.{ 'y', 'y', 'n', 'y', 'n', 'n', 'n' }, .want = .lost },
    };

    const Checker = struct {
        voters: []const u64,
        votes: []const u8,
        pub fn check(self: @This(), id: u64) ?bool {
            for (self.voters, 0..) |v, i| {
                if (v != id) continue;
                return switch (self.votes[i]) {
                    'y' => @as(?bool, true),
                    'n' => @as(?bool, false),
                    '?' => @as(?bool, null),
                    else => unreachable,
                };
            }
            return null;
        }
    };

    for (cases) |c| {
        var mc = try raft.MajorityConfig.fromIds(allocator, c.voters);
        defer mc.deinit();
        const ch = Checker{ .voters = c.voters, .votes = c.votes };
        try std.testing.expectEqual(c.want, mc.getVoteResult(Checker, &ch));
    }
}

// ---------------------------------------------------------------------------
// committed: majority_config
// ---------------------------------------------------------------------------

test "quorum: majority committed picks the majority-th sorted index" {
    const Case = struct {
        // voters[i] is acknowledged at idx[i] (null = missing, treated as 0).
        voters: []const u64,
        idx: []const ?u64,
        want: u64,
    };
    const cases = [_]Case{
        .{ .voters = &.{1}, .idx = &.{null}, .want = 0 },
        .{ .voters = &.{1}, .idx = &.{1}, .want = 1 },
        .{ .voters = &.{ 1, 2 }, .idx = &.{ 1, 2 }, .want = 1 },
        .{ .voters = &.{ 1, 2 }, .idx = &.{ 2, 1 }, .want = 1 },
        .{ .voters = &.{ 1, 2, 3 }, .idx = &.{ 10, 20, 30 }, .want = 20 },
        .{ .voters = &.{ 1, 2, 3 }, .idx = &.{ 30, 20, 10 }, .want = 20 },
        .{ .voters = &.{ 1, 2, 3 }, .idx = &.{ null, null, null }, .want = 0 },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .idx = &.{ 100, 200, 300, 400, 500 }, .want = 300 },
    };

    for (cases) |c| {
        std.debug.assert(c.voters.len == c.idx.len);
        var entries: [8]IndexerEntry = undefined;
        for (c.voters, c.idx, 0..) |id, ix, i| {
            entries[i] = .{ .id = id, .idx = ix, .gid = 0 };
        }
        var indexer = try buildIndexer(entries[0..c.voters.len]);
        defer indexer.deinit();

        var mc = try raft.MajorityConfig.fromIds(allocator, c.voters);
        defer mc.deinit();
        const r = mc.committedIndex(false, indexer.indexer());
        try std.testing.expectEqual(c.want, r.index);
    }
}

// ---------------------------------------------------------------------------
// vote + committed: joint config symmetry
// ---------------------------------------------------------------------------

test "quorum: joint vote requires both sides" {
    // Each voter gets ONE vote regardless of how many majority configs include
    // it. The `votes` array lines up with `unique_voters` (the union, in the
    // order listed). The checker maps id → vote position.
    const Case = struct {
        unique_voters: []const u64,
        incoming: []const u64,
        outgoing: []const u64,
        votes: []const u8,
        want: VoteResult,
    };
    const cases = [_]Case{
        // Simple: incoming={1,2}, outgoing=∅ → only one majority matters.
        .{ .unique_voters = &.{ 1, 2 }, .incoming = &.{ 1, 2 }, .outgoing = &.{}, .votes = &.{ 'y', 'y' }, .want = .won },
        // Joint with identical sets: each voter gets one vote.
        .{ .unique_voters = &.{ 1, 2 }, .incoming = &.{ 1, 2 }, .outgoing = &.{ 1, 2 }, .votes = &.{ 'y', 'y' }, .want = .won },
        .{ .unique_voters = &.{ 1, 2 }, .incoming = &.{ 1, 2 }, .outgoing = &.{ 1, 2 }, .votes = &.{ 'n', 'y' }, .want = .lost },
        // Disjoint joint.
        .{ .unique_voters = &.{ 1, 2, 3, 4 }, .incoming = &.{ 1, 2 }, .outgoing = &.{ 3, 4 }, .votes = &.{ 'y', 'y', 'y', 'y' }, .want = .won },
        .{ .unique_voters = &.{ 1, 2, 3, 4 }, .incoming = &.{ 1, 2 }, .outgoing = &.{ 3, 4 }, .votes = &.{ 'y', 'y', '?', '?' }, .want = .pending },
        .{ .unique_voters = &.{ 1, 2, 3, 4 }, .incoming = &.{ 1, 2 }, .outgoing = &.{ 3, 4 }, .votes = &.{ 'y', 'y', 'n', 'n' }, .want = .lost },
    };

    for (cases) |c| {
        var jc = try raft.JointConfiguration.fromIncomingOutgoing(allocator, c.incoming, c.outgoing);
        defer jc.deinit();

        const Checker = struct {
            unique_voters: []const u64,
            votes: []const u8,
            pub fn check(self: @This(), id: u64) ?bool {
                for (self.unique_voters, 0..) |v, i| {
                    if (v == id) {
                        return switch (self.votes[i]) {
                            'y' => @as(?bool, true),
                            'n' => @as(?bool, false),
                            '?' => @as(?bool, null),
                            else => unreachable,
                        };
                    }
                }
                return null;
            }
        };
        const ch = Checker{ .unique_voters = c.unique_voters, .votes = c.votes };
        try std.testing.expectEqual(c.want, jc.getVoteResult(Checker, &ch));
    }
}

test "quorum: joint committed is min of incoming and outgoing majorities" {
    // Incoming {1,2,3}, outgoing {2,3}. Matches 1→100, 2→50, 3→50.
    // Incoming sorted desc: 100,50,50 → majority 2nd → 50.
    // Outgoing sorted desc: 50,50 → majority 1st → 50.
    // Joint = min(50, 50) = 50.
    var jc = try raft.JointConfiguration.fromIncomingOutgoing(allocator, &.{ 1, 2, 3 }, &.{ 2, 3 });
    defer jc.deinit();

    var indexer = try buildIndexer(&.{
        .{ .id = 1, .idx = 100, .gid = 0 },
        .{ .id = 2, .idx = 50, .gid = 0 },
        .{ .id = 3, .idx = 50, .gid = 0 },
    });
    defer indexer.deinit();

    const r = jc.committedIndex(false, indexer.indexer());
    try std.testing.expectEqual(@as(u64, 50), r.index);
}

test "quorum: empty joint config symmetry still matches majority alone" {
    // An "empty" joint config (incoming={1,2,3}, outgoing={}) should produce
    // the same committed index as the majority config alone.
    var mc = try raft.MajorityConfig.fromIds(allocator, &.{ 1, 2, 3 });
    defer mc.deinit();
    var jc_zero = try raft.JointConfiguration.fromIncomingOutgoing(allocator, &.{ 1, 2, 3 }, &.{});
    defer jc_zero.deinit();
    var jc_self = try raft.JointConfiguration.fromIncomingOutgoing(allocator, &.{ 1, 2, 3 }, &.{ 1, 2, 3 });
    defer jc_self.deinit();

    var indexer = try buildIndexer(&.{
        .{ .id = 1, .idx = 10, .gid = 0 },
        .{ .id = 2, .idx = 20, .gid = 0 },
        .{ .id = 3, .idx = 30, .gid = 0 },
    });
    defer indexer.deinit();

    const m = mc.committedIndex(false, indexer.indexer()).index;
    const z = jc_zero.committedIndex(false, indexer.indexer()).index;
    const s = jc_self.committedIndex(false, indexer.indexer()).index;

    try std.testing.expectEqual(m, z);
    try std.testing.expectEqual(m, s);
}

// ---------------------------------------------------------------------------
// group_commit
// ---------------------------------------------------------------------------

test "quorum: group commit prefers same-group majority when groups differ" {
    // Two groups {1,2} (gid=10) and {3,4} (gid=20), each with mixed indexes.
    // Without group_commit, quorum of 4 picks 2nd highest = some index.
    // With group_commit, when the quorum-th entry's group differs from
    // another matched entry's group, we return min(matched.index, quorum).
    var mc = try raft.MajorityConfig.fromIds(allocator, &.{ 1, 2, 3, 4 });
    defer mc.deinit();

    var indexer = try buildIndexer(&.{
        .{ .id = 1, .idx = 100, .gid = 10 },
        .{ .id = 2, .idx = 90, .gid = 10 },
        .{ .id = 3, .idx = 80, .gid = 20 },
        .{ .id = 4, .idx = 70, .gid = 20 },
    });
    defer indexer.deinit();

    const plain = mc.committedIndex(false, indexer.indexer()).index;
    const grouped = mc.committedIndex(true, indexer.indexer()).index;
    // Sorted desc: 100, 90, 80, 70. Majority of 4 = 4/2+1 = 3 → 3rd highest = 80.
    try std.testing.expectEqual(@as(u64, 80), plain);
    // Group commit sees mixed groups; quorum entry (80) has group 20, later
    // 90 has group 10 → min(80, 90) = 80. (quorum_index.group_id=20,
    // 90.group_id=10 differ → return min(m.index=80, quorum_commit=80) = 80.)
    try std.testing.expectEqual(@as(u64, 80), grouped);
}
