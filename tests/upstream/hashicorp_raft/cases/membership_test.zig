//! Clean-room tests derived only from externally observable behavior.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/hashicorp_raft/cases/membership_test.zig";

test "HashiCorp Raft: only configured voters contribute voting power" {
    var net = try network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2, 3, 4, 5 },
        .voters = &.{ 1, 2, 3 },
        .learners = &.{4},
    }, .{});
    defer net.deinit();
    const candidate = net.getPeer(1).?;
    candidate.raft.becomeCandidate();
    try std.testing.expectEqual(raft.VoteResult.pending, candidate.raft.poll(1, true));

    try std.testing.expectEqual(raft.VoteResult.pending, candidate.raft.poll(4, true));
    try std.testing.expectEqual(raft.StateRole.candidate, candidate.raft.state);
    try std.testing.expectEqual(raft.VoteResult.pending, candidate.raft.poll(5, true));
    try std.testing.expectEqual(raft.StateRole.candidate, candidate.raft.state);
    try std.testing.expectEqual(raft.VoteResult.won, candidate.raft.poll(2, true));
    try std.testing.expectEqual(raft.StateRole.leader, candidate.raft.state);

    var non_voters = std.AutoHashMap(u64, void).init(allocator);
    defer non_voters.deinit();
    try non_voters.put(1, {});
    try non_voters.put(4, {});
    try non_voters.put(5, {});
    try std.testing.expect(!candidate.raft.progress_tracker.hasQuorum(non_voters));

    var voters = std.AutoHashMap(u64, void).init(allocator);
    defer voters.deinit();
    try voters.put(1, {});
    try voters.put(2, {});
    try std.testing.expect(candidate.raft.progress_tracker.hasQuorum(voters));
}

test "HashiCorp Raft: raft_test.go::TestRaft_ClusterCanRegainStability_WhenNonVoterWithHigherTermJoin" {
    const high_term: u64 = 7;
    var net = try network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2, 3 },
        .voters = &.{ 1, 2 },
        .learners = &.{3},
    }, .{ .pre_vote = true });
    defer net.deinit();

    try net.isolate(3);
    try net.stepLocal(3, .{
        .msg_type = .request_vote,
        .from = 9,
        .to = 3,
        .term = high_term,
    });
    _ = try net.runUntilIdle(100);

    const learner = net.getPeer(3).?;
    try std.testing.expectEqual(high_term, learner.raft.term);
    try net.stepLocal(3, .{ .msg_type = .hup, .from = 3, .to = 3 });
    try std.testing.expectEqual(raft.StateRole.follower, learner.raft.state);
    try std.testing.expect(!learner.raft.promotable);

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);

    net.recover();
    try net.send(&.{.{ .msg_type = .beat, .from = 1, .to = 1 }});
    try std.testing.expectEqual(high_term, net.getPeer(1).?.raft.term);
    try std.testing.expectEqual(raft.StateRole.follower, learner.raft.state);

    const digest = try net.converge(64, 1_000);
    try std.testing.expect(digest.term > high_term);
    try std.testing.expect(digest.leader_id == 1 or digest.leader_id == 2);
    try std.testing.expectEqual(raft.StateRole.follower, learner.raft.state);
    try net.checkSafety();
}
