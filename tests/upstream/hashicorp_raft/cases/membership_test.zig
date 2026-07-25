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
