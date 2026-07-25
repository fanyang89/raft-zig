const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

pub const inventory_target = "tests/upstream/openraft/cases/invariants_test.zig";

test "OpenRaft: duplicate leaders in one term violate election safety" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    const first = net.getPeer(1).?;
    first.raft.term = 1;
    first.raft.state = .leader;
    first.raft.leader_id = 1;
    try net.checkSafety();

    first.raft.state = .follower;
    first.raft.leader_id = 0;
    const second = net.getPeer(2).?;
    second.raft.term = 1;
    second.raft.state = .leader;
    second.raft.leader_id = 2;

    try std.testing.expectError(error.ElectionSafetyViolation, net.checkSafety());
}

test "OpenRaft: valid message history satisfies safety invariants" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try net.isolate(3);
    try net.send(&.{.{ .msg_type = .beat, .from = 1, .to = 1 }});
    net.recover();
    _ = try net.converge(20, 1_000);
    try net.checkSafety();
}

test "OpenRaft: committed log divergence is detected" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});

    const follower = net.getPeer(2).?;
    const index = follower.raft.raft_log.committed;
    const offset: usize = @intCast(index - follower.storage.core.firstIndex());
    follower.storage.core.entries.items[offset].term += 1;

    try std.testing.expectError(error.CommittedLogViolation, net.checkSafety());
}

test "OpenRaft: higher-term leader missing a committed entry violates leader completeness" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try net.isolate(2);
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});

    const witness_peer = net.getPeer(1).?;
    const missing_peer = net.getPeer(2).?;
    const committed_index = witness_peer.raft.raft_log.committed;
    try std.testing.expect(committed_index > 0);
    try std.testing.expectEqual(@as(u64, 0), missing_peer.raft.raft_log.lastIndex());
    try net.checkSafety();

    witness_peer.raft.becomeFollower(witness_peer.raft.term, 0);
    const higher_term = witness_peer.raft.term + 1;
    missing_peer.raft.term = higher_term;
    missing_peer.raft.vote = 2;
    missing_peer.raft.state = .leader;
    missing_peer.raft.leader_id = 2;
    missing_peer.raft.raft_log.unstable.offset = committed_index + 1;
    missing_peer.raft.raft_log.unstable.truncateAndAppend(&.{.{
        .term = higher_term,
        .index = committed_index + 1,
    }});

    try std.testing.expectError(error.Unavailable, missing_peer.raft.raft_log.term(committed_index));
    try std.testing.expectError(error.LeaderCompletenessViolation, net.checkSafety());
}

test "OpenRaft: committed index regression is detected" {
    var net = try network.newNetwork(&.{1});
    defer net.deinit();
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});

    const peer = net.getPeer(1).?;
    try std.testing.expect(peer.raft.raft_log.committed > 0);
    peer.raft.raft_log.committed -= 1;

    try std.testing.expectError(error.CommitRegression, net.checkSafety());
}

test "OpenRaft: term regression is detected" {
    var net = try network.newNetwork(&.{1});
    defer net.deinit();
    const peer = net.getPeer(1).?;
    peer.raft.term = 1;
    try net.checkSafety();
    peer.raft.term = 0;

    try std.testing.expectError(error.TermRegression, net.checkSafety());
}

test "OpenRaft: vote regression within one term is detected" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    const peer = net.getPeer(1).?;
    peer.raft.term = 1;
    peer.raft.vote = 1;
    try net.checkSafety();
    peer.raft.vote = 2;

    try std.testing.expectError(error.VoteRegression, net.checkSafety());
}
