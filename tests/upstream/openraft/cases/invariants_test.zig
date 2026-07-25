const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

pub const inventory_target = "tests/upstream/openraft/cases/invariants_test.zig";

test "OpenRaft: duplicate leaders in one term violate election safety" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    for ([_]u64{ 1, 2 }) |id| {
        const peer = net.getPeer(id).?;
        peer.raft.term = 1;
        peer.raft.state = .leader;
        peer.raft.leader_id = id;
    }

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
