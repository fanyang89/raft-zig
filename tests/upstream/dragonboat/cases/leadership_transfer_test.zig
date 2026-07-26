// Copyright 2015 The etcd Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig from Dragonboat revision
// 076c7f6497dcc18880aed6323246d5079661942c.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;
const payload = "dragonboat-leader-transfer";

pub const inventory_target = "tests/upstream/dragonboat/cases/leadership_transfer_test.zig";

fn proposal(id: u64, data: []const u8) !raft.Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, data) };
    return .{ .msg_type = .propose, .from = id, .to = id, .entries = entries };
}

fn expectPayload(peer: *network.Peer) !void {
    const entries = try peer.raft.raft_log.allEntries();
    defer {
        for (entries) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    for (entries) |entry| {
        if (std.mem.eql(u8, payload, entry.data)) return;
    }
    return error.PayloadNotReplicated;
}

test "Dragonboat: raft_etcd_test.go::TestLeaderTransferWithPreVote" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{
        .pre_vote = true,
        .check_quorum = true,
    });
    defer net.deinit();

    const follower = net.getPeer(2).?;
    follower.raft.randomized_election_timeout = follower.raft.election_timeout + 2;
    for (0..follower.raft.election_timeout) |_| _ = try net.tickPeer(2);

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(u64, 1), net.getPeer(1).?.raft.leader_id);

    try net.send(&.{.{ .msg_type = .transfer_leader, .from = 2, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(2).?.raft.state);
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(1).?.raft.leader_id);
    try std.testing.expectEqual(@as(?u64, null), net.getPeer(1).?.raft.lead_transferee);
    try std.testing.expectEqual(@as(?u64, null), net.getPeer(2).?.raft.lead_transferee);

    const proposal_index = net.getPeer(2).?.raft.raft_log.lastIndex() + 1;
    var message = try proposal(1, payload);
    defer message.deinit(allocator);
    try net.send(&.{message});
    for (1..4) |id| {
        const peer = net.getPeer(id).?;
        try std.testing.expect(peer.raft.raft_log.committed >= proposal_index);
        try expectPayload(peer);
    }

    try net.send(&.{.{ .msg_type = .transfer_leader, .from = 1, .to = 2 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(2).?.raft.state);
    try std.testing.expectEqual(@as(u64, 1), net.getPeer(2).?.raft.leader_id);
    try std.testing.expectEqual(@as(?u64, null), net.getPeer(1).?.raft.lead_transferee);
    try std.testing.expectEqual(@as(?u64, null), net.getPeer(2).?.raft.lead_transferee);
    for (1..4) |id| try expectPayload(net.getPeer(id).?);
}
