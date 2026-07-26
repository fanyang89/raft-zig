// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig from Dragonboat revision
// 076c7f6497dcc18880aed6323246d5079661942c.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;
const Message = raft.Message;

pub const inventory_target = "tests/upstream/dragonboat/cases/election_test.zig";

fn hup(id: u64) Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

fn proposal(id: u64, data: []const u8) !Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, data) };
    return .{ .msg_type = .propose, .from = id, .to = id, .entries = entries };
}

fn expectPreVoteResponse(net: *network.Network, to: u64) !void {
    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    const response = net.pending.items[0];
    try std.testing.expectEqual(raft.MessageType.request_pre_vote_response, response.msg_type);
    try std.testing.expectEqual(@as(u64, 1), response.from);
    try std.testing.expectEqual(to, response.to);
    try std.testing.expectEqual(@as(u64, 11), response.term);
    try std.testing.expect(!response.reject);
}

test "Dragonboat: raft_test.go::TestOneNodeWithHigherTermAndOneNodeWithMostRecentLogCanCompleteElection" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .check_quorum = true });
    defer net.deinit();

    for (1..4) |id| net.getPeer(id).?.raft.becomeFollower(1, raft.invalid_id);

    try net.isolate(3);
    for (0..4) |_| try net.send(&.{hup(3)});

    try net.send(&.{hup(1)});
    var first = try proposal(1, "some data");
    defer first.deinit(allocator);
    try net.send(&.{first});
    var second = try proposal(1, "some data2");
    defer second.deinit(allocator);
    try net.send(&.{second});

    try std.testing.expectEqual(@as(u64, 3), net.getPeer(1).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 3), net.getPeer(2).?.raft.raft_log.committed);
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(2).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.candidate, net.getPeer(3).?.raft.state);
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(1).?.raft.term);
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(2).?.raft.term);
    try std.testing.expectEqual(@as(u64, 5), net.getPeer(3).?.raft.term);

    net.recover();
    try net.isolate(1);

    var elected = false;
    for (0..3) |_| {
        try net.send(&.{hup(3)});
        try std.testing.expect(net.getPeer(3).?.raft.state != .leader);
        try net.send(&.{hup(2)});
        if (net.getPeer(2).?.raft.state == .leader) {
            elected = true;
            break;
        }
    }

    try std.testing.expect(elected);
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(2).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(3).?.raft.state);
}

test "Dragonboat: raft_test.go::TestPreVoteRespWithHigherTerm" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2 }, .{ .pre_vote = true });
    defer net.deinit();
    const target = net.getPeer(1).?;
    target.raft.becomeFollower(10, 2);

    try net.send(&.{.{
        .msg_type = .request_pre_vote_response,
        .from = 2,
        .to = 1,
        .term = 11,
    }});
    try std.testing.expectEqual(@as(u64, 10), target.raft.term);

    try net.send(&.{.{
        .msg_type = .request_pre_vote_response,
        .from = 2,
        .to = 1,
        .term = 20,
        .reject = true,
    }});
    try std.testing.expectEqual(@as(u64, 20), target.raft.term);
}

test "Dragonboat: raft_test.go::TestElectionTickResetAfterGrantVote" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    const voter = net.getPeer(1).?;
    voter.raft.becomeFollower(2, 2);
    voter.raft.election_elapsed = 101;

    try net.stepLocal(1, .{
        .msg_type = .request_vote,
        .from = 2,
        .to = 1,
        .term = 3,
    });

    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    try std.testing.expectEqual(raft.MessageType.request_vote_response, net.pending.items[0].msg_type);
    try std.testing.expect(!net.pending.items[0].reject);
    try std.testing.expectEqual(@as(u64, 2), voter.raft.vote);
    try std.testing.expectEqual(@as(usize, 0), voter.raft.election_elapsed);
}

test "Dragonboat: raft_test.go::TestCastVoteToDifferentNodesIsAllowed" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .pre_vote = true });
    defer net.deinit();
    const voter = net.getPeer(1).?;
    voter.raft.becomeFollower(10, 3);

    try net.stepLocal(1, .{
        .msg_type = .request_pre_vote,
        .from = 2,
        .to = 1,
        .term = 11,
    });
    try expectPreVoteResponse(&net, 2);
    try net.dropPending(0);

    try net.stepLocal(1, .{
        .msg_type = .request_pre_vote,
        .from = 3,
        .to = 1,
        .term = 11,
    });
    try expectPreVoteResponse(&net, 3);

    try std.testing.expectEqual(@as(u64, 10), voter.raft.term);
    try std.testing.expectEqual(raft.invalid_id, voter.raft.vote);
    try std.testing.expectEqual(@as(u64, 3), voter.raft.leader_id);
}
