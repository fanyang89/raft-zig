// Copyright 2015 The etcd Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;
const Message = raft.Message;

pub const inventory_target = "tests/upstream/etcd_raft/cases/learner_test.zig";

fn newLearnerNetwork() !network.Network {
    return network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2, 3 },
        .voters = &.{ 1, 2 },
        .learners = &.{3},
    }, .{});
}

fn hup(id: u64) Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

fn proposal(id: u64, data: []const u8) !Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, data) };
    return .{ .msg_type = .propose, .from = id, .to = id, .entries = entries };
}

fn readIndex(id: u64, context: []const u8) !Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, context) };
    return .{ .msg_type = .read_index, .from = id, .to = id, .entries = entries };
}

fn containsData(peer: *network.Peer, data: []const u8) bool {
    for (peer.storage.core.entries.items) |entry| {
        if (std.mem.eql(u8, entry.data, data)) return true;
    }
    return false;
}

test "etcd/raft: learner does not campaign" {
    var net = try newLearnerNetwork();
    defer net.deinit();

    try net.stepLocal(3, hup(3));

    const learner = net.getPeer(3).?;
    try std.testing.expectEqual(raft.StateRole.follower, learner.raft.state);
    try std.testing.expectEqual(@as(u64, 0), learner.raft.term);
    try std.testing.expectEqual(@as(u64, 0), learner.raft.vote);
    try std.testing.expectEqual(@as(usize, 0), net.pendingCount());

    try net.send(&.{hup(1)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try net.send(&.{.{
        .msg_type = .timeout_now,
        .from = 1,
        .to = 3,
        .term = net.getPeer(1).?.raft.term,
    }});
    try std.testing.expectEqual(raft.StateRole.follower, learner.raft.state);
}

test "etcd/raft: learner responds to a valid vote request" {
    var net = try newLearnerNetwork();
    defer net.deinit();

    try net.stepLocal(3, .{
        .msg_type = .request_vote,
        .from = 2,
        .to = 3,
        .term = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    const response = net.pending.items[0];
    try std.testing.expectEqual(raft.MessageType.request_vote_response, response.msg_type);
    try std.testing.expectEqual(@as(u64, 3), response.from);
    try std.testing.expectEqual(@as(u64, 2), response.to);
    try std.testing.expect(!response.reject);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(3).?.raft.state);
}

test "etcd/raft: learner ignores election timeout" {
    var net = try newLearnerNetwork();
    defer net.deinit();

    const timeout = net.getPeer(3).?.raft.randomized_election_timeout;
    for (0..timeout * 2) |_| _ = try net.tickPeer(3);
    _ = try net.runUntilIdle(100);

    const learner = net.getPeer(3).?;
    try std.testing.expectEqual(raft.StateRole.follower, learner.raft.state);
    try std.testing.expectEqual(@as(u64, 0), learner.raft.term);
    try std.testing.expectEqual(@as(u64, 0), learner.raft.vote);
}

test "etcd/raft: learner replicates without contributing to commit quorum" {
    var net = try newLearnerNetwork();
    defer net.deinit();
    try net.send(&.{hup(1)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);

    const committed_before = net.getPeer(1).?.raft.raft_log.committed;
    try net.isolate(2);
    var blocked = try proposal(1, "learner-only");
    defer blocked.deinit(allocator);
    try net.send(&.{blocked});

    try std.testing.expect(containsData(net.getPeer(3).?, "learner-only"));
    try std.testing.expectEqual(committed_before, net.getPeer(1).?.raft.raft_log.committed);

    net.recover();
    try net.send(&.{.{ .msg_type = .beat, .from = 1, .to = 1 }});
    try std.testing.expect(net.getPeer(1).?.raft.raft_log.committed > committed_before);
    try std.testing.expect(containsData(net.getPeer(2).?, "learner-only"));
    try std.testing.expect(containsData(net.getPeer(3).?, "learner-only"));
    try std.testing.expectEqual(
        net.getPeer(1).?.raft.raft_log.committed,
        net.getPeer(3).?.raft.raft_log.committed,
    );
    try std.testing.expectEqual(
        net.getPeer(3).?.raft.raft_log.committed,
        net.getPeer(1).?.raft.progress_tracker.getPtr(3).?.matched,
    );
}

test "etcd/raft: learner promotion makes the node promotable" {
    var net = try newLearnerNetwork();
    defer net.deinit();
    const first_timeout = net.getPeer(1).?.raft.randomized_election_timeout;
    for (0..first_timeout) |_| _ = try net.tickPeer(1);
    _ = try net.runUntilIdle(100);
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);

    var changes = [_]raft.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 3 }};
    try net.applyConfChangeOnAll(.{ .changes = &changes });
    _ = try net.runUntilIdle(100);

    for ([_]u64{ 1, 2, 3 }) |id| {
        const peer = net.getPeer(id).?;
        try std.testing.expect(peer.raft.progress_tracker.conf.voters.contains(3));
        try std.testing.expect(!peer.raft.progress_tracker.conf.learners.contains(3));
    }
    try std.testing.expect(net.getPeer(3).?.raft.promotable);

    const promoted_timeout = net.getPeer(3).?.raft.randomized_election_timeout;
    for (0..promoted_timeout) |_| _ = try net.tickPeer(3);
    _ = try net.runUntilIdle(100);
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(3).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(1).?.raft.state);
}

test "etcd/raft: only configured voters are promotable" {
    var net = try network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2, 3, 4 },
        .voters = &.{ 1, 2 },
        .learners = &.{3},
    }, .{});
    defer net.deinit();

    try std.testing.expect(net.getPeer(1).?.raft.promotable);
    try std.testing.expect(net.getPeer(2).?.raft.promotable);
    try std.testing.expect(!net.getPeer(3).?.raft.promotable);
    try std.testing.expect(!net.getPeer(4).?.raft.promotable);

    const learner_timeout = net.getPeer(3).?.raft.randomized_election_timeout;
    for (0..learner_timeout) |_| _ = try net.tickPeer(3);
    const non_member_timeout = net.getPeer(4).?.raft.randomized_election_timeout;
    for (0..non_member_timeout) |_| _ = try net.tickPeer(4);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(3).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(4).?.raft.state);
    try std.testing.expectEqual(@as(usize, 0), net.pendingCount());

    const voter_timeout = net.getPeer(1).?.raft.randomized_election_timeout;
    for (0..voter_timeout) |_| _ = try net.tickPeer(1);
    _ = try net.runUntilIdle(100);
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
}

test "etcd/raft: learner is excluded from Safe ReadIndex quorum" {
    {
        var net = try newLearnerNetwork();
        defer net.deinit();
        try net.send(&.{hup(1)});
        try net.isolate(3);

        var request = try readIndex(1, "voter-quorum");
        defer request.deinit(allocator);
        try net.send(&.{request});

        try std.testing.expectEqual(@as(usize, 1), net.getPeer(1).?.raft.read_states.items.len);
        try std.testing.expectEqual(@as(usize, 0), net.getPeer(1).?.raft.read_only.pendingReadCount());
    }
    {
        var net = try newLearnerNetwork();
        defer net.deinit();
        try net.send(&.{hup(1)});
        try net.isolate(2);

        var request = try readIndex(1, "learner-not-quorum");
        defer request.deinit(allocator);
        try net.send(&.{request});

        try std.testing.expectEqual(@as(usize, 0), net.getPeer(1).?.raft.read_states.items.len);
        try std.testing.expectEqual(@as(usize, 1), net.getPeer(1).?.raft.read_only.pendingReadCount());
    }
    {
        var net = try newLearnerNetwork();
        defer net.deinit();
        try net.send(&.{hup(1)});
        const committed = net.getPeer(1).?.raft.raft_log.committed;

        var request = try readIndex(3, "learner-forwarded");
        defer request.deinit(allocator);
        try net.send(&.{request});

        const learner = net.getPeer(3).?;
        try std.testing.expectEqual(@as(usize, 1), learner.raft.read_states.items.len);
        try std.testing.expectEqual(committed, learner.raft.read_states.items[0].index);
        try std.testing.expectEqualStrings(
            "learner-forwarded",
            learner.raft.read_states.items[0].request_ctx,
        );
    }
}

test "etcd/raft: removing learner removes its progress" {
    var net = try newLearnerNetwork();
    defer net.deinit();
    try net.send(&.{hup(1)});

    var changes = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 3 }};
    try net.applyConfChangeOnAll(.{ .changes = &changes });
    _ = try net.runUntilIdle(100);

    for ([_]u64{ 1, 2, 3 }) |id| {
        const peer = net.getPeer(id).?;
        try std.testing.expect(!peer.raft.progress_tracker.conf.learners.contains(3));
        try std.testing.expect(peer.raft.progress_tracker.getPtr(3) == null);
    }

    try net.stepLocal(1, .{ .msg_type = .beat, .from = 1, .to = 1 });
    for (net.pending.items) |message| try std.testing.expect(message.to != 3);

    var singleton = try network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2 },
        .voters = &.{1},
        .learners = &.{2},
    }, .{});
    defer singleton.deinit();
    var remove_learner = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 2 }};
    try singleton.applyConfChange(1, .{ .changes = &remove_learner });
    var remove_voter = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 1 }};
    try std.testing.expectError(
        error.RemovedAllVoters,
        singleton.applyConfChange(1, .{ .changes = &remove_voter }),
    );
    try std.testing.expectEqual(@as(usize, 1), singleton.getPeer(1).?.raft.progress_tracker.conf.voters.incoming.count());
    try std.testing.expect(singleton.getPeer(1).?.raft.progress_tracker.conf.voters.contains(1));
}
