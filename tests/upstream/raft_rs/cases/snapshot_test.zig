// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/snapshot_test.zig";

fn addMinTermPeer(
    net: *network.Network,
    id: u64,
    pre_vote: bool,
    snapshot: ?raft.Snapshot,
) !void {
    const peer = try allocator.create(network.Peer);
    errdefer allocator.destroy(peer);
    peer.storage = raft.MemoryStorage.init();
    errdefer peer.storage.deinit(allocator);
    if (snapshot) |snap| try peer.storage.applySnapshot(allocator, snap);

    var config = raft.defaultConfig();
    config.id = id;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.pre_vote = pre_vote;
    config.load_state_on_startup = snapshot != null;
    config.election_timeout_seed = id;
    peer.raft = try raft.Raft.init(allocator, config, peer.storage.asStorage());
    errdefer peer.raft.deinit();
    try net.peers.put(id, peer);
}

fn newMinTermNetwork(pre_vote: bool) !network.Network {
    var net = network.Network.init();
    errdefer net.deinit();

    var snapshot = raft.Snapshot{ .metadata = .{
        .index = 1,
        .term = 1,
        .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2 }) },
    } };
    defer snapshot.deinit(allocator);
    try addMinTermPeer(&net, 1, pre_vote, snapshot);
    try addMinTermPeer(&net, 2, pre_vote, null);
    try net.checkSafety();
    return net;
}

fn restoreAndPersistSnapshot(peer: *network.Peer) !void {
    var snapshot = raft.Snapshot{ .metadata = .{
        .index = 11,
        .term = 11,
        .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2 }) },
    } };
    defer snapshot.deinit(allocator);

    try std.testing.expect(peer.raft.restoreSnapshot(snapshot));
    const pending = peer.raft.raft_log.unstable.snapshot.?;
    const snapshot_index = pending.metadata.index;
    try peer.storage.applySnapshot(allocator, pending);
    peer.raft.raft_log.stableSnapshot(snapshot_index);
    peer.raft.onPersistSnapshot(snapshot_index);
}

fn newSnapshotLeader() !network.Network {
    var net = try network.newNetwork(&.{ 1, 2 });
    errdefer net.deinit();
    const leader = net.getPeer(1).?;
    try restoreAndPersistSnapshot(leader);
    leader.raft.becomeCandidate();
    try leader.raft.becomeLeader();
    return net;
}

test "raft-rs: sending snapshot sets pending snapshot" {
    var net = try newSnapshotLeader();
    defer net.deinit();
    const leader = net.getPeer(1).?;
    const follower_progress = leader.raft.progress_tracker.getPtr(2).?;
    follower_progress.next_idx = leader.raft.raft_log.firstIndex();

    try net.stepLocal(1, .{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .index = follower_progress.next_idx - 1,
        .reject = true,
    });

    try std.testing.expectEqual(@as(u64, 11), follower_progress.pending_snapshot);
    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    try std.testing.expectEqual(raft.MessageType.snapshot, net.pending.items[0].msg_type);
    try std.testing.expectEqual(@as(u64, 11), net.pending.items[0].snapshot.?.metadata.index);
}

test "raft-rs: append response aborts pending snapshot" {
    var net = try newSnapshotLeader();
    defer net.deinit();
    const follower_progress = net.getPeer(1).?.raft.progress_tracker.getPtr(2).?;
    follower_progress.next_idx = 1;
    follower_progress.becomeSnapshot(11);

    try net.stepLocal(1, .{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .index = 11,
    });

    try std.testing.expectEqual(@as(u64, 0), follower_progress.pending_snapshot);
    try std.testing.expectEqual(@as(u64, 12), follower_progress.next_idx);
}

test "raft-rs: snapshot with minimum term" {
    for ([_]bool{ true, false }) |pre_vote| {
        var net = try newMinTermNetwork(pre_vote);
        defer net.deinit();

        try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});

        const leader = net.getPeer(1).?;
        const follower = net.getPeer(2).?;
        try std.testing.expectEqual(raft.StateRole.leader, leader.raft.state);
        try std.testing.expectEqual(@as(u64, 2), leader.raft.term);
        try std.testing.expectEqual(@as(u64, 1), follower.storage.core.snapshot_data.metadata.index);
        try std.testing.expectEqual(@as(u64, 1), follower.storage.core.snapshot_data.metadata.term);
        try std.testing.expectEqual(@as(u64, 2), follower.raft.raft_log.firstIndex());
        try std.testing.expectEqual(@as(u64, 2), follower.raft.raft_log.lastIndex());
        try std.testing.expectEqual(@as(u64, 1), try follower.raft.raft_log.term(1));
        try std.testing.expectEqual(@as(u64, 2), try follower.raft.raft_log.term(2));
        try std.testing.expectEqual(@as(u64, 2), follower.raft.raft_log.committed);
    }
}
