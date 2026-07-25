// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/snapshot_test.zig";

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
