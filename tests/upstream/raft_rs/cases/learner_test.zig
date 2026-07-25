// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

pub const inventory_target = "tests/upstream/raft_rs/cases/learner_test.zig";

fn transfer(from: u64, to: u64) raft.Message {
    return .{ .msg_type = .transfer_leader, .from = from, .to = to };
}

test "raft-rs: leadership transfer ignores learners and unknown nodes" {
    var net = try network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2, 3 },
        .voters = &.{ 1, 2 },
        .learners = &.{3},
    }, .{});
    defer net.deinit();
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});

    try net.send(&.{transfer(3, 1)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(?u64, null), net.getPeer(1).?.raft.lead_transferee);

    try net.send(&.{transfer(4, 1)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(?u64, null), net.getPeer(1).?.raft.lead_transferee);

    try net.send(&.{transfer(2, 1)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(2).?.raft.state);
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(1).?.raft.leader_id);
}

test "raft-rs: candidate requests a learner vote only after local promotion" {
    var net = try network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2, 3 },
        .voters = &.{ 1, 2 },
        .learners = &.{3},
    }, .{});
    defer net.deinit();
    try net.isolate(2);

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.candidate, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(3).?.raft.state);

    var promote = [_]raft.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 3 }};
    try net.applyConfChange(1, .{ .changes = &promote });
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});

    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expect(net.getPeer(3).?.raft.progress_tracker.conf.learners.contains(3));
}
