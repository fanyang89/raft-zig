// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

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

test "raft-rs: uninitialized learner restores snapshot and continues replication" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    var config = raft.defaultConfig();
    config.id = 3;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.election_timeout_seed = 3;
    var node = try raft.Raft.init(allocator, config, storage.asStorage());
    defer node.deinit();

    try std.testing.expect(!node.promotable);
    var snapshot_message = raft.Message{
        .msg_type = .snapshot,
        .from = 1,
        .to = 3,
        .term = 11,
        .snapshot = .{ .metadata = .{
            .index = 11,
            .term = 11,
            .conf_state = .{
                .voters = try allocator.dupe(u64, &.{ 1, 2 }),
                .learners = try allocator.dupe(u64, &.{3}),
            },
        } },
    };
    defer snapshot_message.deinit(allocator);
    try node.step(&snapshot_message);

    var restored = try node.progress_tracker.conf.toConfState(allocator);
    defer restored.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, restored.voters);
    try std.testing.expectEqualSlices(u64, &.{3}, restored.learners);
    try std.testing.expect(!node.promotable);

    var entries = try allocator.alloc(raft.Entry, 1);
    entries[0] = .{ .index = 12, .term = 11, .data = try allocator.dupe(u8, "after-snapshot") };
    var append_message = raft.Message{
        .msg_type = .append,
        .from = 1,
        .to = 3,
        .term = 11,
        .log_term = 11,
        .index = 11,
        .commit = 12,
        .entries = entries,
    };
    defer append_message.deinit(allocator);
    try node.step(&append_message);

    try std.testing.expectEqual(@as(u64, 12), node.raft_log.lastIndex());
    try std.testing.expectEqual(@as(u64, 12), node.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 11), try node.raft_log.term(12));
}
