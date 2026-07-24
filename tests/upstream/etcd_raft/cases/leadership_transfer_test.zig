// Copyright 2015 The etcd Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;
const Message = raft.Message;
const StateRole = raft.StateRole;

pub const inventory_target = "tests/upstream/etcd_raft/cases/leadership_transfer_test.zig";

fn hup(id: u64) Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

fn transfer(from: u64, to: u64) Message {
    return .{ .msg_type = .transfer_leader, .from = from, .to = to };
}

fn proposal(id: u64, data: []const u8) !Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, data) };
    return .{ .msg_type = .propose, .from = id, .to = id, .entries = entries };
}

fn elect(net: *network.Network, id: u64) !void {
    try net.send(&.{hup(id)});
    try std.testing.expectEqual(StateRole.leader, net.getPeer(id).?.raft.state);
}

test "etcd/raft: transfer leadership to an up-to-date voter" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try elect(&net, 1);

    try net.send(&.{transfer(2, 1)});

    try std.testing.expectEqual(StateRole.follower, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(StateRole.leader, net.getPeer(2).?.raft.state);
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(1).?.raft.leader_id);
    try std.testing.expectEqual(@as(?u64, null), net.getPeer(1).?.raft.lead_transferee);
}

test "etcd/raft: follower forwards leadership transfer" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try elect(&net, 1);

    try net.send(&.{transfer(2, 2)});

    try std.testing.expectEqual(StateRole.leader, net.getPeer(2).?.raft.state);
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(1).?.raft.leader_id);
}

test "etcd/raft: slow transferee catches up before election" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try elect(&net, 1);
    try net.isolate(3);

    var prop = try proposal(1, "committed-before-transfer");
    defer prop.deinit(allocator);
    try net.send(&.{prop});
    try std.testing.expect(net.getPeer(3).?.raft.raft_log.lastIndex() < net.getPeer(1).?.raft.raft_log.lastIndex());

    net.recover();
    try net.send(&.{transfer(3, 1)});

    try std.testing.expectEqual(StateRole.leader, net.getPeer(3).?.raft.state);
    try std.testing.expectEqual(net.getPeer(1).?.raft.raft_log.lastIndex(), net.getPeer(3).?.raft.raft_log.lastIndex());
}

test "etcd/raft: invalid leadership transfer targets are no-ops" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try elect(&net, 1);

    try net.send(&.{transfer(1, 1)});
    try net.send(&.{transfer(4, 1)});

    const leader = net.getPeer(1).?;
    try std.testing.expectEqual(StateRole.leader, leader.raft.state);
    try std.testing.expectEqual(@as(?u64, null), leader.raft.lead_transferee);
}

test "etcd/raft: stalled leadership transfer rejects proposals then times out" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try elect(&net, 1);
    try net.isolate(3);

    try net.stepLocal(1, transfer(3, 1));
    _ = try net.runUntilIdle(100);
    const leader = net.getPeer(1).?;
    try std.testing.expectEqual(@as(?u64, 3), leader.raft.lead_transferee);

    const last_index = leader.raft.raft_log.lastIndex();
    var prop = try proposal(1, "must-be-rejected");
    defer prop.deinit(allocator);
    try net.stepLocal(1, prop);
    try std.testing.expectEqual(last_index, leader.raft.raft_log.lastIndex());

    const timeout = leader.raft.randomized_election_timeout;
    for (0..timeout) |_| _ = try net.tickPeer(1);
    _ = try net.runUntilIdle(1_000);

    try std.testing.expectEqual(StateRole.leader, leader.raft.state);
    try std.testing.expectEqual(@as(?u64, null), leader.raft.lead_transferee);
}
