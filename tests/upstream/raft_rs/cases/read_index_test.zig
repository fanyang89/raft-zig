// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/read_index_test.zig";

fn readIndex(context: []const u8) !raft.Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, context) };
    return .{ .msg_type = .read_index, .to = 1, .entries = entries };
}

test "raft-rs: Safe ReadIndex completes when configuration reduces quorum" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});

    const leader = net.getPeer(1).?;
    const read_index = leader.raft.raft_log.committed;
    try std.testing.expectEqual(@as(u64, 1), read_index);

    const context = "abcdefg";
    var request = try readIndex(context);
    defer request.deinit(allocator);
    try net.stepLocal(1, request);

    try std.testing.expectEqual(@as(usize, 1), leader.raft.read_only.queue.items.len);
    try std.testing.expectEqualStrings(context, leader.raft.read_only.queue.items[0]);
    const pending = leader.raft.read_only.pending.get(context).?;
    try std.testing.expectEqual(read_index, pending.index);
    try std.testing.expectEqualStrings(context, pending.req.entries[0].data);
    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_states.items.len);

    try std.testing.expectEqual(network.Delivery.delivered, (try net.deliverOne()).?);
    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    try std.testing.expectEqual(raft.MessageType.heartbeat_response, net.pending.items[0].msg_type);
    try std.testing.expectEqualStrings(context, net.pending.items[0].context);
    try net.dropPending(0);

    var remove_two = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 2 }};
    try net.applyConfChange(1, .{ .changes = &remove_two });

    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_only.queue.items.len);
    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_only.pending.count());
    try std.testing.expectEqual(@as(usize, 1), leader.raft.read_states.items.len);
    try std.testing.expectEqual(read_index, leader.raft.read_states.items[0].index);
    try std.testing.expectEqualStrings(context, leader.raft.read_states.items[0].request_ctx);
}
