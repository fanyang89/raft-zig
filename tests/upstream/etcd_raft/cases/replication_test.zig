// Copyright 2015 The etcd Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/etcd_raft/cases/replication_test.zig";

fn hup(id: u64) raft.Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

fn proposal(id: u64, data: []const u8) !raft.Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, data) };
    return .{ .msg_type = .propose, .from = id, .to = id, .entries = entries };
}

fn expectPayloads(peer: *network.Peer, expected: []const []const u8) !void {
    var actual: usize = 0;
    for (peer.storage.core.entries.items) |entry| {
        if (entry.index > peer.raft.raft_log.committed) continue;
        if (entry.data.len == 0) continue;
        try std.testing.expect(actual < expected.len);
        try std.testing.expectEqualStrings(expected[actual], entry.data);
        actual += 1;
    }
    try std.testing.expectEqual(expected.len, actual);
}

test "etcd/raft: proposals replicate across consecutive leaders" {
    {
        var net = try network.newNetwork(&.{ 1, 2, 3 });
        defer net.deinit();
        try net.send(&.{hup(1)});
        var first = try proposal(1, "first");
        defer first.deinit(allocator);
        try net.send(&.{first});

        for ([_]u64{ 1, 2, 3 }) |id| {
            const peer = net.getPeer(id).?;
            try std.testing.expectEqual(@as(u64, 2), peer.raft.raft_log.committed);
            try expectPayloads(peer, &.{"first"});
        }
    }
    {
        var net = try network.newNetwork(&.{ 1, 2, 3 });
        defer net.deinit();
        try net.send(&.{hup(1)});
        var first = try proposal(1, "first");
        defer first.deinit(allocator);
        try net.send(&.{first});

        try net.send(&.{hup(2)});
        try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(2).?.raft.state);
        var second = try proposal(2, "second");
        defer second.deinit(allocator);
        try net.send(&.{second});

        for ([_]u64{ 1, 2, 3 }) |id| {
            const peer = net.getPeer(id).?;
            try std.testing.expectEqual(@as(u64, 4), peer.raft.raft_log.committed);
            try expectPayloads(peer, &.{ "first", "second" });
        }
    }
}
