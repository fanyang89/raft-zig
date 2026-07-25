// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/election_test.zig";

test "raft-rs: newer local last-log term rejects a longer stale candidate" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    const peer = net.getPeer(1).?;
    const entries = [_]raft.Entry{
        .{ .term = 2, .index = 1 },
        .{ .term = 1, .index = 2 },
    };
    try peer.storage.setEntries(allocator, &entries);
    try peer.storage.setHardState(.{ .term = 3 });
    peer.raft.term = 3;
    peer.raft.raft_log.persisted = 2;
    peer.raft.raft_log.unstable.offset = 3;

    try net.stepLocal(1, .{
        .msg_type = .request_vote,
        .from = 2,
        .to = 1,
        .term = 3,
        .log_term = 1,
        .index = 1,
    });
    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    try std.testing.expectEqual(raft.MessageType.request_vote_response, net.pending.items[0].msg_type);
    try std.testing.expect(net.pending.items[0].reject);
    try std.testing.expectEqual(@as(u64, 0), peer.storage.core.raft_state.hard_state.vote);
}

test "raft-rs: dueling pre-candidate does not disrupt the leader" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .pre_vote = true });
    defer net.deinit();
    try net.cut(1, 3);

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try net.send(&.{.{ .msg_type = .hup, .from = 3, .to = 3 }});

    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(3).?.raft.state);

    net.recover();
    try net.send(&.{.{ .msg_type = .hup, .from = 3, .to = 3 }});

    const expected = [_]struct {
        id: u64,
        state: raft.StateRole,
        term: u64,
        committed: u64,
        applied: u64,
        last_index: u64,
    }{
        .{ .id = 1, .state = .leader, .term = 1, .committed = 1, .applied = 0, .last_index = 1 },
        .{ .id = 2, .state = .follower, .term = 1, .committed = 1, .applied = 0, .last_index = 1 },
        .{ .id = 3, .state = .follower, .term = 1, .committed = 0, .applied = 0, .last_index = 0 },
    };
    for (expected) |want| {
        const peer = net.getPeer(want.id).?;
        try std.testing.expectEqual(want.state, peer.raft.state);
        try std.testing.expectEqual(want.term, peer.raft.term);
        try std.testing.expectEqual(want.committed, peer.raft.raft_log.committed);
        try std.testing.expectEqual(want.applied, peer.raft.raft_log.applied);
        try std.testing.expectEqual(want.last_index, peer.raft.raft_log.lastIndex());
    }
}
