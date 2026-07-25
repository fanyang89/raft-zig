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
