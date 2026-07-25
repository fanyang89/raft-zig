// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

pub const inventory_target = "tests/upstream/raft_rs/cases/configuration_test.zig";

test "raft-rs: failed last-voter removal preserves configuration" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    var remove_two = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 2 }};
    try net.applyConfChange(1, .{ .changes = &remove_two });

    const peer = net.getPeer(1).?;
    try std.testing.expect(peer.raft.progress_tracker.conf.voters.contains(1));
    try std.testing.expect(!peer.raft.progress_tracker.conf.voters.contains(2));
    try std.testing.expect(peer.raft.progress_tracker.getPtr(2) == null);

    var remove_one = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 1 }};
    try std.testing.expectError(
        error.RemovedAllVoters,
        net.applyConfChange(1, .{ .changes = &remove_one }),
    );
    try std.testing.expect(peer.raft.progress_tracker.conf.voters.contains(1));
    try std.testing.expectEqual(@as(usize, 1), peer.raft.progress_tracker.conf.voters.incoming.count());
}
