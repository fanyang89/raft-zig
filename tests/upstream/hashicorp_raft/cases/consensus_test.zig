//! MPL-2.0 clean-room reimplementation of observable consensus behavior.
//!
//! These tests are independently expressed through raft-zig's public test
//! harness and assert outcomes only; no upstream source or control flow is used.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

pub const inventory_target = "tests/upstream/hashicorp_raft/cases/consensus_test.zig";

fn propose(net: *network.Network, id: u64, data: []const u8) !void {
    var entries = [_]raft.Entry{.{ .data = @constCast(data) }};
    try net.send(&.{.{
        .msg_type = .propose,
        .from = id,
        .to = id,
        .entries = &entries,
    }});
}

fn containsData(peer: *network.Peer, data: []const u8) bool {
    for (peer.storage.core.entries.items) |entry| {
        if (std.mem.eql(u8, entry.data, data)) return true;
    }
    return false;
}

test "HashiCorp Raft: raft_test.go::TestRaft_LeaderFail" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);

    try net.isolate(1);
    try propose(&net, 1, "abandoned-write");
    try std.testing.expect(containsData(net.getPeer(1).?, "abandoned-write"));
    try std.testing.expectEqual(@as(u64, 1), net.getPeer(1).?.raft.raft_log.committed);

    try net.send(&.{.{ .msg_type = .hup, .from = 2, .to = 2 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(2).?.raft.state);
    try propose(&net, 2, "surviving-write");
    const committed_by_new_leader = net.getPeer(2).?.raft.raft_log.committed;
    try std.testing.expect(committed_by_new_leader >= 3);

    net.recover();
    const digest = try net.converge(64, 1_000);
    try std.testing.expectEqual(committed_by_new_leader, digest.committed);
    for ([_]u64{ 1, 2, 3 }) |id| {
        const peer = net.getPeer(id).?;
        try std.testing.expect(!containsData(peer, "abandoned-write"));
        try std.testing.expect(containsData(peer, "surviving-write"));
    }
    try net.checkSafety();
}
