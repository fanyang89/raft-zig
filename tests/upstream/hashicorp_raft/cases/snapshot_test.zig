//! MPL-2.0 clean-room reimplementation of observable snapshot behavior.
//!
//! The scenarios use only raft-zig's deterministic harness and externally
//! visible snapshot/log outcomes; no upstream source or control flow is used.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/hashicorp_raft/cases/snapshot_test.zig";

fn propose(net: *network.Network, id: u64, data: []const u8) !void {
    var entries = [_]raft.Entry{.{ .data = @constCast(data) }};
    try net.send(&.{.{
        .msg_type = .propose,
        .from = id,
        .to = id,
        .entries = &entries,
    }});
}

fn compactCommitted(peer: *network.Peer) !u64 {
    var snapshot = try peer.storage.getSnapshot(allocator, 0, peer.raft.id);
    defer snapshot.deinit(allocator);
    snapshot.data = try allocator.dupe(u8, "snapshot-state");
    const index = snapshot.metadata.index;
    try peer.storage.applyLocalSnapshot(allocator, snapshot);
    try std.testing.expectEqual(index + 1, peer.storage.core.firstIndex());
    return index;
}

fn containsData(peer: *network.Peer, data: []const u8) bool {
    for (peer.storage.core.entries.items) |entry| {
        if (std.mem.eql(u8, entry.data, data)) return true;
    }
    return false;
}

fn sendSnapshot(net: *network.Network, leader: *network.Peer, follower: *network.Peer) !void {
    try net.send(&.{.{ .msg_type = .beat, .from = leader.raft.id, .to = leader.raft.id }});
    try std.testing.expectEqualStrings("snapshot-state", follower.storage.core.snapshot_data.data);

    try net.stepLocal(leader.raft.id, .{
        .msg_type = .snap_status,
        .from = follower.raft.id,
        .to = leader.raft.id,
    });
    _ = try net.runUntilIdle(1_000);
}

test "HashiCorp Raft: raft_test.go::TestRaft_SendSnapshotFollower" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try net.isolate(3);
    try propose(&net, 1, "snapshot-base-a");
    try propose(&net, 1, "snapshot-base-b");

    const leader = net.getPeer(1).?;
    const follower = net.getPeer(3).?;
    const snapshot_index = try compactCommitted(leader);
    try std.testing.expect(follower.raft.raft_log.lastIndex() < snapshot_index);

    net.recover();
    try sendSnapshot(&net, leader, follower);

    try std.testing.expectEqual(snapshot_index, follower.storage.core.snapshot_data.metadata.index);
    try std.testing.expectEqual(snapshot_index + 1, follower.storage.core.firstIndex());
    try std.testing.expectEqual(snapshot_index, follower.raft.raft_log.committed);
    try net.checkSafety();
}

test "HashiCorp Raft: raft_test.go::TestRaft_SendSnapshotAndLogsFollower" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try net.isolate(3);
    try propose(&net, 1, "snapshot-boundary");

    const leader = net.getPeer(1).?;
    const follower = net.getPeer(3).?;
    const snapshot_index = try compactCommitted(leader);
    try std.testing.expect(follower.raft.raft_log.lastIndex() < snapshot_index);

    try net.isolate(2);
    try propose(&net, 1, "post-snapshot-suffix");
    try std.testing.expectEqual(snapshot_index, leader.raft.raft_log.committed);
    try std.testing.expect(leader.raft.raft_log.lastIndex() > snapshot_index);

    net.recover();
    try net.cut(1, 2);
    try sendSnapshot(&net, leader, follower);

    try std.testing.expectEqual(snapshot_index, follower.storage.core.snapshot_data.metadata.index);
    try std.testing.expect(containsData(follower, "post-snapshot-suffix"));
    try std.testing.expect(follower.raft.raft_log.committed > snapshot_index);

    net.recover();
    const digest = try net.converge(64, 1_000);
    try std.testing.expectEqual(leader.raft.raft_log.lastIndex(), digest.last_index);
    for ([_]u64{ 1, 2, 3 }) |id| {
        try std.testing.expect(containsData(net.getPeer(id).?, "post-snapshot-suffix"));
    }
    try net.checkSafety();
}
