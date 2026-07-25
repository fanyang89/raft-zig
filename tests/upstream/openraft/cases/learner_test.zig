const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/openraft/cases/learner_test.zig";

fn proposal(data: []const u8) !raft.Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, data) };
    return .{ .msg_type = .propose, .from = 1, .to = 1, .entries = entries };
}

fn containsData(peer: *network.Peer, data: []const u8) bool {
    for (peer.storage.core.entries.items) |entry| {
        if (std.mem.eql(u8, entry.data, data)) return true;
    }
    return false;
}

test "OpenRaft: isolated learner does not enlarge a single-voter quorum" {
    var net = try network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2 },
        .voters = &.{1},
        .learners = &.{2},
    }, .{});
    defer net.deinit();
    try net.isolate(2);
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});

    const committed_before = net.getPeer(1).?.raft.raft_log.committed;
    var request = try proposal("committed-without-learner");
    defer request.deinit(allocator);
    try net.send(&.{request});

    try std.testing.expect(net.getPeer(1).?.raft.raft_log.committed > committed_before);
    try std.testing.expect(!containsData(net.getPeer(2).?, "committed-without-learner"));

    net.recover();
    try net.send(&.{.{ .msg_type = .beat, .from = 1, .to = 1 }});
    try std.testing.expect(containsData(net.getPeer(2).?, "committed-without-learner"));
    _ = try net.converge(20, 1_000);
}
