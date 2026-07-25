const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/openraft/cases/replication_test.zig";

fn seedPeer(peer: *network.Peer, terms: []const u64, term: u64) !void {
    const entries = try allocator.alloc(raft.Entry, terms.len);
    defer allocator.free(entries);
    for (terms, 0..) |entry_term, index| {
        entries[index] = .{ .term = entry_term, .index = index + 1 };
    }
    try peer.storage.setEntries(allocator, entries);
    try peer.storage.setHardState(.{ .term = term });
    peer.raft.term = term;
    peer.raft.raft_log.persisted = terms.len;
    peer.raft.raft_log.unstable.offset = terms.len + 1;
}

test "OpenRaft: append rejects a missing previous log index without mutation" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    const follower = net.getPeer(2).?;
    try seedPeer(follower, &.{ 1, 1, 2 }, 1);

    var configuration_before = try follower.raft.progress_tracker.conf.toConfState(allocator);
    defer configuration_before.deinit(allocator);

    const entries = try allocator.alloc(raft.Entry, 2);
    entries[0] = .{ .term = 2, .index = 5 };
    entries[1] = .{ .term = 2, .index = 6 };
    var request = raft.Message{
        .msg_type = .append,
        .from = 1,
        .to = 2,
        .term = 2,
        .log_term = 2,
        .index = 4,
        .entries = entries,
    };
    defer request.deinit(allocator);
    try net.stepLocal(2, request);

    try std.testing.expectEqual(@as(u64, 2), follower.raft.term);
    try std.testing.expectEqual(raft.StateRole.follower, follower.raft.state);
    try std.testing.expectEqual(@as(u64, 1), follower.raft.leader_id);
    try std.testing.expectEqual(@as(u64, 2), follower.storage.core.raft_state.hard_state.term);

    try std.testing.expectEqual(@as(u64, 3), follower.raft.raft_log.lastIndex());
    try std.testing.expectEqual(@as(usize, 3), follower.storage.core.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), follower.raft.raft_log.unstable.entries.items.len);
    for (follower.storage.core.entries.items, 0..) |entry, index| {
        try std.testing.expectEqual(@as(u64, index + 1), entry.index);
        try std.testing.expectEqual(([_]u64{ 1, 1, 2 })[index], entry.term);
    }

    var configuration_after = try follower.raft.progress_tracker.conf.toConfState(allocator);
    defer configuration_after.deinit(allocator);
    try std.testing.expect(configuration_before.eql(configuration_after));

    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    const response = net.pending.items[0];
    try std.testing.expectEqual(raft.MessageType.append_response, response.msg_type);
    try std.testing.expectEqual(@as(u64, 2), response.from);
    try std.testing.expectEqual(@as(u64, 1), response.to);
    try std.testing.expectEqual(@as(u64, 2), response.term);
    try std.testing.expect(response.reject);
    try std.testing.expectEqual(@as(u64, 4), response.index);
    try std.testing.expectEqual(@as(u64, 3), response.reject_hint);
    try std.testing.expectEqual(@as(u64, 2), response.log_term);
    try std.testing.expectEqual(@as(u64, 0), response.commit);
    try std.testing.expectEqual(@as(usize, 0), response.entries.len);
}
