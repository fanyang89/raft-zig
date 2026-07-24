const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;
const Message = raft.Message;

pub const inventory_target = "tests/upstream/etcd_raft/cases/read_index_test.zig";

fn hup(id: u64) Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

fn readIndex(id: u64, context: []const u8) !Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, context) };
    return .{ .msg_type = .read_index, .from = id, .to = id, .entries = entries };
}

fn elect(net: *network.Network, id: u64) !void {
    try net.send(&.{hup(id)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(id).?.raft.state);
}

fn expectReadState(peer: *network.Peer, index: u64, context: []const u8) !void {
    try std.testing.expectEqual(@as(usize, 1), peer.raft.read_states.items.len);
    try std.testing.expectEqual(index, peer.raft.read_states.items[0].index);
    try std.testing.expectEqualStrings(context, peer.raft.read_states.items[0].request_ctx);
}

test "etcd/raft: Safe ReadIndex completes with one follower isolated" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try elect(&net, 1);
    try net.isolate(3);

    const committed = net.getPeer(1).?.raft.raft_log.committed;
    var request = try readIndex(1, "leader-read");
    defer request.deinit(allocator);
    try net.send(&.{request});

    const leader = net.getPeer(1).?;
    try expectReadState(leader, committed, "leader-read");
    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_only.pendingReadCount());
}

test "etcd/raft: follower forwards Safe ReadIndex to leader" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try elect(&net, 1);

    const committed = net.getPeer(1).?.raft.raft_log.committed;
    var request = try readIndex(2, "follower-read");
    defer request.deinit(allocator);
    try net.send(&.{request});

    try expectReadState(net.getPeer(2).?, committed, "follower-read");
    try std.testing.expectEqual(@as(usize, 0), net.getPeer(1).?.raft.read_only.pendingReadCount());
}

test "etcd/raft: Safe ReadIndex waits without quorum" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try elect(&net, 1);
    try net.isolate(1);

    var request = try readIndex(1, "isolated-read");
    defer request.deinit(allocator);
    try net.stepLocal(1, request);
    _ = try net.runUntilIdle(100);

    const leader = net.getPeer(1).?;
    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_states.items.len);
    try std.testing.expectEqual(@as(usize, 1), leader.raft.read_only.pendingReadCount());
}

test "etcd/raft: new leader rejects ReadIndex before committing its term" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    const leader = net.getPeer(1).?;
    leader.raft.becomeCandidate();
    try leader.raft.becomeLeader();
    try std.testing.expectEqual(@as(u64, 0), leader.raft.raft_log.committed);

    var request = try readIndex(1, "too-early");
    defer request.deinit(allocator);
    try net.stepLocal(1, request);

    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_states.items.len);
    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_only.pendingReadCount());
}
