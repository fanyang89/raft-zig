// Copyright 2015 The etcd Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

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

test "etcd/raft: new leader postpones ReadIndex until committing its term" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    const leader = net.getPeer(1).?;
    leader.raft.becomeCandidate();
    try leader.raft.becomeLeader();
    try std.testing.expectEqual(@as(u64, 0), leader.raft.raft_log.committed);

    // The leader has not committed any entry in its own term, so a ReadIndex
    // request must be postponed (queued), not dropped and not served.
    var request = try readIndex(1, "too-early");
    defer request.deinit(allocator);
    try net.stepLocal(1, request);

    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_states.items.len);
    try std.testing.expectEqual(@as(usize, 0), leader.raft.read_only.pendingReadCount());
    try std.testing.expectEqual(@as(usize, 1), leader.raft.pendingReadIndexCount());

    // Once the leader commits an entry in its own term, the postponed request
    // is released and served (mirrors etcd TestReadOnlyForNewLeader). The
    // served read index is the commit index at the moment the leader first
    // becomes current-term-committed (the no-op entry becomeLeader appends).
    var prop = try propose(1, "commit-me");
    defer prop.deinit(allocator);
    try net.send(&.{prop});

    try std.testing.expect(leader.raft.raft_log.committed > 0);
    try std.testing.expectEqual(@as(usize, 1), leader.raft.read_states.items.len);
    try std.testing.expect(leader.raft.read_states.items[0].index >= 1);
    try std.testing.expectEqualStrings("too-early", leader.raft.read_states.items[0].request_ctx);
    try std.testing.expectEqual(@as(usize, 0), leader.raft.pendingReadIndexCount());

    // A subsequent ReadIndex on the now-current leader is served immediately.
    var request2 = try readIndex(1, "after");
    defer request2.deinit(allocator);
    try net.send(&.{request2});
    try std.testing.expectEqual(@as(usize, 2), leader.raft.read_states.items.len);
    try std.testing.expectEqualStrings("after", leader.raft.read_states.items[1].request_ctx);
}

fn propose(id: u64, data: []const u8) !Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, data) };
    return .{ .msg_type = .propose, .from = id, .to = id, .entries = entries };
}
