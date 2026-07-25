// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/election_test.zig";

fn seedPeer(peer: *network.Peer, terms: []const u64, hard_state: raft.HardState) !void {
    const entries = try allocator.alloc(raft.Entry, terms.len);
    defer allocator.free(entries);
    for (terms, 0..) |term, index| {
        entries[index] = .{ .term = term, .index = index + 1 };
    }
    try peer.storage.setEntries(allocator, entries);
    try peer.storage.setHardState(hard_state);
    peer.raft.term = hard_state.term;
    peer.raft.vote = hard_state.vote;
    peer.raft.raft_log.persisted = terms.len;
    peer.raft.raft_log.unstable.offset = terms.len + 1;
}

fn expectLogTerms(peer: *network.Peer, expected: []const u64) !void {
    const entries = try peer.raft.raft_log.allEntries();
    defer {
        for (entries) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    try std.testing.expectEqual(expected.len, entries.len);
    for (entries, expected) |entry, term| try std.testing.expectEqual(term, entry.term);
}

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

test "raft-rs: elected leader overwrites uncommitted higher-term logs" {
    var net = try network.newNetwork(&.{ 1, 2, 3, 4, 5 });
    defer net.deinit();

    try seedPeer(net.getPeer(1).?, &.{1}, .{ .term = 1 });
    try seedPeer(net.getPeer(2).?, &.{1}, .{ .term = 1 });
    try seedPeer(net.getPeer(3).?, &.{2}, .{ .term = 2 });
    try seedPeer(net.getPeer(4).?, &.{}, .{ .term = 2, .vote = 3 });
    try seedPeer(net.getPeer(5).?, &.{}, .{ .term = 2, .vote = 3 });

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(1).?.raft.term);

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(u64, 3), net.getPeer(1).?.raft.term);

    for (1..6) |id| try expectLogTerms(net.getPeer(id).?, &.{ 1, 3 });
}
