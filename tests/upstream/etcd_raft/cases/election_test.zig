// Copyright 2015 The etcd Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/etcd_raft/cases/election_test.zig";

const TestNode = struct {
    storage: raft.MemoryStorage,
    raft: raft.Raft,

    fn deinit(self: *TestNode) void {
        self.raft.deinit();
        self.storage.deinit(allocator);
        allocator.destroy(self);
    }
};

fn createNode(id: u64, voters: []const u64, terms: []const u64, hard_state: raft.HardState) !*TestNode {
    const node = try allocator.create(TestNode);
    errdefer allocator.destroy(node);
    node.storage = raft.MemoryStorage.init();
    errdefer node.storage.deinit(allocator);

    if (terms.len != 0) {
        const entries = try allocator.alloc(raft.Entry, terms.len);
        defer allocator.free(entries);
        for (terms, 0..) |term, index| {
            entries[index] = .{ .term = term, .index = index + 1 };
        }
        try node.storage.append(allocator, entries);
    }

    const owned_voters = try allocator.dupe(u64, voters);
    var conf_state = raft.ConfState{ .voters = owned_voters };
    defer conf_state.deinit(allocator);
    try node.storage.setRaftState(allocator, .{
        .hard_state = hard_state,
        .conf_state = conf_state,
    });

    var config = raft.defaultConfig();
    config.id = id;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.election_timeout_seed = id * 97;
    config.load_state_on_startup = !hard_state.isEmpty();
    node.raft = try raft.Raft.init(allocator, config, node.storage.asStorage());
    return node;
}

fn clearMessages(node: *raft.Raft) void {
    for (node.messages.items) |*message| message.deinit(allocator);
    node.messages.clearRetainingCapacity();
}

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

test "etcd/raft: leader election respects quorum availability and log freshness" {
    const Case = struct {
        count: u64,
        isolated: []const u64,
        expected: raft.StateRole,
    };
    const cases = [_]Case{
        .{ .count = 3, .isolated = &.{}, .expected = .leader },
        .{ .count = 3, .isolated = &.{3}, .expected = .leader },
        .{ .count = 3, .isolated = &.{ 2, 3 }, .expected = .candidate },
        .{ .count = 4, .isolated = &.{ 2, 3 }, .expected = .candidate },
        .{ .count = 5, .isolated = &.{ 2, 3 }, .expected = .leader },
    };
    for (cases) |case| {
        var ids: [5]u64 = undefined;
        for (0..case.count) |index| ids[index] = index + 1;
        var net = try network.newNetwork(ids[0..case.count]);
        defer net.deinit();
        for (case.isolated) |id| try net.isolate(id);
        try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
        try std.testing.expectEqual(case.expected, net.getPeer(1).?.raft.state);
        try std.testing.expectEqual(@as(u64, 1), net.getPeer(1).?.raft.term);
    }

    var net = try network.newNetwork(&.{ 1, 2, 3, 4, 5 });
    defer net.deinit();
    try seedPeer(net.getPeer(2).?, &.{1}, 1);
    try seedPeer(net.getPeer(3).?, &.{1}, 1);
    try seedPeer(net.getPeer(4).?, &.{ 1, 1 }, 1);
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(u64, 1), net.getPeer(1).?.raft.term);
}

test "etcd/raft: real vote from any role becomes a granted follower vote" {
    const roles = [_]raft.StateRole{ .follower, .pre_candidate, .candidate, .leader };
    for (roles) |role| {
        var net = try network.newNetwork(&.{ 1, 2, 3 });
        defer net.deinit();
        const peer = net.getPeer(1).?;
        peer.raft.becomeFollower(1, 3);
        switch (role) {
            .follower => {},
            .pre_candidate => peer.raft.becomePreCandidate(),
            .candidate => peer.raft.becomeCandidate(),
            .leader => {
                peer.raft.becomeCandidate();
                try peer.raft.becomeLeader();
            },
        }
        const new_term = peer.raft.term + 1;
        try net.stepLocal(1, .{
            .msg_type = .request_vote,
            .from = 2,
            .to = 1,
            .term = new_term,
            .log_term = new_term,
            .index = 42,
        });

        try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
        const response = net.pending.items[0];
        try std.testing.expectEqual(raft.MessageType.request_vote_response, response.msg_type);
        try std.testing.expect(!response.reject);
        try std.testing.expectEqual(raft.StateRole.follower, peer.raft.state);
        try std.testing.expectEqual(new_term, peer.raft.term);
        try std.testing.expectEqual(@as(u64, 2), peer.raft.vote);
    }
}

test "etcd/raft: one-round vote response matrix" {
    const Response = struct { from: u64, reject: bool };
    const Case = struct {
        voters: []const u64,
        responses: []const Response,
        expected: raft.StateRole,
    };
    const cases = [_]Case{
        .{ .voters = &.{1}, .responses = &.{}, .expected = .leader },
        .{ .voters = &.{ 1, 2, 3 }, .responses = &.{ .{ .from = 2, .reject = false }, .{ .from = 3, .reject = false } }, .expected = .leader },
        .{ .voters = &.{ 1, 2, 3 }, .responses = &.{.{ .from = 2, .reject = false }}, .expected = .leader },
        .{ .voters = &.{ 1, 2, 3 }, .responses = &.{ .{ .from = 2, .reject = true }, .{ .from = 3, .reject = true } }, .expected = .follower },
        .{ .voters = &.{ 1, 2, 3 }, .responses = &.{}, .expected = .candidate },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .responses = &.{ .{ .from = 2, .reject = false }, .{ .from = 3, .reject = false }, .{ .from = 4, .reject = false }, .{ .from = 5, .reject = false } }, .expected = .leader },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .responses = &.{ .{ .from = 2, .reject = false }, .{ .from = 3, .reject = false }, .{ .from = 4, .reject = false } }, .expected = .leader },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .responses = &.{ .{ .from = 2, .reject = false }, .{ .from = 3, .reject = false } }, .expected = .leader },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .responses = &.{ .{ .from = 2, .reject = true }, .{ .from = 3, .reject = true }, .{ .from = 4, .reject = true }, .{ .from = 5, .reject = true } }, .expected = .follower },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .responses = &.{ .{ .from = 2, .reject = false }, .{ .from = 3, .reject = true }, .{ .from = 4, .reject = true }, .{ .from = 5, .reject = true } }, .expected = .follower },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .responses = &.{.{ .from = 2, .reject = false }}, .expected = .candidate },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .responses = &.{ .{ .from = 2, .reject = true }, .{ .from = 3, .reject = true } }, .expected = .candidate },
        .{ .voters = &.{ 1, 2, 3, 4, 5 }, .responses = &.{}, .expected = .candidate },
    };
    for (cases) |case| {
        const node = try createNode(1, case.voters, &.{}, .{});
        defer node.deinit();
        try node.raft.campaign(.election);
        clearMessages(&node.raft);
        for (case.responses) |response| {
            var message = raft.Message{
                .msg_type = .request_vote_response,
                .from = response.from,
                .to = 1,
                .term = 1,
                .reject = response.reject,
            };
            try node.raft.step(&message);
        }
        try std.testing.expectEqual(case.expected, node.raft.state);
        try std.testing.expectEqual(@as(u64, 1), node.raft.term);
    }
}

test "etcd/raft: follower grants at most one candidate per term" {
    const Case = struct { vote: u64, requester: u64, reject: bool };
    const cases = [_]Case{
        .{ .vote = 0, .requester = 2, .reject = false },
        .{ .vote = 0, .requester = 3, .reject = false },
        .{ .vote = 2, .requester = 2, .reject = false },
        .{ .vote = 3, .requester = 3, .reject = false },
        .{ .vote = 2, .requester = 3, .reject = true },
        .{ .vote = 3, .requester = 2, .reject = true },
    };
    for (cases) |case| {
        var net = try network.newNetwork(&.{ 1, 2, 3 });
        defer net.deinit();
        const peer = net.getPeer(1).?;
        peer.raft.becomeFollower(1, 0);
        peer.raft.vote = case.vote;
        try peer.storage.setHardState(.{ .term = 1, .vote = case.vote });
        try net.stepLocal(1, .{
            .msg_type = .request_vote,
            .from = case.requester,
            .to = 1,
            .term = 1,
        });
        try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
        const response = net.pending.items[0];
        try std.testing.expectEqual(raft.MessageType.request_vote_response, response.msg_type);
        try std.testing.expectEqual(@as(u64, 1), response.from);
        try std.testing.expectEqual(case.requester, response.to);
        try std.testing.expectEqual(@as(u64, 1), response.term);
        try std.testing.expectEqual(case.reject, response.reject);
        const expected_vote = if (case.reject) case.vote else case.requester;
        try std.testing.expectEqual(expected_vote, peer.storage.core.raft_state.hard_state.vote);
    }
}

test "etcd/raft: vote requests advertise the candidate last log" {
    const Case = struct { terms: []const u64, current_term: u64, expected_term: u64 };
    const cases = [_]Case{
        .{ .terms = &.{1}, .current_term = 1, .expected_term = 2 },
        .{ .terms = &.{ 1, 2 }, .current_term = 2, .expected_term = 3 },
    };
    for (cases) |case| {
        const node = try createNode(1, &.{ 1, 2, 3 }, case.terms, .{ .term = case.current_term });
        defer node.deinit();
        try node.raft.campaign(.election);
        try std.testing.expectEqual(@as(usize, 2), node.raft.messages.items.len);
        var seen_two = false;
        var seen_three = false;
        for (node.raft.messages.items) |message| {
            try std.testing.expectEqual(raft.MessageType.request_vote, message.msg_type);
            if (message.to == 2) seen_two = true else if (message.to == 3) seen_three = true else return error.UnexpectedVoteTarget;
            try std.testing.expectEqual(@as(u64, 1), message.from);
            try std.testing.expectEqual(case.expected_term, message.term);
            try std.testing.expectEqual(@as(u64, @intCast(case.terms.len)), message.index);
            try std.testing.expectEqual(case.terms[case.terms.len - 1], message.log_term);
        }
        try std.testing.expect(seen_two);
        try std.testing.expect(seen_three);
    }
}

test "etcd/raft: voter compares last-log term before index" {
    const Case = struct {
        local_terms: []const u64,
        candidate_term: u64,
        candidate_index: u64,
        reject: bool,
    };
    const cases = [_]Case{
        .{ .local_terms = &.{1}, .candidate_term = 1, .candidate_index = 1, .reject = false },
        .{ .local_terms = &.{1}, .candidate_term = 1, .candidate_index = 2, .reject = false },
        .{ .local_terms = &.{ 1, 1 }, .candidate_term = 1, .candidate_index = 1, .reject = true },
        .{ .local_terms = &.{1}, .candidate_term = 2, .candidate_index = 1, .reject = false },
        .{ .local_terms = &.{1}, .candidate_term = 2, .candidate_index = 2, .reject = false },
        .{ .local_terms = &.{ 1, 1 }, .candidate_term = 2, .candidate_index = 1, .reject = false },
        .{ .local_terms = &.{2}, .candidate_term = 1, .candidate_index = 1, .reject = true },
        .{ .local_terms = &.{2}, .candidate_term = 1, .candidate_index = 2, .reject = true },
        .{ .local_terms = &.{ 2, 2 }, .candidate_term = 1, .candidate_index = 1, .reject = true },
        .{ .local_terms = &.{ 1, 1 }, .candidate_term = 1, .candidate_index = 1, .reject = true },
        .{ .local_terms = &.{ 2, 1 }, .candidate_term = 1, .candidate_index = 1, .reject = true },
    };
    for (cases) |case| {
        var net = try network.newNetwork(&.{ 1, 2 });
        defer net.deinit();
        const peer = net.getPeer(1).?;
        try seedPeer(peer, case.local_terms, 3);
        try net.stepLocal(1, .{
            .msg_type = .request_vote,
            .from = 2,
            .to = 1,
            .term = 3,
            .log_term = case.candidate_term,
            .index = case.candidate_index,
        });
        try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
        try std.testing.expectEqual(raft.MessageType.request_vote_response, net.pending.items[0].msg_type);
        try std.testing.expectEqual(case.reject, net.pending.items[0].reject);
        try std.testing.expectEqual(
            if (case.reject) @as(u64, 0) else @as(u64, 2),
            peer.storage.core.raft_state.hard_state.vote,
        );
    }
}
