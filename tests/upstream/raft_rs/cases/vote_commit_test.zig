// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/vote_commit_test.zig";

const RequestChange = enum {
    add_voter,
    add_learner_and_voter,
};

const ResponseChange = enum {
    remove_voter,
    leave_joint,
};

fn hup(id: u64) raft.Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

fn proposal(id: u64, entry_type: raft.EntryType, data: []const u8) !raft.Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{
        .entry_type = entry_type,
        .data = try allocator.dupe(u8, data),
    };
    return .{ .msg_type = .propose, .from = id, .to = id, .entries = entries };
}

fn encodeConfChangeV2(cc: raft.ConfChangeV2) ![]u8 {
    const encoded = try allocator.alloc(u8, 12 + cc.changes.len * 9 + cc.context.len);
    @memcpy(encoded[0..4], "RCC2");
    encoded[4] = 1;
    encoded[5] = @intFromEnum(cc.transition);
    std.mem.writeInt(u16, encoded[6..8], @intCast(cc.changes.len), .little);
    var offset: usize = 8;
    for (cc.changes) |change| {
        encoded[offset] = @intFromEnum(change.change_type);
        offset += 1;
        std.mem.writeInt(u64, encoded[offset..][0..8], change.node_id, .little);
        offset += 8;
    }
    std.mem.writeInt(u32, encoded[offset..][0..4], @intCast(cc.context.len), .little);
    @memcpy(encoded[offset + 4 ..], cc.context);
    return encoded;
}

fn requestChange(kind: RequestChange, changes: *[2]raft.ConfChangeSingle) raft.ConfChangeV2 {
    return switch (kind) {
        .add_voter => blk: {
            changes[0] = .{ .change_type = .add_node, .node_id = 4 };
            break :blk .{ .changes = changes[0..1] };
        },
        .add_learner_and_voter => blk: {
            changes[0] = .{ .change_type = .add_learner_node, .node_id = 3 };
            changes[1] = .{ .change_type = .add_node, .node_id = 4 };
            break :blk .{ .changes = changes[0..2] };
        },
    };
}

fn responseChange(kind: ResponseChange, changes: *[1]raft.ConfChangeSingle) raft.ConfChangeV2 {
    return switch (kind) {
        .remove_voter => blk: {
            changes[0] = .{ .change_type = .remove_node, .node_id = 4 };
            break :blk .{ .changes = changes[0..1] };
        },
        .leave_joint => .{},
    };
}

fn requestEntryType(kind: RequestChange) raft.EntryType {
    return switch (kind) {
        .add_voter => .conf_change,
        .add_learner_and_voter => .conf_change_v2,
    };
}

fn responseEntryType(kind: ResponseChange) raft.EntryType {
    return switch (kind) {
        .remove_voter => .conf_change,
        .leave_joint => .conf_change_v2,
    };
}

fn findPending(net: *network.Network, msg_type: raft.MessageType, from: u64, to: u64) ?usize {
    for (net.pending.items, 0..) |message, index| {
        if (message.msg_type == msg_type and message.from == from and message.to == to) return index;
    }
    return null;
}

fn applyChange(net: *network.Network, id: u64, cc_index: u64, cc: raft.ConfChangeV2) !void {
    const peer = net.getPeer(id).?;
    try net.applyConfChange(id, cc);
    peer.raft.commitApply(cc_index);
}

fn proposeUncommittedChange(net: *network.Network, entry_type: raft.EntryType, cc: raft.ConfChangeV2) !u64 {
    try net.ignoreMessageType(.append_response);
    const encoded = try encodeConfChangeV2(cc);
    defer allocator.free(encoded);
    var message = try proposal(1, entry_type, encoded);
    defer message.deinit(allocator);
    try net.send(&.{message});
    net.clearIgnored();
    return net.getPeer(1).?.raft.raft_log.lastIndex();
}

fn makeNodeFourMoreUpToDate(net: *network.Network) !void {
    net.recover();
    try net.cut(1, 2);
    try net.cut(1, 3);
    var message = try proposal(1, .normal, &.{});
    defer message.deinit(allocator);
    try net.send(&.{message});
}

fn commitChangeWithoutNodeFour(net: *network.Network) !void {
    net.recover();
    try net.cut(1, 4);
    try net.ignoreMessageType(.append);
    const peer_two = net.getPeer(2).?;
    try net.stepLocal(1, .{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .term = net.getPeer(1).?.raft.term,
        .index = peer_two.raft.raft_log.lastIndex(),
    });
    try net.stepLocal(1, .{ .msg_type = .beat, .from = 1, .to = 1 });
    _ = try net.runUntilIdle(1_000);
    net.clearIgnored();
}

fn commitChangeOnlyOnNodeFour(net: *network.Network) !void {
    const peer_two = net.getPeer(2).?;
    try net.stepLocal(1, .{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .term = net.getPeer(1).?.raft.term,
        .index = peer_two.raft.raft_log.lastIndex(),
    });
    try net.stepLocal(1, .{ .msg_type = .beat, .from = 1, .to = 1 });
    _ = try net.runUntilIdle(1_000);
}

fn testAdvanceCommitIndexByVoteRequest(use_prevote: bool) !void {
    const changes = [_]RequestChange{ .add_voter, .add_learner_and_voter };
    for (changes) |change_kind| {
        var net = try network.newNetworkWithConfiguration(.{
            .peer_ids = &.{ 1, 2, 3, 4 },
            .voters = &.{ 1, 2, 3 },
            .learners = &.{4},
        }, .{ .pre_vote = use_prevote });
        defer net.deinit();

        try net.send(&.{hup(1)});
        try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);

        var change_buffer: [2]raft.ConfChangeSingle = undefined;
        const cc = requestChange(change_kind, &change_buffer);
        const cc_index = try proposeUncommittedChange(&net, requestEntryType(change_kind), cc);
        try makeNodeFourMoreUpToDate(&net);
        try commitChangeWithoutNodeFour(&net);

        net.recover();
        try net.isolate(1);
        const peer_four = net.getPeer(4).?;
        try std.testing.expect(peer_four.raft.raft_log.committed < cc_index);
        try net.stepLocal(4, hup(4));
        try std.testing.expectEqual(raft.StateRole.follower, peer_four.raft.state);

        const peer_two = net.getPeer(2).?;
        try std.testing.expect(peer_two.raft.raft_log.committed >= cc_index);
        try applyChange(&net, 2, cc_index, cc);

        const request_type: raft.MessageType = if (use_prevote) .request_pre_vote else .request_vote;
        const response_type: raft.MessageType = if (use_prevote) .request_pre_vote_response else .request_vote_response;
        try net.stepLocal(2, hup(2));
        try std.testing.expectEqual(
            if (use_prevote) raft.StateRole.pre_candidate else raft.StateRole.candidate,
            peer_two.raft.state,
        );
        const request_index = findPending(&net, request_type, 2, 4).?;
        const request = net.pending.items[request_index];
        try std.testing.expectEqual(cc_index, request.commit);
        try std.testing.expectEqual(try peer_two.raft.raft_log.term(cc_index), request.commit_term);

        _ = try net.deliverAt(request_index);
        const response_index = findPending(&net, response_type, 4, 2).?;
        try std.testing.expect(net.pending.items[response_index].reject);
        _ = try net.runUntilIdle(1_000);
        try std.testing.expect(peer_two.raft.state != .leader);
        try std.testing.expect(peer_four.raft.raft_log.committed >= cc_index);

        try applyChange(&net, 4, cc_index, cc);
        try net.send(&.{hup(4)});
        try std.testing.expectEqual(raft.StateRole.leader, peer_four.raft.state);
    }
}

fn testAdvanceCommitIndexByVoteResponse(use_prevote: bool) !void {
    const changes = [_]ResponseChange{ .remove_voter, .leave_joint };
    for (changes) |change_kind| {
        var net = try network.newNetworkWithOptions(&.{ 1, 2, 3, 4 }, .{ .pre_vote = use_prevote });
        defer net.deinit();

        if (change_kind == .leave_joint) {
            var enter_joint_changes = [_]raft.ConfChangeSingle{
                .{ .change_type = .add_node, .node_id = 3 },
                .{ .change_type = .add_learner_node, .node_id = 4 },
            };
            try net.applyConfChangeOnAll(.{
                .transition = .explicit,
                .changes = &enter_joint_changes,
            });
        }

        try net.send(&.{hup(1)});
        try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);

        var change_buffer: [1]raft.ConfChangeSingle = undefined;
        const cc = responseChange(change_kind, &change_buffer);
        const cc_index = try proposeUncommittedChange(&net, responseEntryType(change_kind), cc);
        try makeNodeFourMoreUpToDate(&net);
        try commitChangeOnlyOnNodeFour(&net);

        net.recover();
        try net.isolate(1);
        const peer_four = net.getPeer(4).?;
        try std.testing.expect(peer_four.raft.raft_log.committed >= cc_index);
        try applyChange(&net, 4, cc_index, cc);
        try net.stepLocal(4, hup(4));
        try std.testing.expectEqual(raft.StateRole.follower, peer_four.raft.state);

        const peer_two = net.getPeer(2).?;
        try std.testing.expect(peer_two.raft.raft_log.committed < cc_index);
        const request_type: raft.MessageType = if (use_prevote) .request_pre_vote else .request_vote;
        const response_type: raft.MessageType = if (use_prevote) .request_pre_vote_response else .request_vote_response;
        try net.stepLocal(2, hup(2));
        try std.testing.expectEqual(
            if (use_prevote) raft.StateRole.pre_candidate else raft.StateRole.candidate,
            peer_two.raft.state,
        );

        const request_index = findPending(&net, request_type, 2, 4).?;
        _ = try net.deliverAt(request_index);
        const response_index = findPending(&net, response_type, 4, 2).?;
        const response = net.pending.items[response_index];
        try std.testing.expect(response.reject);
        try std.testing.expectEqual(cc_index, response.commit);
        try std.testing.expectEqual(try peer_four.raft.raft_log.term(cc_index), response.commit_term);

        _ = try net.runUntilIdle(1_000);
        try std.testing.expect(peer_two.raft.raft_log.committed >= cc_index);
        try std.testing.expectEqual(raft.StateRole.follower, peer_two.raft.state);

        try applyChange(&net, 2, cc_index, cc);
        try net.send(&.{hup(2)});
        try std.testing.expectEqual(raft.StateRole.leader, peer_two.raft.state);
    }
}

test "raft-rs: test_advance_commit_index_by_direct_vote_request" {
    try testAdvanceCommitIndexByVoteRequest(false);
}

test "raft-rs: test_advance_commit_index_by_direct_vote_response" {
    try testAdvanceCommitIndexByVoteResponse(false);
}

test "raft-rs: test_advance_commit_index_by_prevote_request" {
    try testAdvanceCommitIndexByVoteRequest(true);
}

test "raft-rs: test_advance_commit_index_by_prevote_response" {
    try testAdvanceCommitIndexByVoteResponse(true);
}
