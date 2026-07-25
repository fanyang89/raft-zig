const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

pub const inventory_target = "tests/upstream/openraft/cases/election_test.zig";

fn hup(id: u64) raft.Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

test "OpenRaft: granted pre-vote does not persist term or vote" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .pre_vote = true });
    defer net.deinit();

    const follower = net.getPeer(2).?;
    follower.raft.becomeFollower(2, 1);
    follower.raft.vote = 1;
    try follower.storage.setHardState(.{ .term = 2, .vote = 1 });

    try net.stepLocal(2, .{
        .msg_type = .request_pre_vote,
        .from = 3,
        .to = 2,
        .term = 3,
        .index = follower.raft.raft_log.lastIndex(),
        .log_term = try follower.raft.raft_log.lastTerm(),
    });

    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    const response = net.pending.items[0];
    try std.testing.expectEqual(raft.MessageType.request_pre_vote_response, response.msg_type);
    try std.testing.expect(!response.reject);
    try std.testing.expectEqual(raft.StateRole.follower, follower.raft.state);
    try std.testing.expectEqual(@as(u64, 2), follower.raft.term);
    try std.testing.expectEqual(@as(u64, 1), follower.raft.vote);
    try std.testing.expectEqual(@as(u64, 1), follower.raft.leader_id);
    try std.testing.expectEqual(@as(u64, 2), follower.storage.core.raft_state.hard_state.term);
    try std.testing.expectEqual(@as(u64, 1), follower.storage.core.raft_state.hard_state.vote);
}

test "OpenRaft: pre-vote quorum starts and persists a real election" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .pre_vote = true });
    defer net.deinit();

    try net.stepLocal(1, hup(1));

    const campaigner = net.getPeer(1).?;
    try std.testing.expectEqual(raft.StateRole.pre_candidate, campaigner.raft.state);
    try std.testing.expectEqual(@as(u64, 0), campaigner.raft.term);
    try std.testing.expectEqual(@as(u64, 0), campaigner.raft.vote);
    try std.testing.expectEqual(@as(u64, 0), campaigner.storage.core.raft_state.hard_state.term);
    try std.testing.expectEqual(@as(u64, 0), campaigner.storage.core.raft_state.hard_state.vote);

    var pre_vote_requests: usize = 0;
    for (net.pending.items) |message| {
        if (message.msg_type == .request_pre_vote) pre_vote_requests += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), pre_vote_requests);

    try net.stepLocal(1, .{
        .msg_type = .request_pre_vote_response,
        .from = 2,
        .to = 1,
        .term = 1,
    });

    try std.testing.expectEqual(raft.StateRole.candidate, campaigner.raft.state);
    try std.testing.expectEqual(@as(u64, 1), campaigner.raft.term);
    try std.testing.expectEqual(@as(u64, 1), campaigner.raft.vote);
    try std.testing.expectEqual(@as(u64, 1), campaigner.storage.core.raft_state.hard_state.term);
    try std.testing.expectEqual(@as(u64, 1), campaigner.storage.core.raft_state.hard_state.vote);

    var vote_requests: usize = 0;
    for (net.pending.items) |message| {
        if (message.msg_type == .request_vote) vote_requests += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), vote_requests);
}

test "OpenRaft: pre-vote prevents isolated follower term inflation" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .pre_vote = true });
    defer net.deinit();

    try net.send(&.{hup(1)});
    const leader = net.getPeer(1).?;
    const isolated = net.getPeer(3).?;
    try std.testing.expectEqual(raft.StateRole.leader, leader.raft.state);
    const leader_term = leader.raft.term;
    const isolated_term = isolated.raft.term;

    try net.isolate(3);
    for (0..5) |_| try net.send(&.{hup(3)});

    try std.testing.expectEqual(raft.StateRole.pre_candidate, isolated.raft.state);
    try std.testing.expectEqual(isolated_term, isolated.raft.term);
    try std.testing.expectEqual(isolated_term, isolated.storage.core.raft_state.hard_state.term);
    try std.testing.expectEqual(raft.StateRole.leader, leader.raft.state);
    try std.testing.expectEqual(leader_term, leader.raft.term);

    net.recover();
    try net.send(&.{.{ .msg_type = .beat, .from = 1, .to = 1 }});

    try std.testing.expectEqual(raft.StateRole.leader, leader.raft.state);
    try std.testing.expectEqual(leader_term, leader.raft.term);
    try std.testing.expectEqual(raft.StateRole.follower, isolated.raft.state);
    try std.testing.expectEqual(isolated_term, isolated.raft.term);
    try std.testing.expectEqual(@as(u64, 1), isolated.raft.leader_id);
}

test "OpenRaft: heartbeat lease blocks vote until expiration" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .check_quorum = true });
    defer net.deinit();

    try net.send(&.{hup(1)});
    const follower = net.getPeer(2).?;
    try std.testing.expectEqual(raft.StateRole.follower, follower.raft.state);
    try std.testing.expectEqual(@as(u64, 1), follower.raft.leader_id);

    const term_before = follower.raft.term;
    const vote_before = follower.raft.vote;
    const last_index = follower.raft.raft_log.lastIndex();
    const last_term = try follower.raft.raft_log.lastTerm();
    const request = raft.Message{
        .msg_type = .request_vote,
        .from = 3,
        .to = 2,
        .term = 10,
        .index = last_index,
        .log_term = last_term,
    };

    try net.stepLocal(2, request);

    try std.testing.expectEqual(raft.StateRole.follower, follower.raft.state);
    try std.testing.expectEqual(term_before, follower.raft.term);
    try std.testing.expectEqual(vote_before, follower.raft.vote);
    try std.testing.expectEqual(@as(u64, 1), follower.raft.leader_id);
    try std.testing.expectEqual(term_before, follower.storage.core.raft_state.hard_state.term);
    try std.testing.expectEqual(vote_before, follower.storage.core.raft_state.hard_state.vote);

    follower.raft.election_elapsed = follower.raft.election_timeout;
    try net.stepLocal(2, request);

    try std.testing.expectEqual(raft.StateRole.follower, follower.raft.state);
    try std.testing.expectEqual(@as(u64, 10), follower.raft.term);
    try std.testing.expectEqual(@as(u64, 3), follower.raft.vote);
    try std.testing.expectEqual(@as(u64, 0), follower.raft.leader_id);
    try std.testing.expectEqual(@as(u64, 10), follower.storage.core.raft_state.hard_state.term);
    try std.testing.expectEqual(@as(u64, 3), follower.storage.core.raft_state.hard_state.vote);
    try std.testing.expectEqual(@as(usize, 1), net.pendingCount());
    const response = net.pending.items[0];
    try std.testing.expectEqual(raft.MessageType.request_vote_response, response.msg_type);
    try std.testing.expect(!response.reject);
}
