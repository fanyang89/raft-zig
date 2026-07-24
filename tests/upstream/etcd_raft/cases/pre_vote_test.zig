const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const Message = raft.Message;
const StateRole = raft.StateRole;

pub const inventory_target = "tests/upstream/etcd_raft/cases/pre_vote_test.zig";

fn hup(id: u64) Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

test "etcd/raft: pre-vote elects with quorum" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .pre_vote = true });
    defer net.deinit();

    try net.send(&.{hup(1)});

    try std.testing.expectEqual(StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(@as(u64, 1), net.getPeer(1).?.raft.term);
    try std.testing.expectEqual(StateRole.follower, net.getPeer(2).?.raft.state);
    try std.testing.expectEqual(StateRole.follower, net.getPeer(3).?.raft.state);
}

test "etcd/raft: failed pre-vote does not advance term" {
    var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .pre_vote = true });
    defer net.deinit();
    try net.isolate(1);

    try net.stepLocal(1, hup(1));
    _ = try net.runUntilIdle(100);

    const campaigner = net.getPeer(1).?;
    try std.testing.expectEqual(StateRole.pre_candidate, campaigner.raft.state);
    try std.testing.expectEqual(@as(u64, 0), campaigner.raft.term);
    try std.testing.expectEqual(@as(u64, 0), campaigner.raft.vote);
}

test "etcd/raft: pre-vote does not mutate any role" {
    const roles = [_]StateRole{ .follower, .pre_candidate, .candidate, .leader };
    for (roles) |role| {
        var net = try network.newNetworkWithOptions(&.{ 1, 2, 3 }, .{ .pre_vote = true });
        defer net.deinit();

        const target = net.getPeer(2).?;
        switch (role) {
            .follower => target.raft.becomeFollower(1, 0),
            .pre_candidate => {
                target.raft.becomeFollower(1, 0);
                target.raft.becomePreCandidate();
            },
            .candidate => {
                target.raft.becomeFollower(1, 0);
                target.raft.becomeCandidate();
            },
            .leader => {
                target.raft.becomeFollower(1, 0);
                target.raft.becomeCandidate();
                try target.raft.becomeLeader();
            },
        }

        const state_before = target.raft.state;
        const term_before = target.raft.term;
        const vote_before = target.raft.vote;
        const leader_before = target.raft.leader_id;
        const last_index = target.raft.raft_log.lastIndex();
        const last_term = try target.raft.raft_log.lastTerm();

        try net.stepLocal(2, .{
            .msg_type = .request_pre_vote,
            .from = 1,
            .to = 2,
            .term = term_before + 1,
            .index = last_index,
            .log_term = last_term,
        });

        try std.testing.expectEqual(state_before, target.raft.state);
        try std.testing.expectEqual(term_before, target.raft.term);
        try std.testing.expectEqual(vote_before, target.raft.vote);
        try std.testing.expectEqual(leader_before, target.raft.leader_id);
    }
}
