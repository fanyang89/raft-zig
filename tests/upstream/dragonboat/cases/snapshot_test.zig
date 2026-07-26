// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Adapted and modified for raft-zig from Dragonboat revision
// 076c7f6497dcc18880aed6323246d5079661942c.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/dragonboat/cases/snapshot_test.zig";

fn initNode(storage: *raft.MemoryStorage, id: u64, conf_state: raft.ConfState) !raft.Raft {
    var owned = try raft.cloneConfState(allocator, conf_state);
    defer owned.deinit(allocator);
    try storage.setRaftState(allocator, .{ .conf_state = owned });

    var config = raft.defaultConfig();
    config.id = id;
    config.election_timeout_seed = id;
    return raft.Raft.init(allocator, config, storage.asStorage());
}

fn snapshot(index: u64, term: u64, conf_state: raft.ConfState) !raft.Snapshot {
    return .{ .metadata = .{
        .index = index,
        .term = term,
        .conf_state = try raft.cloneConfState(allocator, conf_state),
    } };
}

test "Dragonboat: raft_test.go::TestNonVotingCanBePromotedBySnapshot" {
    var next_voters = [_]u64{ 1, 2 };
    var learners = [_]u64{3};
    var learner_storage = raft.MemoryStorage.init();
    defer learner_storage.deinit(allocator);
    var learner = try initNode(&learner_storage, 3, .{
        .voters = &next_voters,
        .learners = &learners,
    });
    defer learner.deinit();
    learner.term = 1;

    var learner_snap = try snapshot(20, 2, .{
        .voters = &next_voters,
        .learners = &learners,
    });
    defer learner_snap.deinit(allocator);
    try std.testing.expect(try learner.restoreSnapshot(learner_snap));
    try std.testing.expect(learner.progress_tracker.conf.learners.contains(3));
    try std.testing.expectEqual(@as(u64, 1), learner.term);

    var promoted = [_]u64{ 1, 2, 3 };
    var promotion = try snapshot(21, 3, .{ .voters = &promoted });
    defer promotion.deinit(allocator);
    try std.testing.expect(try learner.restoreSnapshot(promotion));
    try std.testing.expect(learner.progress_tracker.conf.voters.incoming.contains(3));
    try std.testing.expect(!learner.progress_tracker.conf.learners.contains(3));
    try std.testing.expectEqual(@as(u64, 1), learner.term);

    var voters = [_]u64{ 1, 2, 3 };
    var voter_storage = raft.MemoryStorage.init();
    defer voter_storage.deinit(allocator);
    var voter = try initNode(&voter_storage, 3, .{ .voters = &voters });
    defer voter.deinit();
    voter.term = 4;

    var demotion = try snapshot(20, 5, .{
        .voters = &next_voters,
        .learners = &learners,
    });
    defer demotion.deinit(allocator);
    try std.testing.expect(try voter.restoreSnapshot(demotion));
    try std.testing.expect(!voter.progress_tracker.conf.voters.contains(3));
    try std.testing.expect(voter.progress_tracker.conf.learners.contains(3));
    try std.testing.expectEqual(@as(u64, 4), voter.term);

    var invalid_storage = raft.MemoryStorage.init();
    defer invalid_storage.deinit(allocator);
    var invalid_node = try initNode(&invalid_storage, 3, .{ .voters = &voters });
    defer invalid_node.deinit();

    var overlapping = try snapshot(20, 2, .{
        .voters = &voters,
        .learners = &learners,
    });
    defer overlapping.deinit(allocator);
    try std.testing.expect(!try invalid_node.restoreSnapshot(overlapping));

    var auto_leave = try snapshot(20, 2, .{
        .voters = &voters,
        .auto_leave = true,
    });
    defer auto_leave.deinit(allocator);
    try std.testing.expect(!try invalid_node.restoreSnapshot(auto_leave));

    var maximum = try snapshot(std.math.maxInt(u64), 2, .{ .voters = &voters });
    defer maximum.deinit(allocator);
    try std.testing.expect(!try invalid_node.restoreSnapshot(maximum));

    var leader_storage = raft.MemoryStorage.init();
    defer leader_storage.deinit(allocator);
    var leader = try initNode(&leader_storage, 3, .{ .voters = &voters });
    defer leader.deinit();
    leader.becomeCandidate();
    try leader.becomeLeader();
    const leader_term = leader.term;
    try std.testing.expect(!try leader.restoreSnapshot(demotion));
    try std.testing.expectEqual(raft.StateRole.follower, leader.state);
    try std.testing.expectEqual(leader_term + 1, leader.term);
}
