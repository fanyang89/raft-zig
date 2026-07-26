// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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

pub const inventory_target = "tests/upstream/dragonboat/cases/raw_node_test.zig";

const Fixture = struct {
    storage: raft.MemoryStorage,
    node: raft.RawNode,

    fn create() !*Fixture {
        const fixture = try allocator.create(Fixture);
        errdefer allocator.destroy(fixture);
        fixture.storage = raft.MemoryStorage.init();
        errdefer fixture.storage.deinit(allocator);

        var conf_state = raft.ConfState{ .voters = try allocator.dupe(u64, &.{ 1, 2 }) };
        defer conf_state.deinit(allocator);
        try fixture.storage.setRaftState(allocator, .{ .conf_state = conf_state });

        var config = raft.defaultConfig();
        config.id = 1;
        config.election_timeout_seed = 1;
        fixture.node = try raft.RawNode.init(allocator, config, fixture.storage.asStorage());
        return fixture;
    }

    fn deinit(self: *Fixture) void {
        self.node.deinit();
        self.storage.deinit(allocator);
        allocator.destroy(self);
    }

    fn elect(self: *Fixture) !void {
        try self.node.campaign();
        try std.testing.expectEqual(raft.StateRole.candidate, self.node.raftConst().state);
        try self.node.step(.{
            .msg_type = .request_vote_response,
            .from = 2,
            .to = 1,
            .term = self.node.raftConst().term,
        });
        try std.testing.expectEqual(raft.StateRole.leader, self.node.raftConst().state);
    }
};

test "Dragonboat: peer_test.go::TestRaftAPIReportUnreachable" {
    const fixture = try Fixture.create();
    defer fixture.deinit();
    try fixture.elect();

    const progress = fixture.node.raftPtr().progress_tracker.getPtr(2).?;
    progress.becomeReplicate();
    try std.testing.expectEqual(raft.ProgressState.replicate, progress.state);

    try fixture.node.reportUnreachable(2);

    try std.testing.expectEqual(raft.ProgressState.probe, progress.state);
    try std.testing.expectEqual(@as(u64, 0), progress.matched);
    try std.testing.expectEqual(@as(u64, 1), progress.next_idx);
}

test "Dragonboat: peer_test.go::TestRaftAPIReportSnapshotStatus" {
    const Case = struct {
        status: raft.SnapshotStatus,
        expected_next: u64,
    };
    const cases = [_]Case{
        .{ .status = .finish, .expected_next = 11 },
        .{ .status = .failure, .expected_next = 1 },
    };

    for (cases) |case| {
        const fixture = try Fixture.create();
        defer fixture.deinit();
        try fixture.elect();

        const progress = fixture.node.raftPtr().progress_tracker.getPtr(2).?;
        progress.becomeSnapshot(10);
        progress.pending_request_snapshot = 7;

        try fixture.node.reportSnapshot(2, case.status);

        try std.testing.expectEqual(raft.ProgressState.probe, progress.state);
        try std.testing.expectEqual(@as(u64, 0), progress.pending_snapshot);
        try std.testing.expectEqual(@as(u64, 0), progress.pending_request_snapshot);
        try std.testing.expectEqual(case.expected_next, progress.next_idx);
        try std.testing.expect(progress.paused);
    }
}
