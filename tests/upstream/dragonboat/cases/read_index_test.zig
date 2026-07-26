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

pub const inventory_target = "tests/upstream/dragonboat/cases/read_index_test.zig";

fn readIndexMessage(context: []const u8) !raft.Message {
    const entries = try allocator.alloc(raft.Entry, 1);
    errdefer allocator.free(entries);
    entries[0] = .{ .data = try allocator.dupe(u8, context) };
    return .{ .msg_type = .read_index, .from = 1, .to = 1, .entries = entries };
}

test "Dragonboat: readindex_test.go::TestReadIndexIsResetAfterRaftStateChange" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var conf_state = raft.ConfState{ .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }) };
    defer conf_state.deinit(allocator);
    try storage.setRaftState(allocator, .{ .conf_state = conf_state });

    var config = raft.defaultConfig();
    config.id = 1;
    config.election_timeout_seed = 1;
    var node = try raft.Raft.init(allocator, config, storage.asStorage());
    defer node.deinit();

    node.becomeCandidate();
    try node.becomeLeader();
    try node.raft_log.commitTo(node.raft_log.lastIndex());

    var pending = try readIndexMessage("pending-safe");
    defer pending.deinit(allocator);
    try node.step(&pending);
    try std.testing.expectEqual(@as(usize, 1), node.read_only.queue.items.len);
    try std.testing.expectEqual(@as(u32, 1), node.read_only.pending.count());

    node.term += 1;
    var postponed = try readIndexMessage("postponed");
    defer postponed.deinit(allocator);
    try node.step(&postponed);
    try std.testing.expectEqual(@as(usize, 1), node.pendingReadIndexCount());
    try std.testing.expectEqual(@as(usize, 1), node.read_only.queue.items.len);

    var higher_term = raft.Message{
        .msg_type = .heartbeat,
        .from = 2,
        .to = 1,
        .term = node.term + 1,
    };
    defer higher_term.deinit(allocator);
    try node.step(&higher_term);

    try std.testing.expectEqual(raft.StateRole.follower, node.state);
    try std.testing.expectEqual(@as(usize, 0), node.read_only.queue.items.len);
    try std.testing.expectEqual(@as(u32, 0), node.read_only.pending.count());
    try std.testing.expectEqual(@as(usize, 0), node.pendingReadIndexCount());
}
