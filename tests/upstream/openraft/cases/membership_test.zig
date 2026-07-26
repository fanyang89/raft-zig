// Copyright OpenRaft contributors.
// SPDX-License-Identifier: MIT OR Apache-2.0
// Adapted and modified from tests/tests/membership/t21_change_membership_cases.rs at
// OpenRaft revision 0d15d99844e8245ac917ce76ce2e4598665d0e40.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;
const max_peer_count = 7;
const baseline_payload = "openraft-membership-baseline";
const post_change_payload = "openraft-membership-post-change";

pub const inventory_target = "tests/upstream/openraft/cases/membership_test.zig";

const Operation = enum {
    change,
    add,
    remove,
};

const Case = struct {
    operation: Operation,
    old: []const u8,
    members: []const u8,
    new: []const u8,
    joint_quorum_barrier: bool = false,
};

const ProgressSnapshot = struct {
    address: usize = 0,
    matched: u64 = 0,
    next_idx: u64 = 0,
    commit_group_id: u64 = 0,
};

const IdBuffer = struct {
    values: [max_peer_count]u64 = undefined,
    len: usize = 0,

    fn append(self: *IdBuffer, id: u64) void {
        self.values[self.len] = id;
        self.len += 1;
    }

    fn slice(self: *const IdBuffer) []const u64 {
        return self.values[0..self.len];
    }
};

fn raftId(openraft_id: u8) u64 {
    return @as(u64, openraft_id) + 1;
}

fn containsOpenRaftId(ids: []const u8, id: u8) bool {
    return std.mem.indexOfScalar(u8, ids, id) != null;
}

fn mapIds(ids: []const u8) IdBuffer {
    var result = IdBuffer{};
    for (ids) |id| result.append(raftId(id));
    return result;
}

fn onlyIn(left: []const u8, right: []const u8) IdBuffer {
    var result = IdBuffer{};
    for (left) |id| {
        if (!containsOpenRaftId(right, id)) result.append(raftId(id));
    }
    return result;
}

fn membershipUnion(old: []const u8, new: []const u8) IdBuffer {
    var result = IdBuffer{};
    for (0..max_peer_count) |raw_id| {
        const id: u8 = @intCast(raw_id);
        if (containsOpenRaftId(old, id) or containsOpenRaftId(new, id)) {
            result.append(raftId(id));
        }
    }
    return result;
}

fn hup(id: u64) raft.Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

fn beat(id: u64) raft.Message {
    return .{ .msg_type = .beat, .from = id, .to = id };
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

fn decodeConfChangeV2(data: []const u8) !raft.ConfChangeV2 {
    if (data.len < 12 or !std.mem.eql(u8, data[0..4], "RCC2") or data[4] != 1) {
        return error.ConfChangeParseError;
    }
    const transition: raft.ConfChangeTransition = switch (data[5]) {
        0 => .auto_,
        1 => .implicit,
        2 => .explicit,
        else => return error.ConfChangeParseError,
    };
    const change_count = std.mem.readInt(u16, data[6..8], .little);
    const changes_end = 8 + @as(usize, change_count) * 9;
    if (changes_end + 4 > data.len) return error.ConfChangeParseError;

    var changes: []raft.ConfChangeSingle = &.{};
    if (change_count != 0) changes = try allocator.alloc(raft.ConfChangeSingle, change_count);
    errdefer if (changes.len != 0) allocator.free(changes);
    var offset: usize = 8;
    for (changes) |*change| {
        change.* = .{
            .change_type = switch (data[offset]) {
                0 => .add_node,
                1 => .remove_node,
                2 => .add_learner_node,
                else => return error.ConfChangeParseError,
            },
            .node_id = std.mem.readInt(u64, data[offset + 1 ..][0..8], .little),
        };
        offset += 9;
    }

    const context_len = std.mem.readInt(u32, data[offset..][0..4], .little);
    offset += 4;
    if (offset + context_len != data.len) return error.ConfChangeParseError;
    var context: []u8 = &.{};
    if (context_len != 0) context = try allocator.dupe(u8, data[offset..]);
    return .{ .transition = transition, .changes = changes, .context = context };
}

fn entryAt(peer: *network.Peer, index: u64) ?raft.Entry {
    const entries = peer.storage.core.entries.items;
    if (entries.len == 0) return null;
    const first = entries[0].index;
    if (index < first or index - first >= entries.len) return null;
    return entries[@intCast(index - first)];
}

fn applyCommittedPeer(net: *network.Network, id: u64) !void {
    const peer = net.getPeer(id) orelse return error.UnknownPeer;
    while (peer.raft.raft_log.applied < peer.raft.raft_log.committed) {
        const index = peer.raft.raft_log.applied + 1;
        const entry = entryAt(peer, index) orelse return error.MissingCommittedEntry;
        switch (entry.entry_type) {
            .normal => {},
            .conf_change => return error.UnexpectedLegacyConfChange,
            .conf_change_v2 => {
                var cc = try decodeConfChangeV2(entry.data);
                defer cc.deinit(allocator);
                try net.applyConfChange(id, cc);
            },
        }
        peer.raft.commitApply(index);
    }
}

fn applyCommittedAndDrain(net: *network.Network, peer_ids: []const u64) !void {
    _ = try net.runUntilIdle(10_000);
    for (peer_ids) |id| try applyCommittedPeer(net, id);
    _ = try net.runUntilIdle(10_000);
    for (peer_ids) |id| try applyCommittedPeer(net, id);
    _ = try net.runUntilIdle(10_000);
}

fn submitEntry(net: *network.Network, leader_id: u64, entry_type: raft.EntryType, data: []const u8) !u64 {
    const before = net.getPeer(leader_id).?.raft.raft_log.lastIndex();
    var message = try proposal(leader_id, entry_type, data);
    defer message.deinit(allocator);
    try net.send(&.{message});
    const index = net.getPeer(leader_id).?.raft.raft_log.lastIndex();
    try std.testing.expectEqual(before + 1, index);
    return index;
}

fn submitConfChange(net: *network.Network, leader_id: u64, cc: raft.ConfChangeV2) !u64 {
    const encoded = try encodeConfChangeV2(cc);
    defer allocator.free(encoded);
    var decoded = try decodeConfChangeV2(encoded);
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(cc.transition, decoded.transition);
    try std.testing.expectEqual(cc.changes.len, decoded.changes.len);
    return submitEntry(net, leader_id, .conf_change_v2, encoded);
}

fn driveCommitNotice(net: *network.Network, leader_id: u64) !void {
    try net.send(&.{beat(leader_id)});
}

fn expectCommitted(net: *network.Network, ids: []const u64, index: u64) !void {
    for (ids) |id| {
        try std.testing.expect(net.getPeer(id).?.raft.raft_log.committed >= index);
    }
}

fn expectApplied(net: *network.Network, ids: []const u64, index: u64) !void {
    for (ids) |id| {
        try std.testing.expect(net.getPeer(id).?.raft.raft_log.applied >= index);
    }
}

fn expectConfig(
    peer: *network.Peer,
    voters: []const u64,
    outgoing: []const u64,
    learners: []const u64,
    progress: []const u64,
) !void {
    var state = try peer.raft.progress_tracker.conf.toConfState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqualSlices(u64, voters, state.voters);
    try std.testing.expectEqualSlices(u64, outgoing, state.voters_outgoing);
    try std.testing.expectEqualSlices(u64, learners, state.learners);
    try std.testing.expectEqualSlices(u64, &.{}, state.learners_next);
    try std.testing.expect(!state.auto_leave);
    try std.testing.expectEqual(progress.len, peer.raft.progress_tracker.progress.count());
    for (progress) |id| try std.testing.expect(peer.raft.progress_tracker.getPtr(id) != null);
}

fn expectConfigOn(
    net: *network.Network,
    ids: []const u64,
    voters: []const u64,
    outgoing: []const u64,
    learners: []const u64,
    progress: []const u64,
) !void {
    for (ids) |id| try expectConfig(net.getPeer(id).?, voters, outgoing, learners, progress);
}

fn containsPayload(peer: *network.Peer, payload: []const u8) bool {
    for (peer.storage.core.entries.items) |entry| {
        if (entry.entry_type == .normal and std.mem.eql(u8, entry.data, payload)) return true;
    }
    return false;
}

fn buildJointChanges(case: Case, buffer: *[14]raft.ConfChangeSingle) []raft.ConfChangeSingle {
    var len: usize = 0;
    switch (case.operation) {
        .change => {
            for (case.old) |id| {
                if (!containsOpenRaftId(case.new, id)) {
                    buffer[len] = .{ .change_type = .remove_node, .node_id = raftId(id) };
                    len += 1;
                }
            }
            for (case.new) |id| {
                if (!containsOpenRaftId(case.old, id)) {
                    buffer[len] = .{ .change_type = .add_node, .node_id = raftId(id) };
                    len += 1;
                }
            }
        },
        .add => for (case.members) |id| {
            buffer[len] = .{ .change_type = .add_node, .node_id = raftId(id) };
            len += 1;
        },
        .remove => for (case.members) |id| {
            buffer[len] = .{ .change_type = .remove_node, .node_id = raftId(id) };
            len += 1;
        },
    }
    return buffer[0..len];
}

fn runCase(case: Case) !void {
    const old = mapIds(case.old);
    const new = mapIds(case.new);
    const new_only = onlyIn(case.new, case.old);
    const removed = onlyIn(case.old, case.new);
    const active = membershipUnion(case.old, case.new);
    const leader_id = raftId(0);

    var net = try network.newNetworkWithConfiguration(.{
        .peer_ids = active.slice(),
        .voters = old.slice(),
    }, .{});
    defer net.deinit();

    try net.send(&.{hup(leader_id)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(leader_id).?.raft.state);

    const baseline_index = try submitEntry(&net, leader_id, .normal, baseline_payload);
    try driveCommitNotice(&net, leader_id);
    try expectCommitted(&net, old.slice(), baseline_index);
    try applyCommittedAndDrain(&net, active.slice());
    try expectApplied(&net, old.slice(), baseline_index);
    for (old.slice()) |id| try std.testing.expect(containsPayload(net.getPeer(id).?, baseline_payload));

    var learners = IdBuffer{};
    for (new_only.slice()) |id| {
        var learner_change = [_]raft.ConfChangeSingle{.{ .change_type = .add_learner_node, .node_id = id }};
        const learner_index = try submitConfChange(&net, leader_id, .{ .changes = &learner_change });
        try driveCommitNotice(&net, leader_id);
        try expectCommitted(&net, old.slice(), learner_index);
        try applyCommittedAndDrain(&net, active.slice());
        learners.append(id);
        try expectCommitted(&net, &.{id}, learner_index);
        try expectApplied(&net, &.{id}, learner_index);
        try std.testing.expect(containsPayload(net.getPeer(id).?, baseline_payload));
        try expectConfig(net.getPeer(id).?, old.slice(), &.{}, learners.slice(), active.values[0 .. old.len + learners.len]);
    }

    const effective_change = !std.mem.eql(u64, old.slice(), new.slice());
    if (!effective_change) {
        var progress_before: [max_peer_count]ProgressSnapshot = @splat(.{});
        const leader = net.getPeer(leader_id).?;
        for (old.slice()) |id| {
            const progress = leader.raft.progress_tracker.getPtr(id).?;
            progress.commit_group_id = 100 + id;
            progress_before[@intCast(id - 1)] = .{
                .address = @intFromPtr(progress),
                .matched = progress.matched,
                .next_idx = progress.next_idx,
                .commit_group_id = progress.commit_group_id,
            };
        }

        var no_op_changes: [7]raft.ConfChangeSingle = undefined;
        var no_op_len: usize = 0;
        if (case.members.len == 0) {
            no_op_changes[0] = .{
                .change_type = if (case.operation == .remove) .remove_node else .add_node,
                .node_id = 0,
            };
            no_op_len = 1;
        } else {
            for (case.members) |id| {
                no_op_changes[no_op_len] = .{
                    .change_type = if (case.operation == .remove) .remove_node else .add_node,
                    .node_id = raftId(id),
                };
                no_op_len += 1;
            }
        }
        const no_op_index = try submitConfChange(&net, leader_id, .{ .changes = no_op_changes[0..no_op_len] });
        try driveCommitNotice(&net, leader_id);
        try expectCommitted(&net, old.slice(), no_op_index);
        try applyCommittedAndDrain(&net, active.slice());
        try expectApplied(&net, old.slice(), no_op_index);
        try expectConfigOn(&net, old.slice(), old.slice(), &.{}, &.{}, old.slice());
        for (old.slice()) |id| {
            const before = progress_before[@intCast(id - 1)];
            const progress = leader.raft.progress_tracker.getPtr(id).?;
            try std.testing.expectEqual(before.address, @intFromPtr(progress));
            try std.testing.expectEqual(before.commit_group_id, progress.commit_group_id);
            try std.testing.expect(progress.matched >= before.matched);
            try std.testing.expect(progress.next_idx >= before.next_idx);
        }
    } else {
        var change_buffer: [14]raft.ConfChangeSingle = undefined;
        const changes = buildJointChanges(case, &change_buffer);
        const joint_index = try submitConfChange(&net, leader_id, .{
            .transition = .explicit,
            .changes = changes,
        });
        try driveCommitNotice(&net, leader_id);
        try expectCommitted(&net, active.slice(), joint_index);
        try applyCommittedAndDrain(&net, active.slice());
        try expectApplied(&net, active.slice(), joint_index);
        try expectConfigOn(&net, active.slice(), new.slice(), old.slice(), &.{}, active.slice());

        var leave = raft.ConfChangeV2{};
        defer leave.deinit(allocator);
        const leave_index = if (case.joint_quorum_barrier) barrier: {
            for (new.slice()) |id| try net.cut(leader_id, id);
            const index = try submitConfChange(&net, leader_id, leave);
            var old_replicas: usize = 0;
            for (old.slice()) |id| {
                if (net.getPeer(id).?.raft.raft_log.lastIndex() >= index) old_replicas += 1;
            }
            try std.testing.expect(old_replicas >= old.len / 2 + 1);
            for (new.slice()) |id| try std.testing.expect(net.getPeer(id).?.raft.raft_log.lastIndex() < index);
            try std.testing.expect(net.getPeer(leader_id).?.raft.raft_log.committed < index);
            net.recover();
            try driveCommitNotice(&net, leader_id);
            try std.testing.expect(net.getPeer(leader_id).?.raft.raft_log.committed >= index);
            break :barrier index;
        } else try submitConfChange(&net, leader_id, leave);

        try driveCommitNotice(&net, leader_id);
        try expectCommitted(&net, active.slice(), leave_index);
        try applyCommittedAndDrain(&net, active.slice());
        try expectApplied(&net, active.slice(), leave_index);
        try expectConfigOn(&net, active.slice(), new.slice(), &.{}, &.{}, new.slice());
    }

    var removed_last_indexes: [max_peer_count]u64 = @splat(0);
    for (removed.slice()) |id| removed_last_indexes[@intCast(id - 1)] = net.getPeer(id).?.raft.raft_log.lastIndex();

    var post_leader_id = leader_id;
    if (containsOpenRaftId(case.old, 0) and !containsOpenRaftId(case.new, 0)) {
        const removed_leader = net.getPeer(leader_id).?;
        try std.testing.expectEqual(raft.StateRole.leader, removed_leader.raft.state);
        try std.testing.expect(!removed_leader.raft.progress_tracker.conf.voters.contains(leader_id));
        try std.testing.expect(removed_leader.raft.progress_tracker.getPtr(leader_id) == null);
        var dropped = try proposal(leader_id, .normal, "removed-leader-proposal");
        defer dropped.deinit(allocator);
        try std.testing.expectError(error.ProposalDropped, removed_leader.raft.step(&dropped));

        post_leader_id = new.slice()[0];
        try net.send(&.{hup(post_leader_id)});
        try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(post_leader_id).?.raft.state);
        try driveCommitNotice(&net, post_leader_id);
        try applyCommittedAndDrain(&net, active.slice());
    }

    const post_index = try submitEntry(&net, post_leader_id, .normal, post_change_payload);
    try driveCommitNotice(&net, post_leader_id);
    try expectCommitted(&net, new.slice(), post_index);
    try applyCommittedAndDrain(&net, active.slice());
    try expectApplied(&net, new.slice(), post_index);
    for (new.slice()) |id| try std.testing.expect(containsPayload(net.getPeer(id).?, post_change_payload));
    for (removed.slice()) |id| {
        try std.testing.expect(!containsPayload(net.getPeer(id).?, post_change_payload));
        try std.testing.expectEqual(removed_last_indexes[@intCast(id - 1)], net.getPeer(id).?.raft.raft_log.lastIndex());
        try std.testing.expect(net.getPeer(post_leader_id).?.raft.progress_tracker.getPtr(id) == null);
    }
}

test "OpenRaft membership: m0_change_m12" {
    try runCase(.{ .operation = .change, .old = &.{0}, .members = &.{ 1, 2 }, .new = &.{ 1, 2 } });
}

test "OpenRaft membership: m0_change_m123" {
    try runCase(.{ .operation = .change, .old = &.{0}, .members = &.{ 1, 2, 3 }, .new = &.{ 1, 2, 3 } });
}

test "OpenRaft membership: m01_change_m12" {
    try runCase(.{ .operation = .change, .old = &.{ 0, 1 }, .members = &.{ 1, 2 }, .new = &.{ 1, 2 } });
}

test "OpenRaft membership: m01_change_m1" {
    try runCase(.{ .operation = .change, .old = &.{ 0, 1 }, .members = &.{1}, .new = &.{1} });
}

test "OpenRaft membership: m01_change_m2" {
    try runCase(.{ .operation = .change, .old = &.{ 0, 1 }, .members = &.{2}, .new = &.{2} });
}

test "OpenRaft membership: m01_change_m3" {
    try runCase(.{ .operation = .change, .old = &.{ 0, 1 }, .members = &.{3}, .new = &.{3} });
}

test "OpenRaft membership: m012_change_m4" {
    try runCase(.{ .operation = .change, .old = &.{ 0, 1, 2 }, .members = &.{4}, .new = &.{4} });
}

test "OpenRaft membership: m012_change_m456" {
    try runCase(.{
        .operation = .change,
        .old = &.{ 0, 1, 2 },
        .members = &.{ 4, 5, 6 },
        .new = &.{ 4, 5, 6 },
        .joint_quorum_barrier = true,
    });
}

test "OpenRaft membership: m01234_change_m0123" {
    try runCase(.{
        .operation = .change,
        .old = &.{ 0, 1, 2, 3, 4 },
        .members = &.{ 0, 1, 2, 3 },
        .new = &.{ 0, 1, 2, 3 },
    });
}

test "OpenRaft membership: m0_add_m01" {
    try runCase(.{ .operation = .add, .old = &.{0}, .members = &.{ 0, 1 }, .new = &.{ 0, 1 } });
}

test "OpenRaft membership: m0_add_m12" {
    try runCase(.{ .operation = .add, .old = &.{0}, .members = &.{ 1, 2 }, .new = &.{ 0, 1, 2 } });
}

test "OpenRaft membership: m01_add_m" {
    try runCase(.{ .operation = .add, .old = &.{ 0, 1 }, .members = &.{}, .new = &.{ 0, 1 } });
}

test "OpenRaft membership: m012_remove_m01" {
    try runCase(.{ .operation = .remove, .old = &.{ 0, 1, 2 }, .members = &.{ 0, 1 }, .new = &.{2} });
}

test "OpenRaft membership: m012_remove_m3" {
    try runCase(.{ .operation = .remove, .old = &.{ 0, 1, 2 }, .members = &.{3}, .new = &.{ 0, 1, 2 } });
}

test "OpenRaft membership: m012_remove_m" {
    try runCase(.{ .operation = .remove, .old = &.{ 0, 1, 2 }, .members = &.{}, .new = &.{ 0, 1, 2 } });
}

test "OpenRaft membership: m012_remove_m13" {
    try runCase(.{ .operation = .remove, .old = &.{ 0, 1, 2 }, .members = &.{ 1, 3 }, .new = &.{ 0, 2 } });
}
