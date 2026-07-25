// Copyright 2015 The etcd Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/etcd_raft/cases/configuration_test.zig";

fn hup(id: u64) raft.Message {
    return .{ .msg_type = .hup, .from = id, .to = id };
}

fn transfer(from: u64, to: u64) raft.Message {
    return .{ .msg_type = .transfer_leader, .from = from, .to = to };
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

fn encodeConfChangeV2(changes: []const raft.ConfChangeSingle) ![]u8 {
    const encoded = try allocator.alloc(u8, 12 + changes.len * 9);
    @memcpy(encoded[0..4], "RCC2");
    encoded[4] = 1;
    encoded[5] = @intFromEnum(raft.ConfChangeTransition.auto_);
    std.mem.writeInt(u16, encoded[6..8], @intCast(changes.len), .little);
    var offset: usize = 8;
    for (changes) |change| {
        encoded[offset] = @intFromEnum(change.change_type);
        offset += 1;
        std.mem.writeInt(u64, encoded[offset..][0..8], change.node_id, .little);
        offset += 8;
    }
    std.mem.writeInt(u32, encoded[offset..][0..4], 0, .little);
    return encoded;
}

fn freeEntries(entries: []raft.Entry) void {
    for (entries) |*entry| entry.deinit(allocator);
    allocator.free(entries);
}

const ExpectedProgress = struct {
    id: u64,
    next_idx: u64,
};

const ExpectedTracker = struct {
    voters: []const u64,
    voters_outgoing: []const u64 = &.{},
    learners: []const u64 = &.{},
    learners_next: []const u64 = &.{},
    progress: []const ExpectedProgress,
};

const ProgressValues = struct {
    matched: u64,
    next_idx: u64,
    state: raft.ProgressState,
};

fn expectTracker(tracker: *raft.ProgressTracker, expected: ExpectedTracker) !void {
    var state = try tracker.conf.toConfState(allocator);
    defer state.deinit(allocator);

    try std.testing.expectEqualSlices(u64, expected.voters, state.voters);
    try std.testing.expectEqualSlices(u64, expected.voters_outgoing, state.voters_outgoing);
    try std.testing.expectEqualSlices(u64, expected.learners, state.learners);
    try std.testing.expectEqualSlices(u64, expected.learners_next, state.learners_next);
    try std.testing.expect(!state.auto_leave);
    try std.testing.expectEqual(expected.progress.len, tracker.progress.count());
    for (expected.progress) |want| {
        const progress = tracker.getPtr(want.id) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 0), progress.matched);
        try std.testing.expectEqual(want.next_idx, progress.next_idx);
        try std.testing.expectEqual(raft.ProgressState.probe, progress.state);
    }
}

fn nextIndex(last_index: u64) u64 {
    return @max(last_index, 1);
}

fn applySimple(
    tracker: *raft.ProgressTracker,
    last_index: *u64,
    changes: []const raft.ConfChangeSingle,
) !void {
    var result = try raft.ConfChanger.init(tracker).simple(changes);
    defer result.deinit(allocator);
    try tracker.applyConf(result.conf, result.changes, nextIndex(last_index.*));
    last_index.* += 1;
}

fn applyEnterJoint(
    tracker: *raft.ProgressTracker,
    last_index: *u64,
    changes: []const raft.ConfChangeSingle,
) !void {
    var result = try raft.ConfChanger.init(tracker).enterJoint(false, changes);
    defer result.deinit(allocator);
    try tracker.applyConf(result.conf, result.changes, nextIndex(last_index.*));
    last_index.* += 1;
}

fn applyLeaveJoint(tracker: *raft.ProgressTracker, last_index: *u64) !void {
    var result = try raft.ConfChanger.init(tracker).leaveJoint();
    defer result.deinit(allocator);
    try tracker.applyConf(result.conf, result.changes, nextIndex(last_index.*));
    last_index.* += 1;
}

fn expectMapChanges(actual: []const raft.MapChangeEntry, expected: []const raft.MapChangeEntry) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (actual, expected) |got, want| {
        try std.testing.expectEqual(want.id, got.id);
        try std.testing.expectEqual(want.kind, got.kind);
    }
}

fn markProgress(tracker: *raft.ProgressTracker, id: u64) ProgressValues {
    const progress = tracker.at(id);
    const original = ProgressValues{
        .matched = progress.matched,
        .next_idx = progress.next_idx,
        .state = progress.state,
    };
    progress.matched = 37;
    progress.next_idx = 91;
    progress.state = .replicate;
    return original;
}

fn expectMarkedProgress(tracker: *raft.ProgressTracker, id: u64) !void {
    const progress = tracker.at(id);
    try std.testing.expectEqual(@as(u64, 37), progress.matched);
    try std.testing.expectEqual(@as(u64, 91), progress.next_idx);
    try std.testing.expectEqual(raft.ProgressState.replicate, progress.state);
}

fn restoreProgress(tracker: *raft.ProgressTracker, id: u64, original: ProgressValues) void {
    const progress = tracker.at(id);
    progress.matched = original.matched;
    progress.next_idx = original.next_idx;
    progress.state = original.state;
}

test "etcd/raft: confchange/testdata/joint_idempotency.txt::TestConfChangeDataDriven" {
    var tracker = raft.ProgressTracker.init(allocator, 10);
    defer tracker.deinit();
    var last_index: u64 = 0;

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 1 }});
    try expectTracker(&tracker, .{
        .voters = &.{1},
        .progress = &.{.{ .id = 1, .next_idx = 1 }},
    });

    const changes = [_]raft.ConfChangeSingle{
        .{ .change_type = .remove_node, .node_id = 1 },
        .{ .change_type = .remove_node, .node_id = 2 },
        .{ .change_type = .remove_node, .node_id = 9 },
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_node, .node_id = 3 },
        .{ .change_type = .add_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .remove_node, .node_id = 4 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
    };
    {
        const original = markProgress(&tracker, 1);
        defer restoreProgress(&tracker, 1, original);
        var result = try raft.ConfChanger.init(&tracker).enterJoint(false, &changes);
        defer result.deinit(allocator);
        try expectMapChanges(result.changes, &.{
            .{ .id = 2, .kind = .add },
            .{ .id = 3, .kind = .add },
            .{ .id = 4, .kind = .add },
            .{ .id = 4, .kind = .remove },
        });
        try tracker.applyConf(result.conf, result.changes, nextIndex(last_index));
        last_index += 1;
        try expectMarkedProgress(&tracker, 1);
    }
    try expectTracker(&tracker, .{
        .voters = &.{3},
        .voters_outgoing = &.{1},
        .learners = &.{2},
        .learners_next = &.{1},
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
            .{ .id = 3, .next_idx = 1 },
        },
    });

    {
        const original = markProgress(&tracker, 1);
        defer restoreProgress(&tracker, 1, original);
        try applyLeaveJoint(&tracker, &last_index);
        try expectMarkedProgress(&tracker, 1);
    }
    try expectTracker(&tracker, .{
        .voters = &.{3},
        .learners = &.{ 1, 2 },
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
            .{ .id = 3, .next_idx = 1 },
        },
    });
}

test "etcd/raft: confchange/testdata/joint_learners_next.txt::TestConfChangeDataDriven" {
    var tracker = raft.ProgressTracker.init(allocator, 10);
    defer tracker.deinit();
    var last_index: u64 = 0;

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 1 }});
    try expectTracker(&tracker, .{
        .voters = &.{1},
        .progress = &.{.{ .id = 1, .next_idx = 1 }},
    });

    const joint_changes = [_]raft.ConfChangeSingle{
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 1 },
    };
    {
        const original = markProgress(&tracker, 1);
        defer restoreProgress(&tracker, 1, original);
        try applyEnterJoint(&tracker, &last_index, &joint_changes);
        try expectMarkedProgress(&tracker, 1);
    }
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .voters_outgoing = &.{1},
        .learners_next = &.{1},
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
        },
    });

    {
        const original = markProgress(&tracker, 1);
        defer restoreProgress(&tracker, 1, original);
        try applyLeaveJoint(&tracker, &last_index);
        try expectMarkedProgress(&tracker, 1);
    }
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .learners = &.{1},
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
        },
    });
}

test "etcd/raft: confchange/testdata/simple_idempotency.txt::TestConfChangeDataDriven" {
    var tracker = raft.ProgressTracker.init(allocator, 10);
    defer tracker.deinit();
    var last_index: u64 = 0;

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 1 }});
    try expectTracker(&tracker, .{
        .voters = &.{1},
        .progress = &.{.{ .id = 1, .next_idx = 1 }},
    });

    {
        const original = markProgress(&tracker, 1);
        defer restoreProgress(&tracker, 1, original);
        try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 1 }});
        try expectMarkedProgress(&tracker, 1);
    }
    try expectTracker(&tracker, .{
        .voters = &.{1},
        .progress = &.{.{ .id = 1, .next_idx = 1 }},
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 2 }});
    try expectTracker(&tracker, .{
        .voters = &.{ 1, 2 },
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 2 },
        },
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_learner_node, .node_id = 1 }});
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .learners = &.{1},
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 2 },
        },
    });

    {
        const original = markProgress(&tracker, 1);
        defer restoreProgress(&tracker, 1, original);
        try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_learner_node, .node_id = 1 }});
        try expectMarkedProgress(&tracker, 1);
    }
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .learners = &.{1},
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 2 },
        },
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .remove_node, .node_id = 1 }});
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .progress = &.{.{ .id = 2, .next_idx = 2 }},
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .remove_node, .node_id = 1 }});
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .progress = &.{.{ .id = 2, .next_idx = 2 }},
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 3 }});
    try expectTracker(&tracker, .{
        .voters = &.{ 2, 3 },
        .progress = &.{
            .{ .id = 2, .next_idx = 2 },
            .{ .id = 3, .next_idx = 7 },
        },
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .remove_node, .node_id = 3 }});
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .progress = &.{.{ .id = 2, .next_idx = 2 }},
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .remove_node, .node_id = 3 }});
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .progress = &.{.{ .id = 2, .next_idx = 2 }},
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .remove_node, .node_id = 4 }});
    try expectTracker(&tracker, .{
        .voters = &.{2},
        .progress = &.{.{ .id = 2, .next_idx = 2 }},
    });
}

test "etcd/raft: confchange/testdata/simple_promote_demote.txt::TestConfChangeDataDriven" {
    var tracker = raft.ProgressTracker.init(allocator, 10);
    defer tracker.deinit();
    var last_index: u64 = 0;

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 1 }});
    try expectTracker(&tracker, .{
        .voters = &.{1},
        .progress = &.{.{ .id = 1, .next_idx = 1 }},
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 2 }});
    try expectTracker(&tracker, .{
        .voters = &.{ 1, 2 },
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
        },
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 3 }});
    try expectTracker(&tracker, .{
        .voters = &.{ 1, 2, 3 },
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
            .{ .id = 3, .next_idx = 2 },
        },
    });

    const demote_promote = [_]raft.ConfChangeSingle{
        .{ .change_type = .add_learner_node, .node_id = 1 },
        .{ .change_type = .add_node, .node_id = 1 },
    };
    {
        const original = markProgress(&tracker, 1);
        defer restoreProgress(&tracker, 1, original);
        try applySimple(&tracker, &last_index, &demote_promote);
        try expectMarkedProgress(&tracker, 1);
    }
    try expectTracker(&tracker, .{
        .voters = &.{ 1, 2, 3 },
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
            .{ .id = 3, .next_idx = 2 },
        },
    });

    try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_learner_node, .node_id = 2 }});
    try expectTracker(&tracker, .{
        .voters = &.{ 1, 3 },
        .learners = &.{2},
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
            .{ .id = 3, .next_idx = 2 },
        },
    });

    const promote_demote = [_]raft.ConfChangeSingle{
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 2 },
    };
    {
        const original = markProgress(&tracker, 2);
        defer restoreProgress(&tracker, 2, original);
        try applySimple(&tracker, &last_index, &promote_demote);
        try expectMarkedProgress(&tracker, 2);
    }
    try expectTracker(&tracker, .{
        .voters = &.{ 1, 3 },
        .learners = &.{2},
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
            .{ .id = 3, .next_idx = 2 },
        },
    });

    {
        const original = markProgress(&tracker, 2);
        defer restoreProgress(&tracker, 2, original);
        try applySimple(&tracker, &last_index, &.{.{ .change_type = .add_node, .node_id = 2 }});
        try expectMarkedProgress(&tracker, 2);
    }
    try expectTracker(&tracker, .{
        .voters = &.{ 1, 2, 3 },
        .progress = &.{
            .{ .id = 1, .next_idx = 1 },
            .{ .id = 2, .next_idx = 1 },
            .{ .id = 3, .next_idx = 2 },
        },
    });
}

test "etcd/raft: confchange/testdata/zero.txt::TestConfChangeDataDriven" {
    var tracker = raft.ProgressTracker.init(allocator, 10);
    defer tracker.deinit();
    var last_index: u64 = 0;
    const changes = [_]raft.ConfChangeSingle{
        .{ .change_type = .add_node, .node_id = 1 },
        .{ .change_type = .remove_node, .node_id = 0 },
        .{ .change_type = .add_node, .node_id = 0 },
        .{ .change_type = .add_learner_node, .node_id = 0 },
    };

    try applySimple(&tracker, &last_index, &changes);
    try expectTracker(&tracker, .{
        .voters = &.{1},
        .progress = &.{.{ .id = 1, .next_idx = 1 }},
    });
    try std.testing.expect(tracker.getPtr(0) == null);
}

test "etcd/raft: newly added voter gets one check-quorum grace period" {
    var net = try network.newNetworkWithConfiguration(.{
        .peer_ids = &.{ 1, 2 },
        .voters = &.{1},
    }, .{ .check_quorum = true });
    defer net.deinit();
    try net.isolate(2);
    try net.send(&.{hup(1)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);

    var changes = [_]raft.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 2 }};
    try net.applyConfChange(1, .{ .changes = &changes });
    _ = try net.runUntilIdle(100);
    const leader = net.getPeer(1).?;
    try std.testing.expect(leader.raft.progress_tracker.getPtr(2).?.recent_active);

    const timeout = leader.raft.randomized_election_timeout;
    for (0..timeout) |_| _ = try net.tickPeer(1);
    _ = try net.runUntilIdle(100);
    try std.testing.expectEqual(raft.StateRole.leader, leader.raft.state);
    try std.testing.expect(!leader.raft.progress_tracker.getPtr(2).?.recent_active);

    for (0..timeout) |_| _ = try net.tickPeer(1);
    _ = try net.runUntilIdle(100);
    try std.testing.expectEqual(raft.StateRole.follower, leader.raft.state);
}

test "etcd/raft: removing a voter can commit the leader suffix" {
    var net = try network.newNetwork(&.{ 1, 2 });
    defer net.deinit();
    try net.isolate(2);
    const leader = net.getPeer(1).?;
    leader.raft.becomeCandidate();
    try leader.raft.becomeLeader();
    try net.stepLocal(1, .{ .msg_type = .beat, .from = 1, .to = 1 });
    _ = try net.runUntilIdle(100);
    try std.testing.expectEqual(@as(u64, 0), leader.raft.raft_log.committed);

    var changes = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 2 }};
    const encoded = try encodeConfChangeV2(&changes);
    defer allocator.free(encoded);
    var conf_proposal = try proposal(1, .conf_change_v2, encoded);
    defer conf_proposal.deinit(allocator);
    try net.stepLocal(1, conf_proposal);
    _ = try net.runUntilIdle(100);
    const conf_index = leader.raft.raft_log.lastIndex();
    try std.testing.expectEqual(@as(u64, 0), leader.raft.raft_log.committed);

    var normal_proposal = try proposal(1, .normal, "hello");
    defer normal_proposal.deinit(allocator);
    try net.stepLocal(1, normal_proposal);
    _ = try net.runUntilIdle(100);
    try std.testing.expectEqual(conf_index + 1, leader.raft.raft_log.lastIndex());
    try std.testing.expectEqual(@as(u64, 0), leader.raft.raft_log.committed);

    try net.stepLocal(1, .{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .term = leader.raft.term,
        .index = conf_index,
    });
    try std.testing.expectEqual(conf_index, leader.raft.raft_log.committed);
    const committed = (try leader.raft.raft_log.nextEntries(null)).?;
    defer freeEntries(committed);
    try std.testing.expectEqual(@as(usize, 2), committed.len);
    try std.testing.expectEqual(raft.EntryType.normal, committed[0].entry_type);
    try std.testing.expectEqual(raft.EntryType.conf_change_v2, committed[1].entry_type);

    leader.raft.commitApply(conf_index);
    try net.applyConfChange(1, .{ .changes = &changes });
    try std.testing.expectEqual(conf_index + 1, leader.raft.raft_log.committed);
    try std.testing.expectEqual(leader.raft.hardState(), leader.storage.core.raft_state.hard_state);
    try std.testing.expect(leader.raft.progress_tracker.conf.voters.contains(1));
    try std.testing.expect(!leader.raft.progress_tracker.conf.voters.contains(2));

    const suffix = (try leader.raft.raft_log.nextEntries(null)).?;
    defer freeEntries(suffix);
    try std.testing.expectEqual(@as(usize, 1), suffix.len);
    try std.testing.expectEqual(raft.EntryType.normal, suffix[0].entry_type);
    try std.testing.expectEqualStrings("hello", suffix[0].data);
}

test "etcd/raft: unapplied ConfChangeV2 blocks timeout and transfer campaigns" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    try net.send(&.{hup(1)});

    var changes = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 3 }};
    const encoded = try encodeConfChangeV2(&changes);
    defer allocator.free(encoded);
    var request = try proposal(1, .conf_change_v2, encoded);
    defer request.deinit(allocator);
    try net.send(&.{request});

    const follower = net.getPeer(2).?;
    try std.testing.expect(follower.raft.raft_log.applied < follower.raft.raft_log.committed);
    const term_before = follower.raft.term;
    const timeout = follower.raft.randomized_election_timeout;
    for (0..timeout) |_| _ = try net.tickPeer(2);
    try std.testing.expectEqual(raft.StateRole.follower, follower.raft.state);
    try std.testing.expectEqual(term_before, follower.raft.term);
    try std.testing.expectEqual(@as(usize, 0), net.pendingCount());

    try net.send(&.{transfer(2, 1)});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.follower, follower.raft.state);

    const transfer_timeout = net.getPeer(1).?.raft.randomized_election_timeout;
    for (0..transfer_timeout) |_| _ = try net.tickPeer(1);
    _ = try net.runUntilIdle(1_000);
    try std.testing.expectEqual(@as(?u64, null), net.getPeer(1).?.raft.lead_transferee);

    follower.raft.commitApply(follower.raft.raft_log.committed);
    try net.send(&.{transfer(2, 1)});
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(1).?.raft.state);
    try std.testing.expectEqual(raft.StateRole.leader, follower.raft.state);

    const old_leader = net.getPeer(1).?;
    old_leader.raft.commitApply(old_leader.raft.raft_log.committed);
    const old_leader_timeout = old_leader.raft.randomized_election_timeout;
    for (0..old_leader_timeout) |_| _ = try net.tickPeer(1);
    try std.testing.expectEqual(raft.StateRole.candidate, old_leader.raft.state);
}
