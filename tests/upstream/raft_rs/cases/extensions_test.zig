// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/extensions_test.zig";

fn seedLog(peer: *network.Peer, last_index: u64) !void {
    const entries = try allocator.alloc(raft.Entry, last_index);
    defer allocator.free(entries);
    for (entries, 0..) |*entry, i| entry.* = .{ .term = 1, .index = i + 1 };
    try peer.storage.setEntries(allocator, entries);
    peer.raft.raft_log.persisted = last_index;
    peer.raft.raft_log.unstable.offset = last_index + 1;
}

fn expectLog(peer: *network.Peer, committed: u64, applied: u64, last_index: u64) !void {
    try std.testing.expectEqual(committed, peer.raft.raft_log.committed);
    try std.testing.expectEqual(applied, peer.raft.raft_log.applied);
    try std.testing.expectEqual(last_index, peer.raft.raft_log.lastIndex());
}

fn expectProgressCommitted(peer: *network.Peer, expected: [3]u64) !void {
    for (expected, 1..) |committed, id| {
        try std.testing.expectEqual(committed, peer.raft.progress_tracker.at(id).committed_index);
    }
}

test "raft-rs: election priority does not bypass log freshness" {
    const Case = struct {
        logs: [3]bool,
        priorities: [3]i64,
        campaigner: u64,
        expected_state: raft.StateRole,
    };
    const cases = [_]Case{
        .{ .logs = .{ true, false, false }, .priorities = .{ 3, 1, 1 }, .campaigner = 1, .expected_state = .leader },
        .{ .logs = .{ true, false, false }, .priorities = .{ 2, 2, 2 }, .campaigner = 1, .expected_state = .leader },
        .{ .logs = .{ true, false, false }, .priorities = .{ 1, 3, 3 }, .campaigner = 1, .expected_state = .leader },
        .{ .logs = .{ true, true, true }, .priorities = .{ 3, 1, 1 }, .campaigner = 1, .expected_state = .leader },
        .{ .logs = .{ true, true, true }, .priorities = .{ 2, 2, 2 }, .campaigner = 1, .expected_state = .leader },
        .{ .logs = .{ true, true, true }, .priorities = .{ 1, 3, 3 }, .campaigner = 1, .expected_state = .follower },
        .{ .logs = .{ false, true, true }, .priorities = .{ 3, 1, 1 }, .campaigner = 1, .expected_state = .follower },
        .{ .logs = .{ false, true, true }, .priorities = .{ 2, 2, 2 }, .campaigner = 1, .expected_state = .follower },
        .{ .logs = .{ false, true, true }, .priorities = .{ 1, 3, 3 }, .campaigner = 1, .expected_state = .follower },
        .{ .logs = .{ false, false, true }, .priorities = .{ 1, 3, 1 }, .campaigner = 1, .expected_state = .follower },
        .{ .logs = .{ false, false, true }, .priorities = .{ 1, 1, 3 }, .campaigner = 3, .expected_state = .leader },
    };

    for (cases) |case| {
        var net = try network.newNetwork(&.{ 1, 2, 3 });
        defer net.deinit();
        for (case.logs, case.priorities, 1..) |has_log, priority, id| {
            const peer = net.getPeer(id).?;
            peer.raft.setPriority(priority);
            if (has_log) try seedLog(peer, 1);
        }

        try net.send(&.{.{ .msg_type = .hup, .from = case.campaigner, .to = case.campaigner }});
        try std.testing.expectEqual(case.expected_state, net.getPeer(case.campaigner).?.raft.state);
    }
}

test "raft-rs: election responds to runtime priority changes" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    net.getPeer(2).?.raft.setPriority(2);
    net.getPeer(3).?.raft.setPriority(3);
    for (1..4) |id| net.getPeer(id).?.raft.becomeFollower(1, raft.invalid_id);

    try std.testing.expectEqual(@as(i64, 0), net.getPeer(1).?.raft.priority);
    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.follower, net.getPeer(1).?.raft.state);

    const cases = [_]struct { priority: i64, expected_state: raft.StateRole }{
        .{ .priority = 1, .expected_state = .follower },
        .{ .priority = 2, .expected_state = .leader },
        .{ .priority = 3, .expected_state = .leader },
        .{ .priority = 0, .expected_state = .follower },
    };
    for (cases, 0..) |case, i| {
        const peer = net.getPeer(1).?;
        peer.raft.becomeFollower(i + 2, raft.invalid_id);
        peer.raft.setPriority(case.priority);
        try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
        try std.testing.expectEqual(case.expected_state, peer.raft.state);
    }
}

test "raft-rs: group commit requires cross-group replication" {
    const Case = struct {
        matches: []const u64,
        group_ids: []const u64,
        grouped: u64,
        quorum: u64,
    };
    const cases = [_]Case{
        .{ .matches = &.{1}, .group_ids = &.{0}, .grouped = 1, .quorum = 1 },
        .{ .matches = &.{1}, .group_ids = &.{1}, .grouped = 1, .quorum = 1 },
        .{ .matches = &.{ 2, 2, 1 }, .group_ids = &.{ 1, 2, 1 }, .grouped = 2, .quorum = 2 },
        .{ .matches = &.{ 2, 2, 1 }, .group_ids = &.{ 1, 1, 2 }, .grouped = 1, .quorum = 2 },
        .{ .matches = &.{ 2, 2, 1 }, .group_ids = &.{ 1, 0, 1 }, .grouped = 1, .quorum = 2 },
        .{ .matches = &.{ 2, 2, 1 }, .group_ids = &.{ 0, 0, 0 }, .grouped = 1, .quorum = 2 },
        .{ .matches = &.{ 4, 2, 1, 3 }, .group_ids = &.{ 0, 0, 0, 0 }, .grouped = 1, .quorum = 2 },
        .{ .matches = &.{ 4, 2, 1, 3 }, .group_ids = &.{ 1, 0, 0, 0 }, .grouped = 1, .quorum = 2 },
        .{ .matches = &.{ 4, 2, 1, 3 }, .group_ids = &.{ 0, 1, 0, 2 }, .grouped = 2, .quorum = 2 },
        .{ .matches = &.{ 4, 2, 1, 3 }, .group_ids = &.{ 0, 2, 1, 0 }, .grouped = 1, .quorum = 2 },
        .{ .matches = &.{ 4, 2, 1, 3 }, .group_ids = &.{ 1, 1, 1, 1 }, .grouped = 2, .quorum = 2 },
        .{ .matches = &.{ 4, 2, 1, 3 }, .group_ids = &.{ 1, 1, 2, 1 }, .grouped = 1, .quorum = 2 },
        .{ .matches = &.{ 4, 2, 1, 3 }, .group_ids = &.{ 1, 2, 1, 1 }, .grouped = 2, .quorum = 2 },
        .{ .matches = &.{ 4, 2, 1, 3 }, .group_ids = &.{ 4, 3, 2, 1 }, .grouped = 2, .quorum = 2 },
    };

    for (cases) |case| {
        const ids = [_]u64{ 1, 2, 3, 4 };
        var net = try network.newNetwork(ids[0..case.matches.len]);
        defer net.deinit();
        const peer = net.getPeer(1).?;
        var max_index: u64 = 0;
        for (case.matches) |matched| max_index = @max(max_index, matched);
        try seedLog(peer, max_index);
        peer.raft.term = 1;

        var groups: [4][2]u64 = undefined;
        var group_count: usize = 0;
        for (case.matches, case.group_ids, 1..) |matched, group_id, id| {
            const progress = peer.raft.progress_tracker.at(id);
            progress.matched = matched;
            progress.next_idx = matched + 1;
            if (group_id != 0) {
                groups[group_count] = .{ id, group_id };
                group_count += 1;
            }
        }

        try peer.raft.enableGroupCommit(true);
        try peer.raft.assignCommitGroups(groups[0..group_count]);
        try std.testing.expectEqual(@as(u64, 0), peer.raft.raft_log.committed);

        peer.raft.state = .leader;
        try peer.raft.assignCommitGroups(groups[0..group_count]);
        try std.testing.expectEqual(case.grouped, peer.raft.raft_log.committed);

        try peer.raft.enableGroupCommit(false);
        try std.testing.expectEqual(case.quorum, peer.raft.raft_log.committed);
    }
}

test "raft-rs: progress committed index follows replication and is monotonic" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    for (1..4) |id| try expectLog(net.getPeer(id).?, 1, 0, 1);
    try expectProgressCommitted(net.getPeer(1).?, .{ 1, 1, 1 });

    try net.cut(1, 3);
    var testdata = "testdata".*;
    var proposal_entries = [_]raft.Entry{.{ .data = &testdata }};
    const proposal = raft.Message{
        .msg_type = .propose,
        .from = 1,
        .to = 1,
        .entries = &proposal_entries,
    };
    try net.send(&.{ proposal, proposal });
    net.recover();
    try expectLog(net.getPeer(1).?, 3, 0, 3);
    try expectLog(net.getPeer(2).?, 3, 0, 3);
    try expectLog(net.getPeer(3).?, 1, 0, 1);
    try expectProgressCommitted(net.getPeer(1).?, .{ 3, 3, 1 });

    try net.send(&.{.{ .msg_type = .beat, .from = 1, .to = 1 }});
    for (1..4) |id| try expectLog(net.getPeer(id).?, 3, 0, 3);
    try expectProgressCommitted(net.getPeer(1).?, .{ 3, 3, 3 });

    try net.send(&.{.{ .msg_type = .hup, .from = 2, .to = 2 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(2).?.raft.state);
    for (1..4) |id| try expectLog(net.getPeer(id).?, 4, 0, 4);
    try expectProgressCommitted(net.getPeer(2).?, .{ 4, 4, 4 });

    try net.isolate(2);
    var somedata = "somedata".*;
    var two_entries = [_]raft.Entry{ .{ .data = &somedata }, .{ .data = &somedata } };
    try net.send(&.{.{
        .msg_type = .propose,
        .from = 2,
        .to = 2,
        .entries = &two_entries,
    }});
    var one_entry = [_]raft.Entry{.{ .data = &somedata }};
    try net.send(&.{.{
        .msg_type = .propose,
        .from = 2,
        .to = 2,
        .entries = &one_entry,
    }});
    net.recover();
    try net.stepLocal(2, .{
        .msg_type = .append_response,
        .from = 1,
        .to = 2,
        .term = 2,
        .index = 6,
        .commit = 4,
        .reject = true,
        .reject_hint = 4,
    });
    try expectProgressCommitted(net.getPeer(2).?, .{ 4, 4, 4 });
    _ = try net.runUntilIdle(10_000);
    try expectProgressCommitted(net.getPeer(2).?, .{ 7, 7, 7 });

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    try std.testing.expectEqual(raft.StateRole.leader, net.getPeer(1).?.raft.state);
    for (1..4) |id| try expectLog(net.getPeer(id).?, 8, 0, 8);
    try expectProgressCommitted(net.getPeer(1).?, .{ 8, 8, 8 });

    try net.send(&.{ proposal, proposal });
    try expectProgressCommitted(net.getPeer(1).?, .{ 10, 10, 10 });
    try net.stepLocal(1, .{ .msg_type = .append_response, .from = 2, .to = 1, .term = 3, .index = 10, .commit = 9 });
    try net.stepLocal(1, .{ .msg_type = .append_response, .from = 3, .to = 1, .term = 3, .index = 10, .commit = 9 });
    try expectProgressCommitted(net.getPeer(1).?, .{ 10, 10, 10 });
}

fn seedRawNodeStorage(storage: *raft.MemoryStorage, voters: []const u64) !void {
    var conf_state = raft.ConfState{ .voters = try allocator.dupe(u64, voters) };
    defer conf_state.deinit(allocator);
    try storage.setRaftState(allocator, .{ .conf_state = conf_state });
}

test "raft-rs: RawNode setPriority updates priority at runtime" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedRawNodeStorage(&storage, &.{1});
    var config = raft.defaultConfig();
    config.id = 1;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    var node = try raft.RawNode.init(allocator, config, storage.asStorage());
    defer node.deinit();

    for ([_]i64{ 0, 1, 5, 10, 10_000 }) |priority| {
        node.setPriority(priority);
        try std.testing.expectEqual(priority, node.raftConst().priority);
    }
}

test "raft-rs: skip broadcast commit delays only empty commit messages" {
    var net = try network.newNetwork(&.{ 1, 2, 3 });
    defer net.deinit();
    net.getPeer(1).?.raft.skip_broadcast_commit = true;

    try net.send(&.{.{ .msg_type = .hup, .from = 1, .to = 1 }});
    var testdata = "testdata".*;
    var proposal_entries = [_]raft.Entry{.{ .data = &testdata }};
    const proposal = raft.Message{
        .msg_type = .propose,
        .from = 1,
        .to = 1,
        .entries = &proposal_entries,
    };
    try net.send(&.{proposal});
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(1).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 1), net.getPeer(2).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 1), net.getPeer(3).?.raft.raft_log.committed);

    try net.send(&.{.{ .msg_type = .beat, .from = 1, .to = 1 }});
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(2).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 2), net.getPeer(3).?.raft.raft_log.committed);

    net.getPeer(1).?.raft.skip_broadcast_commit = false;
    try net.send(&.{proposal});
    for (1..4) |id| try std.testing.expectEqual(@as(u64, 3), net.getPeer(id).?.raft.raft_log.committed);

    net.getPeer(1).?.raft.skip_broadcast_commit = true;
    try net.send(&.{ proposal, proposal });
    try std.testing.expectEqual(@as(u64, 5), net.getPeer(1).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 4), net.getPeer(2).?.raft.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 4), net.getPeer(3).?.raft.raft_log.committed);

    var conf_change_data = [_]u8{1};
    var conf_change_entries = [_]raft.Entry{.{
        .entry_type = .conf_change_v2,
        .data = &conf_change_data,
    }};
    try net.send(&.{.{
        .msg_type = .propose,
        .from = 1,
        .to = 1,
        .entries = &conf_change_entries,
    }});
    for (1..4) |id| {
        const peer = net.getPeer(id).?;
        try std.testing.expectEqual(@as(u64, 6), peer.raft.raft_log.committed);
    }
    for (1..4) |id| try std.testing.expect(net.getPeer(id).?.raft.shouldBroadcastCommit());
}

test "raft-rs: Inflights setCapacity grows and defers shrinking" {
    const starts = [_]usize{ 16, 112, 120 };
    for (starts) |start| {
        var inflights = raft.Inflights.init(128);
        defer inflights.deinit(allocator);
        for (0..start) |i| try inflights.add(allocator, i);
        inflights.freeTo(allocator, start - 1);
        for (0..16) |i| try inflights.add(allocator, i);
        try std.testing.expectEqual(@as(usize, 16), inflights.count);
        try std.testing.expectEqual(start, inflights.start);

        try inflights.setCapacity(allocator, 1024);
        try std.testing.expectEqual(@as(usize, 1024), inflights.capacity);
        try std.testing.expect(inflights.incoming_capacity == null);
        try std.testing.expectEqual(@as(usize, 1024), inflights.bufferSize());
        if (start < 120) {
            try std.testing.expect(inflights.start != 0);
        } else {
            try std.testing.expectEqual(@as(usize, 0), inflights.start);
        }
    }

    const ShrinkCase = struct { start: usize, count: usize, buffer_capacity: usize };
    const shrink_cases = [_]ShrinkCase{
        .{ .start = 1, .count = 0, .buffer_capacity = 0 },
        .{ .start = 1, .count = 0, .buffer_capacity = 128 },
        .{ .start = 1, .count = 8, .buffer_capacity = 128 },
    };
    var pending_shrink: ?raft.Inflights = null;
    for (shrink_cases, 0..) |case, i| {
        var inflights = raft.Inflights.init(128);
        errdefer inflights.deinit(allocator);
        inflights.start = case.start;
        if (case.buffer_capacity > 0) {
            try inflights.buffer.ensureTotalCapacityPrecise(allocator, case.buffer_capacity);
            try inflights.buffer.appendNTimes(allocator, 0, case.buffer_capacity);
        }
        for (0..case.count) |value| try inflights.add(allocator, value);

        try inflights.setCapacity(allocator, 64);
        if (i < 2) {
            try std.testing.expectEqual(@as(usize, 64), inflights.capacity);
            try std.testing.expect(inflights.incoming_capacity == null);
            try std.testing.expectEqual(@as(usize, 0), inflights.start);
            try std.testing.expectEqual(if (i == 0) @as(usize, 0) else 64, inflights.bufferSize());
            inflights.deinit(allocator);
        } else {
            try std.testing.expectEqual(@as(usize, 128), inflights.capacity);
            try std.testing.expectEqual(@as(?usize, 64), inflights.incoming_capacity);
            try std.testing.expectEqual(@as(usize, 1), inflights.start);
            try std.testing.expectEqual(@as(usize, 128), inflights.bufferSize());
            pending_shrink = inflights;
        }
    }

    var deferred = pending_shrink.?;
    defer deferred.deinit(allocator);
    deferred.freeTo(allocator, 7);
    try std.testing.expectEqual(@as(usize, 64), deferred.capacity);
    try std.testing.expect(deferred.incoming_capacity == null);
    try std.testing.expectEqual(@as(usize, 0), deferred.start);

    for ([_]usize{ 128, 1024 }) |new_capacity| {
        var inflights = raft.Inflights.init(128);
        defer inflights.deinit(allocator);
        inflights.start = 1;
        try inflights.buffer.ensureTotalCapacityPrecise(allocator, 128);
        try inflights.buffer.appendNTimes(allocator, 0, 128);
        for (0..8) |value| try inflights.add(allocator, value);
        try inflights.setCapacity(allocator, 64);
        try inflights.setCapacity(allocator, new_capacity);
        try std.testing.expectEqual(new_capacity, inflights.capacity);
        try std.testing.expect(inflights.incoming_capacity == null);
    }
}

test "raft-rs: joint group commit fixture" {
    const Fixture = struct {
        incoming: []const u64,
        outgoing: []const u64,
        indexes: []const u64,
        group_ids: []const u64,
        expected: u64,
    };
    const fixtures = [_]Fixture{
        .{ .incoming = &.{ 1, 2, 3 }, .outgoing = &.{}, .indexes = &.{ 100, 101, 99 }, .group_ids = &.{ 1, 1, 1 }, .expected = 100 },
        .{ .incoming = &.{ 1, 2, 3 }, .outgoing = &.{}, .indexes = &.{ 100, 101, 99 }, .group_ids = &.{ 1, 1, 2 }, .expected = 99 },
        .{ .incoming = &.{ 1, 2, 3 }, .outgoing = &.{}, .indexes = &.{ 100, 101, 99 }, .group_ids = &.{ 2, 1, 1 }, .expected = 100 },
        .{ .incoming = &.{ 1, 2, 3 }, .outgoing = &.{}, .indexes = &.{ 100, 101, 99 }, .group_ids = &.{ 0, 1, 1 }, .expected = 99 },
        .{ .incoming = &.{ 1, 2, 3 }, .outgoing = &.{}, .indexes = &.{ 100, 101, 99 }, .group_ids = &.{ 0, 1, 2 }, .expected = 99 },
        .{ .incoming = &.{ 1, 2, 3, 4, 5 }, .outgoing = &.{}, .indexes = &.{ 100, 101, 99, 102, 98 }, .group_ids = &.{ 0, 0, 0, 0, 1 }, .expected = 98 },
        .{ .incoming = &.{ 1, 2, 3, 4 }, .outgoing = &.{ 3, 4, 5, 6 }, .indexes = &.{ 101, 99, 100, 102, 103, 1 }, .group_ids = &.{ 1, 0, 1, 1, 0, 2 }, .expected = 1 },
        .{ .incoming = &.{ 1, 2, 3 }, .outgoing = &.{ 4, 5, 6 }, .indexes = &.{ 99, 100, 101, 99, 100, 101 }, .group_ids = &.{ 1, 1, 2, 1, 2, 1 }, .expected = 100 },
        .{ .incoming = &.{ 1, 2, 3 }, .outgoing = &.{ 4, 5, 6 }, .indexes = &.{ 99, 100, 101, 99, 100, 101 }, .group_ids = &.{ 1, 1, 2, 1, 1, 0 }, .expected = 99 },
        .{ .incoming = &.{ 1, 2, 3, 4, 5 }, .outgoing = &.{}, .indexes = &.{ 99, 100, 101, 102, 103 }, .group_ids = &.{ 1, 1, 1, 1, 2 }, .expected = 101 },
        .{ .incoming = &.{ 1, 2, 3, 4, 5 }, .outgoing = &.{ 2, 3, 4, 5, 6 }, .indexes = &.{ 1, 100, 101, 102, 103, 2 }, .group_ids = &.{ 1, 0, 1, 1, 1, 1 }, .expected = 1 },
        .{ .incoming = &.{ 1, 2, 3, 4, 5 }, .outgoing = &.{ 2, 3, 4, 5, 6 }, .indexes = &.{ 3, 100, 101, 102, 103, 2 }, .group_ids = &.{ 0, 1, 1, 1, 1, 1 }, .expected = 3 },
        .{ .incoming = &.{ 1, 2, 3, 4, 5 }, .outgoing = &.{ 2, 3, 4, 5, 6 }, .indexes = &.{ 3, 100, 101, 102, 103, 2 }, .group_ids = &.{ 0, 1, 1, 1, 3, 1 }, .expected = 101 },
        .{ .incoming = &.{ 1, 2, 3, 4, 5 }, .outgoing = &.{ 2, 3, 4, 5, 6 }, .indexes = &.{ 3, 100, 101, 102, 103, 2 }, .group_ids = &.{ 0, 1, 1, 1, 1, 3 }, .expected = 2 },
    };

    for (fixtures) |fixture| {
        var indexer = raft.AckIndexer.init(allocator);
        defer indexer.deinit();
        for (fixture.indexes, fixture.group_ids, 1..) |index, group_id, id| {
            try indexer.set(id, .{ .index = index, .group_id = group_id });
        }
        var joint = try raft.JointConfiguration.fromIncomingOutgoing(
            allocator,
            fixture.incoming,
            fixture.outgoing,
        );
        defer joint.deinit();
        const result = joint.committedIndex(true, indexer.indexer());
        try std.testing.expectEqual(fixture.expected, result.index);
    }
}
