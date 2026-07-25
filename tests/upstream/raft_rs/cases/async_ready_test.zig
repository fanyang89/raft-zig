// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/async_ready_test.zig";

fn newSnapshot(index: u64, term: u64, voters: []const u64) !raft.Snapshot {
    return .{ .metadata = .{
        .index = index,
        .term = term,
        .conf_state = .{ .voters = try allocator.dupe(u64, voters) },
    } };
}

fn applyInitialSnapshot(storage: *raft.MemoryStorage, index: u64, term: u64, voters: []const u64) !void {
    var snapshot = try newSnapshot(index, term, voters);
    defer snapshot.deinit(allocator);
    try storage.applySnapshot(allocator, snapshot);
}

fn newNode(storage: *raft.MemoryStorage, id: u64) !raft.RawNode {
    var config = raft.defaultConfig();
    config.id = id;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.load_state_on_startup = true;
    config.election_timeout_seed = 42;
    return raft.RawNode.init(allocator, config, storage.asStorage());
}

fn newEntries(term: u64, first: u64, count: usize) ![]raft.Entry {
    const entries = try allocator.alloc(raft.Entry, count);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    for (entries, 0..) |*entry, offset| {
        entry.* = .{
            .term = term,
            .index = first + offset,
            .data = try allocator.dupe(u8, "hello"),
        };
        initialized += 1;
    }
    return entries;
}

fn sendAppend(
    node: *raft.RawNode,
    from: u64,
    term: u64,
    prev_index: u64,
    prev_term: u64,
    commit: u64,
    entries: []raft.Entry,
) !void {
    try node.step(.{
        .msg_type = .append,
        .from = from,
        .to = node.raftConst().id,
        .term = term,
        .index = prev_index,
        .log_term = prev_term,
        .commit = commit,
        .entries = entries,
    });
}

fn sendSnapshot(node: *raft.RawNode, from: u64, term: u64, snapshot: raft.Snapshot) !void {
    try node.step(.{
        .msg_type = .snapshot,
        .from = from,
        .to = node.raftConst().id,
        .term = term,
        .snapshot = snapshot,
    });
}

fn expectEntryRange(entries: []const raft.Entry, first: u64, last: u64, term: u64) !void {
    try std.testing.expect(first <= last);
    try std.testing.expectEqual(last - first + 1, entries.len);
    for (entries, first..) |entry, index| {
        try std.testing.expectEqual(index, entry.index);
        try std.testing.expectEqual(term, entry.term);
    }
}

fn expectNoEntries(entries: []const raft.Entry) !void {
    try std.testing.expectEqual(@as(usize, 0), entries.len);
}

fn expectPersistedMessages(rd: raft.Ready, message_type: raft.MessageType, count: usize) !void {
    try std.testing.expect(rd.is_persisted_msg);
    try std.testing.expectEqual(@as(usize, 0), rd.messages().len);
    try std.testing.expectEqual(count, rd.light.messages.len);
    for (rd.light.messages) |message| try std.testing.expectEqual(message_type, message.msg_type);
}

fn expectImmediateMessages(rd: raft.Ready, message_type: raft.MessageType, count: usize) !void {
    try std.testing.expect(!rd.is_persisted_msg);
    try std.testing.expectEqual(count, rd.messages().len);
    for (rd.messages()) |message| try std.testing.expectEqual(message_type, message.msg_type);
}

fn expectImmediateMessagesNonEmpty(rd: raft.Ready, message_type: raft.MessageType) !void {
    try std.testing.expect(!rd.is_persisted_msg);
    try std.testing.expect(rd.messages().len > 0);
    for (rd.messages()) |message| try std.testing.expectEqual(message_type, message.msg_type);
}

fn persistReady(storage: *raft.MemoryStorage, rd: raft.Ready) !void {
    if (rd.hs) |hs| try storage.setHardState(hs);
    if (rd.snapshot) |snapshot| try storage.applySnapshot(allocator, snapshot);
    if (rd.entries.len > 0) try storage.append(allocator, rd.entries);
}

test "raft-rs: test_raw_node_with_async_apply" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try applyInitialSnapshot(&storage, 1, 1, &.{1});

    var node = try newNode(&storage, 1);
    defer node.deinit();
    try node.campaign();
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(raft.StateRole.leader, rd.ss.?.role);
        try persistReady(&storage, rd);
        var light = try node.advance(rd);
        defer light.deinit(allocator);
    }

    var last_index = node.raftConst().raft_log.lastIndex();
    const counts = [_]u64{ 1, 4, 7, 10, 3, 6, 9, 2, 5 };
    var last_applied = node.raftConst().raft_log.applied;
    for (counts) |count| {
        for (0..count) |_| try node.propose("", "hello world!");

        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try expectEntryRange(rd.entries, last_index + 1, last_index + count, 2);
        try expectNoEntries(rd.light.committed_entries);
        try std.testing.expect(rd.must_sync);
        try persistReady(&storage, rd);

        var light = try node.advanceAppend(rd);
        defer light.deinit(allocator);
        try expectEntryRange(light.committed_entries, last_index + 1, last_index + count, 2);
        try std.testing.expectEqual(last_index + count, light.commit_index.?);

        node.advanceApplyTo(last_index + 1);
        try std.testing.expect(node.raftConst().raft_log.applied > last_applied);
        try std.testing.expectEqual(last_index + 1, node.raftConst().raft_log.applied);
        try std.testing.expect(!node.hasReady());
        last_applied = node.raftConst().raft_log.applied;
        last_index += count;
    }
}

test "raft-rs: test_raw_node_entries_after_snapshot" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try applyInitialSnapshot(&storage, 1, 1, &.{ 1, 2 });

    var node = try newNode(&storage, 1);
    defer node.deinit();

    try sendAppend(&node, 2, 2, 1, 1, 5, try newEntries(2, 2, 18));
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), rd.number);
        try std.testing.expectEqual(raft.StateRole.follower, rd.ss.?.role);
        try std.testing.expectEqual(@as(u64, 2), rd.hs.?.term);
        try std.testing.expectEqual(@as(u64, 5), rd.hs.?.commit);
        try expectEntryRange(rd.entries, 2, 19, 2);
        try expectNoEntries(rd.light.committed_entries);
        try expectPersistedMessages(rd, .append_response, 1);
        try persistReady(&storage, rd);

        var light = try node.advance(rd);
        defer light.deinit(allocator);
        try std.testing.expectEqual(@as(?u64, null), light.commit_index);
        try expectEntryRange(light.committed_entries, 2, 5, 2);
        try std.testing.expectEqual(@as(usize, 0), light.messages.len);
    }

    try sendSnapshot(&node, 2, 3, try newSnapshot(10, 3, &.{ 1, 2 }));
    try sendAppend(&node, 2, 3, 10, 3, 12, try newEntries(3, 11, 3));
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 2), rd.number);
        try std.testing.expectEqual(@as(u64, 3), rd.hs.?.term);
        try std.testing.expectEqual(@as(u64, 12), rd.hs.?.commit);
        try std.testing.expectEqual(@as(u64, 10), rd.snapshot.?.metadata.index);
        try expectEntryRange(rd.entries, 11, 13, 3);
        try expectNoEntries(rd.light.committed_entries);
        try expectPersistedMessages(rd, .append_response, 2);
        try persistReady(&storage, rd);

        var light = try node.advance(rd);
        defer light.deinit(allocator);
        try std.testing.expectEqual(@as(?u64, null), light.commit_index);
        try expectEntryRange(light.committed_entries, 11, 12, 3);
        try std.testing.expectEqual(@as(usize, 0), light.messages.len);
    }
}

test "raft-rs: test_raw_node_overwrite_entries" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try applyInitialSnapshot(&storage, 1, 1, &.{ 1, 2, 3 });

    var node = try newNode(&storage, 1);
    defer node.deinit();

    try sendAppend(&node, 2, 2, 1, 1, 1, try newEntries(2, 2, 3));
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), rd.number);
        try expectEntryRange(rd.entries, 2, 4, 2);
        try expectNoEntries(rd.light.committed_entries);
        try expectPersistedMessages(rd, .append_response, 1);
        try persistReady(&storage, rd);
        var light = try node.advance(rd);
        defer light.deinit(allocator);
        try expectNoEntries(light.committed_entries);
    }

    try sendAppend(&node, 3, 3, 3, 2, 5, try newEntries(3, 4, 3));
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 2), rd.number);
        try std.testing.expectEqual(@as(u64, 5), rd.hs.?.commit);
        try expectEntryRange(rd.entries, 4, 6, 3);
        try expectEntryRange(rd.light.committed_entries, 2, 3, 2);
        try expectPersistedMessages(rd, .append_response, 1);
        try persistReady(&storage, rd);
        var light = try node.advance(rd);
        defer light.deinit(allocator);
        try expectEntryRange(light.committed_entries, 4, 5, 3);
    }

    const stored = try storage.allEntries(allocator);
    defer {
        for (stored) |*entry| entry.deinit(allocator);
        allocator.free(stored);
    }
    try expectEntryRange(stored[0..2], 2, 3, 2);
    try expectEntryRange(stored[2..], 4, 6, 3);
}

test "raft-rs: test_async_ready_leader" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try applyInitialSnapshot(&storage, 1, 1, &.{ 1, 2, 3 });

    var node = try newNode(&storage, 1);
    defer node.deinit();
    node.raftPtr().becomeCandidate();
    try node.raftPtr().becomeLeader();
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), rd.number);
        try std.testing.expectEqual(raft.StateRole.leader, rd.ss.?.role);
        try persistReady(&storage, rd);
        var light = try node.advance(rd);
        defer light.deinit(allocator);
    }

    try std.testing.expectEqual(@as(u64, 2), node.raftConst().term);
    var first_index = node.raftConst().raft_log.lastIndex();
    const follower = node.raftPtr().progress_tracker.getPtr(2).?;
    follower.matched = 1;
    follower.becomeReplicate();

    for (0..10) |batch| {
        for (0..10) |_| try node.propose("", "hello world!");
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(batch + 2, rd.number);
        try expectEntryRange(rd.entries, first_index + batch * 10 + 1, first_index + batch * 10 + 10, 2);
        try expectImmediateMessagesNonEmpty(rd, .append);
        try storage.append(allocator, rd.entries);
        try node.advanceAppendAsync(rd);
    }

    node.onPersistReady(4);
    try std.testing.expectEqual(first_index + 30, node.raftConst().raft_log.persisted);
    try std.testing.expect(!node.hasReady());

    try node.step(.{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .term = 2,
        .index = first_index + 100,
    });
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 12), rd.number);
        try std.testing.expectEqual(first_index + 30, rd.hs.?.commit);
        try expectEntryRange(rd.light.committed_entries, first_index, first_index + 30, 2);
        try std.testing.expect(rd.messages().len > 0);
        try storage.setHardState(rd.hs.?);
        try node.advanceAppendAsync(rd);
    }

    node.onPersistReady(8);
    try std.testing.expectEqual(first_index + 70, node.raftConst().raft_log.persisted);
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 13), rd.number);
        try std.testing.expectEqual(first_index + 70, rd.hs.?.commit);
        try expectEntryRange(rd.light.committed_entries, first_index + 31, first_index + 70, 2);
        try std.testing.expect(rd.messages().len > 0);
        try storage.setHardState(rd.hs.?);

        var light = try node.advanceAppend(rd);
        defer light.deinit(allocator);
        try std.testing.expectEqual(first_index + 100, light.commit_index.?);
        try expectEntryRange(light.committed_entries, first_index + 71, first_index + 100, 2);
        try std.testing.expect(light.messages.len > 0);
    }

    first_index += 100;
    for (0..10) |_| try node.propose("", "hello world!");
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 14), rd.number);
        try expectEntryRange(rd.entries, first_index + 1, first_index + 10, 2);
        try expectImmediateMessagesNonEmpty(rd, .append);
        try storage.append(allocator, rd.entries);
        try node.advanceAppendAsync(rd);
    }

    try node.step(.{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .term = 2,
        .index = first_index + 9,
    });
    try node.step(.{
        .msg_type = .append_response,
        .from = 3,
        .to = 1,
        .term = 2,
        .index = first_index + 10,
    });
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 15), rd.number);
        try std.testing.expectEqual(first_index + 9, rd.hs.?.commit);
        try expectNoEntries(rd.entries);
        try expectNoEntries(rd.light.committed_entries);
        try std.testing.expect(rd.messages().len > 0);
        for (rd.messages()) |message| {
            try std.testing.expectEqual(raft.MessageType.append, message.msg_type);
            try std.testing.expectEqual(first_index + 9, message.commit);
        }

        var light = try node.advanceAppend(rd);
        defer light.deinit(allocator);
        try std.testing.expectEqual(first_index + 10, light.commit_index.?);
        try expectEntryRange(light.committed_entries, first_index + 1, first_index + 10, 2);
        try std.testing.expect(light.messages.len > 0);
    }
}

test "raft-rs: test_async_ready_follower" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try applyInitialSnapshot(&storage, 1, 1, &.{ 1, 2 });

    var node = try newNode(&storage, 1);
    defer node.deinit();
    var first_index: u64 = 1;
    var ready_number: u64 = 0;

    for (0..3) |round| {
        for (0..10) |batch| {
            const prev_index = first_index + batch * 3;
            try sendAppend(
                &node,
                2,
                2,
                prev_index,
                if (round == 0 and batch == 0) 1 else 2,
                prev_index + 3,
                try newEntries(2, prev_index + 1, 3),
            );
            var rd = try node.getReady();
            defer rd.deinit(allocator);
            try std.testing.expectEqual(ready_number + batch + 1, rd.number);
            try std.testing.expectEqual(prev_index + 3, rd.hs.?.commit);
            try expectEntryRange(rd.entries, prev_index + 1, prev_index + 3, 2);
            try expectNoEntries(rd.light.committed_entries);
            try expectPersistedMessages(rd, .append_response, 1);
            try persistReady(&storage, rd);
            try node.advanceAppendAsync(rd);
        }

        node.onPersistReady(ready_number + 4);
        try std.testing.expectEqual(first_index + 12, node.raftConst().raft_log.persisted);
        {
            var rd = try node.getReady();
            defer rd.deinit(allocator);
            try std.testing.expectEqual(ready_number + 11, rd.number);
            try std.testing.expectEqual(@as(?raft.HardState, null), rd.hs);
            try expectEntryRange(rd.light.committed_entries, first_index + 1, first_index + 12, 2);
            try std.testing.expectEqual(@as(usize, 0), rd.light.messages.len);

            var light = try node.advanceAppend(rd);
            defer light.deinit(allocator);
            try std.testing.expectEqual(@as(?u64, null), light.commit_index);
            try expectEntryRange(light.committed_entries, first_index + 13, first_index + 30, 2);
            try std.testing.expectEqual(@as(usize, 0), light.messages.len);
        }

        first_index += 30;
        ready_number += 11;
    }

    try sendSnapshot(&node, 2, 2, try newSnapshot(first_index + 5, 2, &.{ 1, 2 }));
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(ready_number + 1, rd.number);
        try std.testing.expectEqual(first_index + 5, rd.snapshot.?.metadata.index);
        try expectNoEntries(rd.entries);
        try expectNoEntries(rd.light.committed_entries);
        try expectPersistedMessages(rd, .append_response, 1);
        try persistReady(&storage, rd);
        try node.advanceAppendAsync(rd);
    }

    try sendAppend(
        &node,
        2,
        2,
        first_index + 5,
        2,
        first_index + 8,
        try newEntries(2, first_index + 6, 9),
    );
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(ready_number + 2, rd.number);
        try expectEntryRange(rd.entries, first_index + 6, first_index + 14, 2);
        try expectNoEntries(rd.light.committed_entries);
        try persistReady(&storage, rd);
        try node.advanceAppendAsync(rd);
    }

    node.onPersistReady(ready_number + 1);
    try std.testing.expectEqual(first_index + 5, node.raftConst().raft_log.persisted);
    node.advanceApplyTo(first_index + 5);
    try std.testing.expectEqual(first_index + 5, node.raftConst().raft_log.applied);
    node.onPersistReady(ready_number + 2);
    try std.testing.expectEqual(first_index + 14, node.raftConst().raft_log.persisted);

    var rd = try node.getReady();
    defer rd.deinit(allocator);
    try std.testing.expectEqual(ready_number + 3, rd.number);
    try expectNoEntries(rd.entries);
    try expectEntryRange(rd.light.committed_entries, first_index + 6, first_index + 8, 2);
    try std.testing.expectEqual(@as(usize, 0), rd.light.messages.len);
}

test "raft-rs: test_async_ready_become_leader" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try applyInitialSnapshot(&storage, 5, 5, &.{ 1, 2, 3 });

    var node = try newNode(&storage, 1);
    defer node.deinit();
    try node.campaign();
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), rd.number);
        try std.testing.expectEqual(raft.StateRole.candidate, rd.ss.?.role);
        try std.testing.expectEqual(@as(u64, 6), rd.hs.?.term);
        try std.testing.expectEqual(@as(u64, 1), rd.hs.?.vote);
        try expectPersistedMessages(rd, .request_vote, 2);
        try persistReady(&storage, rd);
        var light = try node.advanceAppend(rd);
        defer light.deinit(allocator);
    }

    for ([_]u64{ 2, 3 }) |candidate| {
        try node.step(.{
            .msg_type = .request_vote,
            .from = candidate,
            .to = 1,
            .term = 6,
            .log_term = 4,
            .index = 4,
        });
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(candidate, rd.number);
        try std.testing.expectEqual(@as(?raft.HardState, null), rd.hs);
        try expectPersistedMessages(rd, .request_vote_response, 1);
        try node.advanceAppendAsync(rd);
    }

    try node.step(.{
        .msg_type = .request_vote_response,
        .from = 2,
        .to = 1,
        .term = 6,
    });
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 4), rd.number);
        try std.testing.expectEqual(raft.StateRole.leader, rd.ss.?.role);
        try expectEntryRange(rd.entries, 6, 6, 6);
        try expectImmediateMessages(rd, .append, 2);
        try storage.append(allocator, rd.entries);

        var light = try node.advanceAppend(rd);
        defer light.deinit(allocator);
        try std.testing.expectEqual(@as(?u64, null), light.commit_index);
        try expectNoEntries(light.committed_entries);
        try std.testing.expectEqual(@as(usize, 0), light.messages.len);
    }
}

test "raft-rs: test_async_ready_multiple_snapshot" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try applyInitialSnapshot(&storage, 1, 1, &.{ 1, 2 });

    var node = try newNode(&storage, 1);
    defer node.deinit();
    try sendSnapshot(&node, 2, 2, try newSnapshot(10, 2, &.{ 1, 2 }));
    try sendAppend(&node, 2, 2, 10, 2, 12, try newEntries(2, 11, 3));
    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), rd.number);
        try std.testing.expectEqual(@as(u64, 10), rd.snapshot.?.metadata.index);
        try expectEntryRange(rd.entries, 11, 13, 2);
        try expectNoEntries(rd.light.committed_entries);
        try expectPersistedMessages(rd, .append_response, 2);
        try persistReady(&storage, rd);
        try node.advanceAppendAsync(rd);
    }

    try sendSnapshot(&node, 2, 2, try newSnapshot(20, 1, &.{ 1, 2 }));
    node.onPersistReady(1);
    try std.testing.expectEqual(@as(u64, 13), node.raftConst().raft_log.persisted);
    node.advanceApplyTo(10);
    try std.testing.expectEqual(@as(u64, 10), node.raftConst().raft_log.applied);

    {
        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 2), rd.number);
        try std.testing.expectEqual(@as(u64, 20), rd.snapshot.?.metadata.index);
        try std.testing.expectEqual(@as(u64, 1), rd.snapshot.?.metadata.term);
        try expectNoEntries(rd.entries);
        try expectNoEntries(rd.light.committed_entries);
        try expectPersistedMessages(rd, .append_response, 1);
        try persistReady(&storage, rd);

        var light = try node.advanceAppend(rd);
        defer light.deinit(allocator);
        try std.testing.expectEqual(@as(?u64, null), light.commit_index);
        try expectNoEntries(light.committed_entries);
        try std.testing.expectEqual(@as(usize, 0), light.messages.len);
    }
    try std.testing.expectEqual(@as(u64, 20), node.raftConst().raft_log.persisted);
    node.advanceApplyTo(20);
    try std.testing.expectEqual(@as(u64, 20), node.raftConst().raft_log.applied);
}
