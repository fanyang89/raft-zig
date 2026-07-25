// Copyright 2019 TiKV Project Authors
// Copyright 2015 CoreOS, Inc.
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/pagination_test.zig";

const IgnoreSizeHintStorage = struct {
    inner: raft.MemoryStorage = raft.MemoryStorage.init(),
    ignored_size_hint_calls: usize = 0,

    fn deinit(self: *IgnoreSizeHintStorage) void {
        self.inner.deinit(allocator);
    }

    fn asStorage(self: *IgnoreSizeHintStorage) raft.Storage {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn fromContext(ctx: *anyopaque) *IgnoreSizeHintStorage {
        return @ptrCast(@alignCast(ctx));
    }

    fn initialState(ctx: *anyopaque, alloc: std.mem.Allocator) raft.Error!raft.RaftState {
        return fromContext(ctx).inner.initialState(alloc);
    }

    fn entries(
        ctx: *anyopaque,
        alloc: std.mem.Allocator,
        low: u64,
        high: u64,
        max_size: ?u64,
        context: raft.GetEntriesContext,
    ) raft.Error![]raft.Entry {
        const self = fromContext(ctx);
        if (max_size != null) self.ignored_size_hint_calls += 1;
        return self.inner.entries(alloc, low, high, null, context);
    }

    fn term(ctx: *anyopaque, index: u64) raft.Error!u64 {
        return fromContext(ctx).inner.term(index);
    }

    fn firstIndex(ctx: *anyopaque) raft.Error!u64 {
        return fromContext(ctx).inner.firstIndex();
    }

    fn lastIndex(ctx: *anyopaque) raft.Error!u64 {
        return fromContext(ctx).inner.lastIndex();
    }

    fn getSnapshot(
        ctx: *anyopaque,
        alloc: std.mem.Allocator,
        request_index: u64,
        to: u64,
    ) raft.Error!raft.Snapshot {
        return fromContext(ctx).inner.getSnapshot(alloc, request_index, to);
    }

    const vtable: raft.Storage.VTable = .{
        .initial_state = initialState,
        .entries = entries,
        .term = term,
        .first_index = firstIndex,
        .last_index = lastIndex,
        .get_snapshot = getSnapshot,
    };
};

fn expectEntryRange(entries: []const raft.Entry, first: u64, last: u64) !void {
    try std.testing.expect(first <= last);
    try std.testing.expectEqual(last - first + 1, entries.len);
    for (entries, first..) |entry, index| {
        try std.testing.expectEqual(index, entry.index);
        try std.testing.expectEqual(@as(u64, 1), entry.term);
    }
}

fn makeConfig(applied: u64, max_committed_size_per_ready: u64) raft.Config {
    var config = raft.defaultConfig();
    config.id = 1;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.applied = applied;
    config.max_committed_size_per_ready = max_committed_size_per_ready;
    config.load_state_on_startup = true;
    config.election_timeout_seed = 42;
    return config;
}

test "raft-rs: test_committed_entries_pagination_after_restart" {
    var storage = IgnoreSizeHintStorage{};
    defer storage.deinit();

    var snapshot = raft.Snapshot{ .metadata = .{
        .index = 1,
        .term = 1,
        .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }) },
    } };
    defer snapshot.deinit(allocator);
    try storage.inner.applySnapshot(allocator, snapshot);

    var persisted_entries: [10]raft.Entry = undefined;
    var initialized: usize = 0;
    defer for (persisted_entries[0..initialized]) |*entry| entry.deinit(allocator);

    var committed_size: u64 = 0;
    for (&persisted_entries, 2..) |*entry, index| {
        const data = if (index == 11) "boom" else "test data";
        entry.* = .{
            .term = 1,
            .index = @intCast(index),
            .data = try allocator.dupe(u8, data),
        };
        initialized += 1;
        if (index <= 10) committed_size += @intCast(raft.entryApproximateSize(entry.*));
    }
    try storage.inner.append(allocator, &persisted_entries);
    try storage.inner.setHardState(.{ .term = 1, .commit = 10 });

    {
        var persisted_state = try storage.inner.initialState(allocator);
        defer persisted_state.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 10), persisted_state.hard_state.commit);
    }
    try std.testing.expectEqual(@as(u64, 11), try storage.inner.lastIndex());

    const page_size = committed_size - 1;
    var applied_indices: std.ArrayList(u64) = .empty;
    defer applied_indices.deinit(allocator);

    {
        var node = try raft.RawNode.init(allocator, makeConfig(1, page_size), storage.asStorage());
        defer node.deinit();

        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try std.testing.expectEqual(@as(?raft.HardState, null), rd.hs);
        try std.testing.expectEqual(@as(usize, 0), rd.entries.len);
        try expectEntryRange(rd.light.committed_entries, 2, 9);
        for (rd.light.committed_entries) |entry| try applied_indices.append(allocator, entry.index);

        // Advance applies the Ready page, then the node crashes before applying
        // the boundary entry returned by LightReady.
        var light = try node.advance(rd);
        defer light.deinit(allocator);
        try expectEntryRange(light.committed_entries, 10, 10);
        try std.testing.expectEqual(@as(u64, 9), node.raftConst().raft_log.applied);
        try std.testing.expect(storage.ignored_size_hint_calls > 0);
    }

    {
        var persisted_state = try storage.inner.initialState(allocator);
        defer persisted_state.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 10), persisted_state.hard_state.commit);
    }

    {
        var node = try raft.RawNode.init(allocator, makeConfig(9, page_size), storage.asStorage());
        defer node.deinit();
        try std.testing.expectEqual(@as(u64, 9), node.raftConst().raft_log.applied);
        try std.testing.expectEqual(@as(u64, 10), node.raftConst().raft_log.committed);

        var rd = try node.getReady();
        defer rd.deinit(allocator);
        try expectEntryRange(rd.light.committed_entries, 10, 10);
        for (rd.light.committed_entries) |entry| try applied_indices.append(allocator, entry.index);

        try node.raftPtr().raft_log.commitTo(11);
        var light = try node.advance(rd);
        defer light.deinit(allocator);
        try expectEntryRange(light.committed_entries, 11, 11);
        try std.testing.expectEqual(@as(?u64, 11), light.commit_index);
        try std.testing.expectEqual(@as(u64, 10), node.raftConst().raft_log.applied);
        for (light.committed_entries) |entry| try applied_indices.append(allocator, entry.index);

        node.advanceApply();
        try std.testing.expectEqual(@as(u64, 11), node.raftConst().raft_log.applied);
        try std.testing.expect(!node.hasReady());
    }

    try std.testing.expectEqualSlices(u64, &.{ 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 }, applied_indices.items);
}
