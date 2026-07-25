// Copyright 2019 TiKV Project Authors
// Licensed under the Apache License, Version 2.0.
// Adapted and modified for raft-zig; see ../LICENSE.upstream.

const std = @import("std");
const raft = @import("raft_zig");
const network = @import("raft_test_network");

const allocator = std.testing.allocator;

pub const inventory_target = "tests/upstream/raft_rs/cases/replication_test.zig";

test "raft-rs: handle append entries matrix" {
    const Case = struct {
        term: u64,
        log_term: u64,
        index: u64,
        commit: u64,
        entries: []const raft.Entry = &.{},
        expected_last_index: u64,
        expected_committed: u64,
        expected_reject: bool,
    };
    const cases = [_]Case{
        .{ .term = 2, .log_term = 3, .index = 2, .commit = 3, .expected_last_index = 2, .expected_committed = 0, .expected_reject = true },
        .{ .term = 2, .log_term = 3, .index = 3, .commit = 3, .expected_last_index = 2, .expected_committed = 0, .expected_reject = true },
        .{ .term = 2, .log_term = 1, .index = 1, .commit = 1, .expected_last_index = 2, .expected_committed = 1, .expected_reject = false },
        .{ .term = 2, .log_term = 0, .index = 0, .commit = 1, .entries = &.{.{ .term = 2, .index = 1 }}, .expected_last_index = 1, .expected_committed = 1, .expected_reject = false },
        .{ .term = 2, .log_term = 2, .index = 2, .commit = 3, .entries = &.{ .{ .term = 2, .index = 3 }, .{ .term = 2, .index = 4 } }, .expected_last_index = 4, .expected_committed = 3, .expected_reject = false },
        .{ .term = 2, .log_term = 2, .index = 2, .commit = 4, .entries = &.{.{ .term = 2, .index = 3 }}, .expected_last_index = 3, .expected_committed = 3, .expected_reject = false },
        .{ .term = 2, .log_term = 1, .index = 1, .commit = 4, .entries = &.{.{ .term = 2, .index = 2 }}, .expected_last_index = 2, .expected_committed = 2, .expected_reject = false },
        .{ .term = 1, .log_term = 1, .index = 1, .commit = 3, .expected_last_index = 2, .expected_committed = 1, .expected_reject = false },
        .{ .term = 1, .log_term = 1, .index = 1, .commit = 3, .entries = &.{.{ .term = 2, .index = 2 }}, .expected_last_index = 2, .expected_committed = 2, .expected_reject = false },
        .{ .term = 2, .log_term = 2, .index = 2, .commit = 3, .expected_last_index = 2, .expected_committed = 2, .expected_reject = false },
        .{ .term = 2, .log_term = 2, .index = 2, .commit = 4, .expected_last_index = 2, .expected_committed = 2, .expected_reject = false },
    };

    for (cases) |case| {
        var net = try network.newNetwork(&.{1});
        defer net.deinit();
        const peer = net.getPeer(1).?;
        const initial_entries = [_]raft.Entry{
            .{ .term = 1, .index = 1 },
            .{ .term = 2, .index = 2 },
        };
        try peer.storage.setEntries(allocator, &initial_entries);
        peer.raft.raft_log.persisted = 2;
        peer.raft.raft_log.unstable.offset = 3;
        peer.raft.becomeFollower(2, raft.invalid_id);

        var message_entries: [2]raft.Entry = undefined;
        for (case.entries, 0..) |entry, index| message_entries[index] = entry;
        var message = raft.Message{
            .msg_type = .append,
            .from = 2,
            .to = 1,
            .term = case.term,
            .log_term = case.log_term,
            .index = case.index,
            .commit = case.commit,
            .entries = message_entries[0..case.entries.len],
        };
        try peer.raft.handleAppendEntries(&message);

        try std.testing.expectEqual(case.expected_last_index, peer.raft.raft_log.lastIndex());
        try std.testing.expectEqual(case.expected_committed, peer.raft.raft_log.committed);
        try std.testing.expectEqual(@as(usize, 1), peer.raft.messages.items.len);
        try std.testing.expectEqual(raft.MessageType.append_response, peer.raft.messages.items[0].msg_type);
        try std.testing.expectEqual(case.expected_reject, peer.raft.messages.items[0].reject);
    }
}
