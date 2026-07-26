// Copyright 2015 The etcd Authors
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

pub const inventory_target = "tests/upstream/dragonboat/cases/ownership_test.zig";

const EntrySpec = struct {
    index: u64,
    term: u64,
    data: []const u8,
};

fn makeEntries(specs: []const EntrySpec) ![]raft.Entry {
    const entries = try allocator.alloc(raft.Entry, specs.len);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    for (entries, specs) |*entry, spec| {
        entry.* = .{
            .index = spec.index,
            .term = spec.term,
            .data = try allocator.dupe(u8, spec.data),
        };
        initialized += 1;
    }
    return entries;
}

fn expectEntries(entries: []const raft.Entry, specs: []const EntrySpec) !void {
    try std.testing.expectEqual(specs.len, entries.len);
    for (entries, specs) |entry, spec| {
        try std.testing.expectEqual(spec.index, entry.index);
        try std.testing.expectEqual(spec.term, entry.term);
        try std.testing.expectEqualStrings(spec.data, entry.data);
    }
}

const MergePath = enum {
    tail_truncate,
    before_offset,
    at_offset,
};

fn deinitEntries(entries: []raft.Entry) void {
    for (entries) |*entry| entry.deinit(allocator);
    allocator.free(entries);
}

fn runMergeScenario(
    path: MergePath,
    replacement: []const EntrySpec,
    expected: []const EntrySpec,
) !void {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var snapshot = raft.Snapshot{ .metadata = .{
        .index = 4,
        .term = 1,
        .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2 }) },
    } };
    defer snapshot.deinit(allocator);
    try storage.applySnapshot(allocator, snapshot);

    var config = raft.defaultConfig();
    config.id = 1;
    config.load_state_on_startup = true;
    config.election_timeout_seed = 1;
    var node = try raft.RawNode.init(allocator, config, storage.asStorage());
    defer node.deinit();

    const original = [_]EntrySpec{
        .{ .index = 5, .term = 1, .data = "five-old" },
        .{ .index = 6, .term = 1, .data = "six-old" },
        .{ .index = 7, .term = 1, .data = "seven-old" },
    };
    try node.step(.{
        .msg_type = .append,
        .from = 2,
        .to = 1,
        .term = 1,
        .index = 4,
        .log_term = 1,
        .commit = 4,
        .entries = try makeEntries(&original),
    });

    {
        var old_ready = try node.getReady();
        defer old_ready.deinit(allocator);
        try expectEntries(old_ready.entries, &original);

        const unstable = &node.raftPtr().raft_log.unstable;
        try std.testing.expectEqual(@as(u64, 5), unstable.offset);
        switch (path) {
            .tail_truncate => {
                try std.testing.expect(replacement[0].index > unstable.offset);
                try std.testing.expect(replacement[0].index < unstable.offset + unstable.entries.items.len);
                try node.step(.{
                    .msg_type = .append,
                    .from = 2,
                    .to = 1,
                    .term = 2,
                    .index = replacement[0].index - 1,
                    .log_term = 1,
                    .commit = 4,
                    .entries = try makeEntries(replacement),
                });
            },
            .before_offset, .at_offset => {
                if (path == .before_offset) {
                    try std.testing.expect(replacement[0].index < unstable.offset);
                } else {
                    try std.testing.expectEqual(unstable.offset, replacement[0].index);
                }
                const entries = try makeEntries(replacement);
                defer deinitEntries(entries);
                unstable.truncateAndAppend(entries);
            },
        }

        try expectEntries(old_ready.entries, &original);
        try expectEntries(unstable.entries.items, expected);
    }

    try expectEntries(node.raftConst().raft_log.unstable.entries.items, expected);
    {
        var current_ready = try node.getReady();
        defer current_ready.deinit(allocator);
        try expectEntries(current_ready.entries, expected);
    }
    try expectEntries(node.raftConst().raft_log.unstable.entries.items, expected);
}

test "Dragonboat: inmemory_etcd_test.go::TestEntryMergeThreadSafety" {
    try runMergeScenario(
        .tail_truncate,
        &.{
            .{ .index = 7, .term = 2, .data = "seven-tail" },
            .{ .index = 8, .term = 2, .data = "eight-tail" },
        },
        &.{
            .{ .index = 5, .term = 1, .data = "five-old" },
            .{ .index = 6, .term = 1, .data = "six-old" },
            .{ .index = 7, .term = 2, .data = "seven-tail" },
            .{ .index = 8, .term = 2, .data = "eight-tail" },
        },
    );
    try runMergeScenario(
        .before_offset,
        &.{
            .{ .index = 4, .term = 2, .data = "four-before" },
            .{ .index = 5, .term = 2, .data = "five-before" },
        },
        &.{
            .{ .index = 4, .term = 2, .data = "four-before" },
            .{ .index = 5, .term = 2, .data = "five-before" },
        },
    );
    try runMergeScenario(
        .at_offset,
        &.{
            .{ .index = 5, .term = 2, .data = "five-at" },
            .{ .index = 6, .term = 2, .data = "six-at" },
        },
        &.{
            .{ .index = 5, .term = 2, .data = "five-at" },
            .{ .index = 6, .term = 2, .data = "six-at" },
        },
    );
}
