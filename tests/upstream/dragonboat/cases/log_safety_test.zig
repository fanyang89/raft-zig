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

pub const inventory_target = "tests/upstream/dragonboat/cases/log_safety_test.zig";

test "Dragonboat: logentry_test.go::TestLogAppendPanicWhenAppendingCommittedEntry" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    const entries = [_]raft.Entry{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
        .{ .index = 3, .term = 2 },
        .{ .index = 4, .term = 3 },
    };
    try storage.setEntries(allocator, &entries);

    var log = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer log.deinit();
    try log.commitTo(2);
    const old_last_index = log.lastIndex();
    const old_committed = log.committed;
    const old_offset = log.unstable.offset;

    try std.testing.expectError(error.Fatal, log.append(&entries));
    try std.testing.expectEqual(old_last_index, log.lastIndex());
    try std.testing.expectEqual(old_committed, log.committed);
    try std.testing.expectEqual(old_offset, log.unstable.offset);
    try std.testing.expectEqual(@as(usize, 0), log.unstable.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), try log.term(2));
}

test "Dragonboat: entryutils_test.go::TestCheckEntriesToAppendWillPanicWhenTermMovesBack" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    const prefix = [_]raft.Entry{.{ .index = 100, .term = 100 }};
    try storage.setEntries(allocator, &prefix);
    var log = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer log.deinit();

    const moves_back = [_]raft.Entry{.{ .index = 101, .term = 99 }};
    try std.testing.expectError(error.Fatal, log.append(&moves_back));
    try std.testing.expectEqual(@as(u64, 100), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 100), try log.term(100));

    const internal_move_back = [_]raft.Entry{
        .{ .index = 101, .term = 101 },
        .{ .index = 102, .term = 100 },
    };
    try std.testing.expectError(error.Fatal, log.append(&internal_move_back));
    try std.testing.expectEqual(@as(u64, 100), log.lastIndex());

    var conflict_storage = raft.MemoryStorage.init();
    defer conflict_storage.deinit(allocator);
    const existing = [_]raft.Entry{
        .{ .index = 100, .term = 90 },
        .{ .index = 101, .term = 100 },
        .{ .index = 102, .term = 100 },
    };
    try conflict_storage.setEntries(allocator, &existing);
    var conflict_log = try raft.RaftLog.init(allocator, conflict_storage.asStorage(), 0);
    defer conflict_log.deinit();
    const replacement = [_]raft.Entry{
        .{ .index = 101, .term = 99 },
        .{ .index = 102, .term = 101 },
    };
    try std.testing.expectEqual(@as(u64, 102), try conflict_log.append(&replacement));
    try std.testing.expectEqual(@as(u64, 90), try conflict_log.term(100));
    try std.testing.expectEqual(@as(u64, 99), try conflict_log.term(101));
    try std.testing.expectEqual(@as(u64, 101), try conflict_log.term(102));
    try std.testing.expectEqual(@as(u64, 100), conflict_log.persisted);
}

test "Dragonboat: entryutils_test.go::TestCheckEntriesToAppendWillPanicWhenIndexHasHole" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    const prefix = [_]raft.Entry{.{ .index = 100, .term = 100 }};
    try storage.setEntries(allocator, &prefix);
    var log = try raft.RaftLog.init(allocator, storage.asStorage(), 0);
    defer log.deinit();

    const leading_hole = [_]raft.Entry{.{ .index = 102, .term = 101 }};
    try std.testing.expectError(error.Fatal, log.append(&leading_hole));
    const rejected = try log.maybeAppend(99, 999, 100, &leading_hole);
    try std.testing.expect(!rejected.term_matched);

    const internal_hole = [_]raft.Entry{
        .{ .index = 101, .term = 101 },
        .{ .index = 103, .term = 101 },
    };
    try std.testing.expectError(error.Fatal, log.append(&internal_hole));
    try std.testing.expectError(error.Fatal, log.maybeAppend(100, 100, 100, &leading_hole));
    try std.testing.expectEqual(@as(u64, 100), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 100), log.persisted);

    var compacted_storage = raft.MemoryStorage.init();
    defer compacted_storage.deinit(allocator);
    var snap = raft.Snapshot{ .metadata = .{ .index = 100, .term = 100 } };
    defer snap.deinit(allocator);
    try compacted_storage.applySnapshot(allocator, snap);
    var compacted_log = try raft.RaftLog.init(allocator, compacted_storage.asStorage(), 0);
    defer compacted_log.deinit();
    try std.testing.expect(!(try compacted_log.matchTerm(99, 0)));
    try std.testing.expect(try compacted_log.matchTerm(100, 100));
    const snapshot_term_regression = [_]raft.Entry{.{ .index = 101, .term = 99 }};
    try std.testing.expectError(error.Fatal, compacted_log.append(&snapshot_term_regression));

    var extreme_storage = raft.MemoryStorage.init();
    defer extreme_storage.deinit(allocator);
    var extreme_snapshot = raft.Snapshot{ .metadata = .{
        .index = std.math.maxInt(u64) - 1,
        .term = 1,
    } };
    defer extreme_snapshot.deinit(allocator);
    try extreme_storage.applySnapshot(allocator, extreme_snapshot);
    var extreme_log = try raft.RaftLog.init(allocator, extreme_storage.asStorage(), 0);
    defer extreme_log.deinit();
    const final_entry = [_]raft.Entry{.{ .index = std.math.maxInt(u64), .term = 1 }};
    try std.testing.expectError(error.Fatal, extreme_log.append(&final_entry));
    try std.testing.expectEqual(std.math.maxInt(u64) - 1, extreme_log.lastIndex());
}
