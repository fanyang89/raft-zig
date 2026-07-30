const std = @import("std");
const options = @import("raft_zig_options");
const grpc_log = @import("grpc_lite").log;

pub const Kind = enum {
    applied_exceeds_committed,
    committed_exceeds_last_index,
    persisted_exceeds_last_index,
    unstable_range_overflows,
    unstable_entry_index_mismatch,
    unstable_snapshot_offset_mismatch,
    leader_id_mismatch,
    missing_progress,
    unexpected_progress,
    learner_is_voter,
    learner_next_invalid,
    non_joint_state_invalid,
    progress_next_index_invalid,
    progress_match_exceeds_log,
    progress_inflights_invalid,
};

pub const Violation = struct {
    kind: Kind,
    peer_id: u64 = 0,
    expected: u64 = 0,
    actual: u64 = 0,
};

pub fn checkRaft(raft: anytype) ?Violation {
    const raft_log = &raft.raft_log;
    const unstable = &raft_log.unstable;
    const unstable_len = std.math.cast(u64, unstable.entries.items.len) orelse return .{
        .kind = .unstable_range_overflows,
    };
    const unstable_end = @addWithOverflow(unstable.offset, unstable_len);
    if (unstable_end[1] != 0) return .{
        .kind = .unstable_range_overflows,
        .actual = unstable.offset,
    };
    for (unstable.entries.items, 0..) |entry, i| {
        const expected = unstable.offset + @as(u64, @intCast(i));
        if (entry.index != expected) return .{
            .kind = .unstable_entry_index_mismatch,
            .expected = expected,
            .actual = entry.index,
        };
    }
    if (unstable.snapshot) |snapshot| {
        if (snapshot.metadata.index == std.math.maxInt(u64) or
            unstable.offset != snapshot.metadata.index + 1)
        {
            return .{
                .kind = .unstable_snapshot_offset_mismatch,
                .expected = snapshot.metadata.index,
                .actual = unstable.offset,
            };
        }
    }

    const last_index = raft_log.lastIndex();
    if (checkLogOrder(raft_log)) |violation| return violation;
    if (raft_log.committed > last_index) return .{
        .kind = .committed_exceeds_last_index,
        .expected = last_index,
        .actual = raft_log.committed,
    };
    if (raft_log.persisted > last_index) return .{
        .kind = .persisted_exceeds_last_index,
        .expected = last_index,
        .actual = raft_log.persisted,
    };

    if ((raft.state == .leader and raft.leader_id != raft.id) or
        ((raft.state == .candidate or raft.state == .pre_candidate) and raft.leader_id != 0))
    {
        return .{
            .kind = .leader_id_mismatch,
            .peer_id = raft.id,
            .expected = if (raft.state == .leader) raft.id else 0,
            .actual = raft.leader_id,
        };
    }

    const tracker = &raft.progress_tracker;
    const conf = &tracker.conf;
    if (checkConfiguredPeers(conf.voters.incoming.voters, tracker)) |violation| return violation;
    if (checkConfiguredPeers(conf.voters.outgoing.voters, tracker)) |violation| return violation;

    var learners = conf.learners.keyIterator();
    while (learners.next()) |key| {
        const id = key.*;
        if (!tracker.progress.contains(id)) return missingProgress(id);
        if (conf.voters.contains(id)) return .{ .kind = .learner_is_voter, .peer_id = id };
    }

    var learners_next = conf.learners_next.keyIterator();
    while (learners_next.next()) |key| {
        const id = key.*;
        if (!tracker.progress.contains(id)) return missingProgress(id);
        if (!conf.voters.outgoing.contains(id) or conf.voters.incoming.contains(id) or conf.learners.contains(id)) {
            return .{ .kind = .learner_next_invalid, .peer_id = id };
        }
    }

    if (conf.voters.outgoing.isEmpty() and (conf.learners_next.count() != 0 or conf.auto_leave)) {
        return .{ .kind = .non_joint_state_invalid };
    }

    var progress_it = tracker.progress.map.iterator();
    while (progress_it.next()) |entry| {
        const id = entry.key_ptr.*;
        const progress = entry.value_ptr;
        if (!conf.voters.contains(id) and !conf.learners.contains(id) and !conf.learners_next.contains(id)) {
            return .{ .kind = .unexpected_progress, .peer_id = id };
        }
        if (progress.next_idx == 0 or
            (progress.state != .snapshot and progress.next_idx <= progress.matched))
        {
            return .{
                .kind = .progress_next_index_invalid,
                .peer_id = id,
                .expected = progress.matched +| 1,
                .actual = progress.next_idx,
            };
        }
        if (raft.state == .leader and progress.matched > last_index) return .{
            .kind = .progress_match_exceeds_log,
            .peer_id = id,
            .expected = last_index,
            .actual = progress.matched,
        };
        if (!validInflights(progress)) return .{
            .kind = .progress_inflights_invalid,
            .peer_id = id,
            .expected = progress.inflights.capacity,
            .actual = progress.inflights.count,
        };
    }

    return null;
}

pub fn assertRaft(raft: anytype) void {
    if (!options.invariant_checks) return;
    if (checkRaft(raft)) |violation| {
        // KCOV_EXCL_START
        grpc_log.err(
            @src(),
            "raft invariant failed: {s}, peer={}, expected={}, actual={}",
            .{ @tagName(violation.kind), violation.peer_id, violation.expected, violation.actual },
        );
        @panic("raft invariant failed");
        // KCOV_EXCL_STOP
    }
}

fn checkConfiguredPeers(peers: anytype, tracker: anytype) ?Violation {
    var it = peers.keyIterator();
    while (it.next()) |key| {
        if (!tracker.progress.contains(key.*)) return missingProgress(key.*);
    }
    return null;
}

fn missingProgress(id: u64) Violation {
    return .{ .kind = .missing_progress, .peer_id = id };
}

fn validInflights(progress: anytype) bool {
    const inflights = &progress.inflights;
    if (inflights.count > inflights.capacity or inflights.count > inflights.buffer.items.len) return false;
    if (inflights.buffer.items.len > inflights.capacity) return false;
    if (progress.state != .replicate and inflights.count != 0) return false;
    if (inflights.count > 0 and
        (inflights.capacity == 0 or inflights.start >= inflights.capacity or inflights.start >= inflights.buffer.items.len)) return false;
    if (inflights.incoming_capacity) |incoming| {
        if (incoming >= inflights.capacity or inflights.count == 0) return false;
    }
    var i: usize = 1;
    var previous = if (inflights.count == 0) 0 else inflights.buffer.items[inflights.start];
    while (i < inflights.count) : (i += 1) {
        const index = (inflights.start + i) % inflights.capacity;
        const current = inflights.buffer.items[index];
        if (current <= previous) return false;
        previous = current;
    }
    return true;
}

// KCOV_EXCL_START
test "fast invariant violation carries index details" {
    const Log = struct {
        applied: u64,
        committed: u64,
    };

    const violation = checkLogOrder(Log{ .applied = 4, .committed = 3 }).?;
    try std.testing.expectEqual(Kind.applied_exceeds_committed, violation.kind);
    try std.testing.expectEqual(@as(u64, 3), violation.expected);
    try std.testing.expectEqual(@as(u64, 4), violation.actual);
}

test "fast invariant checker accepts a valid raft state" {
    const Progress = @import("progress.zig").Progress;
    const ProgressTracker = @import("progress_tracker.zig").ProgressTracker;
    const Entry = struct { index: u64 };
    const Snapshot = struct { metadata: struct { index: u64 } };
    const MockLog = struct {
        applied: u64 = 0,
        committed: u64 = 0,
        persisted: u64 = 0,
        unstable: struct {
            entries: std.ArrayList(Entry) = .empty,
            offset: u64 = 1,
            snapshot: ?Snapshot = null,
        } = .{},
        last_index: u64 = 0,

        fn lastIndex(self: *const @This()) u64 {
            return self.last_index;
        }
    };
    const Role = enum { follower, candidate, pre_candidate, leader };
    const MockRaft = struct {
        id: u64 = 1,
        state: Role = .follower,
        leader_id: u64 = 0,
        raft_log: MockLog = .{},
        progress_tracker: ProgressTracker,
    };

    var tracker = ProgressTracker.init(std.testing.allocator, 4);
    try tracker.conf.voters.incoming.add(1);
    try tracker.progress.put(1, Progress.init(std.testing.allocator, 1, 4));

    var raft = MockRaft{ .progress_tracker = tracker };
    defer raft.progress_tracker.deinit();
    defer raft.raft_log.unstable.entries.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?Violation, null), checkRaft(&raft));

    _ = raft.progress_tracker.progress.remove(1);
    const violation = checkRaft(&raft).?;
    try std.testing.expectEqual(Kind.missing_progress, violation.kind);
    try std.testing.expectEqual(@as(u64, 1), violation.peer_id);
}

test "fast invariant checker diagnoses corrupt raft state" {
    const Progress = @import("progress.zig").Progress;
    const ProgressTracker = @import("progress_tracker.zig").ProgressTracker;
    const Entry = struct { index: u64 };
    const Snapshot = struct { metadata: struct { index: u64 } };
    const MockLog = struct {
        applied: u64 = 0,
        committed: u64 = 0,
        persisted: u64 = 0,
        unstable: struct {
            entries: std.ArrayList(Entry) = .empty,
            offset: u64 = 1,
            snapshot: ?Snapshot = null,
        } = .{},
        last_index: u64 = 0,

        fn lastIndex(self: *const @This()) u64 {
            return self.last_index;
        }
    };
    const Role = enum { follower, candidate, pre_candidate, leader };
    const MockRaft = struct {
        id: u64 = 1,
        state: Role = .follower,
        leader_id: u64 = 0,
        raft_log: MockLog = .{},
        progress_tracker: ProgressTracker,
    };

    var tracker = ProgressTracker.init(std.testing.allocator, 4);
    try tracker.conf.voters.incoming.add(1);
    try tracker.progress.put(1, Progress.init(std.testing.allocator, 1, 4));
    var raft = MockRaft{ .progress_tracker = tracker };
    defer raft.progress_tracker.deinit();
    defer raft.raft_log.unstable.entries.deinit(std.testing.allocator);

    raft.raft_log.unstable.offset = std.math.maxInt(u64);
    try raft.raft_log.unstable.entries.append(std.testing.allocator, .{ .index = std.math.maxInt(u64) });
    try std.testing.expectEqual(Kind.unstable_range_overflows, checkRaft(&raft).?.kind);
    raft.raft_log.unstable.entries.clearRetainingCapacity();
    raft.raft_log.unstable.offset = 1;

    try raft.raft_log.unstable.entries.append(std.testing.allocator, .{ .index = 2 });
    var violation = checkRaft(&raft).?;
    try std.testing.expectEqual(Kind.unstable_entry_index_mismatch, violation.kind);
    try std.testing.expectEqual(@as(u64, 1), violation.expected);
    try std.testing.expectEqual(@as(u64, 2), violation.actual);
    raft.raft_log.unstable.entries.clearRetainingCapacity();

    raft.raft_log.unstable.snapshot = .{ .metadata = .{ .index = 2 } };
    violation = checkRaft(&raft).?;
    try std.testing.expectEqual(Kind.unstable_snapshot_offset_mismatch, violation.kind);
    try std.testing.expectEqual(@as(u64, 2), violation.expected);
    try std.testing.expectEqual(@as(u64, 1), violation.actual);
    raft.raft_log.unstable.snapshot = null;

    raft.raft_log.committed = 1;
    violation = checkRaft(&raft).?;
    try std.testing.expectEqual(Kind.committed_exceeds_last_index, violation.kind);
    try std.testing.expectEqual(@as(u64, 1), violation.actual);
    raft.raft_log.committed = 0;
    raft.raft_log.persisted = 1;
    try std.testing.expectEqual(Kind.persisted_exceeds_last_index, checkRaft(&raft).?.kind);
    raft.raft_log.persisted = 0;

    raft.state = .leader;
    raft.leader_id = 2;
    violation = checkRaft(&raft).?;
    try std.testing.expectEqual(Kind.leader_id_mismatch, violation.kind);
    try std.testing.expectEqual(@as(u64, 1), violation.peer_id);
    try std.testing.expectEqual(@as(u64, 1), violation.expected);
    try std.testing.expectEqual(@as(u64, 2), violation.actual);
    raft.state = .follower;
    raft.leader_id = 0;

    try raft.progress_tracker.conf.learners.put(1, {});
    try std.testing.expectEqual(Kind.learner_is_voter, checkRaft(&raft).?.kind);
    _ = raft.progress_tracker.conf.learners.remove(1);

    try raft.progress_tracker.conf.learners_next.put(1, {});
    try std.testing.expectEqual(Kind.learner_next_invalid, checkRaft(&raft).?.kind);
    _ = raft.progress_tracker.conf.learners_next.remove(1);
    raft.progress_tracker.conf.auto_leave = true;
    try std.testing.expectEqual(Kind.non_joint_state_invalid, checkRaft(&raft).?.kind);
    raft.progress_tracker.conf.auto_leave = false;

    try raft.progress_tracker.progress.put(2, Progress.init(std.testing.allocator, 1, 4));
    try std.testing.expectEqual(Kind.unexpected_progress, checkRaft(&raft).?.kind);
    _ = raft.progress_tracker.progress.remove(2);

    const progress = raft.progress_tracker.getPtr(1).?;
    progress.next_idx = 0;
    violation = checkRaft(&raft).?;
    try std.testing.expectEqual(Kind.progress_next_index_invalid, violation.kind);
    try std.testing.expectEqual(@as(u64, 1), violation.expected);
    try std.testing.expectEqual(@as(u64, 0), violation.actual);
    progress.next_idx = 2;
    progress.matched = 1;
    raft.state = .leader;
    raft.leader_id = 1;
    violation = checkRaft(&raft).?;
    try std.testing.expectEqual(Kind.progress_match_exceeds_log, violation.kind);
    try std.testing.expectEqual(@as(u64, 0), violation.expected);
    try std.testing.expectEqual(@as(u64, 1), violation.actual);
    raft.state = .follower;
    raft.leader_id = 0;
    progress.matched = 0;
    progress.next_idx = 1;
    progress.inflights.count = 1;
    violation = checkRaft(&raft).?;
    try std.testing.expectEqual(Kind.progress_inflights_invalid, violation.kind);
    try std.testing.expectEqual(@as(u64, 4), violation.expected);
    try std.testing.expectEqual(@as(u64, 1), violation.actual);
}
// KCOV_EXCL_STOP

fn checkLogOrder(log: anytype) ?Violation {
    if (log.applied > log.committed) return .{
        .kind = .applied_exceeds_committed,
        .expected = log.committed,
        .actual = log.applied,
    };
    return null;
}
