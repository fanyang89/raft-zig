//! Raft snapshot lifecycle tests.
//!
//! Ports key scenarios from `ref/raftpp/tests/raft_snap_test.cc` — the
//! snapshot installation, abort, and tracker-rebuild paths that are currently
//! 100% uncovered in raft-zig.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;
const MemoryStorage = raft.MemoryStorage;
const Config = raft.Config;
const Entry = raft.Entry;
const Message = raft.Message;
const Snapshot = raft.Snapshot;
const ProgressState = raft.ProgressState;

fn makeConfig(id: u64) Config {
    var c = raft.defaultConfig();
    c.id = id;
    c.election_tick = 10;
    c.heartbeat_tick = 1;
    c.election_timeout_seed = id * 13;
    return c;
}

fn newStorage(voters: []const u64) !MemoryStorage {
    var storage = MemoryStorage.init();
    const v = try allocator.dupe(u64, voters);
    var cs = raft.ConfState{ .voters = v };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);
    return storage;
}

test "snap: restore snapshot updates tracker configuration" {
    var storage = try newStorage(&.{ 1, 2, 3 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    // Original voters are {1, 2, 3}.
    try std.testing.expect(node.progress_tracker.conf.voters.incoming.contains(1));
    try std.testing.expect(node.progress_tracker.conf.voters.incoming.contains(2));
    try std.testing.expect(!node.progress_tracker.conf.voters.incoming.contains(5));

    // Restore a snapshot with voters {1, 4, 5}.
    const new_voters = try allocator.dupe(u64, &.{ 1, 4, 5 });
    var snap = Snapshot{
        .metadata = .{
            .index = 11,
            .term = 1,
            .conf_state = .{ .voters = new_voters },
        },
    };
    defer snap.deinit(allocator);

    _ = node.restoreSnapshot(snap);

    // Tracker should now have voters {1, 4, 5}.
    try std.testing.expect(node.progress_tracker.conf.voters.incoming.contains(1));
    try std.testing.expect(node.progress_tracker.conf.voters.incoming.contains(4));
    try std.testing.expect(node.progress_tracker.conf.voters.incoming.contains(5));
    try std.testing.expect(!node.progress_tracker.conf.voters.incoming.contains(2));
}

test "snap: restore snapshot updates tracker learners" {
    var storage = try newStorage(&.{ 1, 2 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try std.testing.expectEqual(@as(usize, 0), node.progress_tracker.conf.learners.count());

    // Snapshot must include node 1 in voters (restoreSnapshot checks id).
    const voters = try allocator.dupe(u64, &.{1});
    const learners = try allocator.dupe(u64, &.{5});
    var snap = Snapshot{
        .metadata = .{
            .index = 10,
            .term = 1,
            .conf_state = .{ .voters = voters, .learners = learners },
        },
    };
    defer snap.deinit(allocator);

    _ = node.restoreSnapshot(snap);

    try std.testing.expect(node.progress_tracker.conf.learners.contains(5));
    try std.testing.expect(node.progress_tracker.progress.contains(5));
}

test "snap: restore snapshot advances committed index" {
    var storage = try newStorage(&.{1});
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try std.testing.expectEqual(@as(u64, 0), node.raft_log.committed);

    var snap = Snapshot{
        .metadata = .{ .index = 100, .term = 5, .conf_state = .{ .voters = try allocator.dupe(u64, &.{1}) } },
    };
    defer snap.deinit(allocator);

    _ = node.restoreSnapshot(snap);

    try std.testing.expectEqual(@as(u64, 100), node.raft_log.committed);
    try std.testing.expectEqual(@as(u64, 101), node.raft_log.unstable.offset);
}

test "snap: pending snapshot pauses replication" {
    var storage = try newStorage(&.{ 1, 2, 3 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    // Make node 1 a real leader so appendEntry/broadcastAppend emit messages.
    node.becomeCandidate();
    try node.becomeLeader();

    // Put node 2's progress into snapshot state (paused); node 3 stays normal.
    const pr2 = node.progress_tracker.getPtr(2).?;
    pr2.state = .snapshot;
    pr2.pending_snapshot = 10;
    try std.testing.expect(pr2.isPaused());

    // Propose and broadcast. Replication proceeds to node 3 but must skip the
    // paused node 2.
    var entries = [_]Entry{.{ .term = node.term, .index = node.raft_log.lastIndex() + 1 }};
    _ = try node.appendEntry(&entries);
    try node.broadcastAppend();

    var got_append_to_3 = false;
    for (node.messages.items) |m| {
        if (m.to == 3 and m.msg_type == .append) got_append_to_3 = true;
        if (m.to == 2) try std.testing.expect(m.msg_type != .append);
    }
    try std.testing.expect(got_append_to_3);
}

test "snap: snapshot failure resets to probe" {
    var storage = try newStorage(&.{ 1, 2 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    node.state = .leader;
    node.term = 1;
    node.leader_id = 1;

    const pr = node.progress_tracker.getPtr(2).?;
    pr.state = .snapshot;
    pr.pending_snapshot = 10;
    pr.recent_active = true;

    // Send MSG_SNAP_STATUS with reject=true.
    var msg = Message{
        .msg_type = .snap_status,
        .from = 2,
        .to = 1,
        .reject = true,
    };
    try node.step(&msg);
    msg.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 0), pr.pending_snapshot);
    try std.testing.expectEqual(ProgressState.probe, pr.state);
}

test "snap: snapshot succeed keeps probe but paused" {
    var storage = try newStorage(&.{ 1, 2 });
    defer storage.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    node.state = .leader;
    node.term = 1;
    node.leader_id = 1;

    const pr = node.progress_tracker.getPtr(2).?;
    pr.state = .snapshot;
    pr.pending_snapshot = 10;
    pr.recent_active = true;

    var msg = Message{
        .msg_type = .snap_status,
        .from = 2,
        .to = 1,
        .reject = false,
    };
    try node.step(&msg);
    msg.deinit(allocator);

    try std.testing.expectEqual(ProgressState.probe, pr.state);
    try std.testing.expect(pr.paused);
}
