//! Inflights backpressure unit tests.
//!
//! These exercise the `Inflights` / `Progress` windowing primitives directly
//! (fill/pause, slide-on-ack, heartbeat frees-one-slot, probe-state bypass).
//! They are intentionally unit-scoped: they do not drive the Raft message
//! path. End-to-end flow-control coverage through AppendEntries remains
//! tracked as planned upstream cases (etcd TestMsgAppFlowControl*).

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;
const MemoryStorage = raft.MemoryStorage;
const Config = raft.Config;
const Entry = raft.Entry;

fn makeConfig(id: u64) Config {
    var c = raft.defaultConfig();
    c.id = id;
    c.election_tick = 10;
    c.heartbeat_tick = 1;
    c.election_timeout_seed = id * 17;
    c.max_inflight_messages = 2; // Small window for testing.
    return c;
}

test "flow: inflight window fills and pauses" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    const v = try allocator.dupe(u64, &.{ 1, 2 });
    var cs = raft.ConfState{ .voters = v };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    node.state = .leader;
    node.term = 1;
    node.leader_id = 1;

    const pr = node.progress_tracker.getPtr(2).?;
    pr.becomeReplicate();
    pr.matched = 5;
    pr.next_idx = 6;

    // Fill the inflight window (max_inflight=2).
    try pr.inflights.add(allocator, 6);
    try pr.inflights.add(allocator, 7);

    // Window is full → isPaused returns true.
    try std.testing.expect(pr.inflights.full());
    try std.testing.expect(pr.isPaused());
}

test "flow: append-response slides window forward" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    const v = try allocator.dupe(u64, &.{ 1, 2 });
    var cs = raft.ConfState{ .voters = v };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    node.state = .leader;
    node.term = 1;
    node.leader_id = 1;

    const pr = node.progress_tracker.getPtr(2).?;
    pr.becomeReplicate();
    pr.matched = 5;
    pr.next_idx = 6;

    // Fill the window.
    try pr.inflights.add(allocator, 6);
    try pr.inflights.add(allocator, 7);
    try std.testing.expect(pr.inflights.full());

    // Simulate append-response acknowledging index 6.
    pr.inflights.freeTo(allocator, 6);
    try std.testing.expect(!pr.inflights.full());

    // Now there's room for one more.
    try pr.inflights.add(allocator, 8);
    try std.testing.expect(pr.inflights.full());
}

test "flow: heartbeat-response frees one slot" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    const v = try allocator.dupe(u64, &.{ 1, 2 });
    var cs = raft.ConfState{ .voters = v };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    node.state = .leader;
    node.term = 1;
    node.leader_id = 1;

    const pr = node.progress_tracker.getPtr(2).?;
    pr.becomeReplicate();
    pr.matched = 5;
    pr.next_idx = 6;

    // Fill the window.
    try pr.inflights.add(allocator, 6);
    try pr.inflights.add(allocator, 7);
    try std.testing.expect(pr.inflights.full());

    // Simulate heartbeat-response: freeFirstOne.
    pr.inflights.freeFirstOne(allocator);
    try std.testing.expect(!pr.inflights.full());

    // One slot freed.
    try pr.inflights.add(allocator, 8);
    try std.testing.expect(pr.inflights.full());
}

test "flow: probe state does not use inflights" {
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    const v = try allocator.dupe(u64, &.{ 1, 2 });
    var cs = raft.ConfState{ .voters = v };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);

    var node = try raft.Raft.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    node.state = .leader;
    node.term = 1;
    node.leader_id = 1;

    const pr = node.progress_tracker.getPtr(2).?;
    pr.becomeProbe();
    try std.testing.expectEqual(@as(usize, 0), pr.inflights.count);

    // In probe state, isPaused depends on the paused flag, not inflights.
    pr.paused = false;
    try std.testing.expect(!pr.isPaused());

    pr.paused = true;
    try std.testing.expect(pr.isPaused());
}
