//! ProgressTracker + Progress integration tests.
//!
//! Per-follower Progress behavior is covered inline in `src/progress.zig`.
//! This file adds higher-level scenarios that exercise Progress inside a
//! ProgressTracker, mirroring the integration cases in raftpp's
//! `tests/progress_test.cc`.

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;
const Progress = raft.Progress;
const ProgressTracker = raft.ProgressTracker;
const ProgressState = raft.ProgressState;

test "progress tracker quorum recently active" {
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();

    // Bootstrap {1, 2, 3}.
    {
        const cc = [_]raft.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 1 }};
        var r = try raft.ConfChanger.init(&tr).simple(&cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }
    {
        const cc = [_]raft.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 2 }};
        var r = try raft.ConfChanger.init(&tr).simple(&cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }
    {
        const cc = [_]raft.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 3 }};
        var r = try raft.ConfChanger.init(&tr).simple(&cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }

    // Initially all peers are marked recent_active by applyConf, so the first
    // sweep from node 1's perspective sees {1,2,3} → quorum.
    try std.testing.expect(try tr.quorumRecentlyActive(1));

    // Second sweep: peers 2 and 3 were flipped to inactive by the prior call.
    // Only node 1 is in the active set → no quorum (need majority of {1,2,3}).
    try std.testing.expect(!try tr.quorumRecentlyActive(1));
}

test "progress tracker max committed index aggregates peers" {
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();

    const ids = [_]u64{ 1, 2, 3 };
    for (ids) |id| {
        const cc = [_]raft.ConfChangeSingle{.{ .change_type = .add_node, .node_id = id }};
        var r = try raft.ConfChanger.init(&tr).simple(&cc);
        defer r.deinit(allocator);
        try tr.applyConf(r.conf, r.changes, 0);
    }

    // Set matched indexes: 1→10, 2→20, 3→30. Sorted desc: 30,20,10. Quorum
    // of 3 is 2 → pick 2nd highest = 20.
    tr.at(1).matched = 10;
    tr.at(2).matched = 20;
    tr.at(3).matched = 30;

    const r = tr.maxCommittedIndex();
    try std.testing.expectEqual(@as(u64, 20), r.index);
}

test "progress state transitions through probe → replicate → snapshot" {
    var p = Progress.init(allocator, 1, 8);
    defer p.deinit();
    try std.testing.expectEqual(ProgressState.probe, p.state);

    p.matched = 5;
    p.becomeReplicate();
    try std.testing.expectEqual(ProgressState.replicate, p.state);
    try std.testing.expectEqual(@as(u64, 6), p.next_idx);

    p.becomeSnapshot(20);
    try std.testing.expectEqual(ProgressState.snapshot, p.state);
    try std.testing.expectEqual(@as(u64, 20), p.pending_snapshot);
    try std.testing.expect(p.isPaused());

    p.matched = 25;
    try std.testing.expect(p.isSnapshotCaughtUp());

    p.becomeProbe();
    try std.testing.expectEqual(ProgressState.probe, p.state);
    // After snapshot with pending=20 and matched=25, probe picks max(26, 21)=26.
    try std.testing.expectEqual(@as(u64, 26), p.next_idx);
}
