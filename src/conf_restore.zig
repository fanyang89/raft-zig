//! Restore a ProgressTracker from a serialized ConfState.
//!
//! Used after a snapshot install (or fresh start) to rebuild the cluster
//! configuration from a wire-format ConfState.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const conf_changer_mod = @import("conf_changer.zig");
const progress_tracker_mod = @import("progress_tracker.zig");

const Error = error_model.Error;
const ConfState = types.ConfState;
const ConfChangeSingle = types.ConfChangeSingle;
const ConfChangeType = types.ConfChangeType;
const ConfChanger = conf_changer_mod.ConfChanger;
const ProgressTracker = progress_tracker_mod.ProgressTracker;

const log = @import("grpc_lite").log;

/// Split a ConfState into the two change batches needed to reproduce it:
///   * outgoing — AddNode for every voter in `voters_outgoing`.
///   * incoming — AddNode for every voter in `voters` plus the AddNode/
///     AddLearnerNode entries that produce the right learner sets.
///
/// Caller owns both returned slices.
pub fn toConfChangeSingle(
    allocator: std.mem.Allocator,
    cs: ConfState,
) !struct { outgoing: []ConfChangeSingle, incoming: []ConfChangeSingle } {
    var outgoing: std.ArrayList(ConfChangeSingle) = .empty;
    errdefer outgoing.deinit(allocator);
    var incoming: std.ArrayList(ConfChangeSingle) = .empty;
    errdefer incoming.deinit(allocator);

    // outgoing: add every voter in voters_outgoing.
    for (cs.voters_outgoing) |id| {
        try outgoing.append(allocator, .{ .change_type = .add_node, .node_id = id });
    }
    // incoming: remove each voter_outgoing, then add each voter + learner.
    for (cs.voters_outgoing) |id| {
        try incoming.append(allocator, .{ .change_type = .remove_node, .node_id = id });
    }
    for (cs.voters) |id| {
        try incoming.append(allocator, .{ .change_type = .add_node, .node_id = id });
    }
    for (cs.learners) |id| {
        try incoming.append(allocator, .{ .change_type = .add_learner_node, .node_id = id });
    }
    for (cs.learners_next) |id| {
        try incoming.append(allocator, .{ .change_type = .add_learner_node, .node_id = id });
    }

    const outgoing_owned = try outgoing.toOwnedSlice(allocator);
    errdefer allocator.free(outgoing_owned);
    return .{
        .outgoing = outgoing_owned,
        .incoming = try incoming.toOwnedSlice(allocator),
    };
}

/// Rebuild `tracker` so its configuration matches `cs`. `next_idx` is the
/// starting next-index for any new Progress entries.
pub fn restore(
    tracker: *ProgressTracker,
    next_idx: u64,
    cs: ConfState,
) Error!void {
    var rebuilt = ProgressTracker.init(tracker.allocator, tracker.max_inflight);
    errdefer rebuilt.deinit();
    rebuilt.group_commit = tracker.group_commit;
    try restoreEmpty(&rebuilt, next_idx, cs);

    var old = tracker.*;
    tracker.* = rebuilt;
    old.deinit();
}

fn restoreEmpty(tracker: *ProgressTracker, next_idx: u64, cs: ConfState) Error!void {
    const allocator = tracker.allocator;
    const split = toConfChangeSingle(allocator, cs) catch return error.OutOfMemory;
    defer allocator.free(split.outgoing);
    defer allocator.free(split.incoming);

    if (split.outgoing.len == 0) {
        var changer = ConfChanger.init(tracker);
        var pair = changer.checkAndCopy() catch return error.OutOfMemory;
        defer pair.deinit();
        for (split.incoming) |cc| {
            const one = [_]ConfChangeSingle{cc};
            try ConfChanger.applyChanges(&pair.cfg, &pair.prs, &one);
        }
        try conf_changer_mod.checkInvariants(pair.cfg, pair.prs);
        const changes = try pair.prs.changes.toOwnedSlice(allocator);
        defer allocator.free(changes);
        const cfg_copy = pair.cfg.clone() catch return error.OutOfMemory;
        var cfg_clone = cfg_copy;
        defer cfg_clone.deinit();
        try tracker.applyConf(cfg_clone, changes, next_idx);
    } else {
        // Replay outgoing changes via Simple, then enter joint with incoming.
        for (split.outgoing) |cc| {
            const one = [_]ConfChangeSingle{cc};
            var r = ConfChanger.init(tracker).simple(&one) catch |e| {
                log.warn(@src(), "simple failed during restore: {s}", .{@errorName(e)}); // KCOV_EXCL_LINE
                return e;
            };
            defer r.deinit(allocator);
            try tracker.applyConf(r.conf, r.changes, next_idx);
        }
        var r = ConfChanger.init(tracker).enterJoint(cs.auto_leave, split.incoming) catch |e| {
            log.warn(@src(), "enterJoint failed during restore: {s}", .{@errorName(e)});
            return e;
        };
        defer r.deinit(allocator);
        try tracker.applyConf(r.conf, r.changes, next_idx);
    }
}

// KCOV_EXCL_START
test "restore builds a simple config from a ConfState" {
    const allocator = std.testing.allocator;
    var tr = ProgressTracker.init(allocator, 8);
    defer tr.deinit();

    const voters = try allocator.dupe(u64, &.{ 1, 2, 3 });
    const learners = try allocator.dupe(u64, &.{4});
    var cs = ConfState{
        .voters = voters,
        .learners = learners,
    };
    defer cs.deinit(allocator);

    try restore(&tr, 0, cs);

    try std.testing.expectEqual(@as(usize, 3), tr.conf.voters.incoming.count());
    try std.testing.expect(tr.conf.voters.incoming.contains(1));
    try std.testing.expect(tr.conf.voters.incoming.contains(2));
    try std.testing.expect(tr.conf.voters.incoming.contains(3));
    try std.testing.expectEqual(@as(usize, 1), tr.conf.learners.count());
    try std.testing.expect(tr.conf.learners.contains(4));
    try std.testing.expect(tr.conf.voters.outgoing.isEmpty());
}

test "restore replaces an existing configuration" {
    const allocator = std.testing.allocator;
    var tracker = ProgressTracker.init(allocator, 8);
    defer tracker.deinit();
    try restore(&tracker, 1, .{ .voters = @constCast(&[_]u64{ 1, 2 }) });
    try restore(&tracker, 2, .{ .voters = @constCast(&[_]u64{1}) });

    try std.testing.expectEqual(@as(usize, 1), tracker.progress.count());
    try std.testing.expect(tracker.getPtr(1) != null);
    try std.testing.expect(tracker.getPtr(2) == null);
}

test "toConfChangeSingle splits outgoing and incoming streams" {
    const allocator = std.testing.allocator;
    const voters = try allocator.dupe(u64, &.{ 1, 2 });
    const voters_outgoing = try allocator.dupe(u64, &.{ 1, 2, 3 });
    const learners = try allocator.dupe(u64, &.{4});
    var cs = ConfState{
        .voters = voters,
        .voters_outgoing = voters_outgoing,
        .learners = learners,
    };
    defer cs.deinit(allocator);

    const split = try toConfChangeSingle(allocator, cs);
    defer allocator.free(split.outgoing);
    defer allocator.free(split.incoming);

    // outgoing: 3 AddNode entries (for {1,2,3}).
    try std.testing.expectEqual(@as(usize, 3), split.outgoing.len);
    for (split.outgoing) |cc| try std.testing.expectEqual(ConfChangeType.add_node, cc.change_type);

    // incoming: 3 RemoveNode (for {1,2,3}) + 2 AddNode (for {1,2}) + 1 AddLearnerNode (for {4}).
    try std.testing.expectEqual(@as(usize, 6), split.incoming.len);
    try std.testing.expectEqual(ConfChangeType.remove_node, split.incoming[0].change_type);
    try std.testing.expectEqual(ConfChangeType.add_learner_node, split.incoming[5].change_type);
}

test "restore rebuilds a joint configuration" {
    const allocator = std.testing.allocator;
    var tracker = ProgressTracker.init(allocator, 8);
    defer tracker.deinit();

    const state = ConfState{
        .voters = @constCast(&[_]u64{ 1, 2, 4 }),
        .voters_outgoing = @constCast(&[_]u64{ 1, 2, 3 }),
        .learners = @constCast(&[_]u64{5}),
        .learners_next = @constCast(&[_]u64{3}),
        .auto_leave = true,
    };
    try restore(&tracker, 10, state);

    var restored = try tracker.conf.toConfState(allocator);
    defer restored.deinit(allocator);
    try std.testing.expect(state.eql(restored));
    try std.testing.expectEqual(@as(u64, 10), tracker.getPtr(5).?.next_idx);
}

test "conf state splitting cleans up every allocation failure" {
    const Helper = struct {
        fn run(allocator: std.mem.Allocator, state: ConfState) !void {
            const split = try toConfChangeSingle(allocator, state);
            defer allocator.free(split.outgoing);
            defer allocator.free(split.incoming);
        }
    };
    const state = ConfState{
        .voters = @constCast(&[_]u64{ 1, 2, 4 }),
        .voters_outgoing = @constCast(&[_]u64{ 1, 2, 3 }),
        .learners = @constCast(&[_]u64{5}),
        .learners_next = @constCast(&[_]u64{3}),
        .auto_leave = true,
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helper.run, .{state});
}

test "restore rejects a configuration that removes all voters" {
    const allocator = std.testing.allocator;
    var empty = ProgressTracker.init(allocator, 8);
    defer empty.deinit();
    try std.testing.expectError(error.RemovedAllVoters, restore(&empty, 1, .{
        .voters_outgoing = @constCast(&[_]u64{1}),
    }));
}
// KCOV_EXCL_STOP
