//! ConfChanger tests.
//!
//! The original testdata files use a textual directive DSL (e.g.
//! `simple v1 l2 r3`). We inline the same scenarios as Zig parameterized
//! matrices. Each scenario sets up a tracker, runs the named operation, and
//! asserts either the resulting configuration or the expected error.
//!
//! Directive syntax:
//!   * `v1` — add voter 1 (ADD_NODE)
//!   * `l2` — add learner 2 (ADD_LEARNER_NODE)
//!   * `r3` — remove node 3 (REMOVE_NODE)

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;
const ConfChangeSingle = raft.ConfChangeSingle;
const ConfChanger = raft.ConfChanger;
const ProgressTracker = raft.ProgressTracker;
const ProgressState = raft.ProgressState;

fn parseInto(buf: []ConfChangeSingle, tokens: []const []const u8) []const ConfChangeSingle {
    std.debug.assert(tokens.len <= buf.len);
    for (tokens, 0..) |t, i| {
        buf[i] = parseOne(t);
    }
    return buf[0..tokens.len];
}

fn parseOne(token: []const u8) ConfChangeSingle {
    std.debug.assert(token.len >= 2);
    const op = token[0];
    const id = std.fmt.parseInt(u64, token[1..], 10) catch unreachable;
    return switch (op) {
        'v' => .{ .change_type = .add_node, .node_id = id },
        'l' => .{ .change_type = .add_learner_node, .node_id = id },
        'r' => .{ .change_type = .remove_node, .node_id = id },
        else => unreachable,
    };
}

/// Apply a `simple` directive, asserting the result. The tracker ends up
/// updated if the operation succeeds.
fn simpleOk(tr: *ProgressTracker, tokens: []const []const u8, next_idx: u64) !void {
    var buf: [16]ConfChangeSingle = undefined;
    const ccs = parseInto(&buf, tokens);
    var r = try ConfChanger.init(tr).simple(ccs);
    defer r.deinit(allocator);
    try tr.applyConf(r.conf, r.changes, next_idx);
}

fn simpleErr(tr: *ProgressTracker, tokens: []const []const u8, expected: anyerror) !void {
    var buf: [16]ConfChangeSingle = undefined;
    const ccs = parseInto(&buf, tokens);
    try std.testing.expectError(expected, ConfChanger.init(tr).simple(ccs));
}

fn enterJointOk(tr: *ProgressTracker, auto_leave: bool, tokens: []const []const u8, next_idx: u64) !void {
    var buf: [16]ConfChangeSingle = undefined;
    const ccs = parseInto(&buf, tokens);
    var r = try ConfChanger.init(tr).enterJoint(auto_leave, ccs);
    defer r.deinit(allocator);
    try tr.applyConf(r.conf, r.changes, next_idx);
}

fn leaveJointOk(tr: *ProgressTracker, next_idx: u64) !void {
    var r = try ConfChanger.init(tr).leaveJoint();
    defer r.deinit(allocator);
    try tr.applyConf(r.conf, r.changes, next_idx);
}

/// Format the tracker's voters into a sorted []const u64 for assertion. The
/// caller owns the returned slice.
fn sortedVoters(tr: *ProgressTracker) ![]u64 {
    const out = try allocator.alloc(u64, tr.conf.voters.incoming.count());
    var i: usize = 0;
    var it = tr.conf.voters.incoming.voters.keyIterator();
    while (it.next()) |k| : (i += 1) out[i] = k.*;
    std.mem.sort(u64, out, {}, std.sort.asc(u64));
    return out;
}

// ===========================================================================
// simple_*.txt scenarios
// ===========================================================================

test "confchange: simple_safety error paths" {
    // Mirrors simple_safety.txt:
    //   simple l1        → RemovedAllVoters (can't have only learners)
    //   simple v1        → voters=(1)
    //   simple v2 l3     → voters=(1 2) learners=(3)  [bootstrap to 2 voters + learner]
    //   simple r1 v5     → MultipleVotersChangedWithoutJoint
    //   simple r1 r2     → RemovedAllVoters (drops the last voters; learners don't count)
    //   simple v3 v4     → MultipleVotersChangedWithoutJoint (from {1,2})
    //   simple l1 v5     → MultipleVotersChangedWithoutJoint
    //   simple l1 l2     → RemovedAllVoters
    //   simple r1 (only) → RemovedAllVoters
    var tr = ProgressTracker.init(allocator, 10);
    defer tr.deinit();

    try simpleErr(&tr, &.{"l1"}, error.RemovedAllVoters);
    try simpleOk(&tr, &.{"v1"}, 0);
    try simpleOk(&tr, &.{ "v2", "l3" }, 0);

    try simpleErr(&tr, &.{ "r1", "v5" }, error.MultipleVotersChangedWithoutJoint);
    try simpleErr(&tr, &.{ "r1", "r2" }, error.RemovedAllVoters);
    try simpleErr(&tr, &.{ "v3", "v4" }, error.MultipleVotersChangedWithoutJoint);
    try simpleErr(&tr, &.{ "l1", "v5" }, error.MultipleVotersChangedWithoutJoint);
    try simpleErr(&tr, &.{ "l1", "l2" }, error.RemovedAllVoters);

    // Remove down to {1} (learner 3 stays).
    try simpleOk(&tr, &.{"r2"}, 0);
    const voters = try sortedVoters(&tr);
    defer allocator.free(voters);
    try std.testing.expectEqualSlices(u64, &.{1}, voters);

    // Removing the last voter → RemovedAllVoters.
    try simpleErr(&tr, &.{"r1"}, error.RemovedAllVoters);
}

test "confchange: simple_promote_demote learner↔voter transitions" {
    var tr = ProgressTracker.init(allocator, 10);
    defer tr.deinit();

    // Bootstrap {1, 2}.
    try simpleOk(&tr, &.{"v1"}, 0);
    try simpleOk(&tr, &.{"v2"}, 0);
    // Add learner 3.
    try simpleOk(&tr, &.{"l3"}, 0);
    try std.testing.expect(tr.conf.learners.contains(3));

    // Promote learner 3 to voter.
    try simpleOk(&tr, &.{"v3"}, 0);
    try std.testing.expect(!tr.conf.learners.contains(3));
    try std.testing.expect(tr.conf.voters.incoming.contains(3));

    // Demote voter 2 to learner.
    try simpleOk(&tr, &.{"l2"}, 0);
    try std.testing.expect(tr.conf.learners.contains(2));
    try std.testing.expect(!tr.conf.voters.incoming.contains(2));
}

test "confchange: simple_idempotency repeated ops" {
    var tr = ProgressTracker.init(allocator, 10);
    defer tr.deinit();

    try simpleOk(&tr, &.{"v1"}, 0);
    try simpleOk(&tr, &.{"v2"}, 0);

    // Adding the same voter twice is a no-op (diff_count == 0).
    try simpleOk(&tr, &.{"v2"}, 0);
    try std.testing.expectEqual(@as(usize, 2), tr.conf.voters.incoming.count());

    // Removing a missing voter is a no-op.
    try simpleOk(&tr, &.{"r9"}, 0);
    try std.testing.expectEqual(@as(usize, 2), tr.conf.voters.incoming.count());
}

// ===========================================================================
// joint_*.txt scenarios
// ===========================================================================

test "confchange: joint_safety enter/leave from empty" {
    var tr = ProgressTracker.init(allocator, 10);
    defer tr.deinit();

    // leave-joint from non-joint → error.
    try std.testing.expectError(error.LeaveNonJointConfig, ConfChanger.init(&tr).leaveJoint());

    // enter-joint from empty → error.
    {
        var buf: [16]ConfChangeSingle = undefined;
        const empty_ccs = parseInto(&buf, &.{});
        try std.testing.expectError(error.ZeroVoterConfigJoint, ConfChanger.init(&tr).enterJoint(false, empty_ccs));
    }

    // Bootstrap via simple, then enter-joint with no ops is allowed.
    try simpleOk(&tr, &.{"v1"}, 0);
    try enterJointOk(&tr, false, &.{}, 0);
    try std.testing.expect(raft.joint(tr.conf));

    // enter-joint again → ConfigAlreadyJoint.
    {
        var buf: [16]ConfChangeSingle = undefined;
        const empty_ccs = parseInto(&buf, &.{});
        try std.testing.expectError(error.ConfigAlreadyJoint, ConfChanger.init(&tr).enterJoint(false, empty_ccs));
    }

    // leave-joint succeeds.
    try leaveJointOk(&tr, 0);
    try std.testing.expect(!raft.joint(tr.conf));
}

test "confchange: joint_idempotency repeated enter" {
    var tr = ProgressTracker.init(allocator, 10);
    defer tr.deinit();

    try simpleOk(&tr, &.{"v1"}, 0);
    try enterJointOk(&tr, false, &.{"v2"}, 0);
    {
        var buf: [16]ConfChangeSingle = undefined;
        const ccs = parseInto(&buf, &.{"v3"});
        try std.testing.expectError(error.ConfigAlreadyJoint, ConfChanger.init(&tr).enterJoint(false, ccs));
    }
}

test "confchange: joint_autoleave flag honored" {
    var tr = ProgressTracker.init(allocator, 10);
    defer tr.deinit();

    try simpleOk(&tr, &.{"v1"}, 0);
    try simpleOk(&tr, &.{"v2"}, 0);
    try enterJointOk(&tr, true, &.{"v3"}, 0);
    try std.testing.expect(tr.conf.auto_leave);
}

test "confchange: joint_learners_next promoted on leave" {
    // Bootstrap {1,2}.
    var tr = ProgressTracker.init(allocator, 10);
    defer tr.deinit();
    try simpleOk(&tr, &.{"v1"}, 0);
    try simpleOk(&tr, &.{"v2"}, 0);

    // Enter joint: demote 1 to learner_next while removing from incoming.
    // MakeLearner on a voter in outgoing puts it in learners_next.
    try enterJointOk(&tr, false, &.{"l1"}, 0);
    try std.testing.expect(tr.conf.learners_next.contains(1));

    // leave-joint: learners_next → learners, outgoing-only voters removed.
    try leaveJointOk(&tr, 0);
    try std.testing.expect(tr.conf.learners.contains(1));
    try std.testing.expect(tr.conf.learners_next.count() == 0);
}

// ===========================================================================
// zero.txt edge case
// ===========================================================================

test "confchange: zero node ids in changes are ignored" {
    var tr = ProgressTracker.init(allocator, 10);
    defer tr.deinit();
    try simpleOk(&tr, &.{"v1"}, 0);

    // A ConfChangeSingle with node_id=0 is a no-op.
    const cc = [_]ConfChangeSingle{.{ .change_type = .add_node, .node_id = 0 }};
    var r = try ConfChanger.init(&tr).simple(&cc);
    defer r.deinit(allocator);
    try tr.applyConf(r.conf, r.changes, 0);

    try std.testing.expectEqual(@as(usize, 1), tr.conf.voters.incoming.count());
    try std.testing.expectEqual(@as(usize, 0), tr.progress.count() - 1); // no new peer added
}
