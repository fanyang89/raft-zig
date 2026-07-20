//! Public Status snapshot of a Raft node.
//!
//! Mirrors `Status` in `include/raftpp/core/status.h`. The `progress` field is
//! left as an opaque pointer for now; the `ProgressTracker` module fills it in
//! once that layer is ported.

const std = @import("std");

const state_role = @import("state_role.zig");
const types = @import("types.zig");

pub const StateRole = state_role.StateRole;
pub const SoftState = state_role.SoftState;
pub const HardState = types.HardState;

/// Read-only view of a Raft node at a point in time.
pub const Status = struct {
    /// The ID of the current node.
    id: u64,
    /// Persisted vote/commit state.
    hard_state: HardState,
    /// Non-persisted leadership state.
    soft_state: SoftState,
    /// Index of the last applied entry.
    applied: u64,
    /// Replication progress; `null` when the node is not the leader.
    /// TODO raft-zig: replace with `*const ProgressTracker` once ported.
    progress: ?*const anyopaque = null,
};

test "status compiles with default progress" {
    const s = Status{
        .id = 1,
        .hard_state = .{},
        .soft_state = .{ .leader_id = 0, .role = .follower },
        .applied = 0,
    };
    try std.testing.expectEqual(@as(u64, 1), s.id);
    try std.testing.expect(s.progress == null);
}
