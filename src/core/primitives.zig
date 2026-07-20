//! Core scalar constants shared across the raft implementation.

const std = @import("std");

/// Sentinel for an unknown or invalid log index. Matches `kInvalidIndex` in raftpp.
pub const invalid_index: u64 = 0;

/// Sentinel for an unknown or invalid node id. Matches `kInvalidId` in raftpp.
pub const invalid_id: u64 = 0;

/// Default heartbeat interval in ticks. Matches `kHeartbeatTick` in raftpp.
pub const default_heartbeat_tick: usize = 2;

test "sentinel constants are zero" {
    try std.testing.expectEqual(@as(u64, 0), invalid_index);
    try std.testing.expectEqual(@as(u64, 0), invalid_id);
}
