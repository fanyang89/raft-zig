//! Core scalar constants shared across the raft implementation.

const std = @import("std");

/// Sentinel for an unknown or invalid log index.
pub const invalid_index: u64 = 0;

/// Sentinel for an unknown or invalid node id.
pub const invalid_id: u64 = 0;

/// Default heartbeat interval in ticks.
pub const default_heartbeat_tick: usize = 2;

// KCOV_EXCL_START
test "sentinel constants are zero" {
    try std.testing.expectEqual(@as(u64, 0), invalid_index);
    try std.testing.expectEqual(@as(u64, 0), invalid_id);
}
// KCOV_EXCL_STOP
