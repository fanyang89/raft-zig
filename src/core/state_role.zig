//! Role tracking for the Raft state machine.

const std = @import("std");

/// Raft node role. The numeric values are private; they are not transmitted on
/// the wire directly.
pub const StateRole = enum(u8) {
    follower = 0,
    candidate = 1,
    leader = 2,
    pre_candidate = 3,
};

pub fn roleName(role: StateRole) []const u8 {
    return switch (role) {
        .follower => "Follower",
        .candidate => "Candidate",
        .leader => "Leader",
        .pre_candidate => "PreCandidate",
    };
}

/// SoftState is the non-persisted portion of Raft status. Two SoftStates with
/// identical fields describe the same leadership state.
pub const SoftState = struct {
    leader_id: u64,
    role: StateRole,

    pub fn eql(self: SoftState, other: SoftState) bool {
        return self.leader_id == other.leader_id and self.role == other.role;
    }
};

// KCOV_EXCL_START
test "role names are stable" {
    try std.testing.expectEqualStrings("Leader", roleName(.leader));
    try std.testing.expectEqualStrings("PreCandidate", roleName(.pre_candidate));
}

test "softstate eql" {
    const a = SoftState{ .leader_id = 1, .role = .follower };
    const b = SoftState{ .leader_id = 1, .role = .follower };
    const c = SoftState{ .leader_id = 2, .role = .candidate };
    try std.testing.expect(a.eql(b));
    try std.testing.expect(!a.eql(c));
}
// KCOV_EXCL_STOP
