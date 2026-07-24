//! Raft node configuration + validation.
//!
//! Holds the user-tunable knobs that `Raft.init` consumes. `validate` checks
//! invariants and returns the first violation as a Zig error.

const std = @import("std");

const error_model = @import("core/error.zig");
const read_only_mod = @import("read_only.zig");
const primitives = @import("core/primitives.zig");

const Error = error_model.Error;
const ReadOnlyOption = read_only_mod.ReadOnlyOption;

/// Default heartbeat spacing in ticks.
pub const default_heartbeat_tick: usize = 2;

/// A full Raft configuration.
pub const Config = struct {
    /// Node identity. Must be non-zero and present in the initial peer set.
    id: u64 = 0,
    /// Election timeout in ticks. Must be greater than `heartbeat_tick`.
    election_tick: usize = default_heartbeat_tick * 10,
    /// Heartbeat interval in ticks.
    heartbeat_tick: usize = default_heartbeat_tick,
    /// Already-applied index the node should start from.
    applied: u64 = 0,
    /// Soft cap on the bytes of entries per AppendEntries. 0 = unlimited.
    max_size_per_message: u64 = 0,
    /// Maximum number of in-flight AppendEntries per follower.
    max_inflight_messages: usize = 256,
    /// Leader steps down if a quorum isn't heard from within an election
    /// timeout.
    check_quorum: bool = false,
    /// Use pre-vote (Phase 1) before kicking off a real election.
    pre_vote: bool = false,
    /// Lower bound for the randomized election timeout. 0 = `election_tick`.
    min_election_tick: usize = 0,
    /// Upper bound for the randomized election timeout. 0 = `2 * election_tick`.
    max_election_tick: usize = 0,
    /// Linearizable read-index strategy.
    read_only_option: ReadOnlyOption = .safe,
    /// Skip broadcasting commit when only the leader's match advances.
    skip_broadcast_commit: bool = false,
    /// Batch multiple AppendEntries to the same peer into one message.
    batch_append: bool = false,
    /// Election priority. Higher wins ties.
    priority: i64 = 0,
    /// Maximum total bytes of uncommitted entries.
    max_uncommitted_size: u64 = std.math.maxInt(u64),
    /// Maximum bytes of committed entries returned per Ready.
    max_committed_size_per_ready: u64 = std.math.maxInt(u64),
    /// Allow applying entries that haven't been persisted yet (raft-zig only).
    max_apply_unpersisted_log_limit: u64 = 0,
    /// Reject MsgPropose on followers (do not forward to the leader).
    disable_proposal_forwarding: bool = false,
    /// Load persisted HardState on startup instead of starting fresh.
    load_state_on_startup: bool = false,
    /// Optional PRNG seed for the randomized election timeout. `null` mixes
    /// in `std.time.nanoTimestamp` so production nodes don't share a seed.
    election_timeout_seed: ?u64 = null,

    pub fn minElectionTick(self: Config) usize {
        if (self.min_election_tick == 0) return self.election_tick;
        return self.min_election_tick;
    }

    pub fn maxElectionTick(self: Config) usize {
        if (self.max_election_tick == 0) return 2 * self.election_tick;
        return self.max_election_tick;
    }

    pub fn validate(self: Config) Error!void {
        if (self.id == primitives.invalid_id) return error.InvalidNodeId;
        if (self.heartbeat_tick == 0) return error.HeartbeatTickTooSmall;
        if (self.election_tick <= self.heartbeat_tick) return error.ElectionTickTooSmall;

        const min_timeout = self.minElectionTick();
        const max_timeout = self.maxElectionTick();
        if (min_timeout < self.election_tick) return error.InvalidConfig;
        if (min_timeout >= max_timeout) return error.InvalidConfig;

        if (self.max_inflight_messages == 0) return error.MaxInflightMessagesTooSmall;

        if (self.read_only_option == .lease_based and !self.check_quorum) {
            return error.LeaseBasedReadRequiresCheckQuorum;
        }

        if (self.max_uncommitted_size < self.max_size_per_message) {
            return error.InvalidConfig;
        }

        return;
    }
};

/// Safe default configuration: zeroed id and conservative defaults. Tests use
/// this and then patch `id` and `election_tick`/`heartbeat_tick` as needed.
pub fn defaultConfig() Config {
    return .{};
}

test "validate accepts the default config after patching id" {
    var c = defaultConfig();
    c.id = 1;
    try c.validate();
}

test "validate rejects zero id" {
    var c = defaultConfig();
    try std.testing.expectError(error.InvalidNodeId, c.validate());
}

test "validate rejects heartbeat >= election" {
    var c = defaultConfig();
    c.id = 1;
    c.heartbeat_tick = 5;
    c.election_tick = 5;
    try std.testing.expectError(error.ElectionTickTooSmall, c.validate());
}

test "validate rejects lease_based without check_quorum" {
    var c = defaultConfig();
    c.id = 1;
    c.read_only_option = .lease_based;
    try std.testing.expectError(error.LeaseBasedReadRequiresCheckQuorum, c.validate());
}

test "min/max election tick default to election_tick and 2 * election_tick" {
    const c = defaultConfig();
    try std.testing.expectEqual(c.election_tick, c.minElectionTick());
    try std.testing.expectEqual(2 * c.election_tick, c.maxElectionTick());
}
