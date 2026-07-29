//! Configuration for the Raftor orchestration layer.
//!
//! Wraps a `raft.Config` and adds orchestration knobs: node identity, listen
//! address, initial peers, tick interval, snapshot thresholds.

const std = @import("std");

const raft_config_mod = @import("raft_config.zig");
const raw_node_mod = @import("raw_node.zig");
const fs_mod = @import("fs.zig");
const cluster_membership_mod = @import("cluster_membership.zig");

const Config = raft_config_mod.Config;
const Peer = raw_node_mod.Peer;
const ClusterId = cluster_membership_mod.ClusterId;

pub const LegacySnapshotMembership = struct {
    peers: []const Peer,
    retired_node_ids: []const u64 = &.{},
};

pub const LegacyMembershipMigration = struct {
    peers: []const Peer,
    retired_node_ids: []const u64 = &.{},
    membership_index: u64,
    snapshot: ?LegacySnapshotMembership = null,
};

pub const RaftorConfig = struct {
    /// Underlying Raft configuration. `id` must be set.
    raft: Config = .{},
    /// Stable cluster identity. Null explicitly enables legacy ID-only
    /// bootstrap and restart behavior.
    cluster_id: ?ClusterId = null,
    /// This node's network listen address (e.g. "127.0.0.1:9000").
    listen_addr: []const u8 = "",
    /// Address advertised to peers. Falls back to `listen_addr` only when
    /// empty.
    advertise_addr: []const u8 = "",
    /// Select fresh-storage join instead of bootstrap during auto-detection.
    join: bool = false,
    /// Initial cluster peers. In durable mode every `Peer.context` is that
    /// peer's network address. Bootstrap creates a local one-node membership
    /// when this is empty; join requires nonempty seed peers.
    initial_peers: []const Peer = &.{},
    /// Explicit operator-supplied data for upgrading storage that predates
    /// durable cluster membership.
    legacy_membership_migration: ?LegacyMembershipMigration = null,
    /// Data directory for WAL storage. Empty selects MemoryStorage.
    data_dir: []const u8 = "",
    /// Borrowed filesystem used when `data_dir` is non-empty. The default uses
    /// the host filesystem. A custom backend must outlive the Raftor.
    file_system: ?fs_mod.Fs = null,
    /// Interval between ticks in milliseconds. The event loop sleeps this
    /// long when idle.
    tick_interval_ms: u64 = 100,
    /// Maximum number of transport messages and events processed per tick.
    transport_poll_budget: usize = 256,
    /// Maximum number of proposals retained in the cross-thread queue.
    max_queued_proposals: usize = 4096,
    /// Maximum payload and context bytes retained in the proposal queue.
    max_queued_proposal_bytes: usize = 64 * 1024 * 1024,
    /// Maximum number of queued proposals submitted to Raft per tick.
    proposal_drain_budget: usize = 4096,
    /// Maximum number of queued read-index requests submitted per tick.
    read_index_drain_budget: usize = 256,
    /// Maximum number of read-index requests retained in the cross-thread queue.
    max_queued_read_indexes: usize = 256,
    /// Maximum context bytes retained in the read-index queue.
    max_queued_read_index_bytes: usize = 4 * 1024 * 1024,
    /// Number of applied entries above which a snapshot is triggered.
    /// 0 = disabled.
    snapshot_entries_threshold: u64 = 10_000,
    /// Tick interval between automatic snapshots. 0 = disabled.
    snapshot_interval_ticks: u64 = 0,
    /// Minimum ticks between snapshot retry attempts (rate limiting).
    snapshot_retry_min_ticks: u64 = 10,
    /// Whether to verify CRC32C entry checksums on apply.
    checksum_enabled: bool = false,
    /// Proposal timeout after leaving the ingress queue, in ticks.
    /// Time spent queued is excluded. 0 = no timeout.
    proposal_timeout_ticks: u64 = 0,
    /// Read-index timeout after leaving the ingress queue, in ticks.
    /// Time spent queued is excluded. 0 = no timeout.
    read_index_timeout_ticks: u64 = 0,

    pub fn nodeId(self: RaftorConfig) u64 {
        return self.raft.id;
    }
};

test "raftor config defaults" {
    const c = RaftorConfig{};
    try std.testing.expectEqual(@as(u64, 100), c.tick_interval_ms);
    try std.testing.expectEqual(@as(usize, 256), c.transport_poll_budget);
    try std.testing.expectEqual(@as(usize, 4096), c.max_queued_proposals);
    try std.testing.expectEqual(@as(usize, 64 * 1024 * 1024), c.max_queued_proposal_bytes);
    try std.testing.expectEqual(@as(usize, 4096), c.proposal_drain_budget);
    try std.testing.expectEqual(@as(usize, 256), c.read_index_drain_budget);
    try std.testing.expectEqual(@as(usize, 256), c.max_queued_read_indexes);
    try std.testing.expectEqual(@as(usize, 4 * 1024 * 1024), c.max_queued_read_index_bytes);
    try std.testing.expectEqual(@as(u64, 10_000), c.snapshot_entries_threshold);
    try std.testing.expectEqual(@as(usize, 0), c.initial_peers.len);
    try std.testing.expectEqual(@as(?fs_mod.Fs, null), c.file_system);
    try std.testing.expectEqual(@as(?ClusterId, null), c.cluster_id);
    try std.testing.expect(c.legacy_membership_migration == null);
    try std.testing.expect(!c.join);
}
