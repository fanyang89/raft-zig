//! Configuration for the Raftor orchestration layer.
//!
//! Wraps a `raft.Config` and adds orchestration knobs: node identity, listen
//! address, initial peers, tick interval, snapshot thresholds.

const std = @import("std");

const raft_config_mod = @import("raft_config.zig");
const raw_node_mod = @import("raw_node.zig");
const fs_mod = @import("fs.zig");

const Config = raft_config_mod.Config;
const Peer = raw_node_mod.Peer;

pub const RaftorConfig = struct {
    /// Underlying Raft configuration. `id` must be set.
    raft: Config = .{},
    /// This node's network address (e.g. "127.0.0.1:9000"). Used by future
    /// RPC transports; NoopTransport ignores it.
    listen_addr: []const u8 = "",
    /// Initial cluster peers for bootstrap. If empty, a single-node cluster
    /// is created with just this node.
    initial_peers: []const Peer = &.{},
    /// Data directory for future WAL storage. MemoryStorage ignores it.
    data_dir: []const u8 = "",
    /// Borrowed filesystem used when `data_dir` is non-empty. The default uses
    /// the host filesystem. A custom backend must outlive the Raftor.
    file_system: ?fs_mod.Fs = null,
    /// Interval between ticks in milliseconds. The event loop sleeps this
    /// long when idle.
    tick_interval_ms: u64 = 100,
    /// Number of applied entries above which a snapshot is triggered.
    /// 0 = disabled.
    snapshot_entries_threshold: u64 = 10_000,
    /// Tick interval between automatic snapshots. 0 = disabled.
    snapshot_interval_ticks: u64 = 0,
    /// Minimum ticks between snapshot retry attempts (rate limiting).
    snapshot_retry_min_ticks: u64 = 10,
    /// Maximum number of uncommitted entries allowed before proposals are
    /// rejected.
    max_uncommitted_entries: u64 = std.math.maxInt(u64),
    /// Whether to verify CRC32C entry checksums on apply.
    checksum_enabled: bool = false,
    /// Proposal timeout in ticks. 0 = no timeout.
    proposal_timeout_ticks: u64 = 0,
    /// Read-index timeout in ticks. 0 = no timeout.
    read_index_timeout_ticks: u64 = 0,

    pub fn nodeId(self: RaftorConfig) u64 {
        return self.raft.id;
    }
};

test "raftor config defaults" {
    const c = RaftorConfig{};
    try std.testing.expectEqual(@as(u64, 100), c.tick_interval_ms);
    try std.testing.expectEqual(@as(u64, 10_000), c.snapshot_entries_threshold);
    try std.testing.expectEqual(@as(usize, 0), c.initial_peers.len);
    try std.testing.expectEqual(@as(?fs_mod.Fs, null), c.file_system);
}
