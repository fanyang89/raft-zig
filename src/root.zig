//! raft-zig: a Zig implementation of the RAFT consensus algorithm.
//!
//! This module is the public entry point. It re-exports the stable types that
//! applications and integrations depend on. Lower-level modules are also
//! exported for experimentation but may evolve before 1.0.
//!
//! The port mirrors the layered architecture of raftpp:
//!   * `core/`     — plain data types, errors, roles, status snapshots, and
//!                    entry-sizing utilities.
//!   * `inflights` — per-follower in-flight tracking ring buffer.
//!   * `ack_indexer` — quorum helper: vote outcomes and the acknowledged-index
//!     lookup vtable used by quorum math.
//!   * `storage` / `memory_storage` — `Storage` / `WritableStorage` vtables
//!     and the default in-memory backend.
//!   * `read_only` — linearizable read-index queue.
//!   * Future layers (`log`, `progress`, `raft`, `raw_node`, `raftor`,
//!     `wal`, `rpc`) will be added under `src/` as they are ported.

const std = @import("std");

const version_info = @import("version.zig");
const primitives = @import("core/primitives.zig");
const types = @import("core/types.zig");
const error_model = @import("core/error.zig");
const state_role = @import("core/state_role.zig");
const status = @import("core/status.zig");
const util = @import("core/util.zig");
const inflights_mod = @import("inflights.zig");
const ack_indexer_mod = @import("ack_indexer.zig");
const storage_mod = @import("storage.zig");
const memory_storage_mod = @import("memory_storage.zig");
const read_only_mod = @import("read_only.zig");
const unstable_log_mod = @import("unstable_log.zig");
const raft_log_mod = @import("raft_log.zig");
const majority_conf_mod = @import("majority_conf.zig");
const joint_conf_mod = @import("joint_conf.zig");
const tracker_conf_mod = @import("tracker_conf.zig");
const progress_mod = @import("progress.zig");
const progress_tracker_mod = @import("progress_tracker.zig");
const conf_changer_mod = @import("conf_changer.zig");
const conf_restore_mod = @import("conf_restore.zig");
const raft_config_mod = @import("raft_config.zig");
const raft_mod = @import("raft.zig");

pub const core = .{
    .primitives = primitives,
    .types = types,
    .error_model = error_model,
    .state_role = state_role,
    .status = status,
    .util = util,
};

pub const Error = error_model.Error;
pub const errorName = error_model.name;

pub const EntryType = types.EntryType;
pub const MessageType = types.MessageType;
pub const ConfChangeType = types.ConfChangeType;
pub const ConfChangeTransition = types.ConfChangeTransition;
pub const Entry = types.Entry;
pub const HardState = types.HardState;
pub const ConfState = types.ConfState;
pub const SnapshotMetadata = types.SnapshotMetadata;
pub const Snapshot = types.Snapshot;
pub const ConfChangeSingle = types.ConfChangeSingle;
pub const ConfChangeV2 = types.ConfChangeV2;
pub const ConfChange = types.ConfChange;
pub const Message = types.Message;

pub const invalid_index = primitives.invalid_index;
pub const invalid_id = primitives.invalid_id;

pub const StateRole = state_role.StateRole;
pub const SoftState = state_role.SoftState;
pub const roleName = state_role.roleName;

pub const Status = status.Status;

pub const entry_message_overhead = util.entry_message_overhead;
pub const entryApproximateSize = util.entryApproximateSize;
pub const limitSize = util.limitSize;
pub const IndexTerm = util.IndexTerm;

pub const Inflights = inflights_mod.Inflights;

pub const VoteResult = ack_indexer_mod.VoteResult;
pub const Index = ack_indexer_mod.Index;
pub const AckedIndexer = ack_indexer_mod.AckedIndexer;
pub const AckIndexer = ack_indexer_mod.AckIndexer;

pub const RaftState = storage_mod.RaftState;
pub const GetEntriesFor = storage_mod.GetEntriesFor;
pub const GetEntriesContext = storage_mod.GetEntriesContext;
pub const Storage = storage_mod.Storage;
pub const WritableStorage = storage_mod.WritableStorage;
pub const cloneConfState = storage_mod.cloneConfState;
pub const cloneSnapshot = storage_mod.cloneSnapshot;
pub const cloneEntry = storage_mod.cloneEntry;

pub const MemoryStorageCore = memory_storage_mod.MemoryStorageCore;
pub const MemoryStorage = memory_storage_mod.MemoryStorage;

pub const ReadOnlyOption = read_only_mod.ReadOnlyOption;
pub const ReadState = read_only_mod.ReadState;
pub const ReadIndexStatus = read_only_mod.ReadIndexStatus;
pub const ReadOnly = read_only_mod.ReadOnly;

pub const Unstable = unstable_log_mod.Unstable;
pub const RaftLog = raft_log_mod.RaftLog;
pub const MaybeAppendResult = raft_log_mod.MaybeAppendResult;
pub const FindConflictByTermResult = raft_log_mod.FindConflictByTermResult;
pub const CommitInfo = raft_log_mod.CommitInfo;

pub const MajorityConfig = majority_conf_mod.MajorityConfig;
pub const CommittedIndexResult = majority_conf_mod.CommittedIndexResult;
pub const majority = majority_conf_mod.majority;

pub const JointConfiguration = joint_conf_mod.JointConfiguration;

pub const TrackerConfiguration = tracker_conf_mod.TrackerConfiguration;

pub const ProgressState = progress_mod.ProgressState;
pub const progressStateName = progress_mod.progressStateName;
pub const Progress = progress_mod.Progress;
pub const ProgressMap = progress_mod.ProgressMap;

pub const MapChangeKind = progress_tracker_mod.MapChangeKind;
pub const MapChangeEntry = progress_tracker_mod.MapChangeEntry;
pub const CountVoteResult = progress_tracker_mod.CountVoteResult;
pub const ProgressTracker = progress_tracker_mod.ProgressTracker;

pub const IncrChangeMap = conf_changer_mod.IncrChangeMap;
pub const ConfChangeResult = conf_changer_mod.ConfChangeResult;
pub const ConfChanger = conf_changer_mod.ConfChanger;
pub const joint = conf_changer_mod.joint;
pub const checkInvariants = conf_changer_mod.checkInvariants;

pub const restore = conf_restore_mod.restore;
pub const toConfChangeSingle = conf_restore_mod.toConfChangeSingle;

pub const Config = raft_config_mod.Config;
pub const defaultConfig = raft_config_mod.defaultConfig;
pub const default_heartbeat_tick = raft_config_mod.default_heartbeat_tick;

pub const UncommittedState = raft_mod.UncommittedState;
pub const CampaignType = raft_mod.CampaignType;
pub const campaign_pre_election = raft_mod.campaign_pre_election;
pub const campaign_election = raft_mod.campaign_election;
pub const campaign_transfer = raft_mod.campaign_transfer;
pub const Raft = raft_mod.Raft;

pub const version = version_info.string;

test "version is parseable" {
    _ = try std.SemanticVersion.parse(version);
}

test "re-exported modules compile" {
    _ = core;
    _ = primitives;
    _ = types;
    _ = error_model;
    _ = state_role;
    _ = status;
    _ = util;
    _ = inflights_mod;
    _ = ack_indexer_mod;
    _ = storage_mod;
    _ = memory_storage_mod;
    _ = read_only_mod;
    _ = unstable_log_mod;
    _ = raft_log_mod;
    _ = majority_conf_mod;
    _ = joint_conf_mod;
    _ = tracker_conf_mod;
    _ = progress_mod;
    _ = progress_tracker_mod;
    _ = conf_changer_mod;
    _ = conf_restore_mod;
    _ = raft_config_mod;
    _ = raft_mod;
    _ = version_info;
}
