//! raft-zig: a Zig implementation of the RAFT consensus algorithm.
//!
//! This module is the public entry point. It re-exports the stable types that
//! applications and integrations depend on. Lower-level modules are also
//! exported for experimentation but may evolve before 1.0.
//!
//! Layered architecture:
//!   * `core/`     — plain data types, errors, roles, status snapshots, and
//!                    entry-sizing utilities.
//!   * `inflights` — per-follower in-flight tracking ring buffer.
//!   * `ack_indexer` — quorum helper: vote outcomes and the acknowledged-index
//!     lookup vtable used by quorum math.
//!   * `storage` / `memory_storage` — `Storage` / `WritableStorage` vtables
//!     and the default in-memory backend.
//!   * `read_only` — linearizable read-index queue.
//!   * `log`, `progress`, `raft`, `raw_node`, `raftor`, `wal`, `rpc` — the
//!     consensus log, replication tracking, state machine, user-facing API,
//!     orchestration loop, write-ahead log, and pluggable transport.

const std = @import("std");
const builtin = @import("builtin");

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
const invariant_mod = @import("invariant.zig");
const raw_node_mod = @import("raw_node.zig");
const state_machine_mod = @import("state_machine.zig");
const transport_mod = @import("transport.zig");
const proposal_tracker_mod = @import("proposal_tracker.zig");
const raftor_config_mod = @import("raftor_config.zig");
const ready_processor_mod = @import("ready_processor.zig");
const raftor_mod = @import("raftor.zig");
const fs_mod = @import("fs.zig");
const fs_testing_mod = @import("fs/testing.zig");
const wal_mod = @import("wal.zig");
const codec_mod = @import("codec.zig");
const loopback_transport_mod = @import("loopback_transport.zig");
const proposal_queue_mod = @import("proposal_queue.zig");
const request_context_mod = @import("request_context.zig");
const cluster_membership_mod = @import("cluster_membership.zig");
const rpc_inbound_mailbox = @import("rpc/inbound_mailbox.zig");
const rpc_peer_manager = @import("rpc/peer_manager.zig");
const rpc_grpc_transport = @import("rpc/grpc_lite_transport.zig");

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
pub const request_context = request_context_mod;
pub const ClusterId = cluster_membership_mod.ClusterId;
pub const PeerEndpoint = cluster_membership_mod.PeerEndpoint;
pub const ClusterMembership = cluster_membership_mod.ClusterMembership;
pub const decodeClusterMembership = cluster_membership_mod.decode;
pub const MembershipContext = cluster_membership_mod.MembershipContext;
pub const decodeMembershipContext = cluster_membership_mod.decodeMembershipContext;
pub const deriveClusterMembership = cluster_membership_mod.deriveClusterMembership;
pub const collectEffectiveMemberIds = cluster_membership_mod.collectEffectiveMemberIds;

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
pub const cloneMessage = storage_mod.cloneMessage;
pub const shareEntry = storage_mod.shareEntry;

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
pub const InvariantKind = invariant_mod.Kind;
pub const InvariantViolation = invariant_mod.Violation;
pub const checkRaftInvariants = invariant_mod.checkRaft;

pub const Peer = raw_node_mod.Peer;
pub const SnapshotStatus = raw_node_mod.SnapshotStatus;
pub const isLocalMessage = raw_node_mod.isLocalMessage;
pub const isResponseMessage = raw_node_mod.isResponseMessage;
pub const LightReady = raw_node_mod.LightReady;
pub const Ready = raw_node_mod.Ready;
pub const RawNodeStatus = raw_node_mod.Status;
pub const RawNode = raw_node_mod.RawNode;

pub const ApplyResult = state_machine_mod.ApplyResult;
pub const SnapshotWriter = state_machine_mod.SnapshotWriter;
pub const SnapshotReader = state_machine_mod.SnapshotReader;
pub const StateMachine = state_machine_mod.StateMachine;
pub const MockStateMachine = state_machine_mod.MockStateMachine;

pub const MessageCallback = transport_mod.MessageCallback;
pub const PeerEventKind = transport_mod.PeerEventKind;
pub const PeerEvent = transport_mod.PeerEvent;
pub const PeerEventCallback = transport_mod.PeerEventCallback;
pub const Transport = transport_mod.Transport;
pub const NoopTransport = transport_mod.NoopTransport;

pub const ProposalResult = proposal_tracker_mod.ProposalResult;
pub const ReadIndexResult = proposal_tracker_mod.ReadIndexResult;
pub const ProposalCallback = proposal_tracker_mod.ProposalCallback;
pub const ReadIndexCallback = proposal_tracker_mod.ReadIndexCallback;
pub const ProposalTracker = proposal_tracker_mod.ProposalTracker;

pub const RaftorConfig = raftor_config_mod.RaftorConfig;
pub const LegacySnapshotMembership = raftor_config_mod.LegacySnapshotMembership;
pub const LegacyMembershipMigration = raftor_config_mod.LegacyMembershipMigration;

pub const StartupMode = raftor_mod.StartupMode;
pub const RaftorDependencies = raftor_mod.RaftorDependencies;
pub const ReadyPhase = ready_processor_mod.ReadyPhase;
pub const NodeStatus = raftor_mod.NodeStatus;
pub const Raftor = raftor_mod.Raftor;

pub const Fs = fs_mod.Fs;
pub const FsError = fs_mod.Error;
pub const FileHandle = fs_mod.Handle;
pub const FsOpenMode = fs_mod.OpenMode;
pub const FsDirListing = fs_mod.DirListing;
pub const FsDirEntryKind = fs_mod.EntryKind;
pub const realFileSystem = fs_mod.realFileSystem;

pub const FsTestBackend = if (builtin.is_test) fs_testing_mod.Backend else void;
pub const FsTestFixture = if (builtin.is_test) fs_testing_mod.FsFixture else void;

pub const WAL = wal_mod.WAL;
pub const WALStorage = wal_mod.WALStorage;
pub const WalFileSystem = wal_mod.WalFileSystem;
pub const WalFileSystemError = wal_mod.WalFileSystemError;
pub const WalFileHandle = wal_mod.WalFileHandle;
pub const WalOpenMode = wal_mod.WalOpenMode;
pub const WalDirListing = wal_mod.WalDirListing;
pub const WalDirEntryKind = wal_mod.WalDirEntryKind;
pub const linuxWalFileSystem = realFileSystem;

pub const encodeMessage = codec_mod.encodeMessage;
pub const decodeMessage = codec_mod.decodeMessage;
pub const encodeFramed = codec_mod.encodeFramed;
pub const decodeFramed = codec_mod.decodeFramed;

pub const LoopbackNetwork = loopback_transport_mod.LoopbackNetwork;
pub const LoopbackTransport = loopback_transport_mod.LoopbackTransport;
pub const TransportIdentity = transport_mod.TransportIdentity;

pub const ProposalQueue = proposal_queue_mod.ProposalQueue;
pub const ReadIndexQueue = proposal_queue_mod.ReadIndexQueue;

pub const InboundMailbox = rpc_inbound_mailbox.InboundMailbox;
pub const PeerManager = rpc_peer_manager.PeerManager;
pub const PeerLifecycleState = rpc_peer_manager.LifecycleState;
pub const GrpcLiteTransport = rpc_grpc_transport.GrpcLiteTransport;
pub const GrpcLiteTransportConfig = rpc_grpc_transport.Config;

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
    _ = raw_node_mod;
    _ = state_machine_mod;
    _ = transport_mod;
    _ = proposal_tracker_mod;
    _ = raftor_config_mod;
    _ = ready_processor_mod;
    _ = raftor_mod;
    _ = wal_mod;
    _ = codec_mod;
    _ = loopback_transport_mod;
    _ = proposal_queue_mod;
    _ = request_context_mod;
    _ = cluster_membership_mod;
    _ = rpc_inbound_mailbox;
    _ = rpc_peer_manager;
    _ = rpc_grpc_transport;
    _ = version_info;
}
