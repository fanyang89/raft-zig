//! Stable public API surface for downstream consumers.
//!
//! Mirrors `tests/public_api_test.zig` in grpc-lite: this test fails to compile
//! if a re-exported symbol disappears, which keeps the public API stable across
//! refactors.

const std = @import("std");
const raft = @import("raft_zig");

test "stable public API compiles for downstream consumers" {
    comptime {
        // Errors / primitives.
        _ = raft.Error;
        _ = raft.errorName;
        _ = raft.invalid_index;
        _ = raft.invalid_id;
        _ = raft.entry_message_overhead;
        _ = raft.entryApproximateSize;
        _ = raft.limitSize;
        _ = raft.IndexTerm;

        // Wire types.
        _ = raft.EntryType;
        _ = raft.MessageType;
        _ = raft.ConfChangeType;
        _ = raft.ConfChangeTransition;
        _ = raft.Entry;
        _ = raft.HardState;
        _ = raft.ConfState;
        _ = raft.SnapshotMetadata;
        _ = raft.Snapshot;
        _ = raft.ConfChangeSingle;
        _ = raft.ConfChangeV2;
        _ = raft.ConfChange;
        _ = raft.Message;

        // Roles / status.
        _ = raft.StateRole;
        _ = raft.SoftState;
        _ = raft.roleName;
        _ = raft.Status;

        // Inflights.
        _ = raft.Inflights;

        // Quorum helpers.
        _ = raft.VoteResult;
        _ = raft.Index;
        _ = raft.AckedIndexer;
        _ = raft.AckIndexer;

        // Storage.
        _ = raft.RaftState;
        _ = raft.GetEntriesFor;
        _ = raft.GetEntriesContext;
        _ = raft.Storage;
        _ = raft.WritableStorage;
        _ = raft.cloneConfState;
        _ = raft.cloneSnapshot;
        _ = raft.cloneEntry;
        _ = raft.MemoryStorageCore;
        _ = raft.MemoryStorage;

        // Read-only queue.
        _ = raft.ReadOnlyOption;
        _ = raft.ReadState;
        _ = raft.ReadIndexStatus;
        _ = raft.ReadOnly;

        // Log layer.
        _ = raft.Unstable;
        _ = raft.RaftLog;
        _ = raft.MaybeAppendResult;
        _ = raft.FindConflictByTermResult;
        _ = raft.CommitInfo;

        // Configuration / progress cluster.
        _ = raft.MajorityConfig;
        _ = raft.CommittedIndexResult;
        _ = raft.majority;
        _ = raft.JointConfiguration;
        _ = raft.TrackerConfiguration;
        _ = raft.ProgressState;
        _ = raft.progressStateName;
        _ = raft.Progress;
        _ = raft.ProgressMap;
        _ = raft.MapChangeKind;
        _ = raft.MapChangeEntry;
        _ = raft.CountVoteResult;
        _ = raft.ProgressTracker;
        _ = raft.IncrChangeMap;
        _ = raft.ConfChangeResult;
        _ = raft.ConfChanger;
        _ = raft.joint;
        _ = raft.checkInvariants;
        _ = raft.restore;
        _ = raft.toConfChangeSingle;

        // Raft state machine.
        _ = raft.Config;
        _ = raft.defaultConfig;
        _ = raft.default_heartbeat_tick;
        _ = raft.UncommittedState;
        _ = raft.CampaignType;
        _ = raft.campaign_pre_election;
        _ = raft.campaign_election;
        _ = raft.campaign_transfer;
        _ = raft.Raft;

        // RawNode.
        _ = raft.Peer;
        _ = raft.SnapshotStatus;
        _ = raft.isLocalMessage;
        _ = raft.isResponseMessage;
        _ = raft.LightReady;
        _ = raft.Ready;
        _ = raft.RawNodeStatus;
        _ = raft.RawNode;

        // Raftor layer.
        _ = raft.ApplyResult;
        _ = raft.SnapshotWriter;
        _ = raft.SnapshotReader;
        _ = raft.StateMachine;
        _ = raft.MockStateMachine;
        _ = raft.MessageCallback;
        _ = raft.Transport;
        _ = raft.NoopTransport;
        _ = raft.ProposalResult;
        _ = raft.ReadIndexResult;
        _ = raft.ProposalCallback;
        _ = raft.ReadIndexCallback;
        _ = raft.ProposalTracker;
        _ = raft.RaftorConfig;
        _ = raft.NodeStatus;
        _ = raft.Raftor;
    }

    _ = try std.SemanticVersion.parse(raft.version);
}

test "error name round-trips" {
    try std.testing.expectEqualStrings("StepLocalMsg", raft.errorName(error.StepLocalMsg));
    try std.testing.expectEqualStrings("OutOfMemory", raft.errorName(error.OutOfMemory));
}

test "default message type is hup" {
    const m = raft.Message{};
    try std.testing.expectEqual(raft.MessageType.hup, m.msg_type);
}

test "get entries context canAsync mirrors raftpp" {
    try std.testing.expectEqual(true, raft.GetEntriesContext.empty_(true).canAsync());
    try std.testing.expectEqual(false, raft.GetEntriesContext.empty_(false).canAsync());
}

test "memory storage vtable wiring is callable" {
    const allocator = std.testing.allocator;
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);

    var raw = [_]raft.Entry{.{ .index = 1, .term = 1 }};
    try storage.setEntries(allocator, &raw);

    const writable = storage.asWritableStorage();
    try std.testing.expectEqual(@as(u64, 1), try writable.firstIndex());

    const read_iface = storage.asStorage();
    try std.testing.expectEqual(@as(u64, 1), try read_iface.firstIndex());
}
