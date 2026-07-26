//! Error model for raft-zig.
//!
//! Zig error unions cannot carry payloads, so the model is split:
//!
//! * Enum-style failures use the `Error` error set below. They compose with
//!   `try` and stay zero-cost.
//! * Validation failures that need a human-readable explanation return an
//!   `Error` value together with a separate `Message` struct produced by the
//!   caller (e.g. config validation logs the message before returning
//!   `error.InvalidConfig`).

const std = @import("std");

/// Universal error set for raft-zig. Every public API that can fail returns one
/// of these values.
pub const Error = error{
    // Storage errors.
    Compacted,
    Unavailable,
    LogTemporarilyUnavailable,
    SnapshotOutOfDate,
    SnapshotTemporarilyUnavailable,

    // Metadata errors.
    MetadataFileTooSmall,
    InvalidMetadataHeader,
    MetadataCrcMismatch,
    HardStateParseError,
    ConfStateParseError,
    ClusterMembershipParseError,
    InvalidClusterMembership,
    MissingClusterMembership,
    PeerAddressBookParseError,

    // Segment errors.
    CurrentSegmentNotFound,
    SegmentNotOpen,
    InvalidSegmentHeader,

    // io_uring errors.
    IoUringNotBuilt,
    IoUringNotLinux,
    IoUringInitFailed,
    IoUringProbeMissingOp,

    // WAL errors.
    CorruptEntryRecord,
    EntryParseError,
    WalOpenFailed,
    WalReadFailed,
    WalWriteFailed,
    WalSyncFailed,
    WalTruncateFailed,
    WalDeleteFailed,
    WalStatFailed,
    WalCreateDirectoryFailed,
    WalRenameFailed,
    WalCloseFailed,
    WalMetadataCorrupt,
    IncarnationExhausted,
    ContextSequenceExhausted,
    EventLoopBusy,

    // RaftLog errors.
    ZeroEntriesInSlice,

    // Raft state machine errors.
    StepLocalMsg,
    StepPeerNotFound,
    ProposalDropped,
    RequestSnapshotDropped,
    ChecksumMismatch,
    AlreadyStarted,
    ShuttingDown,
    DuplicateRequest,
    LostLeadership,
    IncompatibleStorage,
    ConfChangeParseError,

    // RPC transport errors.
    AddressPortMissing,
    AddressPortInvalid,
    AddressPortOutOfRange,
    BindFailed,
    ListenFailed,
    UdpBindFailed,
    UdpRecvStartFailed,
    ConnectionClosed,
    InvalidMagic,
    HeaderParseFailed,
    HandshakeParseFailed,
    PayloadParseFailed,
    HandshakeTooShort,
    HandshakeInvalidMagic,
    HandshakeBufferTooSmall,
    MessageTooLarge,
    TransportBackpressure,
    TransportIdentityMismatch,
    MessageSourceMismatch,
    MessageDestinationMismatch,
    LocalMessageOnTransport,
    Timeout,

    // Config validation errors.
    InvalidNodeId,
    HeartbeatTickTooSmall,
    ElectionTickTooSmall,
    MaxInflightMessagesTooSmall,
    LeaseBasedReadRequiresCheckQuorum,
    ListenAddressEmpty,
    DataDirectoryEmpty,
    NodeIdNotInInitialPeers,
    ClusterIdRequired,
    ClusterIdMismatch,
    PeerAddressMissing,
    DuplicatePeerId,
    NodeRetired,
    LegacyMembershipMigrationRequired,

    // ConfChange validation errors.
    LearnersNextMustBeEmpty,
    AutoLeaveMustBeFalse,
    ConfigAlreadyJoint,
    ZeroVoterConfigJoint,
    LeaveNonJointConfig,
    RemovedAllVoters,
    CannotApplySimpleInJointConfig,
    MultipleVotersChangedWithoutJoint,
    MissingPeerAddress,
    ConflictingPeerAddress,
    UnexpectedPeerAddress,
    RetiredNodeId,
    MalformedMembershipContext,

    // Validation failures that carry a caller-provided message.
    InvalidConfig,
    ConfChangeError,
    Fatal,

    // Generic allocation / I/O pass-through.
    OutOfMemory,
};

/// Stable identifier for each `Error` value, suitable for logging, metrics, and
/// data-driven tests. The order here must stay in sync with `Error`.
pub fn name(e: Error) []const u8 {
    return switch (e) {
        error.Compacted => "Compacted",
        error.Unavailable => "Unavailable",
        error.LogTemporarilyUnavailable => "LogTemporarilyUnavailable",
        error.SnapshotOutOfDate => "SnapshotOutOfDate",
        error.SnapshotTemporarilyUnavailable => "SnapshotTemporarilyUnavailable",
        error.MetadataFileTooSmall => "MetadataFileTooSmall",
        error.InvalidMetadataHeader => "InvalidMetadataHeader",
        error.MetadataCrcMismatch => "MetadataCrcMismatch",
        error.HardStateParseError => "HardStateParseError",
        error.ConfStateParseError => "ConfStateParseError",
        error.ClusterMembershipParseError => "ClusterMembershipParseError",
        error.InvalidClusterMembership => "InvalidClusterMembership",
        error.MissingClusterMembership => "MissingClusterMembership",
        error.PeerAddressBookParseError => "PeerAddressBookParseError",
        error.CurrentSegmentNotFound => "CurrentSegmentNotFound",
        error.SegmentNotOpen => "SegmentNotOpen",
        error.InvalidSegmentHeader => "InvalidSegmentHeader",
        error.IoUringNotBuilt => "IoUringNotBuilt",
        error.IoUringNotLinux => "IoUringNotLinux",
        error.IoUringInitFailed => "IoUringInitFailed",
        error.IoUringProbeMissingOp => "IoUringProbeMissingOp",
        error.CorruptEntryRecord => "CorruptEntryRecord",
        error.EntryParseError => "EntryParseError",
        error.WalOpenFailed => "WalOpenFailed",
        error.WalReadFailed => "WalReadFailed",
        error.WalWriteFailed => "WalWriteFailed",
        error.WalSyncFailed => "WalSyncFailed",
        error.WalTruncateFailed => "WalTruncateFailed",
        error.WalDeleteFailed => "WalDeleteFailed",
        error.WalStatFailed => "WalStatFailed",
        error.WalCreateDirectoryFailed => "WalCreateDirectoryFailed",
        error.WalRenameFailed => "WalRenameFailed",
        error.WalCloseFailed => "WalCloseFailed",
        error.WalMetadataCorrupt => "WalMetadataCorrupt",
        error.IncarnationExhausted => "IncarnationExhausted",
        error.ContextSequenceExhausted => "ContextSequenceExhausted",
        error.EventLoopBusy => "EventLoopBusy",
        error.ZeroEntriesInSlice => "ZeroEntriesInSlice",
        error.StepLocalMsg => "StepLocalMsg",
        error.StepPeerNotFound => "StepPeerNotFound",
        error.ProposalDropped => "ProposalDropped",
        error.RequestSnapshotDropped => "RequestSnapshotDropped",
        error.ChecksumMismatch => "ChecksumMismatch",
        error.AlreadyStarted => "AlreadyStarted",
        error.ShuttingDown => "ShuttingDown",
        error.DuplicateRequest => "DuplicateRequest",
        error.LostLeadership => "LostLeadership",
        error.IncompatibleStorage => "IncompatibleStorage",
        error.ConfChangeParseError => "ConfChangeParseError",
        error.AddressPortMissing => "AddressPortMissing",
        error.AddressPortInvalid => "AddressPortInvalid",
        error.AddressPortOutOfRange => "AddressPortOutOfRange",
        error.BindFailed => "BindFailed",
        error.ListenFailed => "ListenFailed",
        error.UdpBindFailed => "UdpBindFailed",
        error.UdpRecvStartFailed => "UdpRecvStartFailed",
        error.ConnectionClosed => "ConnectionClosed",
        error.InvalidMagic => "InvalidMagic",
        error.HeaderParseFailed => "HeaderParseFailed",
        error.HandshakeParseFailed => "HandshakeParseFailed",
        error.PayloadParseFailed => "PayloadParseFailed",
        error.HandshakeTooShort => "HandshakeTooShort",
        error.HandshakeInvalidMagic => "HandshakeInvalidMagic",
        error.HandshakeBufferTooSmall => "HandshakeBufferTooSmall",
        error.MessageTooLarge => "MessageTooLarge",
        error.TransportBackpressure => "TransportBackpressure",
        error.TransportIdentityMismatch => "TransportIdentityMismatch",
        error.MessageSourceMismatch => "MessageSourceMismatch",
        error.MessageDestinationMismatch => "MessageDestinationMismatch",
        error.LocalMessageOnTransport => "LocalMessageOnTransport",
        error.Timeout => "Timeout",
        error.InvalidNodeId => "InvalidNodeId",
        error.HeartbeatTickTooSmall => "HeartbeatTickTooSmall",
        error.ElectionTickTooSmall => "ElectionTickTooSmall",
        error.MaxInflightMessagesTooSmall => "MaxInflightMessagesTooSmall",
        error.LeaseBasedReadRequiresCheckQuorum => "LeaseBasedReadRequiresCheckQuorum",
        error.ListenAddressEmpty => "ListenAddressEmpty",
        error.DataDirectoryEmpty => "DataDirectoryEmpty",
        error.NodeIdNotInInitialPeers => "NodeIdNotInInitialPeers",
        error.ClusterIdRequired => "ClusterIdRequired",
        error.ClusterIdMismatch => "ClusterIdMismatch",
        error.PeerAddressMissing => "PeerAddressMissing",
        error.DuplicatePeerId => "DuplicatePeerId",
        error.NodeRetired => "NodeRetired",
        error.LegacyMembershipMigrationRequired => "LegacyMembershipMigrationRequired",
        error.LearnersNextMustBeEmpty => "LearnersNextMustBeEmpty",
        error.AutoLeaveMustBeFalse => "AutoLeaveMustBeFalse",
        error.ConfigAlreadyJoint => "ConfigAlreadyJoint",
        error.ZeroVoterConfigJoint => "ZeroVoterConfigJoint",
        error.LeaveNonJointConfig => "LeaveNonJointConfig",
        error.RemovedAllVoters => "RemovedAllVoters",
        error.CannotApplySimpleInJointConfig => "CannotApplySimpleInJointConfig",
        error.MultipleVotersChangedWithoutJoint => "MultipleVotersChangedWithoutJoint",
        error.MissingPeerAddress => "MissingPeerAddress",
        error.ConflictingPeerAddress => "ConflictingPeerAddress",
        error.UnexpectedPeerAddress => "UnexpectedPeerAddress",
        error.RetiredNodeId => "RetiredNodeId",
        error.MalformedMembershipContext => "MalformedMembershipContext",
        error.InvalidConfig => "InvalidConfig",
        error.ConfChangeError => "ConfChangeError",
        error.Fatal => "Fatal",
        error.OutOfMemory => "OutOfMemory",
    };
}

test "name covers every error value" {
    const cases = [_]Error{
        error.Compacted,
        error.Unavailable,
        error.StepLocalMsg,
        error.InvalidConfig,
        error.OutOfMemory,
        error.MultipleVotersChangedWithoutJoint,
    };
    for (cases) |e| try std.testing.expect(name(e).len > 0);
}
