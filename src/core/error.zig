//! Error model for raft-zig.
//!
//! raftpp uses a variant `RaftError` type carrying either enum codes or
//! message-carrying structs (`InvalidConfigError`, `ConfChangeError`,
//! `FatalError`). Zig error unions cannot carry payloads, so we split the model:
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
    // Storage errors (mirror StorageErrorCode in raftpp).
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

    // RaftLog errors.
    ZeroEntriesInSlice,

    // Raft state machine errors (mirror RaftErrorCode in raftpp).
    StepLocalMsg,
    StepPeerNotFound,
    ProposalDropped,
    RequestSnapshotDropped,
    ChecksumMismatch,
    AlreadyStarted,
    ShuttingDown,
    LostLeadership,
    IncompatibleStorage,
    ConfChangeParseError,

    // RPC transport errors (mirror RpcErrorCode in raftpp).
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
    Timeout,

    // Config validation errors (mirror ConfigErrorCode in raftpp).
    InvalidNodeId,
    HeartbeatTickTooSmall,
    ElectionTickTooSmall,
    MaxInflightMessagesTooSmall,
    LeaseBasedReadRequiresCheckQuorum,
    ListenAddressEmpty,
    DataDirectoryEmpty,
    NodeIdNotInInitialPeers,

    // ConfChange validation errors (mirror ConfChangeErrorCode in raftpp).
    LearnersNextMustBeEmpty,
    AutoLeaveMustBeFalse,
    ConfigAlreadyJoint,
    ZeroVoterConfigJoint,
    LeaveNonJointConfig,
    RemovedAllVoters,
    CannotApplySimpleInJointConfig,
    MultipleVotersChangedWithoutJoint,

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
        error.ZeroEntriesInSlice => "ZeroEntriesInSlice",
        error.StepLocalMsg => "StepLocalMsg",
        error.StepPeerNotFound => "StepPeerNotFound",
        error.ProposalDropped => "ProposalDropped",
        error.RequestSnapshotDropped => "RequestSnapshotDropped",
        error.ChecksumMismatch => "ChecksumMismatch",
        error.AlreadyStarted => "AlreadyStarted",
        error.ShuttingDown => "ShuttingDown",
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
        error.Timeout => "Timeout",
        error.InvalidNodeId => "InvalidNodeId",
        error.HeartbeatTickTooSmall => "HeartbeatTickTooSmall",
        error.ElectionTickTooSmall => "ElectionTickTooSmall",
        error.MaxInflightMessagesTooSmall => "MaxInflightMessagesTooSmall",
        error.LeaseBasedReadRequiresCheckQuorum => "LeaseBasedReadRequiresCheckQuorum",
        error.ListenAddressEmpty => "ListenAddressEmpty",
        error.DataDirectoryEmpty => "DataDirectoryEmpty",
        error.NodeIdNotInInitialPeers => "NodeIdNotInInitialPeers",
        error.LearnersNextMustBeEmpty => "LearnersNextMustBeEmpty",
        error.AutoLeaveMustBeFalse => "AutoLeaveMustBeFalse",
        error.ConfigAlreadyJoint => "ConfigAlreadyJoint",
        error.ZeroVoterConfigJoint => "ZeroVoterConfigJoint",
        error.LeaveNonJointConfig => "LeaveNonJointConfig",
        error.RemovedAllVoters => "RemovedAllVoters",
        error.CannotApplySimpleInJointConfig => "CannotApplySimpleInJointConfig",
        error.MultipleVotersChangedWithoutJoint => "MultipleVotersChangedWithoutJoint",
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
