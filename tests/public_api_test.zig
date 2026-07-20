//! Stable public API surface for downstream consumers.
//!
//! Mirrors `tests/public_api_test.zig` in grpc-lite: this test fails to compile
//! if a re-exported symbol disappears, which keeps the public API stable across
//! refactors.

const std = @import("std");
const raft = @import("raft_zig");

test "stable public API compiles for downstream consumers" {
    comptime {
        _ = raft.Error;
        _ = raft.errorName;
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
        _ = raft.invalid_index;
        _ = raft.invalid_id;
        _ = raft.StateRole;
        _ = raft.SoftState;
        _ = raft.roleName;
        _ = raft.Status;
    }

    _ = try std.SemanticVersion.parse(raft.version);
}

test "error name round-trips" {
    try std.testing.expectEqualStrings(
        "StepLocalMsg",
        raft.errorName(error.StepLocalMsg),
    );
    try std.testing.expectEqualStrings(
        "OutOfMemory",
        raft.errorName(error.OutOfMemory),
    );
}

test "default message type is hup" {
    const m = raft.Message{};
    try std.testing.expectEqual(raft.MessageType.hup, m.msg_type);
}
