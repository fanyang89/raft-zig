//! Plain owned Zig structs for Raft wire and in-memory types.
//!
//! Field names and shapes follow the conventional Raft message layout. The
//! core consensus logic has no external schema dependency; a future `codec`
//! module can serialize these structs onto the wire (protobuf, capnp, or a
//! custom framing) without changing the core API.

const std = @import("std");

const primitives = @import("primitives.zig");

pub const invalid_index = primitives.invalid_index;
pub const invalid_id = primitives.invalid_id;

/// Raft log entry kind.
pub const EntryType = enum(u8) {
    normal = 0,
    conf_change = 1,
    conf_change_v2 = 2,
};

/// Internal Raft message kind. The numeric values are stable on the wire.
pub const MessageType = enum(u8) {
    hup = 0,
    beat = 1,
    propose = 2,
    append = 3,
    append_response = 4,
    request_vote = 5,
    request_vote_response = 6,
    snapshot = 7,
    heartbeat = 8,
    heartbeat_response = 9,
    unreachable_peer = 10,
    snap_status = 11,
    check_quorum = 12,
    transfer_leader = 13,
    timeout_now = 14,
    read_index = 15,
    read_index_resp = 16,
    request_pre_vote = 17,
    request_pre_vote_response = 18,
};

/// Configuration change operation. Mirrors `ConfChangeType`.
pub const ConfChangeType = enum(u8) {
    add_node = 0,
    remove_node = 1,
    add_learner_node = 2,
};

/// Strategy for entering joint consensus. Mirrors `ConfChangeTransition`.
pub const ConfChangeTransition = enum(u8) {
    /// Use the simple protocol when possible, otherwise implicit joint consensus.
    auto_ = 0,
    /// Always use joint consensus and leave automatically.
    implicit = 1,
    /// Remain in the joint configuration until the application proposes a no-op.
    explicit = 2,
};

/// A single log entry.
///
/// `data` holds either the application payload (for `normal` entries) or an
/// encoded `ConfChange`/`ConfChangeV2` (for config entries). `context` is an
/// opaque application-provided payload. Both buffers are owned by the entry and
/// released in `deinit`.
pub const Entry = struct {
    entry_type: EntryType = .normal,
    term: u64 = 0,
    index: u64 = 0,
    data: []u8 = &.{},
    context: []u8 = &.{},
    checksum: u32 = 0,

    pub fn deinit(self: *Entry, allocator: std.mem.Allocator) void {
        if (self.data.len != 0) allocator.free(self.data);
        if (self.context.len != 0) allocator.free(self.context);
        self.data = &.{};
        self.context = &.{};
    }

    /// Reset to a default-constructed entry without freeing buffers; useful for
    /// stack-allocated entries that never owned memory.
    pub fn reset(self: *Entry) void {
        self.* = .{};
    }
};

/// HardState is persisted across restarts and records the vote and commit index.
pub const HardState = struct {
    term: u64 = 0,
    vote: u64 = 0,
    commit: u64 = 0,

    pub fn isEmpty(self: HardState) bool {
        return self.term == 0 and self.vote == 0 and self.commit == 0;
    }
};

/// Cluster membership configuration. Mirrors `ConfState` in the schema.
pub const ConfState = struct {
    voters: []u64 = &.{},
    learners: []u64 = &.{},
    voters_outgoing: []u64 = &.{},
    learners_next: []u64 = &.{},
    auto_leave: bool = false,

    pub fn deinit(self: *ConfState, allocator: std.mem.Allocator) void {
        if (self.voters.len != 0) allocator.free(self.voters);
        if (self.learners.len != 0) allocator.free(self.learners);
        if (self.voters_outgoing.len != 0) allocator.free(self.voters_outgoing);
        if (self.learners_next.len != 0) allocator.free(self.learners_next);
        self.voters = &.{};
        self.learners = &.{};
        self.voters_outgoing = &.{};
        self.learners_next = &.{};
    }

    /// Structural equality on slice contents; used by tests and confchange logic.
    pub fn eql(self: ConfState, other: ConfState) bool {
        return std.mem.eql(u64, self.voters, other.voters) and
            std.mem.eql(u64, self.learners, other.learners) and
            std.mem.eql(u64, self.voters_outgoing, other.voters_outgoing) and
            std.mem.eql(u64, self.learners_next, other.learners_next) and
            self.auto_leave == other.auto_leave;
    }
};

/// SnapshotMetadata identifies a snapshot by position and the active config.
pub const SnapshotMetadata = struct {
    index: u64 = 0,
    term: u64 = 0,
    // ConfState reference is owned externally; the snapshot only needs read
    // access for broadcasting and comparison. The encoder copies it.
    conf_state: ConfState = .{},

    pub fn deinit(self: *SnapshotMetadata, allocator: std.mem.Allocator) void {
        self.conf_state.deinit(allocator);
    }
};

/// Snapshot bundles an opaque state-machine image with its metadata.
pub const Snapshot = struct {
    data: []u8 = &.{},
    metadata: SnapshotMetadata = .{},

    pub fn deinit(self: *Snapshot, allocator: std.mem.Allocator) void {
        if (self.data.len != 0) allocator.free(self.data);
        self.metadata.deinit(allocator);
        self.data = &.{};
    }
};

/// A single configuration change operation used inside `ConfChangeV2`.
pub const ConfChangeSingle = struct {
    change_type: ConfChangeType = .add_node,
    node_id: u64 = 0,
};

/// ConfChangeV2 initiates membership changes and supports both the simple
/// single-change protocol and full joint consensus.
pub const ConfChangeV2 = struct {
    transition: ConfChangeTransition = .auto_,
    changes: []ConfChangeSingle = &.{},
    context: []u8 = &.{},

    pub fn deinit(self: *ConfChangeV2, allocator: std.mem.Allocator) void {
        if (self.changes.len != 0) allocator.free(self.changes);
        if (self.context.len != 0) allocator.free(self.context);
        self.changes = &.{};
        self.context = &.{};
    }
};

/// Legacy single-shot configuration change. Kept for wire compatibility.
pub const ConfChange = struct {
    change_type: ConfChangeType = .add_node,
    node_id: u64 = 0,
    context: []u8 = &.{},
    id: u64 = 0,

    pub fn deinit(self: *ConfChange, allocator: std.mem.Allocator) void {
        if (self.context.len != 0) allocator.free(self.context);
        self.context = &.{};
    }
};

/// Message is the unit exchanged between Raft nodes.
///
/// Slice and entry buffers are owned by the sender. Receivers clone via
/// `clone(allocator)` if the message must outlive the inbound queue.
pub const Message = struct {
    msg_type: MessageType = .hup,
    to: u64 = 0,
    from: u64 = 0,
    term: u64 = 0,
    log_term: u64 = 0,
    index: u64 = 0,
    entries: []Entry = &.{},
    commit: u64 = 0,
    commit_term: u64 = 0,
    snapshot: ?Snapshot = null,
    request_snapshot: u64 = 0,
    reject: bool = false,
    reject_hint: u64 = 0,
    context: []u8 = &.{},
    priority: i64 = 0,

    pub fn deinit(self: *Message, allocator: std.mem.Allocator) void {
        for (self.entries) |*e| e.deinit(allocator);
        if (self.entries.len != 0) allocator.free(self.entries);
        if (self.snapshot) |*s| s.deinit(allocator);
        if (self.context.len != 0) allocator.free(self.context);
        self.entries = &.{};
        self.snapshot = null;
        self.context = &.{};
    }
};

test "default entry is empty" {
    var e = Entry{};
    try std.testing.expectEqual(@as(u64, 0), e.index);
    try std.testing.expectEqual(@as(u64, 0), e.term);
    try std.testing.expectEqualStrings("", e.data);
    e.deinit(std.testing.allocator);
}

test "hardstate isEmpty" {
    try std.testing.expect((HardState{}).isEmpty());
    try std.testing.expect(!(HardState{ .term = 1 }).isEmpty());
}

test "confstate eql compares slices" {
    var a = ConfState{};
    var b = ConfState{};
    try std.testing.expect(a.eql(b));
    a.auto_leave = true;
    try std.testing.expect(!a.eql(b));
    a.deinit(std.testing.allocator);
    b.deinit(std.testing.allocator);
}
