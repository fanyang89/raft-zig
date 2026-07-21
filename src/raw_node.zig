//! User-facing RawNode API: wraps Raft with Ready/Advance batching.
//!
//! Ports `include/raftpp/core/raw_node.h` and `lib/core/raw_node.cc`. The
//! application drives Raft through three concepts:
//!
//!   * `tick()` — advance time by one tick; the returned bool indicates a
//!     state change worth flushing.
//!   * `hasReady()` / `getReady()` — collect pending state changes (HardState,
//!     SoftState, unstable entries, snapshot, committed entries, outbound
//!     messages) into one `Ready` value. The caller owns every slice.
//!   * `advance(rd)` / `advanceAppend(rd)` / `advanceApply()` — tell RawNode
//!     that the Ready has been persisted (and optionally applied) so the
//!     internal cursors can advance.
//!
//! RawNode also exposes thin wrappers (propose, campaign, transfer_leader,
//! read_index, report_unreachable, report_snapshot, step) that build the
//! appropriate Message and forward to the underlying Raft.

const std = @import("std");

const error_model = @import("core/error.zig");
const primitives = @import("core/primitives.zig");
const types = @import("core/types.zig");
const util = @import("core/util.zig");
const storage_mod = @import("storage.zig");
const read_only_mod = @import("read_only.zig");
const raft_config_mod = @import("raft_config.zig");
const raft_mod = @import("raft.zig");
const state_role_mod = @import("core/state_role.zig");

const Error = error_model.Error;
const invalid_id = primitives.invalid_id;

const Entry = types.Entry;
const EntryType = types.EntryType;
const MessageType = types.MessageType;
const Message = types.Message;
const HardState = types.HardState;
const Snapshot = types.Snapshot;
const ConfChangeV2 = types.ConfChangeV2;
const ConfChangeSingle = types.ConfChangeSingle;

const ReadState = read_only_mod.ReadState;
const Storage = storage_mod.Storage;
const Config = raft_config_mod.Config;
const Raft = raft_mod.Raft;
const StateRole = state_role_mod.StateRole;
const SoftState = state_role_mod.SoftState;

const cloneEntry = storage_mod.cloneEntry;
const cloneSnapshot = storage_mod.cloneSnapshot;
const setEntryChecksum = util.setEntryChecksum;
const encodeConfChangeV2 = util.encodeConfChangeV2;
const decodeConfChangeV2 = util.decodeConfChangeV2;

const log = std.log.scoped(.raft_zig_raw_node);

// ===========================================================================
// Peer / SnapshotStatus / message classification
// ===========================================================================

pub const Peer = struct {
    id: u64 = 0,
    context: ?[]const u8 = null,
};

pub const SnapshotStatus = enum(u8) { finish, failure };

pub fn isLocalMessage(t: MessageType) bool {
    return switch (t) {
        .hup, .beat, .unreachable_peer, .snap_status, .check_quorum => true,
        else => false,
    };
}

pub fn isResponseMessage(t: MessageType) bool {
    return switch (t) {
        .append_response,
        .request_vote_response,
        .heartbeat_response,
        .unreachable_peer,
        .request_pre_vote_response,
        => true,
        else => false,
    };
}

// ===========================================================================
// Ready / LightReady / ReadyRecord
// ===========================================================================

pub const LightReady = struct {
    commit_index: ?u64 = null,
    committed_entries: []Entry = &.{},
    messages: []Message = &.{},

    pub fn deinit(self: *LightReady, allocator: std.mem.Allocator) void {
        for (self.committed_entries) |*e| e.deinit(allocator);
        if (self.committed_entries.len != 0) allocator.free(self.committed_entries);
        for (self.messages) |*m| m.deinit(allocator);
        if (self.messages.len != 0) allocator.free(self.messages);
        self.* = .{};
    }
};

pub const Ready = struct {
    /// Monotonic Ready number assigned by RawNode; matches the entry in
    /// `RawNode.records` until the corresponding advance is called.
    number: u64 = 0,
    /// Set when the SoftState (leader_id / role) changed since the last Ready.
    ss: ?SoftState = null,
    /// Set when HardState (term/vote/commit) changed.
    hs: ?HardState = null,
    /// Pending read-index responses waiting for the application.
    read_states: []ReadState = &.{},
    /// Unstable entries the caller must persist before calling advance.
    entries: []Entry = &.{},
    /// Pending snapshot the caller must persist before calling advance.
    snapshot: ?Snapshot = null,
    /// True when the underlying Raft is not the leader; in that case the
    /// Ready's outbound messages must be sent after entries are persisted
    /// (matches raftpp's `is_persisted_msg`).
    is_persisted_msg: bool = false,
    /// Light portion (committed entries + outbound messages). Owned.
    light: LightReady = .{},
    /// True when the caller must fsync after persisting this Ready.
    must_sync: bool = false,

    pub fn deinit(self: *Ready, allocator: std.mem.Allocator) void {
        for (self.read_states) |*rs| rs.deinit(allocator);
        if (self.read_states.len != 0) allocator.free(self.read_states);
        for (self.entries) |*e| e.deinit(allocator);
        if (self.entries.len != 0) allocator.free(self.entries);
        if (self.snapshot) |*s| s.deinit(allocator);
        self.light.deinit(allocator);
        self.* = .{};
    }

    /// Messages the integrator should dispatch. Persisted-message flag
    /// suppresses the light.messages slice so callers don't double-send
    /// before persistence completes.
    pub fn messages(self: Ready) []const Message {
        if (self.is_persisted_msg) return &.{};
        return self.light.messages;
    }
};

const ReadyRecord = struct {
    number: u64,
    /// (index, term) of the last entry in the Ready's `entries` slice.
    last_entry: ?[2]u64 = null,
    /// (index, term) of the Ready's `snapshot`, if any.
    snapshot: ?[2]u64 = null,
};

// ===========================================================================
// RawNode
// ===========================================================================

pub const RawNode = struct {
    raft: Raft,
    prev_ss: SoftState,
    prev_hs: HardState,
    max_number: u64,
    records: std.ArrayList(ReadyRecord),
    commit_since_index: u64,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: Config, store: Storage) Error!RawNode {
        std.debug.assert(config.id != 0);
        const raft = try Raft.init(allocator, config, store);
        return .{
            .raft = raft,
            .prev_ss = raft.softState(),
            .prev_hs = raft.hardState(),
            .max_number = 0,
            .records = .empty,
            .commit_since_index = config.applied,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RawNode) void {
        self.records.deinit(self.allocator);
        self.raft.deinit();
        self.* = undefined;
    }

    pub fn raftPtr(self: *RawNode) *Raft {
        return &self.raft;
    }

    pub fn raftConst(self: *const RawNode) *const Raft {
        return &self.raft;
    }

    // -----------------------------------------------------------------------
    // Simple wrappers
    // -----------------------------------------------------------------------

    pub fn tick(self: *RawNode) Error!bool {
        return self.raft.tick();
    }

    pub fn ping(self: *RawNode) void {
        self.raft.ping();
    }

    pub fn setPriority(self: *RawNode, priority: i64) void {
        self.raft.setPriority(priority);
    }

    pub fn requestSnapshot(self: *RawNode) Error!void {
        return self.raft.requestSnapshot();
    }

    pub fn transferLeader(self: *RawNode, transferee: u64) Error!void {
        var m = Message{ .msg_type = .transfer_leader, .from = transferee };
        try self.raft.step(&m);
        m.deinit(self.allocator);
    }

    pub fn reportUnreachable(self: *RawNode, id: u64) Error!void {
        var m = Message{ .msg_type = .unreachable_peer, .from = id };
        try self.raft.step(&m);
        m.deinit(self.allocator);
    }

    pub fn reportSnapshot(self: *RawNode, id: u64, status: SnapshotStatus) Error!void {
        var m = Message{
            .msg_type = .snap_status,
            .from = id,
            .reject = status == .failure,
        };
        try self.raft.step(&m);
        m.deinit(self.allocator);
    }

    pub fn readIndex(self: *RawNode, ctx: []const u8) Error!void {
        var entries = try self.allocator.alloc(Entry, 1);
        entries[0] = .{ .data = try self.allocator.dupe(u8, ctx) };
        var m = Message{
            .msg_type = .read_index,
            .entries = entries,
        };
        defer m.deinit(self.allocator);
        try self.raft.step(&m);
    }

    pub fn campaign(self: *RawNode) Error!void {
        var m = Message{ .msg_type = .hup };
        try self.raft.step(&m);
        m.deinit(self.allocator);
    }

    pub fn propose(self: *RawNode, ctx: []const u8, data: []const u8) Error!void {
        // Same pattern as proposeConfChange: defer m.deinit handles cleanup.
        var entries = try self.allocator.alloc(Entry, 1);
        var e = Entry{
            .data = try self.allocator.dupe(u8, data),
            .context = try self.allocator.dupe(u8, ctx),
        };
        setEntryChecksum(&e);
        entries[0] = e;

        var m = Message{
            .msg_type = .propose,
            .from = self.raft.id,
            .entries = entries,
        };
        defer m.deinit(self.allocator);
        try self.raft.step(&m);
    }

    pub fn proposeConfChange(self: *RawNode, ctx: []const u8, cc: ConfChangeV2) Error!void {
        // All allocations land inside `m`, which is cleaned up by `defer
        // m.deinit`. No errdefers — they would double-free the entry data
        // when defer fires after the errdefer on the error path.
        const cc_bytes = try encodeConfChangeV2(self.allocator, cc);
        var entries = try self.allocator.alloc(Entry, 1);
        var e = Entry{
            .entry_type = .conf_change_v2,
            .data = cc_bytes,
            .context = try self.allocator.dupe(u8, ctx),
        };
        setEntryChecksum(&e);
        entries[0] = e;

        var m = Message{
            .msg_type = .propose,
            .from = self.raft.id,
            .entries = entries,
        };
        defer m.deinit(self.allocator);
        try self.raft.step(&m);
    }

    pub fn applyConfChange(self: *RawNode, cc: ConfChangeV2) Error!types.ConfState {
        return self.raft.applyConfChange(cc);
    }

    pub fn step(self: *RawNode, m_in: Message) Error!void {
        var m = m_in;
        defer m.deinit(self.allocator);
        if (isLocalMessage(m.msg_type)) return error.StepLocalMsg;
        if (self.raft.progress_tracker.getPtr(m.from) != null or !isResponseMessage(m.msg_type)) {
            try self.raft.step(&m);
            return;
        }
        return error.StepPeerNotFound;
    }

    pub fn getStatus(self: *const RawNode) Status {
        return .{
            .id = self.raft.id,
            .hard_state = self.raft.hardState(),
            .soft_state = self.raft.softState(),
            .applied = self.raft.raft_log.applied,
            .progress = if (self.raft.state == .leader) @as(?*const anyopaque, @ptrCast(&self.raft.progress_tracker)) else null,
        };
    }

    // -----------------------------------------------------------------------
    // Ready protocol
    // -----------------------------------------------------------------------

    pub fn hasReady(self: *const RawNode) bool {
        if (self.raft.messages.items.len > 0) return true;
        if (!self.raft.softState().eql(self.prev_ss)) return true;
        const hs = self.raft.hardState();
        if (!hardStateEql(hs, self.prev_hs)) return true;
        if (self.raft.read_states.items.len > 0) return true;
        if (self.raft.raft_log.unstable.entries.items.len > 0) return true;
        if (self.raft.raft_log.unstable.snapshot) |s| {
            if (s.metadata.index > 0) return true;
        }
        if (self.raft.raft_log.hasNextEntriesSince(self.commit_since_index)) return true;
        return false;
    }

    pub fn getReady(self: *RawNode) Error!Ready {
        self.max_number += 1;

        var rd = Ready{ .number = self.max_number };
        var record = ReadyRecord{ .number = self.max_number };

        // On transition to leader, drop any prior records — old pending
        // persists can no longer be matched.
        if (self.prev_ss.role != .leader and self.raft.state == .leader) {
            self.records.clearRetainingCapacity();
        }

        const ss = self.raft.softState();
        if (!ss.eql(self.prev_ss)) rd.ss = ss;

        const hs = self.raft.hardState();
        if (!hardStateEql(hs, self.prev_hs)) {
            if (hs.vote != self.prev_hs.vote or hs.term != self.prev_hs.term) {
                rd.must_sync = true;
            }
            rd.hs = hs;
        }

        if (self.raft.read_states.items.len > 0) {
            rd.read_states = try self.raft.read_states.toOwnedSlice(self.allocator);
        }

        if (self.raft.raft_log.unstable.snapshot) |s| {
            if (s.metadata.index > 0) {
                rd.snapshot = try cloneSnapshot(self.allocator, s);
                std.debug.assert(self.commit_since_index <= s.metadata.index);
                self.commit_since_index = s.metadata.index;
                std.debug.assert(!self.raft.raft_log.hasNextEntriesSince(self.commit_since_index));
                record.snapshot = .{ s.metadata.index, s.metadata.term };
                rd.must_sync = true;
            }
        }

        const unstable_ents = self.raft.raft_log.unstable.entries.items;
        if (unstable_ents.len > 0) {
            var entries = try self.allocator.alloc(Entry, unstable_ents.len);
            for (unstable_ents, 0..) |e, i| entries[i] = try cloneEntry(self.allocator, e);
            rd.entries = entries;
            rd.must_sync = true;
            const last = unstable_ents[unstable_ents.len - 1];
            record.last_entry = .{ last.index, last.term };
        }

        rd.is_persisted_msg = self.raft.state != .leader;
        rd.light = try self.getLightReady();
        try self.records.append(self.allocator, record);
        return rd;
    }

    fn getLightReady(self: *RawNode) Error!LightReady {
        var rd = LightReady{};
        const max_size = self.raft.max_committed_size_per_ready;

        if (try self.raft.raft_log.nextEntriesSince(self.commit_since_index, max_size)) |ents| {
            rd.committed_entries = ents;
        }

        self.raft.reduceUncommittedSize(rd.committed_entries);

        if (rd.committed_entries.len > 0) {
            const last = rd.committed_entries[rd.committed_entries.len - 1];
            std.debug.assert(self.commit_since_index < last.index);
            self.commit_since_index = last.index;
        }

        if (self.raft.messages.items.len > 0) {
            rd.messages = try self.raft.messages.toOwnedSlice(self.allocator);
        }

        return rd;
    }

    pub fn advanceAppend(self: *RawNode, rd: Ready) Error!LightReady {
        try self.commitReady(rd);
        self.onPersistReady(self.max_number);
        var light_rd = try self.getLightReady();

        if (self.raft.state != .leader and light_rd.messages.len > 0) {
            @panic("not leader but has new msg after advance");
        }

        const hs = self.raft.hardState();
        if (hs.commit > self.prev_hs.commit) {
            light_rd.commit_index = hs.commit;
            self.prev_hs.commit = hs.commit;
        } else {
            std.debug.assert(hs.commit == self.prev_hs.commit);
        }

        std.debug.assert(hardStateEql(hs, self.prev_hs));
        return light_rd;
    }

    pub fn advanceAppendAsync(self: *RawNode, rd: Ready) Error!void {
        try self.commitReady(rd);
    }

    pub fn advanceApplyTo(self: *RawNode, applied: u64) void {
        self.raft.commitApply(applied);
    }

    pub fn advanceApply(self: *RawNode) void {
        self.raft.commitApply(self.commit_since_index);
    }

    pub fn advance(self: *RawNode, rd: Ready) Error!LightReady {
        const applied = self.commit_since_index;
        const light_rd = try self.advanceAppend(rd);
        self.advanceApplyTo(applied);
        return light_rd;
    }

    fn commitReady(self: *RawNode, rd: Ready) Error!void {
        if (rd.ss) |ss| self.prev_ss = ss;
        if (rd.hs) |hs| self.prev_hs = hs;

        std.debug.assert(self.records.items.len > 0);
        const record = self.records.items[self.records.items.len - 1];
        std.debug.assert(record.number == rd.number);

        if (record.snapshot) |snap| {
            self.raft.raft_log.stableSnapshot(snap[0]);
        }
        if (record.last_entry) |le| {
            self.raft.raft_log.stableEntries(le[0], le[1]);
        }
    }

    pub fn onPersistReady(self: *RawNode, number: u64) void {
        var index: u64 = 0;
        var term: u64 = 0;
        var snap_index: u64 = 0;

        while (self.records.items.len > 0) {
            const record = self.records.items[0];
            if (record.number > number) break;
            _ = self.records.orderedRemove(0);

            if (record.snapshot) |s| {
                snap_index = s[0];
                index = 0;
                term = 0;
            }
            if (record.last_entry) |le| {
                index = le[0];
                term = le[1];
            }
        }

        if (snap_index != 0) self.raft.onPersistSnapshot(snap_index);
        if (index != 0) {
            self.raft.onPersistEntries(index, term) catch {};
        }
    }

    pub fn onEntriesFetched(self: *RawNode, ctx: anytype) void {
        // Async entry-fetch callback; we don't expose GetEntriesContext here.
        // The integrator should call this with the context returned from a
        // LogTemporarilyUnavailable error. The body mirrors raftpp's
        // implementation: re-issue SendAppend / SendAppendAggressively if we
        // are still the leader at the same term.
        _ = self;
        _ = ctx;
        // Implementation intentionally minimal; full async-WAL support lands
        // with the wal module in a later phase.
    }
};

// ===========================================================================
// Status snapshot returned by `getStatus`
// ===========================================================================

pub const Status = struct {
    id: u64,
    hard_state: HardState,
    soft_state: SoftState,
    applied: u64,
    /// Borrowed pointer to the underlying ProgressTracker; non-null only when
    /// this node is the leader. Same caveat as `core.status.Status`.
    progress: ?*const anyopaque = null,
};

// ===========================================================================
// HardState equality (replaces raftpp's capnp_util::equal<msg::HardState>)
// ===========================================================================

fn hardStateEql(a: HardState, b: HardState) bool {
    return a.term == b.term and a.vote == b.vote and a.commit == b.commit;
}

// ===========================================================================
// Smoke tests
// ===========================================================================

const MemoryStorage = @import("memory_storage.zig").MemoryStorage;

fn seedStorage(allocator: std.mem.Allocator, storage: *MemoryStorage, voters: []const u64) !void {
    const v = try allocator.dupe(u64, voters);
    var cs = types.ConfState{ .voters = v };
    try storage.setRaftState(allocator, .{ .conf_state = cs });
    cs.deinit(allocator);
}

fn makeConfig(id: u64) Config {
    var config = raft_config_mod.defaultConfig();
    config.id = id;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.election_timeout_seed = 42;
    return config;
}

test "rawnode: single-node campaign produces leader SoftState" {
    const allocator = std.testing.allocator;
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedStorage(allocator, &storage, &.{1});

    var node = try RawNode.init(allocator, makeConfig(1), storage.asStorage());
    defer node.deinit();

    try node.campaign();
    try std.testing.expect(node.hasReady());

    var rd = try node.getReady();
    defer rd.deinit(allocator);
    try std.testing.expect(rd.ss != null);
    try std.testing.expectEqual(StateRole.leader, rd.ss.?.role);
}

test "isLocalMessage and isResponseMessage classification" {
    try std.testing.expect(isLocalMessage(.hup));
    try std.testing.expect(isLocalMessage(.beat));
    try std.testing.expect(isLocalMessage(.check_quorum));
    try std.testing.expect(!isLocalMessage(.append));

    try std.testing.expect(isResponseMessage(.append_response));
    try std.testing.expect(isResponseMessage(.heartbeat_response));
    try std.testing.expect(!isResponseMessage(.append));
    try std.testing.expect(!isResponseMessage(.heartbeat));
}
