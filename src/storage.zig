//! Storage interface and entry-query context types.
//!
//! Ports `include/raftpp/core/storage.h` and `lib/core/storage.cc`. Raft core
//! uses `Storage` for read-only access; the orchestration layer persists
//! entries, hardstate, and snapshots via `WritableStorage`. The interface is a
//! vtable struct so custom backends (`MemoryStorage`, `WALStorage`, user
//! implementations) plug in without inheritance.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");

pub const Error = error_model.Error;
pub const Entry = types.Entry;
pub const Snapshot = types.Snapshot;
pub const HardState = types.HardState;
pub const ConfState = types.ConfState;

/// Persisted state supplied to a freshly started Raft node.
pub const RaftState = struct {
    hard_state: HardState = .{},
    conf_state: ConfState = .{},

    pub fn deinit(self: *RaftState, allocator: std.mem.Allocator) void {
        self.hard_state = .{};
        self.conf_state.deinit(allocator);
    }

    /// Deep clone using `allocator`. The caller owns the result.
    pub fn clone(self: RaftState, allocator: std.mem.Allocator) !RaftState {
        return .{
            .hard_state = self.hard_state,
            .conf_state = try cloneConfState(allocator, self.conf_state),
        };
    }
};

/// Copy a ConfState's slices into freshly owned allocations.
pub fn cloneConfState(allocator: std.mem.Allocator, src: ConfState) !ConfState {
    return .{
        .voters = try allocator.dupe(u64, src.voters),
        .learners = try allocator.dupe(u64, src.learners),
        .voters_outgoing = try allocator.dupe(u64, src.voters_outgoing),
        .learners_next = try allocator.dupe(u64, src.learners_next),
        .auto_leave = src.auto_leave,
    };
}

/// Copy a Snapshot (data + metadata.conf_state) into fresh allocations.
pub fn cloneSnapshot(allocator: std.mem.Allocator, src: Snapshot) !Snapshot {
    return .{
        .data = try allocator.dupe(u8, src.data),
        .metadata = .{
            .index = src.metadata.index,
            .term = src.metadata.term,
            .conf_state = try cloneConfState(allocator, src.metadata.conf_state),
        },
    };
}

/// Copy an Entry (data + context buffers) into fresh allocations.
pub fn cloneEntry(allocator: std.mem.Allocator, src: Entry) !Entry {
    return .{
        .entry_type = src.entry_type,
        .term = src.term,
        .index = src.index,
        .data = try allocator.dupe(u8, src.data),
        .context = try allocator.dupe(u8, src.context),
        .checksum = src.checksum,
    };
}

/// Caller reason for fetching entries. Drives async-fetch behavior in
/// `MemoryStorage` and future WAL backends.
pub const GetEntriesFor = enum(u8) {
    send_append,
    gen_ready,
    transfer_leader,
    commit_by_vote,
    empty,
};

/// Tagged payload carried alongside `GetEntriesFor`.
pub const GetEntriesContext = union(GetEntriesFor) {
    send_append: struct { to: u64, term: u64, aggressively: bool },
    gen_ready: void,
    transfer_leader: void,
    commit_by_vote: void,
    empty: struct { can_async: bool },

    /// Mirrors `GetEntriesContext::CanAsync()` in raftpp. Only `send_append`
    /// and `empty.can_async = true` may trigger async LogTemporarilyUnavailable.
    pub fn canAsync(self: GetEntriesContext) bool {
        return switch (self) {
            .empty => |e| e.can_async,
            .send_append => true,
            else => false,
        };
    }

    pub fn empty_(can_async_val: bool) GetEntriesContext {
        return .{ .empty = .{ .can_async = can_async_val } };
    }
};

/// Type-erased read-only Storage. Implementations provide a `VTable`; callers
/// pass `Storage` by value (two words).
pub const Storage = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        initial_state: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) Error!RaftState,
        entries: *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            low: u64,
            high: u64,
            max_size: ?u64,
            context: GetEntriesContext,
        ) Error![]Entry,
        term: *const fn (ctx: *anyopaque, idx: u64) Error!u64,
        first_index: *const fn (ctx: *anyopaque) Error!u64,
        last_index: *const fn (ctx: *anyopaque) Error!u64,
        get_snapshot: *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            request_index: u64,
            to: u64,
        ) Error!Snapshot,
    };

    pub fn initialState(self: Storage, allocator: std.mem.Allocator) Error!RaftState {
        return self.vtable.initial_state(self.ctx, allocator);
    }

    pub fn entries(
        self: Storage,
        allocator: std.mem.Allocator,
        low: u64,
        high: u64,
        max_size: ?u64,
        context: GetEntriesContext,
    ) Error![]Entry {
        return self.vtable.entries(self.ctx, allocator, low, high, max_size, context);
    }

    pub fn term(self: Storage, idx: u64) Error!u64 {
        return self.vtable.term(self.ctx, idx);
    }

    pub fn firstIndex(self: Storage) Error!u64 {
        return self.vtable.first_index(self.ctx);
    }

    pub fn lastIndex(self: Storage) Error!u64 {
        return self.vtable.last_index(self.ctx);
    }

    pub fn getSnapshot(
        self: Storage,
        allocator: std.mem.Allocator,
        request_index: u64,
        to: u64,
    ) Error!Snapshot {
        return self.vtable.get_snapshot(self.ctx, allocator, request_index, to);
    }
};

/// Read+write storage used by the orchestration layer. Adds mutation entry
/// points on top of the same `ctx` handle.
pub const WritableStorage = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        // Reads (mirror Storage.VTable).
        initial_state: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) Error!RaftState,
        entries: *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            low: u64,
            high: u64,
            max_size: ?u64,
            context: GetEntriesContext,
        ) Error![]Entry,
        term: *const fn (ctx: *anyopaque, idx: u64) Error!u64,
        first_index: *const fn (ctx: *anyopaque) Error!u64,
        last_index: *const fn (ctx: *anyopaque) Error!u64,
        get_snapshot: *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            request_index: u64,
            to: u64,
        ) Error!Snapshot,

        // Writes.
        append: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, to_append: []const Entry) Error!void,
        set_hard_state: *const fn (ctx: *anyopaque, hs: HardState) Error!void,
        set_conf_state: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, cs: ConfState) Error!void,
        apply_snapshot: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, snap: Snapshot) Error!void,
        sync_: *const fn (ctx: *anyopaque) Error!void,
    };

    pub fn initialState(self: WritableStorage, allocator: std.mem.Allocator) Error!RaftState {
        return self.vtable.initial_state(self.ctx, allocator);
    }

    pub fn entries(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        low: u64,
        high: u64,
        max_size: ?u64,
        context: GetEntriesContext,
    ) Error![]Entry {
        return self.vtable.entries(self.ctx, allocator, low, high, max_size, context);
    }

    pub fn term(self: WritableStorage, idx: u64) Error!u64 {
        return self.vtable.term(self.ctx, idx);
    }

    pub fn firstIndex(self: WritableStorage) Error!u64 {
        return self.vtable.first_index(self.ctx);
    }

    pub fn lastIndex(self: WritableStorage) Error!u64 {
        return self.vtable.last_index(self.ctx);
    }

    pub fn getSnapshot(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        request_index: u64,
        to: u64,
    ) Error!Snapshot {
        return self.vtable.get_snapshot(self.ctx, allocator, request_index, to);
    }

    pub fn append(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        to_append: []const Entry,
    ) Error!void {
        return self.vtable.append(self.ctx, allocator, to_append);
    }

    pub fn setHardState(self: WritableStorage, hs: HardState) Error!void {
        return self.vtable.set_hard_state(self.ctx, hs);
    }

    pub fn setConfState(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        cs: ConfState,
    ) Error!void {
        return self.vtable.set_conf_state(self.ctx, allocator, cs);
    }

    pub fn applySnapshot(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        snapshot: Snapshot,
    ) Error!void {
        return self.vtable.apply_snapshot(self.ctx, allocator, snapshot);
    }

    pub fn sync(self: WritableStorage) Error!void {
        return self.vtable.sync_(self.ctx);
    }
};

test "raft state clone is deep" {
    const allocator = std.testing.allocator;
    var original = RaftState{
        .hard_state = .{ .term = 3, .vote = 5, .commit = 4 },
        .conf_state = .{
            .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }),
            .learners = try allocator.dupe(u64, &.{4}),
        },
    };
    defer original.deinit(allocator);

    var copy = try original.clone(allocator);
    defer copy.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, copy.conf_state.voters);
    try std.testing.expect(copy.conf_state.voters.ptr != original.conf_state.voters.ptr);
}

test "get entries context canAsync" {
    try std.testing.expectEqual(true, (GetEntriesContext.empty_(true)).canAsync());
    try std.testing.expectEqual(false, (GetEntriesContext.empty_(false)).canAsync());
    try std.testing.expectEqual(true, (GetEntriesContext{ .send_append = .{
        .to = 1,
        .term = 1,
        .aggressively = false,
    } }).canAsync());
    try std.testing.expectEqual(false, (GetEntriesContext{ .gen_ready = {} }).canAsync());
}
