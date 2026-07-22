//! In-memory Storage backend.
//!
//! Ports `include/raftpp/core/memory_storage.h` and
//! `lib/core/memory_storage.cc`. `MemoryStorageCore` holds the actual state;
//! `MemoryStorage` is the public wrapper.
//!
//! raftpp guards every entry point with `std::mutex`. Zig 0.16 dropped the
//! blocking `std.Thread.Mutex` in favor of Io-integrated locks, which conflicts
//! with the explicit-allocator, no-hidden-runtime style this module mirrors.
//! Raft core is a single-threaded event loop by design, so the storage API is
//! documented as not safe to call concurrently from multiple threads — callers
//! that need cross-thread access must wrap it (for example with a queue plus a
//! dedicated storage task).

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const util = @import("core/util.zig");
const storage_mod = @import("storage.zig");

pub const Error = error_model.Error;
pub const Entry = types.Entry;
pub const Snapshot = types.Snapshot;
pub const HardState = types.HardState;
pub const ConfState = types.ConfState;
pub const SnapshotMetadata = types.SnapshotMetadata;
pub const RaftState = storage_mod.RaftState;
pub const GetEntriesContext = storage_mod.GetEntriesContext;
pub const Storage = storage_mod.Storage;
pub const WritableStorage = storage_mod.WritableStorage;

const log = std.log.scoped(.raft_zig_memory_storage);

/// Non-locking core. Tests and single-threaded callers use this directly.
pub const MemoryStorageCore = struct {
    raft_state: RaftState,
    entries: std.ArrayList(Entry),
    snapshot_metadata: SnapshotMetadata,
    trigger_snapshot_unavailable: bool,
    trigger_log_unavailable: bool,
    get_entries_context: ?GetEntriesContext,

    pub fn init() MemoryStorageCore {
        return .{
            .raft_state = .{},
            .entries = .empty,
            .snapshot_metadata = .{},
            .trigger_snapshot_unavailable = false,
            .trigger_log_unavailable = false,
            .get_entries_context = null,
        };
    }

    pub fn deinit(self: *MemoryStorageCore, allocator: std.mem.Allocator) void {
        self.raft_state.deinit(allocator);
        for (self.entries.items) |*e| e.deinit(allocator);
        self.entries.deinit(allocator);
        self.snapshot_metadata.deinit(allocator);
        self.* = undefined;
    }

    pub fn setHardState(self: *MemoryStorageCore, hs: HardState) void {
        self.raft_state.hard_state = hs;
    }

    pub fn hasEntryAt(self: MemoryStorageCore, index: u64) bool {
        return self.entries.items.len > 0 and index >= self.firstIndex() and index <= self.lastIndex();
    }

    /// Update hard_state.commit/term to the entry at `index`. Asserts the entry
    /// exists, matching raftpp's `ASSERT(HasEntryAt(...))`.
    pub fn commitTo(self: *MemoryStorageCore, index: u64) void {
        std.debug.assert(self.hasEntryAt(index));
        const diff = index - self.firstIndex();
        self.raft_state.hard_state.commit = index;
        self.raft_state.hard_state.term = self.entries.items[diff].term;
    }

    pub fn applySnapshot(self: *MemoryStorageCore, allocator: std.mem.Allocator, snap: Snapshot) Error!void {
        const meta = snap.metadata;
        if (self.firstIndex() > meta.index) return error.SnapshotOutOfDate;

        self.snapshot_metadata.deinit(allocator);
        self.snapshot_metadata = .{
            .index = meta.index,
            .term = meta.term,
            .conf_state = try storage_mod.cloneConfState(allocator, meta.conf_state),
        };

        self.raft_state.hard_state.term = @max(self.raft_state.hard_state.term, meta.term);
        self.raft_state.hard_state.commit = meta.index;
        for (self.entries.items) |*e| e.deinit(allocator);
        self.entries.clearRetainingCapacity();

        self.raft_state.conf_state.deinit(allocator);
        self.raft_state.conf_state = try storage_mod.cloneConfState(allocator, meta.conf_state);
    }

    pub fn applyLocalSnapshot(self: *MemoryStorageCore, allocator: std.mem.Allocator, snap: Snapshot) Error!void {
        const meta = snap.metadata;
        if (self.firstIndex() > meta.index) return error.SnapshotOutOfDate;

        self.snapshot_metadata.deinit(allocator);
        self.snapshot_metadata = .{
            .index = meta.index,
            .term = meta.term,
            .conf_state = try storage_mod.cloneConfState(allocator, meta.conf_state),
        };

        self.raft_state.hard_state.term = @max(self.raft_state.hard_state.term, meta.term);
        if (self.raft_state.hard_state.commit < meta.index) {
            self.raft_state.hard_state.commit = meta.index;
        }
        self.raft_state.conf_state.deinit(allocator);
        self.raft_state.conf_state = try storage_mod.cloneConfState(allocator, meta.conf_state);

        try self.compact(allocator, meta.index + 1);
    }

    pub fn compact(self: *MemoryStorageCore, allocator: std.mem.Allocator, compact_index: u64) Error!void {
        if (compact_index <= self.firstIndex()) return;

        if (compact_index > self.lastIndex() + 1) {
            log.warn("compact not received raft logs: compact_index={} last_index={}", .{ compact_index, self.lastIndex() });
            return error.Fatal;
        }

        if (self.entries.items.len == 0) return;

        const offset = compact_index - self.entries.items[0].index;
        const drop_count = @min(offset, self.entries.items.len);
        var i: usize = 0;
        while (i < drop_count) : (i += 1) self.entries.items[i].deinit(allocator);
        // Shift remaining entries forward.
        std.mem.copyForwards(Entry, self.entries.items[0..], self.entries.items[drop_count..]);
        self.entries.shrinkRetainingCapacity(self.entries.items.len - drop_count);
    }

    pub fn append(self: *MemoryStorageCore, allocator: std.mem.Allocator, ents: []const Entry) Error!void {
        return self.mayAppend(allocator, ents);
    }

    pub fn mayAppend(self: *MemoryStorageCore, allocator: std.mem.Allocator, ents: []const Entry) Error!void {
        if (ents.len == 0) return;

        const new_appended = ents[0].index;
        if (self.firstIndex() > new_appended) {
            log.warn("overwrite compacted raft logs: compacted={} new_appended={}", .{ self.firstIndex() - 1, new_appended });
            return error.Fatal;
        }
        if (self.lastIndex() + 1 < new_appended) {
            log.warn("raft logs should be continuous: last_index={} new_appended={}", .{ self.lastIndex(), new_appended });
            return error.Fatal;
        }

        const diff = new_appended - self.firstIndex();
        if (diff < self.entries.items.len) {
            var i: usize = diff;
            while (i < self.entries.items.len) : (i += 1) self.entries.items[i].deinit(allocator);
            self.entries.shrinkRetainingCapacity(diff);
        }
        try self.entries.ensureUnusedCapacity(allocator, ents.len);
        for (ents) |ent| {
            try self.entries.append(allocator, try storage_mod.cloneEntry(allocator, ent));
        }
    }

    pub fn triggerSnapshotUnavailable(self: *MemoryStorageCore) void {
        self.trigger_snapshot_unavailable = true;
    }

    pub fn triggerLogUnavailable(self: *MemoryStorageCore) void {
        self.trigger_log_unavailable = true;
    }

    pub fn takeGetEntriesContext(self: *MemoryStorageCore) ?GetEntriesContext {
        const ctx = self.get_entries_context;
        self.get_entries_context = null;
        return ctx;
    }

    pub fn firstIndex(self: MemoryStorageCore) u64 {
        if (self.entries.items.len == 0) return self.snapshot_metadata.index + 1;
        return self.entries.items[0].index;
    }

    pub fn lastIndex(self: MemoryStorageCore) u64 {
        if (self.entries.items.len == 0) return self.snapshot_metadata.index;
        return self.entries.items[self.entries.items.len - 1].index;
    }

    /// Build a snapshot at `hard_state.commit`, deriving the term from the
    /// entry at that index (or from `snapshot_metadata` when they coincide).
    pub fn snapshot(self: MemoryStorageCore, allocator: std.mem.Allocator) Error!Snapshot {
        const commit = self.raft_state.hard_state.commit;
        if (commit < self.snapshot_metadata.index) {
            log.warn("commit {} < snapshot_metadata.index {}", .{ commit, self.snapshot_metadata.index });
            return error.Fatal;
        }

        var term = self.snapshot_metadata.term;
        if (commit > self.snapshot_metadata.index) {
            const offset = self.entries.items[0].index;
            if (commit - offset >= self.entries.items.len) {
                log.warn("commit {} out of range (last={})", .{ commit, self.lastIndex() });
                return error.Fatal;
            }
            term = self.entries.items[commit - offset].term;
        }

        return .{
            .data = try allocator.alloc(u8, 0),
            .metadata = .{
                .index = commit,
                .term = term,
                .conf_state = try storage_mod.cloneConfState(allocator, self.raft_state.conf_state),
            },
        };
    }
};

/// Single-threaded `WritableStorage` backed by `MemoryStorageCore`. Callers
/// that need cross-thread access must coordinate externally.
pub const MemoryStorage = struct {
    core: MemoryStorageCore,

    pub fn init() MemoryStorage {
        return .{ .core = MemoryStorageCore.init() };
    }

    pub fn deinit(self: *MemoryStorage, allocator: std.mem.Allocator) void {
        self.core.deinit(allocator);
        self.* = undefined;
    }

    pub fn initialState(self: *MemoryStorage, allocator: std.mem.Allocator) Error!RaftState {
        return self.core.raft_state.clone(allocator);
    }

    pub fn entries(
        self: *MemoryStorage,
        allocator: std.mem.Allocator,
        low: u64,
        high: u64,
        max_size: ?u64,
        context: GetEntriesContext,
    ) Error![]Entry {
        if (low < self.core.firstIndex()) return error.Compacted;
        if (high > self.core.lastIndex() + 1) {
            log.warn("index out of bound: last={} high={}", .{ self.core.lastIndex() + 1, high });
            return error.Fatal;
        }

        if (self.core.trigger_log_unavailable and context.canAsync()) {
            self.core.get_entries_context = context;
            return error.LogTemporarilyUnavailable;
        }

        const offset = self.core.entries.items[0].index;
        const lo: usize = @intCast(low - offset);
        const hi: usize = @intCast(high - offset);
        var result = try allocator.alloc(Entry, hi - lo);
        var actual_len: usize = 0;
        errdefer {
            for (result[0..actual_len]) |*e| e.deinit(allocator);
            allocator.free(result);
        }
        for (self.core.entries.items[lo..hi]) |ent| {
            result[actual_len] = try storage_mod.cloneEntry(allocator, ent);
            actual_len += 1;
        }
        if (max_size) |m| {
            var slice = result[0..actual_len];
            util.limitSize(&slice, m);
            // Free truncated tail.
            for (slice.len..actual_len) |i| result[i].deinit(allocator);
            actual_len = slice.len;
        }
        // Shrink the allocation if LimitSize trimmed entries.
        return allocator.realloc(result, actual_len) catch return result[0..actual_len];
    }

    pub fn setEntries(self: *MemoryStorage, allocator: std.mem.Allocator, src: []const Entry) !void {
        for (self.core.entries.items) |*e| e.deinit(allocator);
        self.core.entries.clearRetainingCapacity();
        try self.core.entries.ensureTotalCapacity(allocator, src.len);
        for (src) |ent| {
            try self.core.entries.append(allocator, try storage_mod.cloneEntry(allocator, ent));
        }
    }

    pub fn append(self: *MemoryStorage, allocator: std.mem.Allocator, ents: []const Entry) Error!void {
        return self.core.append(allocator, ents);
    }

    pub fn mayAppend(self: *MemoryStorage, allocator: std.mem.Allocator, ents: []const Entry) Error!void {
        return self.core.mayAppend(allocator, ents);
    }

    pub fn compact(self: *MemoryStorage, allocator: std.mem.Allocator, idx: u64) Error!void {
        return self.core.compact(allocator, idx);
    }

    pub fn setRaftState(self: *MemoryStorage, allocator: std.mem.Allocator, raft_state: RaftState) !void {
        self.core.raft_state.deinit(allocator);
        self.core.raft_state = try raft_state.clone(allocator);
    }

    pub fn setConfState(self: *MemoryStorage, allocator: std.mem.Allocator, cs: ConfState) Error!void {
        self.core.raft_state.conf_state.deinit(allocator);
        self.core.raft_state.conf_state = try storage_mod.cloneConfState(allocator, cs);
    }

    pub fn setHardState(self: *MemoryStorage, hs: HardState) Error!void {
        self.core.setHardState(hs);
    }

    pub fn triggerSnapshotUnavailable(self: *MemoryStorage) void {
        self.core.trigger_snapshot_unavailable = true;
    }

    pub fn triggerLogUnavailable(self: *MemoryStorage, enable: bool) void {
        self.core.trigger_log_unavailable = enable;
    }

    pub fn takeGetEntriesContext(self: *MemoryStorage) ?GetEntriesContext {
        return self.core.takeGetEntriesContext();
    }

    pub fn applySnapshot(self: *MemoryStorage, allocator: std.mem.Allocator, snapshot: Snapshot) Error!void {
        return self.core.applySnapshot(allocator, snapshot);
    }

    pub fn applyLocalSnapshot(self: *MemoryStorage, allocator: std.mem.Allocator, snapshot: Snapshot) Error!void {
        return self.core.applyLocalSnapshot(allocator, snapshot);
    }

    pub fn allEntries(self: *MemoryStorage, allocator: std.mem.Allocator) ![]Entry {
        var result = try allocator.alloc(Entry, self.core.entries.items.len);
        var actual_len: usize = 0;
        errdefer {
            for (result[0..actual_len]) |*e| e.deinit(allocator);
            allocator.free(result);
        }
        for (self.core.entries.items) |ent| {
            result[actual_len] = try storage_mod.cloneEntry(allocator, ent);
            actual_len += 1;
        }
        return result;
    }

    pub fn sync_(self: *MemoryStorage) Error!void {
        _ = self;
    }

    pub fn term(self: *MemoryStorage, idx: u64) Error!u64 {
        if (idx == self.core.snapshot_metadata.index) return self.core.snapshot_metadata.term;

        const offset = self.core.firstIndex();
        if (idx < offset) return error.Compacted;
        if (idx > self.core.lastIndex()) return error.Unavailable;
        return self.core.entries.items[idx - offset].term;
    }

    pub fn firstIndex(self: *MemoryStorage) Error!u64 {
        return self.core.firstIndex();
    }

    pub fn lastIndex(self: *MemoryStorage) Error!u64 {
        return self.core.lastIndex();
    }

    pub fn getSnapshot(
        self: *MemoryStorage,
        allocator: std.mem.Allocator,
        request_index: u64,
        to: u64,
    ) Error!Snapshot {
        _ = to;
        if (self.core.trigger_snapshot_unavailable) {
            self.core.trigger_snapshot_unavailable = false;
            return error.SnapshotTemporarilyUnavailable;
        }

        var snap = try self.core.snapshot(allocator);
        if (snap.metadata.index < request_index) {
            // Rebuild with the requested index, preserving the term.
            const new_data = snap.data;
            snap.data = &.{};
            const snap_term = snap.metadata.term;
            const new_conf = snap.metadata.conf_state;
            snap.metadata.conf_state = .{};
            snap.deinit(allocator);
            return .{
                .data = new_data,
                .metadata = .{
                    .index = request_index,
                    .term = snap_term,
                    .conf_state = new_conf,
                },
            };
        }
        return snap;
    }

    /// VTable wiring for `asWritableStorage` / `asStorage`.
    fn initial_state_impl(ctx: *anyopaque, allocator: std.mem.Allocator) Error!RaftState {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.initialState(allocator);
    }

    fn entries_impl(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        low: u64,
        high: u64,
        max_size: ?u64,
        context: GetEntriesContext,
    ) Error![]Entry {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.entries(allocator, low, high, max_size, context);
    }

    fn term_impl(ctx: *anyopaque, idx: u64) Error!u64 {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.term(idx);
    }

    fn first_index_impl(ctx: *anyopaque) Error!u64 {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.firstIndex();
    }

    fn last_index_impl(ctx: *anyopaque) Error!u64 {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.lastIndex();
    }

    fn get_snapshot_impl(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        request_index: u64,
        to: u64,
    ) Error!Snapshot {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.getSnapshot(allocator, request_index, to);
    }

    fn append_impl(ctx: *anyopaque, allocator: std.mem.Allocator, to_append: []const Entry) Error!void {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.append(allocator, to_append);
    }

    fn set_hard_state_impl(ctx: *anyopaque, hs: HardState) Error!void {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.setHardState(hs);
    }

    fn set_conf_state_impl(ctx: *anyopaque, allocator: std.mem.Allocator, cs: ConfState) Error!void {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.setConfState(allocator, cs);
    }

    fn apply_snapshot_impl(ctx: *anyopaque, allocator: std.mem.Allocator, snapshot: Snapshot) Error!void {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.applySnapshot(allocator, snapshot);
    }

    fn apply_local_snapshot_impl(ctx: *anyopaque, allocator: std.mem.Allocator, snap: Snapshot) Error!void {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.applyLocalSnapshot(allocator, snap);
    }

    fn sync_impl(ctx: *anyopaque) Error!void {
        const self: *MemoryStorage = @ptrCast(@alignCast(ctx));
        return self.sync_();
    }

    pub const writable_vtable: WritableStorage.VTable = .{
        .initial_state = initial_state_impl,
        .entries = entries_impl,
        .term = term_impl,
        .first_index = first_index_impl,
        .last_index = last_index_impl,
        .get_snapshot = get_snapshot_impl,
        .append = append_impl,
        .set_hard_state = set_hard_state_impl,
        .set_conf_state = set_conf_state_impl,
        .apply_snapshot = apply_snapshot_impl,
        .apply_local_snapshot = apply_local_snapshot_impl,
        .sync_ = sync_impl,
    };

    /// Borrow `self` as a writable storage interface. The returned value
    /// borrows `self` and is invalidated when `self` is moved or destroyed.
    pub fn asWritableStorage(self: *MemoryStorage) WritableStorage {
        return .{ .ctx = self, .vtable = &writable_vtable };
    }

    pub const read_vtable: Storage.VTable = .{
        .initial_state = initial_state_impl,
        .entries = entries_impl,
        .term = term_impl,
        .first_index = first_index_impl,
        .last_index = last_index_impl,
        .get_snapshot = get_snapshot_impl,
    };

    pub fn asStorage(self: *MemoryStorage) Storage {
        return .{ .ctx = self, .vtable = &read_vtable };
    }
};

test "memory storage term lookup with compaction boundaries" {
    const allocator = std.testing.allocator;
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);

    var raw = [_]Entry{
        .{ .index = 3, .term = 3 },
        .{ .index = 4, .term = 4 },
        .{ .index = 5, .term = 5 },
    };
    try storage.setEntries(allocator, &raw);

    try std.testing.expectError(error.Compacted, storage.term(2));
    try std.testing.expectEqual(@as(u64, 3), try storage.term(3));
    try std.testing.expectEqual(@as(u64, 4), try storage.term(4));
    try std.testing.expectEqual(@as(u64, 5), try storage.term(5));
    try std.testing.expectError(error.Unavailable, storage.term(6));
}

test "memory storage first and last index reflect append and compact" {
    const allocator = std.testing.allocator;
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);

    var raw = [_]Entry{
        .{ .index = 3, .term = 3 },
        .{ .index = 4, .term = 4 },
        .{ .index = 5, .term = 5 },
    };
    try storage.setEntries(allocator, &raw);
    try std.testing.expectEqual(@as(u64, 3), try storage.firstIndex());
    try std.testing.expectEqual(@as(u64, 5), try storage.lastIndex());

    var more = [_]Entry{.{ .index = 6, .term = 5 }};
    try storage.append(allocator, &more);
    try std.testing.expectEqual(@as(u64, 6), try storage.lastIndex());

    try storage.compact(allocator, 4);
    try std.testing.expectEqual(@as(u64, 4), try storage.firstIndex());
}

test "memory storage entries honor bounds and max size" {
    const allocator = std.testing.allocator;
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);

    var raw = [_]Entry{
        .{ .index = 3, .term = 3 },
        .{ .index = 4, .term = 4 },
        .{ .index = 5, .term = 5 },
        .{ .index = 6, .term = 6 },
    };
    try storage.setEntries(allocator, &raw);

    try std.testing.expectError(error.Compacted, storage.entries(allocator, 2, 6, null, .{ .empty = .{ .can_async = false } }));

    {
        const got = try storage.entries(allocator, 3, 4, null, .{ .empty = .{ .can_async = false } });
        defer {
            for (got) |*e| e.deinit(allocator);
            allocator.free(got);
        }
        try std.testing.expectEqual(@as(usize, 1), got.len);
        try std.testing.expectEqual(@as(u64, 3), got[0].index);
    }
    {
        const got = try storage.entries(allocator, 4, 7, 0, .{ .empty = .{ .can_async = false } });
        defer {
            for (got) |*e| e.deinit(allocator);
            allocator.free(got);
        }
        try std.testing.expectEqual(@as(usize, 1), got.len);
        try std.testing.expectEqual(@as(u64, 4), got[0].index);
    }
}

test "memory storage append truncate and reject gap" {
    const allocator = std.testing.allocator;
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);

    var raw = [_]Entry{
        .{ .index = 3, .term = 3 },
        .{ .index = 4, .term = 4 },
        .{ .index = 5, .term = 5 },
    };
    try storage.setEntries(allocator, &raw);

    // Truncate-and-replace: entry 4 term changes, 5 dropped.
    var replacement = [_]Entry{.{ .index = 4, .term = 5 }};
    try storage.append(allocator, &replacement);

    const got = try storage.allEntries(allocator);
    defer {
        for (got) |*e| e.deinit(allocator);
        allocator.free(got);
    }
    try std.testing.expectEqual(@as(usize, 2), got.len);
    try std.testing.expectEqual(@as(u64, 3), got[0].index);
    try std.testing.expectEqual(@as(u64, 5), got[1].term);

    // Gap should fail.
    var gap = [_]Entry{.{ .index = 2, .term = 3 }};
    try std.testing.expectError(error.Fatal, storage.mayAppend(allocator, &gap));
}

test "memory storage apply snapshot" {
    const allocator = std.testing.allocator;
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);

    var snap = Snapshot{
        .metadata = .{
            .index = 4,
            .term = 4,
            .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }) },
        },
    };
    defer snap.deinit(allocator);
    try storage.applySnapshot(allocator, snap);

    var stale = Snapshot{ .metadata = .{ .index = 3, .term = 3 } };
    defer stale.deinit(allocator);
    try std.testing.expectError(error.SnapshotOutOfDate, storage.applySnapshot(allocator, stale));
}

test "memory storage get snapshot honors trigger and rebuilds index" {
    const allocator = std.testing.allocator;
    var storage = MemoryStorage.init();
    defer storage.deinit(allocator);

    var raw = [_]Entry{
        .{ .index = 3, .term = 3 },
        .{ .index = 4, .term = 4 },
        .{ .index = 5, .term = 5 },
    };
    try storage.setEntries(allocator, &raw);

    const voters = try allocator.dupe(u64, &.{ 1, 2, 3 });
    var raft_state = RaftState{
        .hard_state = .{ .term = 0, .vote = 0, .commit = 5 },
        .conf_state = .{ .voters = voters },
    };
    defer raft_state.deinit(allocator);
    try storage.setRaftState(allocator, raft_state);

    {
        var snap = try storage.getSnapshot(allocator, 0, 0);
        defer snap.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 5), snap.metadata.index);
        try std.testing.expectEqual(@as(u64, 5), snap.metadata.term);
    }

    storage.triggerSnapshotUnavailable();
    try std.testing.expectError(error.SnapshotTemporarilyUnavailable, storage.getSnapshot(allocator, 0, 0));
}
