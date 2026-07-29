//! Storage interface and entry-query context types.
//!
//! Raft core uses `Storage` for read-only access; the orchestration layer
//! persists entries, hardstate, and snapshots via `WritableStorage`. The
//! interface is a
//! vtable struct so custom backends (`MemoryStorage`, `WALStorage`, user
//! implementations) plug in without inheritance.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const ClusterMembership = @import("cluster_membership.zig").ClusterMembership;

pub const Error = error_model.Error;
pub const Entry = types.Entry;
pub const Message = types.Message;
pub const Snapshot = types.Snapshot;
pub const HardState = types.HardState;
pub const ConfState = types.ConfState;

/// Persisted state supplied to a freshly started Raft node.
pub const RaftState = struct {
    hard_state: HardState = .{},
    conf_state: ConfState = .{},
    cluster_membership: ?ClusterMembership = null,
    membership_index: u64 = 0,

    pub fn deinit(self: *RaftState, allocator: std.mem.Allocator) void {
        self.hard_state = .{};
        self.conf_state.deinit(allocator);
        if (self.cluster_membership) |*membership| membership.deinit(allocator);
        self.cluster_membership = null;
        self.membership_index = 0;
    }

    /// Deep clone using `allocator`. The caller owns the result.
    pub fn clone(self: RaftState, allocator: std.mem.Allocator) !RaftState {
        var conf_state = try cloneConfState(allocator, self.conf_state);
        errdefer conf_state.deinit(allocator);
        var cluster_membership: ?ClusterMembership = null;
        if (self.cluster_membership) |membership| cluster_membership = try membership.clone(allocator);
        errdefer if (cluster_membership) |*membership| membership.deinit(allocator);
        return .{
            .hard_state = self.hard_state,
            .conf_state = conf_state,
            .cluster_membership = cluster_membership,
            .membership_index = self.membership_index,
        };
    }
};

/// Copy a ConfState's slices into freshly owned allocations.
pub fn cloneConfState(allocator: std.mem.Allocator, src: ConfState) !ConfState {
    const voters: []u64 = if (src.voters.len == 0) &.{} else try allocator.dupe(u64, src.voters);
    errdefer if (voters.len != 0) allocator.free(voters);
    const learners: []u64 = if (src.learners.len == 0) &.{} else try allocator.dupe(u64, src.learners);
    errdefer if (learners.len != 0) allocator.free(learners);
    const voters_outgoing: []u64 = if (src.voters_outgoing.len == 0) &.{} else try allocator.dupe(u64, src.voters_outgoing);
    errdefer if (voters_outgoing.len != 0) allocator.free(voters_outgoing);
    const learners_next: []u64 = if (src.learners_next.len == 0) &.{} else try allocator.dupe(u64, src.learners_next);
    return .{
        .voters = voters,
        .learners = learners,
        .voters_outgoing = voters_outgoing,
        .learners_next = learners_next,
        .auto_leave = src.auto_leave,
    };
}

/// Copy a Snapshot and all nested buffers into fresh allocations.
pub fn cloneSnapshot(allocator: std.mem.Allocator, src: Snapshot) !Snapshot {
    const membership: []u8 = if (src.membership.len == 0) &.{} else try allocator.dupe(u8, src.membership);
    errdefer if (membership.len != 0) allocator.free(membership);
    const data: []u8 = if (src.data.len == 0) &.{} else try allocator.dupe(u8, src.data);
    errdefer if (data.len != 0) allocator.free(data);
    return .{
        .membership = membership,
        .data = data,
        .metadata = .{
            .index = src.metadata.index,
            .term = src.metadata.term,
            .conf_state = try cloneConfState(allocator, src.metadata.conf_state),
        },
    };
}

/// Copy an Entry (data + context buffers) into fresh allocations.
pub fn cloneEntry(allocator: std.mem.Allocator, src: Entry) !Entry {
    var result = Entry{
        .entry_type = src.entry_type,
        .term = src.term,
        .index = src.index,
        .checksum = src.checksum,
    };
    errdefer result.deinit(allocator);
    try result.setDataCopy(allocator, src.data);
    result.context = if (src.context.len == 0) &.{} else try allocator.dupe(u8, src.context);
    return result;
}

/// Share immutable Entry data while copying the context buffer.
pub fn shareEntry(allocator: std.mem.Allocator, src: Entry) !Entry {
    return src.share(allocator);
}

pub fn shareEntries(allocator: std.mem.Allocator, src: []const Entry) ![]Entry {
    var entries = try allocator.alloc(Entry, src.len);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    for (src) |entry| {
        entries[initialized] = try shareEntry(allocator, entry);
        initialized += 1;
    }
    return entries;
}

/// Copy a Message and all nested owned buffers into fresh allocations.
pub fn cloneMessage(allocator: std.mem.Allocator, src: Message) !Message {
    var entries = try allocator.alloc(Entry, src.entries.len);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    for (src.entries) |entry| {
        entries[initialized] = try cloneEntry(allocator, entry);
        initialized += 1;
    }

    var snapshot: ?Snapshot = null;
    if (src.snapshot) |value| snapshot = try cloneSnapshot(allocator, value);
    errdefer if (snapshot) |*value| value.deinit(allocator);

    const context: []u8 = if (src.context.len == 0) &.{} else try allocator.dupe(u8, src.context);
    return .{
        .msg_type = src.msg_type,
        .to = src.to,
        .from = src.from,
        .term = src.term,
        .log_term = src.log_term,
        .index = src.index,
        .entries = entries,
        .commit = src.commit,
        .commit_term = src.commit_term,
        .snapshot = snapshot,
        .request_snapshot = src.request_snapshot,
        .reject = src.reject,
        .reject_hint = src.reject_hint,
        .context = context,
        .priority = src.priority,
    };
}

/// Share immutable Entry data while copying all other Message-owned buffers.
pub fn shareMessage(allocator: std.mem.Allocator, src: Message) !Message {
    const entries = try shareEntries(allocator, src.entries);
    errdefer {
        for (entries) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }

    var snapshot: ?Snapshot = null;
    if (src.snapshot) |value| snapshot = try cloneSnapshot(allocator, value);
    errdefer if (snapshot) |*value| value.deinit(allocator);

    const context: []u8 = if (src.context.len == 0) &.{} else try allocator.dupe(u8, src.context);
    return .{
        .msg_type = src.msg_type,
        .to = src.to,
        .from = src.from,
        .term = src.term,
        .log_term = src.log_term,
        .index = src.index,
        .entries = entries,
        .commit = src.commit,
        .commit_term = src.commit_term,
        .snapshot = snapshot,
        .request_snapshot = src.request_snapshot,
        .reject = src.reject,
        .reject_hint = src.reject_hint,
        .context = context,
        .priority = src.priority,
    };
}

/// Caller reason for fetching entries. Drives async-fetch behavior in
/// `MemoryStorage` and `WALStorage` backends.
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

    /// Only `send_append` and `empty.can_async = true` may trigger async
    /// LogTemporarilyUnavailable.
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
        set_membership_state: *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            conf_state: ConfState,
            cluster_membership: ClusterMembership,
            membership_index: u64,
        ) Error!void,
        migrate_legacy_membership: *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            current_membership: ClusterMembership,
            membership_index: u64,
            snapshot_membership: ?ClusterMembership,
        ) Error!void,
        apply_snapshot: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, snap: Snapshot) Error!void,
        apply_local_snapshot: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, snap: Snapshot) Error!void,
        local_snapshot: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) Error!?Snapshot,
        reserve_incarnation: *const fn (ctx: *anyopaque) Error!u64,
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

    pub fn setMembershipState(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        conf_state: ConfState,
        cluster_membership: ClusterMembership,
        membership_index: u64,
    ) Error!void {
        return self.vtable.set_membership_state(
            self.ctx,
            allocator,
            conf_state,
            cluster_membership,
            membership_index,
        );
    }

    pub fn applySnapshot(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        snapshot: Snapshot,
    ) Error!void {
        return self.vtable.apply_snapshot(self.ctx, allocator, snapshot);
    }

    pub fn migrateLegacyMembership(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        current_membership: ClusterMembership,
        membership_index: u64,
        snapshot_membership: ?ClusterMembership,
    ) Error!void {
        return self.vtable.migrate_legacy_membership(
            self.ctx,
            allocator,
            current_membership,
            membership_index,
            snapshot_membership,
        );
    }

    pub fn applyLocalSnapshot(
        self: WritableStorage,
        allocator: std.mem.Allocator,
        snapshot: Snapshot,
    ) Error!void {
        return self.vtable.apply_local_snapshot(self.ctx, allocator, snapshot);
    }

    pub fn localSnapshot(self: WritableStorage, allocator: std.mem.Allocator) Error!?Snapshot {
        return self.vtable.local_snapshot(self.ctx, allocator);
    }

    pub fn reserveIncarnation(self: WritableStorage) Error!u64 {
        return self.vtable.reserve_incarnation(self.ctx);
    }

    pub fn sync(self: WritableStorage) Error!void {
        return self.vtable.sync_(self.ctx);
    }

    /// The returned read-only view borrows this interface value.
    pub fn asStorage(self: *WritableStorage) Storage {
        return .{ .ctx = self, .vtable = &storage_adapter_vtable };
    }
};

pub fn validateLegacyMembershipMigration(
    current_membership: ClusterMembership,
    membership_index: u64,
    hard_state: HardState,
    conf_state: ConfState,
    snapshot: ?Snapshot,
    snapshot_membership: ?ClusterMembership,
) Error!void {
    current_membership.validate(conf_state) catch return error.InvalidClusterMembership;
    if (membership_index != 0 and hard_state.commit == 0) return error.InvalidMembershipIndex;
    if (hard_state.commit != 0 and membership_index > hard_state.commit) return error.InvalidMembershipIndex;

    if (snapshot) |local_snapshot| {
        if (membership_index < local_snapshot.metadata.index) return error.InvalidMembershipIndex;
        const historical = snapshot_membership orelse return error.LegacySnapshotMigrationRequired;
        historical.validate(local_snapshot.metadata.conf_state) catch return error.InvalidClusterMembership;
        if (!std.mem.eql(u8, &current_membership.cluster_id, &historical.cluster_id)) {
            return error.InvalidClusterMembership;
        }
        for (historical.retired_node_ids) |retired_id| {
            if (!containsSorted(current_membership.retired_node_ids, retired_id)) {
                return error.InvalidClusterMembership;
            }
        }
    } else if (snapshot_membership != null) {
        return error.InvalidClusterMembership;
    }
}

fn containsSorted(ids: []const u64, id: u64) bool {
    var low: usize = 0;
    var high = ids.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (ids[mid] < id) {
            low = mid + 1;
        } else if (ids[mid] > id) {
            high = mid;
        } else {
            return true;
        }
    }
    return false;
}

fn writableStorage(ctx: *anyopaque) *WritableStorage {
    return @ptrCast(@alignCast(ctx));
}

fn writableInitialState(ctx: *anyopaque, allocator: std.mem.Allocator) Error!RaftState {
    return writableStorage(ctx).initialState(allocator);
}

fn writableEntries(ctx: *anyopaque, allocator: std.mem.Allocator, low: u64, high: u64, max_size: ?u64, context: GetEntriesContext) Error![]Entry {
    return writableStorage(ctx).entries(allocator, low, high, max_size, context);
}

fn writableTerm(ctx: *anyopaque, idx: u64) Error!u64 {
    return writableStorage(ctx).term(idx);
}

fn writableFirstIndex(ctx: *anyopaque) Error!u64 {
    return writableStorage(ctx).firstIndex();
}

fn writableLastIndex(ctx: *anyopaque) Error!u64 {
    return writableStorage(ctx).lastIndex();
}

fn writableSnapshot(ctx: *anyopaque, allocator: std.mem.Allocator, request_index: u64, to: u64) Error!Snapshot {
    return writableStorage(ctx).getSnapshot(allocator, request_index, to);
}

const storage_adapter_vtable: Storage.VTable = .{
    .initial_state = writableInitialState,
    .entries = writableEntries,
    .term = writableTerm,
    .first_index = writableFirstIndex,
    .last_index = writableLastIndex,
    .get_snapshot = writableSnapshot,
};

test "raft state clone is deep" {
    const allocator = std.testing.allocator;
    var original = RaftState{
        .hard_state = .{ .term = 3, .vote = 5, .commit = 4 },
        .conf_state = .{
            .voters = try allocator.dupe(u64, &.{ 1, 2, 3 }),
            .learners = try allocator.dupe(u64, &.{4}),
        },
        .cluster_membership = .{
            .cluster_id = .{1} ++ .{0} ** 15,
            .peers = try allocator.dupe(@import("cluster_membership.zig").PeerEndpoint, &.{
                .{ .node_id = 1, .address = try allocator.dupe(u8, "node-1") },
            }),
        },
        .membership_index = 4,
    };
    defer original.deinit(allocator);

    var copy = try original.clone(allocator);
    defer copy.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, copy.conf_state.voters);
    try std.testing.expect(copy.conf_state.voters.ptr != original.conf_state.voters.ptr);
    try std.testing.expectEqual(@as(u64, 4), copy.membership_index);
    try std.testing.expect(copy.cluster_membership.?.peers.ptr != original.cluster_membership.?.peers.ptr);
    try std.testing.expect(copy.cluster_membership.?.peers[0].address.ptr != original.cluster_membership.?.peers[0].address.ptr);
}

test "clone helpers clean up allocation failures" {
    const Helpers = struct {
        fn cloneEntryAll(allocator: std.mem.Allocator) !void {
            var cloned = try cloneEntry(allocator, .{
                .data = @constCast("data"),
                .context = @constCast("context"),
            });
            defer cloned.deinit(allocator);
        }

        fn cloneSnapshotAll(allocator: std.mem.Allocator) !void {
            var cloned = try cloneSnapshot(allocator, .{
                .membership = @constCast("membership"),
                .data = @constCast("snapshot"),
                .metadata = .{
                    .conf_state = .{
                        .voters = @constCast(&[_]u64{ 1, 2 }),
                        .learners = @constCast(&[_]u64{3}),
                        .voters_outgoing = @constCast(&[_]u64{4}),
                        .learners_next = @constCast(&[_]u64{5}),
                    },
                },
            });
            defer cloned.deinit(allocator);
        }

        fn shareEntryAll(allocator: std.mem.Allocator) !void {
            var source = Entry{};
            try source.setDataCopy(allocator, "data");
            defer source.deinit(allocator);
            source.context = try allocator.dupe(u8, "context");

            var shared = try shareEntry(allocator, source);
            defer shared.deinit(allocator);
        }

        fn shareEntriesAll(allocator: std.mem.Allocator) !void {
            const source = [_]Entry{
                .{ .data = "first" },
                .{ .data = "second", .context = @constCast("context") },
            };
            const shared = try shareEntries(allocator, &source);
            defer {
                for (shared) |*entry| entry.deinit(allocator);
                allocator.free(shared);
            }
        }

        fn shareMessageAll(allocator: std.mem.Allocator) !void {
            const entries = [_]Entry{.{ .data = "entry" }};
            var shared = try shareMessage(allocator, .{
                .entries = @constCast(&entries),
                .context = @constCast("message-context"),
            });
            defer shared.deinit(allocator);
        }

        fn cloneMessageAll(allocator: std.mem.Allocator) !void {
            var entries = [_]Entry{.{
                .data = @constCast("entry"),
                .context = @constCast("entry-context"),
            }};
            var cloned = try cloneMessage(allocator, .{
                .entries = entries[0..],
                .context = @constCast("message-context"),
                .snapshot = .{
                    .membership = @constCast("membership"),
                    .data = @constCast("snapshot"),
                    .metadata = .{
                        .conf_state = .{ .voters = @constCast(&[_]u64{ 1, 2 }) },
                    },
                },
            });
            defer cloned.deinit(allocator);
        }

        fn cloneRaftStateAll(allocator: std.mem.Allocator) !void {
            var state = RaftState{
                .conf_state = .{ .voters = @constCast(&[_]u64{ 1, 2 }) },
                .cluster_membership = .{
                    .cluster_id = .{1} ++ .{0} ** 15,
                    .peers = @constCast(&[_]@import("cluster_membership.zig").PeerEndpoint{
                        .{ .node_id = 1, .address = @constCast("node-1") },
                        .{ .node_id = 2, .address = @constCast("node-2") },
                    }),
                },
            };
            var cloned = try state.clone(allocator);
            defer cloned.deinit(allocator);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helpers.cloneEntryAll, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helpers.shareEntryAll, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helpers.shareEntriesAll, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helpers.shareMessageAll, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helpers.cloneSnapshotAll, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helpers.cloneMessageAll, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Helpers.cloneRaftStateAll, .{});
}

test "shareEntry retains managed data and copies context" {
    const allocator = std.testing.allocator;
    var source = Entry{};
    try source.setDataCopy(allocator, "payload");
    source.context = try allocator.dupe(u8, "context");

    var first = try shareEntry(allocator, source);
    var second = try shareEntry(allocator, first);
    try std.testing.expectEqual(@intFromPtr(source.data.ptr), @intFromPtr(first.data.ptr));
    try std.testing.expectEqual(@intFromPtr(first.data.ptr), @intFromPtr(second.data.ptr));
    try std.testing.expect(first.context.ptr != source.context.ptr);

    source.deinit(allocator);
    try std.testing.expectEqualStrings("payload", first.data);
    first.deinit(allocator);
    try std.testing.expectEqualStrings("payload", second.data);
    second.deinit(allocator);
}

test "shareEntry copies unmanaged data once" {
    const source = Entry{ .data = "payload" };
    var shared = try shareEntry(std.testing.allocator, source);
    defer shared.deinit(std.testing.allocator);

    try std.testing.expect(shared.data.ptr != source.data.ptr);
    var retained = try shareEntry(std.testing.allocator, shared);
    defer retained.deinit(std.testing.allocator);
    try std.testing.expectEqual(@intFromPtr(shared.data.ptr), @intFromPtr(retained.data.ptr));
}

test "shared Entry data releases across threads" {
    const allocator = std.heap.smp_allocator;
    var source = Entry{};
    try source.setDataCopy(allocator, "payload");
    var first = try shareEntry(allocator, source);
    var second = try shareEntry(allocator, source);
    source.deinit(allocator);

    const Release = struct {
        fn run(entry: Entry) void {
            var owned = entry;
            owned.deinit(std.heap.smp_allocator);
        }
    };
    const first_thread = try std.Thread.spawn(.{}, Release.run, .{first});
    first.reset();
    const second_thread = try std.Thread.spawn(.{}, Release.run, .{second});
    second.reset();
    first_thread.join();
    second_thread.join();
}

test "cloneMessage deeply copies nested buffers" {
    var entries = [_]Entry{.{
        .data = @constCast("entry"),
        .context = @constCast("entry-context"),
    }};
    const source = Message{
        .entries = entries[0..],
        .context = @constCast("message-context"),
        .snapshot = .{
            .membership = @constCast("membership"),
            .data = @constCast("snapshot"),
            .metadata = .{ .conf_state = .{ .voters = @constCast(&[_]u64{ 1, 2 }) } },
        },
    };
    var cloned = try cloneMessage(std.testing.allocator, source);
    defer cloned.deinit(std.testing.allocator);

    try std.testing.expect(cloned.entries.ptr != source.entries.ptr);
    try std.testing.expect(cloned.entries[0].data.ptr != source.entries[0].data.ptr);
    try std.testing.expect(cloned.entries[0].context.ptr != source.entries[0].context.ptr);
    try std.testing.expect(cloned.context.ptr != source.context.ptr);
    try std.testing.expect(cloned.snapshot.?.data.ptr != source.snapshot.?.data.ptr);
    try std.testing.expect(cloned.snapshot.?.membership.ptr != source.snapshot.?.membership.ptr);
    try std.testing.expect(cloned.snapshot.?.metadata.conf_state.voters.ptr != source.snapshot.?.metadata.conf_state.voters.ptr);
    try std.testing.expectEqualSlices(u8, source.snapshot.?.data, cloned.snapshot.?.data);
    try std.testing.expectEqualSlices(u8, source.snapshot.?.membership, cloned.snapshot.?.membership);
}

test "cloneSnapshot deeply copies membership" {
    const source = Snapshot{
        .membership = @constCast("membership"),
        .data = @constCast("state"),
        .metadata = .{ .conf_state = .{ .voters = @constCast(&[_]u64{1}) } },
    };
    var cloned = try cloneSnapshot(std.testing.allocator, source);
    defer cloned.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, source.membership, cloned.membership);
    try std.testing.expect(cloned.membership.ptr != source.membership.ptr);
    cloned.membership[0] = 'M';
    try std.testing.expectEqual(@as(u8, 'm'), source.membership[0]);
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
