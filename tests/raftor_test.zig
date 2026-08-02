//! Raftor integration tests.
//!
//! End-to-end tests that exercise the full Raftor pipeline: create →
//! campaign → propose → apply. Single-node only (multi-node requires RPC).

const std = @import("std");
const raft = @import("raft_zig");
const fault = @import("harness/fault_fs.zig");

const allocator = std.testing.allocator;
const Raftor = raft.Raftor;
const RaftorConfig = raft.RaftorConfig;
const MockStateMachine = raft.MockStateMachine;
const StateRole = raft.StateRole;
const durable_cluster_id: raft.ClusterId = .{1} ++ .{0} ** 15;

const SyncFailingStorage = struct {
    inner: raft.WritableStorage,
    fail_sync: bool = false,
    fail_conf_state: bool = false,
    fail_incarnation: bool = false,
    fail_entries: bool = false,
    successful_syncs: usize = 0,

    fn cast(ctx: *anyopaque) *SyncFailingStorage {
        return @ptrCast(@alignCast(ctx));
    }

    fn initialState(ctx: *anyopaque, alloc: std.mem.Allocator) raft.Error!raft.RaftState {
        return cast(ctx).inner.initialState(alloc);
    }

    fn entries(ctx: *anyopaque, alloc: std.mem.Allocator, low: u64, high: u64, max_size: ?u64, request_ctx: raft.GetEntriesContext) raft.Error![]raft.Entry {
        const self = cast(ctx);
        if (self.fail_entries) return error.OutOfMemory;
        return self.inner.entries(alloc, low, high, max_size, request_ctx);
    }

    fn term(ctx: *anyopaque, index: u64) raft.Error!u64 {
        return cast(ctx).inner.term(index);
    }

    fn firstIndex(ctx: *anyopaque) raft.Error!u64 {
        return cast(ctx).inner.firstIndex();
    }

    fn lastIndex(ctx: *anyopaque) raft.Error!u64 {
        return cast(ctx).inner.lastIndex();
    }

    fn getSnapshot(ctx: *anyopaque, alloc: std.mem.Allocator, request_index: u64, to: u64) raft.Error!raft.Snapshot {
        return cast(ctx).inner.getSnapshot(alloc, request_index, to);
    }

    fn append(ctx: *anyopaque, alloc: std.mem.Allocator, values: []const raft.Entry) raft.Error!void {
        return cast(ctx).inner.append(alloc, values);
    }

    fn setHardState(ctx: *anyopaque, hard_state: raft.HardState) raft.Error!void {
        return cast(ctx).inner.setHardState(hard_state);
    }

    fn setConfState(ctx: *anyopaque, alloc: std.mem.Allocator, conf_state: raft.ConfState) raft.Error!void {
        const self = cast(ctx);
        if (self.fail_conf_state) return error.WalWriteFailed;
        return self.inner.setConfState(alloc, conf_state);
    }

    fn setMembershipState(
        ctx: *anyopaque,
        alloc: std.mem.Allocator,
        conf_state: raft.ConfState,
        cluster_membership: raft.ClusterMembership,
        membership_index: u64,
    ) raft.Error!void {
        const self = cast(ctx);
        if (self.fail_conf_state) return error.WalWriteFailed;
        return self.inner.setMembershipState(alloc, conf_state, cluster_membership, membership_index);
    }

    fn applySnapshot(ctx: *anyopaque, alloc: std.mem.Allocator, snapshot: raft.Snapshot) raft.Error!void {
        return cast(ctx).inner.applySnapshot(alloc, snapshot);
    }

    fn migrateLegacyMembership(
        ctx: *anyopaque,
        alloc: std.mem.Allocator,
        current_membership: raft.ClusterMembership,
        membership_index: u64,
        snapshot_membership: ?raft.ClusterMembership,
    ) raft.Error!void {
        return cast(ctx).inner.migrateLegacyMembership(alloc, current_membership, membership_index, snapshot_membership);
    }

    fn applyLocalSnapshot(ctx: *anyopaque, alloc: std.mem.Allocator, snapshot: raft.Snapshot) raft.Error!void {
        return cast(ctx).inner.applyLocalSnapshot(alloc, snapshot);
    }

    fn localSnapshot(ctx: *anyopaque, alloc: std.mem.Allocator) raft.Error!?raft.Snapshot {
        return cast(ctx).inner.localSnapshot(alloc);
    }

    fn reserveIncarnation(ctx: *anyopaque) raft.Error!u64 {
        const self = cast(ctx);
        if (self.fail_incarnation) return error.WalSyncFailed;
        return self.inner.reserveIncarnation();
    }

    fn sync(ctx: *anyopaque) raft.Error!void {
        const self = cast(ctx);
        if (self.fail_sync) return error.WalSyncFailed;
        try self.inner.sync();
        self.successful_syncs += 1;
    }

    fn writableStorage(self: *SyncFailingStorage) raft.WritableStorage {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable: raft.WritableStorage.VTable = .{
        .initial_state = initialState,
        .entries = entries,
        .term = term,
        .first_index = firstIndex,
        .last_index = lastIndex,
        .get_snapshot = getSnapshot,
        .append = append,
        .set_hard_state = setHardState,
        .set_conf_state = setConfState,
        .set_membership_state = setMembershipState,
        .migrate_legacy_membership = migrateLegacyMembership,
        .apply_snapshot = applySnapshot,
        .apply_local_snapshot = applyLocalSnapshot,
        .local_snapshot = localSnapshot,
        .reserve_incarnation = reserveIncarnation,
        .sync_ = sync,
    };
};

const RecordingTransport = struct {
    const EventKind = enum { add, remove };
    const LifecycleEvent = enum {
        add_peer,
        set_message_callback,
        set_peer_event_callback,
        start,
        stop,
        clear_message_callback,
        clear_peer_event_callback,
    };
    const Event = struct {
        kind: EventKind,
        node_id: u64,
        address: [64]u8 = undefined,
        address_len: usize = 0,
        successful_syncs: usize = 0,

        fn addressSlice(self: *const Event) []const u8 {
            return self.address[0..self.address_len];
        }
    };

    events: std.ArrayList(Event) = .empty,
    callback: ?raft.MessageCallback = null,
    peer_event_callback: ?raft.PeerEventCallback = null,
    inbound_messages: std.ArrayList(raft.Message) = .empty,
    peer_events: std.ArrayList(raft.PeerEvent) = .empty,
    lifecycle_events: [64]LifecycleEvent = undefined,
    lifecycle_events_len: usize = 0,
    allocator: std.mem.Allocator,
    sync_counter: ?*const usize = null,
    fail_add: bool = false,
    fail_remove: bool = false,
    fail_start: bool = false,
    start_count: usize = 0,
    stop_count: usize = 0,
    stop_call_count: usize = 0,
    delivered_message_count: usize = 0,
    delivered_peer_event_count: usize = 0,
    stopped: bool = false,
    before_message_ctx: ?*anyopaque = null,
    before_message: ?*const fn (*anyopaque) void = null,
    identity_value: raft.TransportIdentity = .{ .cluster_id = .{0} ** 16, .node_id = 0 },

    fn init(alloc: std.mem.Allocator) RecordingTransport {
        return .{ .allocator = alloc };
    }

    fn deinit(self: *RecordingTransport) void {
        for (self.inbound_messages.items) |*message| message.deinit(self.allocator);
        self.inbound_messages.deinit(self.allocator);
        self.peer_events.deinit(self.allocator);
        self.events.deinit(self.allocator);
        self.* = undefined;
    }

    fn clear(self: *RecordingTransport) void {
        self.events.clearRetainingCapacity();
    }

    fn cast(ctx: *anyopaque) *RecordingTransport {
        return @ptrCast(@alignCast(ctx));
    }

    fn recordLifecycle(self: *RecordingTransport, event: LifecycleEvent) void {
        std.debug.assert(self.lifecycle_events_len < self.lifecycle_events.len);
        self.lifecycle_events[self.lifecycle_events_len] = event;
        self.lifecycle_events_len += 1;
    }

    fn queueMessage(self: *RecordingTransport, message: raft.Message) !void {
        const cloned = try raft.cloneMessage(self.allocator, message);
        errdefer {
            var owned = cloned;
            owned.deinit(self.allocator);
        }
        try self.inbound_messages.append(self.allocator, cloned);
    }

    fn queuePeerEvent(self: *RecordingTransport, event: raft.PeerEvent) !void {
        try self.peer_events.append(self.allocator, event);
    }

    fn appendEvent(self: *RecordingTransport, kind: EventKind, node_id: u64, address: []const u8) raft.Error!void {
        if (address.len > 64) return error.MessageTooLarge;
        var event = Event{
            .kind = kind,
            .node_id = node_id,
            .successful_syncs = if (self.sync_counter) |counter| counter.* else 0,
        };
        @memcpy(event.address[0..address.len], address);
        event.address_len = address.len;
        try self.events.append(self.allocator, event);
    }

    fn start(ctx: *anyopaque) raft.Error!void {
        const self = cast(ctx);
        self.recordLifecycle(.start);
        self.start_count += 1;
        if (self.fail_start) return error.ConnectionClosed;
        self.stopped = false;
    }

    fn stop(ctx: *anyopaque) void {
        const self = cast(ctx);
        self.stop_call_count += 1;
        if (self.stopped) return;
        self.stopped = true;
        self.stop_count += 1;
        self.recordLifecycle(.stop);
    }

    fn addPeer(ctx: *anyopaque, node_id: u64, address: []const u8) raft.Error!bool {
        const self = cast(ctx);
        if (self.fail_add) return error.ConnectionClosed;
        self.recordLifecycle(.add_peer);
        try self.appendEvent(.add, node_id, address);
        return true;
    }

    fn removePeer(ctx: *anyopaque, node_id: u64) raft.Error!void {
        const self = cast(ctx);
        if (self.fail_remove) return error.ConnectionClosed;
        try self.appendEvent(.remove, node_id, "");
    }

    fn send(_: *anyopaque, _: []const raft.Message) raft.Error!void {}

    fn setMessageCallback(ctx: *anyopaque, callback: ?raft.MessageCallback) void {
        const self = cast(ctx);
        self.recordLifecycle(if (callback == null) .clear_message_callback else .set_message_callback);
        self.callback = callback;
    }

    fn setPeerEventCallback(ctx: *anyopaque, callback: ?raft.PeerEventCallback) void {
        const self = cast(ctx);
        self.recordLifecycle(if (callback == null) .clear_peer_event_callback else .set_peer_event_callback);
        self.peer_event_callback = callback;
    }

    fn pollOne(ctx: *anyopaque) raft.Error!bool {
        const self = cast(ctx);
        if (self.stopped) return false;
        if (self.inbound_messages.items.len > 0) {
            const callback = self.callback orelse return false;
            if (self.before_message) |before_message| before_message(self.before_message_ctx.?);
            try callback.invoke(self.inbound_messages.orderedRemove(0));
            self.delivered_message_count += 1;
            return true;
        }
        if (self.peer_events.items.len > 0) {
            const callback = self.peer_event_callback orelse return false;
            try callback.invoke(self.peer_events.orderedRemove(0));
            self.delivered_peer_event_count += 1;
            return true;
        }
        return false;
    }

    fn transport(self: *RecordingTransport) raft.Transport {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn transportWithIdentity(self: *RecordingTransport, value: raft.TransportIdentity) raft.Transport {
        self.identity_value = value;
        return .{ .ctx = self, .vtable = &identity_vtable };
    }

    fn identity(ctx: *anyopaque) raft.TransportIdentity {
        return cast(ctx).identity_value;
    }

    const vtable: raft.Transport.VTable = .{
        .start = start,
        .stop = stop,
        .add_peer = addPeer,
        .remove_peer = removePeer,
        .send = send,
        .set_message_callback = setMessageCallback,
        .set_peer_event_callback = setPeerEventCallback,
        .poll_one = pollOne,
    };

    const identity_vtable: raft.Transport.VTable = .{
        .start = start,
        .stop = stop,
        .add_peer = addPeer,
        .remove_peer = removePeer,
        .send = send,
        .set_message_callback = setMessageCallback,
        .set_peer_event_callback = setPeerEventCallback,
        .poll_one = pollOne,
        .identity = identity,
    };
};

const FailingStateMachine = struct {
    inner: *MockStateMachine,
    fail_data: []const u8,
    fail_snapshot: bool = false,
    snapshot_attempts: usize = 0,

    fn cast(ctx: *anyopaque) *FailingStateMachine {
        return @ptrCast(@alignCast(ctx));
    }

    fn apply(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self = cast(ctx);
        if (std.mem.eql(u8, entry.data, self.fail_data)) return error.OutOfMemory;
        return MockStateMachine.applyImpl(self.inner, entry);
    }

    fn takeSnapshot(ctx: *anyopaque, alloc: std.mem.Allocator, applied_index: u64, applied_term: u64, conf_state: raft.ConfState) raft.Error!raft.Snapshot {
        const self = cast(ctx);
        self.snapshot_attempts += 1;
        if (self.fail_snapshot) return error.OutOfMemory;
        return MockStateMachine.takeSnapshotImpl(self.inner, alloc, applied_index, applied_term, conf_state);
    }

    fn restoreSnapshot(ctx: *anyopaque, metadata: raft.SnapshotMetadata, reader: raft.SnapshotReader) raft.Error!void {
        return MockStateMachine.restoreSnapshotImpl(cast(ctx).inner, metadata, reader);
    }

    fn stateMachine(self: *FailingStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable: raft.StateMachine.VTable = .{
        .apply = apply,
        .take_snapshot = takeSnapshot,
        .restore_snapshot = restoreSnapshot,
    };
};

const DurableStateMachine = struct {
    allocator: std.mem.Allocator,
    state: std.ArrayList(u8) = .empty,
    last_applied_index: u64 = 0,
    durable_applied: raft.DurableApplied = .{},
    restore_count: usize = 0,
    fail_restore: bool = false,

    fn init(alloc: std.mem.Allocator) DurableStateMachine {
        return .{ .allocator = alloc };
    }

    fn deinit(self: *DurableStateMachine) void {
        self.state.deinit(self.allocator);
        self.* = undefined;
    }

    fn cast(ctx: *anyopaque) *DurableStateMachine {
        return @ptrCast(@alignCast(ctx));
    }

    fn apply(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self = cast(ctx);
        try self.state.ensureUnusedCapacity(self.allocator, entry.data.len);
        self.state.appendSliceAssumeCapacity(entry.data);
        self.last_applied_index = entry.index;
        self.durable_applied = .{ .index = entry.index, .term = entry.term };
        return .{};
    }

    fn takeSnapshot(ctx: *anyopaque, alloc: std.mem.Allocator, applied_index: u64, applied_term: u64, conf_state: raft.ConfState) raft.Error!raft.Snapshot {
        const self = cast(ctx);
        const data = try alloc.dupe(u8, self.state.items);
        errdefer alloc.free(data);
        return .{
            .data = data,
            .metadata = .{
                .index = applied_index,
                .term = applied_term,
                .conf_state = try raft.cloneConfState(alloc, conf_state),
            },
        };
    }

    fn restoreSnapshot(ctx: *anyopaque, metadata: raft.SnapshotMetadata, reader: raft.SnapshotReader) raft.Error!void {
        const self = cast(ctx);
        if (self.fail_restore) return error.OutOfMemory;
        var restored: std.ArrayList(u8) = .empty;
        errdefer restored.deinit(self.allocator);
        var buffer: [64]u8 = undefined;
        while (true) {
            const count = try reader.read(&buffer);
            if (count == 0) break;
            try restored.appendSlice(self.allocator, buffer[0..count]);
        }
        self.state.deinit(self.allocator);
        self.state = restored;
        self.last_applied_index = metadata.index;
        self.durable_applied = .{ .index = metadata.index, .term = metadata.term };
        self.restore_count += 1;
    }

    fn durableApplied(ctx: *anyopaque) raft.Error!raft.DurableApplied {
        return cast(ctx).durable_applied;
    }

    fn stateMachine(self: *DurableStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn durableStateMachine(self: *DurableStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &durable_vtable };
    }

    const vtable: raft.StateMachine.VTable = .{
        .apply = apply,
        .take_snapshot = takeSnapshot,
        .restore_snapshot = restoreSnapshot,
    };

    const durable_vtable: raft.StateMachine.VTable = .{
        .apply = apply,
        .take_snapshot = takeSnapshot,
        .restore_snapshot = restoreSnapshot,
        .durable_applied = durableApplied,
    };
};

const ErrorTester = struct {
    completed: bool = false,
    err: ?raft.Error = null,

    fn proposalCb(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *ErrorTester = @ptrCast(@alignCast(ctx));
        self.completed = true;
        if (result == .err) self.err = result.err;
    }

    fn proposalCallback(self: *ErrorTester) raft.ProposalCallback {
        return .{ .ctx = self, .function = proposalCb };
    }

    fn readCb(ctx: *anyopaque, result: raft.ReadIndexResult) void {
        const self: *ErrorTester = @ptrCast(@alignCast(ctx));
        self.completed = true;
        if (result == .err) self.err = result.err;
    }

    fn readCallback(self: *ErrorTester) raft.ReadIndexCallback {
        return .{ .ctx = self, .function = readCb };
    }
};

fn makeConfig(id: u64) RaftorConfig {
    var rc = RaftorConfig{};
    rc.raft.id = id;
    rc.raft.election_tick = 10;
    rc.raft.heartbeat_tick = 1;
    rc.raft.election_timeout_seed = id * 999;
    return rc;
}

fn makeDurableConfig(id: u64, address: []const u8) RaftorConfig {
    var config = makeConfig(id);
    config.cluster_id = durable_cluster_id;
    config.advertise_addr = address;
    return config;
}

fn seedMembership(
    storage: *raft.MemoryStorage,
    conf_state: raft.ConfState,
    peers: []raft.PeerEndpoint,
    retired_node_ids: []u64,
    membership_index: u64,
    hard_state: raft.HardState,
) !void {
    try storage.setMembershipState(allocator, conf_state, .{
        .cluster_id = .{1} ++ .{0} ** 15,
        .peers = peers,
        .retired_node_ids = retired_node_ids,
    }, membership_index);
    try storage.setHardState(hard_state);
}

fn stageCommittedConfChange(r: *Raftor, term: u64, index: u64, cc: raft.ConfChangeV2) !void {
    const data = try raft.core.util.encodeConfChangeV2(allocator, cc);
    const entries = try allocator.alloc(raft.Entry, 1);
    entries[0] = .{
        .entry_type = .conf_change_v2,
        .term = term,
        .index = index,
        .data = data,
    };
    try r.getRawNode().step(.{
        .msg_type = .append,
        .from = 2,
        .to = 1,
        .term = term,
        .index = index - 1,
        .log_term = if (index == 1) 0 else term,
        .commit = index,
        .entries = entries,
    });
}

fn makeReadyEntries(term: u64, first: u64, count: usize) ![]raft.Entry {
    const entries = try allocator.alloc(raft.Entry, count);
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    for (entries, 0..) |*entry, offset| {
        entry.* = .{
            .term = term,
            .index = first + offset,
            .data = try allocator.dupe(u8, "entry"),
        };
        initialized += 1;
    }
    return entries;
}

fn seedRecoveryStorage(storage: *raft.MemoryStorage) !void {
    var snapshot = raft.Snapshot{
        .data = try allocator.dupe(u8, "snapshot-5"),
        .metadata = .{
            .index = 5,
            .term = 2,
            .conf_state = .{ .voters = try allocator.dupe(u64, &.{1}) },
        },
    };
    defer snapshot.deinit(allocator);
    try storage.applySnapshot(allocator, snapshot);

    const entries = try makeReadyEntries(3, 6, 3);
    defer {
        for (entries) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    try storage.append(allocator, entries);
    try storage.setHardState(.{ .term = 3, .commit = 8 });
}

fn seedCommitOnlyStorage(storage: *raft.MemoryStorage) !void {
    try storage.setConfState(allocator, .{ .voters = @constCast(&[_]u64{ 1, 2 }) });
    const entries = try makeReadyEntries(1, 1, 1);
    defer {
        for (entries) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    try storage.append(allocator, entries);
    try storage.setHardState(.{ .term = 1 });
}

fn stageCommitOnlyReady(r: *Raftor) !void {
    try r.getRawNode().step(.{
        .msg_type = .append,
        .from = 2,
        .to = 1,
        .term = 1,
        .index = 1,
        .log_term = 1,
        .commit = 1,
    });
}

fn expectUnmodifiedFreshStorage(storage: *raft.MemoryStorage) !void {
    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expect(state.hard_state.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), state.conf_state.voters.len);
    try std.testing.expectEqual(@as(usize, 0), state.conf_state.learners.len);
    try std.testing.expectEqual(@as(usize, 0), state.conf_state.voters_outgoing.len);
    try std.testing.expectEqual(@as(usize, 0), state.conf_state.learners_next.len);
    try std.testing.expect(state.cluster_membership == null);
    try std.testing.expectEqual(@as(u64, 0), state.membership_index);
    try std.testing.expectEqual(@as(u64, 0), try storage.lastIndex());
    try std.testing.expectEqual(@as(u64, 0), storage.incarnation);
}

fn stageSnapshotAndSuffix(r: *Raftor) !void {
    const snapshot_data = try allocator.dupe(u8, "snapshot-10");
    const voters = allocator.dupe(u64, &.{ 1, 2 }) catch |err| {
        allocator.free(snapshot_data);
        return err;
    };
    try r.getRawNode().step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 3,
        .snapshot = .{
            .data = snapshot_data,
            .metadata = .{
                .index = 10,
                .term = 3,
                .conf_state = .{ .voters = voters },
            },
        },
    });
    try r.getRawNode().step(.{
        .msg_type = .append,
        .from = 2,
        .to = 1,
        .term = 3,
        .index = 10,
        .log_term = 3,
        .commit = 12,
        .entries = try makeReadyEntries(3, 11, 3),
    });
}

fn processOneReady(r: *Raftor) !void {
    try std.testing.expect(try r.processReadyStep());
    while (r.getReadyPhase() != null) try std.testing.expect(try r.processReadyStep());
}

const ProposalTester = struct {
    applied: bool = false,
    response: ?[]u8 = null,

    fn cb(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *ProposalTester = @ptrCast(@alignCast(ctx));
        if (result == .ok) {
            self.applied = true;
        }
    }

    fn callback(self: *ProposalTester) raft.ProposalCallback {
        return .{ .ctx = self, .function = cb };
    }
};

test "raftor: create and campaign to leader" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    try std.testing.expectEqual(StateRole.follower, r.getStatus().role);

    try r.campaign();
    try std.testing.expect(r.isLeader());
    try std.testing.expectEqual(StateRole.leader, r.getStatus().role);
}

test "raftor: propose data is applied to state machine" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    try r.campaign();

    var tester = ProposalTester{};
    try r.propose("hello world", tester.callback());

    var i: usize = 0;
    while (i < 10) : (i += 1) _ = try r.tick();

    try std.testing.expect(tester.applied);
    // The noop entry (from becomeLeader) and the proposed entry are both applied.
    try std.testing.expectEqual(@as(usize, 2), sm.applied.items.len);
    try std.testing.expectEqualStrings("hello world", sm.applied.items[1]);
}

test "raftor: proposal queue applies count and byte backpressure" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.max_queued_proposals = 1;
    config.max_queued_proposal_bytes = raft.request_context.header_size + 3;
    const r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    var oversized = ErrorTester{};
    try std.testing.expectError(error.ProposalBackpressure, r.propose("four", oversized.proposalCallback()));
    try std.testing.expect(!oversized.completed);

    var accepted = ErrorTester{};
    try r.propose("one", accepted.proposalCallback());
    const queued = r.getStatus();
    try std.testing.expectEqual(@as(usize, 1), queued.queued_proposals);
    try std.testing.expectEqual(raft.request_context.header_size + 3, queued.queued_proposal_bytes);

    var rejected = ErrorTester{};
    try std.testing.expectError(error.ProposalBackpressure, r.propose("two", rejected.proposalCallback()));
    try std.testing.expect(!rejected.completed);
    try std.testing.expectEqual(@as(usize, 1), r.getStatus().queued_proposals);
}

test "raftor: read-index queue applies count and byte backpressure" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.max_queued_read_indexes = 1;
    config.max_queued_read_index_bytes = raft.request_context.header_size + 3;
    const r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    var oversized = ErrorTester{};
    try std.testing.expectError(error.ReadIndexBackpressure, r.readIndex("four", oversized.readCallback()));
    try std.testing.expect(!oversized.completed);

    var accepted = ErrorTester{};
    try r.readIndex("one", accepted.readCallback());
    const queued = r.getStatus();
    try std.testing.expectEqual(@as(usize, 1), queued.queued_read_indexes);
    try std.testing.expectEqual(raft.request_context.header_size + 3, queued.queued_read_index_bytes);

    var rejected = ErrorTester{};
    try std.testing.expectError(error.ReadIndexBackpressure, r.readIndex("two", rejected.readCallback()));
    try std.testing.expect(!rejected.completed);
    try std.testing.expectEqual(@as(usize, 1), r.getStatus().queued_read_indexes);
}

test "raftor: callback observes applied index and cannot reenter event loop" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();
    try r.campaign();

    const Callback = struct {
        raftor: *Raftor,
        applied_index: ?u64 = null,
        reentry_error: ?raft.Error = null,

        fn invoke(ctx: *anyopaque, result: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .ok) self.applied_index = self.raftor.getStatus().applied_index;
            _ = self.raftor.tick() catch |err| {
                self.reentry_error = err;
                return;
            };
        }
    };
    var callback = Callback{ .raftor = r };
    try r.propose("payload", .{ .ctx = &callback, .function = Callback.invoke });
    for (0..16) |_| _ = try r.tick();
    try std.testing.expectEqual(sm.last_applied_index, callback.applied_index.?);
    try std.testing.expectEqual(error.EventLoopBusy, callback.reentry_error.?);
}

test "raftor: multiple proposals all applied" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    try r.campaign();

    var testers: [3]ProposalTester = undefined;
    try r.propose("a", testers[0].callback());
    try r.propose("b", testers[1].callback());
    try r.propose("c", testers[2].callback());

    var i: usize = 0;
    while (i < 10) : (i += 1) _ = try r.tick();

    for (testers) |t| try std.testing.expect(t.applied);
    // noop + 3 proposals = 4 applied entries.
    try std.testing.expectEqual(@as(usize, 4), sm.applied.items.len);
}

test "raftor: getStatus reports correct applied index" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    try r.campaign();

    var tester = ProposalTester{};
    try r.propose("data", tester.callback());

    var i: usize = 0;
    while (i < 10) : (i += 1) _ = try r.tick();

    const status = r.getStatus();
    try std.testing.expectEqual(@as(u64, 1), status.id);
    try std.testing.expect(r.isLeader());
    try std.testing.expect(status.commit_index >= 2);
    try std.testing.expect(status.applied_index >= 2);
}

test "raftor: poll does not advance logical time" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const node = try Raftor.create(allocator, .{
        .raft = .{
            .id = 1,
            .election_tick = 3,
            .heartbeat_tick = 1,
        },
    }, sm.stateMachine());
    defer node.destroy();

    for (0..10) |_| try std.testing.expect(!(try node.poll()));
    const status = node.getStatus();
    try std.testing.expectEqual(StateRole.follower, status.role);
    try std.testing.expectEqual(@as(u64, 0), status.term);
}

test "raftor: noop transport collects outbound messages" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    try r.campaign();

    // Single-node leader has no peers, so no outbound messages.
    // (Transport is internal NoopTransport — no way to inspect sent messages
    // after the createWithTransport refactor.)
}

test "raftor: read index completes after apply" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    try r.campaign();

    var read_done = false;
    const ReadTester = struct {
        done: *bool,
        fn cb(ctx: *anyopaque, result: raft.ReadIndexResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .ok) self.done.* = true;
        }
    };
    var rt = ReadTester{ .done = &read_done };
    try r.readIndex("read1", .{ .ctx = &rt, .function = ReadTester.cb });

    var i: usize = 0;
    while (i < 10) : (i += 1) _ = try r.tick();

    try std.testing.expect(read_done);
}

test "raftor: duplicate user read contexts use independent internal contexts" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();
    try r.campaign();

    const ReadTester = struct {
        completed: usize = 0,
        fn callback(ctx: *anyopaque, result: raft.ReadIndexResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .ok) self.completed += 1;
        }
    };
    var first = ReadTester{};
    var second = ReadTester{};
    try r.readIndex("same", .{ .ctx = &first, .function = ReadTester.callback });
    try r.readIndex("same", .{ .ctx = &second, .function = ReadTester.callback });
    for (0..16) |_| _ = try r.tick();
    try std.testing.expectEqual(@as(usize, 1), first.completed);
    try std.testing.expectEqual(@as(usize, 1), second.completed);
}

test "raftor: paged ReadIndex waits for its applied index" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    var config = makeConfig(1);
    config.raft.max_committed_size_per_ready = 0;
    const r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();
    try r.campaign();

    try r.getRawNode().propose("", "a");
    try r.getRawNode().propose("", "b");
    try r.getRawNode().propose("", "c");
    while (r.getReadyPhase() != raft.ReadyPhase.apply_advanced_committed) {
        try std.testing.expect(try r.processReadyStep());
    }
    try std.testing.expectEqual(@as(u64, 4), r.getStatus().commit_index);

    const ReadTester = struct {
        state_machine: *MockStateMachine,
        applied_at_completion: ?u64 = null,

        fn cb(ctx: *anyopaque, result: raft.ReadIndexResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .ok) self.applied_at_completion = self.state_machine.last_applied_index;
        }
    };
    var read = ReadTester{ .state_machine = &sm };
    try r.readIndex("paged-read", .{ .ctx = &read, .function = ReadTester.cb });

    _ = try r.tick();
    try std.testing.expect(read.applied_at_completion == null);
    _ = try r.tick();

    try std.testing.expectEqual(r.getStatus().commit_index, read.applied_at_completion.?);
    try std.testing.expectEqual(@as(u64, 4), read.applied_at_completion.?);
}

test "raftor: stop terminates run loop" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    // Start and immediately stop. Since run() blocks, we can't test it
    // directly in a single-threaded test. But we can verify stop() sets
    // the running flag to false.
    r.stop();
    try std.testing.expect(!r.isRunning());
}

test "raftor: transport lifecycle follows membership hydration" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2") },
    };
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{ 1, 2 }) }, &peers, &.{}, 1, .{});
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    r.stop();
    r.stop();
    try std.testing.expectEqual(@as(usize, 1), transport.stop_call_count);
    try std.testing.expect(transport.callback != null);
    try std.testing.expect(transport.peer_event_callback != null);
    r.destroy();

    try std.testing.expectEqual(@as(usize, 1), transport.start_count);
    try std.testing.expectEqual(@as(usize, 1), transport.stop_count);
    try std.testing.expect(transport.callback == null);
    try std.testing.expect(transport.peer_event_callback == null);
    try std.testing.expectEqualSlices(
        RecordingTransport.LifecycleEvent,
        &.{
            .add_peer,
            .set_message_callback,
            .set_peer_event_callback,
            .start,
            .stop,
            .clear_message_callback,
            .clear_peer_event_callback,
        },
        transport.lifecycle_events[0..transport.lifecycle_events_len],
    );
}

test "raftor: transport start failure clears callbacks and unwinds" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2") },
    };
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{ 1, 2 }) }, &peers, &.{}, 1, .{});
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    transport.fail_start = true;
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    try std.testing.expectError(error.ConnectionClosed, Raftor.createWithDependencies(
        allocator,
        makeConfig(1),
        .restart,
        .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = sm.stateMachine(),
        },
    ));
    try std.testing.expect(transport.callback == null);
    try std.testing.expect(transport.peer_event_callback == null);
    try std.testing.expectEqual(@as(usize, 1), transport.stop_call_count);
    try std.testing.expectEqualSlices(
        RecordingTransport.LifecycleEvent,
        &.{
            .add_peer,
            .set_message_callback,
            .set_peer_event_callback,
            .start,
            .clear_message_callback,
            .clear_peer_event_callback,
            .stop,
        },
        transport.lifecycle_events[0..transport.lifecycle_events_len],
    );
}

test "raftor: stopped transport does not deliver queued callbacks" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();

    try transport.queueMessage(.{ .msg_type = .heartbeat, .from = 2, .to = 1 });
    try transport.queuePeerEvent(.{ .peer_id = 2, .kind = .@"unreachable" });
    r.stop();
    try std.testing.expect(!(try transport.transport().pollOne()));
    try std.testing.expectEqual(@as(usize, 0), transport.delivered_message_count);
    try std.testing.expectEqual(@as(usize, 0), transport.delivered_peer_event_count);
}

test "raftor: transport poll budget drains bursts" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.transport_poll_budget = 3;
    const r = try Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();
    for (0..5) |_| try transport.queueMessage(.{ .msg_type = .hup });

    _ = try r.tick();
    try std.testing.expectEqual(@as(usize, 3), transport.delivered_message_count);
    try std.testing.expectEqual(@as(usize, 2), transport.inbound_messages.items.len);
    _ = try r.tick();
    try std.testing.expectEqual(@as(usize, 5), transport.delivered_message_count);
    try std.testing.expectEqual(@as(usize, 0), transport.inbound_messages.items.len);
}

test "raftor: proposal drain budget preserves transport progress" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.proposal_drain_budget = 2;
    config.transport_poll_budget = 1;
    const r = try Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();

    var proposals = [_]ErrorTester{ .{}, .{}, .{} };
    for (&proposals, 0..) |*proposal, i| {
        const data = try std.fmt.allocPrint(allocator, "proposal-{}", .{i});
        defer allocator.free(data);
        try r.propose(data, proposal.proposalCallback());
    }
    try transport.queueMessage(.{ .msg_type = .hup });

    _ = try r.tick();
    try std.testing.expect(proposals[0].completed);
    try std.testing.expect(proposals[1].completed);
    try std.testing.expect(!proposals[2].completed);
    try std.testing.expectEqual(@as(usize, 1), r.getStatus().queued_proposals);
    try std.testing.expectEqual(@as(usize, 1), transport.delivered_message_count);

    _ = try r.tick();
    try std.testing.expect(proposals[2].completed);
    try std.testing.expectEqual(@as(usize, 0), r.getStatus().queued_proposals);
}

test "raftor: read-index drain budget preserves transport progress" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.read_index_drain_budget = 2;
    config.transport_poll_budget = 1;
    const r = try Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();

    var reads = [_]ErrorTester{ .{}, .{}, .{} };
    for (&reads, 0..) |*read, i| {
        const ctx = try std.fmt.allocPrint(allocator, "read-{}", .{i});
        defer allocator.free(ctx);
        try r.readIndex(ctx, read.readCallback());
    }
    try transport.queueMessage(.{ .msg_type = .hup });

    _ = try r.tick();
    try std.testing.expect(reads[0].completed);
    try std.testing.expect(reads[1].completed);
    try std.testing.expect(!reads[2].completed);
    try std.testing.expectEqual(@as(usize, 1), transport.delivered_message_count);

    _ = try r.tick();
    try std.testing.expect(reads[2].completed);
}

test "raftor: zero transport poll budget is invalid" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.transport_poll_budget = 0;
    try std.testing.expectError(error.InvalidConfig, Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    }));
    try std.testing.expectEqual(@as(usize, 0), transport.start_count);
}

test "raftor: zero proposal queue limits are invalid" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.max_queued_proposals = 0;
    try std.testing.expectError(error.InvalidConfig, Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    }));

    config.max_queued_proposals = 1;
    config.max_queued_proposal_bytes = 0;
    try std.testing.expectError(error.InvalidConfig, Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    }));
    try std.testing.expectEqual(@as(usize, 0), transport.start_count);
}

test "raftor: zero proposal drain budget is invalid" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.proposal_drain_budget = 0;
    try std.testing.expectError(error.InvalidConfig, Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    }));
    try std.testing.expectEqual(@as(usize, 0), transport.start_count);
}

test "raftor: zero read-index drain budget is invalid" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.read_index_drain_budget = 0;
    try std.testing.expectError(error.InvalidConfig, Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    }));
    try std.testing.expectEqual(@as(usize, 0), transport.start_count);
}

test "raftor: zero read-index queue limits are invalid" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.max_queued_read_indexes = 0;
    try std.testing.expectError(error.InvalidConfig, Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    }));

    config.max_queued_read_indexes = 1;
    config.max_queued_read_index_bytes = 0;
    try std.testing.expectError(error.InvalidConfig, Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    }));
    try std.testing.expectEqual(@as(usize, 0), transport.start_count);
}

test "raftor: peer events map to raft reports" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try storage.setConfState(allocator, .{ .voters = @constCast(&[_]u64{ 1, 2 }) });
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();

    const raft_state = r.getRawNode().raftPtr();
    raft_state.becomeCandidate();
    try raft_state.becomeLeader();
    while (try r.processReadyStep()) {}
    const progress = raft_state.progress_tracker.getPtr(2).?;

    progress.becomeReplicate();
    try transport.queuePeerEvent(.{ .peer_id = 2, .kind = .@"unreachable" });
    _ = try r.tick();
    try std.testing.expectEqual(raft.ProgressState.probe, progress.state);

    progress.becomeReplicate();
    try transport.queuePeerEvent(.{ .peer_id = 2, .kind = .identity_rejected });
    _ = try r.tick();
    try std.testing.expectEqual(raft.ProgressState.probe, progress.state);

    progress.becomeSnapshot(10);
    try transport.queuePeerEvent(.{ .peer_id = 2, .kind = .snapshot_failure });
    _ = try r.tick();
    try std.testing.expectEqual(raft.ProgressState.probe, progress.state);
    try std.testing.expect(progress.paused);
}

test "raftor: stop terminates queued requests exactly once" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    var proposal = ErrorTester{};
    var read = ErrorTester{};
    try r.propose("queued", proposal.proposalCallback());
    try r.readIndex("queued-read", read.readCallback());
    r.stop();
    r.stop();
    try std.testing.expectEqual(error.ShuttingDown, proposal.err.?);
    try std.testing.expectEqual(error.ShuttingDown, read.err.?);
    try std.testing.expectError(error.ShuttingDown, r.propose("late", proposal.proposalCallback()));
    try std.testing.expectError(error.ShuttingDown, r.readIndex("late-read", read.readCallback()));
}

test "raftor: destroy terminates queued requests" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    var proposal = ErrorTester{};
    var read = ErrorTester{};
    try r.propose("queued", proposal.proposalCallback());
    try r.readIndex("queued-read", read.readCallback());
    r.destroy();
    try std.testing.expectEqual(error.ShuttingDown, proposal.err.?);
    try std.testing.expectEqual(error.ShuttingDown, read.err.?);
}

test "raftor: shutdown callback can stop again" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    const r = try Raftor.create(allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();

    const Callback = struct {
        raftor: *Raftor,
        calls: usize = 0,
        rejected: bool = false,

        fn invoke(ctx: *anyopaque, result: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .err and result.err == error.ShuttingDown) self.calls += 1;
            self.raftor.stop();
            self.raftor.propose("reentrant", .{ .ctx = self, .function = invoke }) catch |err| {
                self.rejected = err == error.ShuttingDown;
            };
        }
    };
    var callback = Callback{ .raftor = r };
    try r.propose("queued", .{ .ctx = &callback, .function = Callback.invoke });
    r.stop();
    try std.testing.expectEqual(@as(usize, 1), callback.calls);
    try std.testing.expect(callback.rejected);
}

test "raftor: concurrent stop completes every accepted request once" {
    const thread_allocator = std.heap.smp_allocator;
    var sm = MockStateMachine.init(thread_allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.tick_interval_ms = 1;
    const r = try Raftor.create(thread_allocator, config, sm.stateMachine());
    defer r.destroy();

    const producer_count = 4;
    const requests_per_producer = 64;
    const Record = struct {
        accepted: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        callbacks: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

        fn proposal(ctx: *anyopaque, _: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.callbacks.fetchAdd(1, .monotonic);
        }
        fn read(ctx: *anyopaque, _: raft.ReadIndexResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.callbacks.fetchAdd(1, .monotonic);
        }
    };
    var records: [producer_count][requests_per_producer]Record = undefined;
    for (&records) |*producer_records| {
        for (producer_records) |*record| record.* = .{};
    }
    var attempts = std.atomic.Value(usize).init(0);

    const RunState = struct {
        raftor: *Raftor,
        err: ?raft.Error = null,
        fn run(self: *@This()) void {
            self.raftor.run() catch |err| {
                self.err = err;
            };
        }
    };
    var run_state = RunState{ .raftor = r };
    const run_thread = try std.Thread.spawn(.{}, RunState.run, .{&run_state});
    while (!r.isRunning()) std.atomic.spinLoopHint();

    const Producer = struct {
        raftor: *Raftor,
        records: *[requests_per_producer]Record,
        attempts: *std.atomic.Value(usize),
        unexpected_error: ?raft.Error = null,

        fn run(self: *@This()) void {
            for (self.records, 0..) |*record, index| {
                _ = self.attempts.fetchAdd(1, .release);
                const result = if (index % 2 == 0)
                    self.raftor.propose("value", .{ .ctx = record, .function = Record.proposal })
                else
                    self.raftor.readIndex("read", .{ .ctx = record, .function = Record.read });
                if (result) |_| {
                    record.accepted.store(true, .release);
                } else |err| {
                    if (err != error.ShuttingDown) self.unexpected_error = err;
                }
            }
        }
    };
    var producers: [producer_count]Producer = undefined;
    var producer_threads: [producer_count]std.Thread = undefined;
    for (&producers, &producer_threads, &records) |*producer, *thread, *producer_records| {
        producer.* = .{ .raftor = r, .records = producer_records, .attempts = &attempts };
        thread.* = try std.Thread.spawn(.{}, Producer.run, .{producer});
    }
    while (attempts.load(.acquire) < requests_per_producer) std.atomic.spinLoopHint();
    r.stop();
    for (producer_threads) |thread| thread.join();
    run_thread.join();

    try std.testing.expect(run_state.err == null);
    for (producers) |producer| try std.testing.expect(producer.unexpected_error == null);
    for (records) |producer_records| {
        for (producer_records) |record| {
            const expected: usize = if (record.accepted.load(.acquire)) 1 else 0;
            try std.testing.expectEqual(expected, record.callbacks.load(.acquire));
        }
    }
}

test "raftor: destroy waits for run to exit" {
    const thread_allocator = std.heap.smp_allocator;
    var sm = MockStateMachine.init(thread_allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.tick_interval_ms = 100;
    const r = try Raftor.create(thread_allocator, config, sm.stateMachine());
    var destroyed = false;
    errdefer if (!destroyed) r.destroy();

    const RunState = struct {
        raftor: *Raftor,
        err: ?raft.Error = null,

        fn run(self: *@This()) void {
            self.raftor.run() catch |err| {
                self.err = err;
            };
        }
    };
    var run_state = RunState{ .raftor = r };
    const run_thread = try std.Thread.spawn(.{}, RunState.run, .{&run_state});
    while (!r.isRunning()) std.atomic.spinLoopHint();

    r.destroy();
    destroyed = true;
    run_thread.join();
    try std.testing.expect(run_state.err == null);
}

test "raftor: destroy waits for concurrent stop callbacks" {
    const thread_allocator = std.heap.smp_allocator;
    var sm = MockStateMachine.init(thread_allocator);
    defer sm.deinit();
    const r = try Raftor.create(thread_allocator, makeConfig(1), sm.stateMachine());
    var destroy_owns_raftor = false;
    errdefer if (!destroy_owns_raftor) r.destroy();

    const Callback = struct {
        entered: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        release: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        exited: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        fn invoke(ctx: *anyopaque, _: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
            self.exited.store(true, .release);
        }
    };
    var callback = Callback{};
    try r.propose("queued", .{ .ctx = &callback, .function = Callback.invoke });

    const StopState = struct {
        raftor: *Raftor,
        fn run(self: *@This()) void {
            self.raftor.stop();
        }
    };
    var stop_state = StopState{ .raftor = r };
    const stop_thread = try std.Thread.spawn(.{}, StopState.run, .{&stop_state});
    defer {
        callback.release.store(true, .release);
        stop_thread.join();
    }
    while (!callback.entered.load(.acquire)) std.atomic.spinLoopHint();

    const DestroyState = struct {
        raftor: *Raftor,
        callback_exited: *std.atomic.Value(bool),
        started: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        completed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        returned_before_callback_exit: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.raftor.destroy();
            if (!self.callback_exited.load(.acquire)) self.returned_before_callback_exit.store(true, .release);
            self.completed.store(true, .release);
        }
    };
    var destroy_state = DestroyState{ .raftor = r, .callback_exited = &callback.exited };
    const destroy_thread = try std.Thread.spawn(.{}, DestroyState.run, .{&destroy_state});
    destroy_owns_raftor = true;
    defer {
        callback.release.store(true, .release);
        destroy_thread.join();
    }
    while (!destroy_state.started.load(.acquire)) std.atomic.spinLoopHint();
    for (0..1000) |_| std.atomic.spinLoopHint();
    try std.testing.expect(!destroy_state.completed.load(.acquire));

    callback.release.store(true, .release);
    while (!destroy_state.completed.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(destroy_state.completed.load(.acquire));
    try std.testing.expect(!destroy_state.returned_before_callback_exit.load(.acquire));
}

test "raftor: getStatus is safe while run and producers are active" {
    const thread_allocator = std.heap.smp_allocator;
    const proposal_count = 128;
    var sm = MockStateMachine.init(thread_allocator);
    defer sm.deinit();
    var config = makeConfig(1);
    config.tick_interval_ms = 1;
    const r = try Raftor.create(thread_allocator, config, sm.stateMachine());
    defer r.destroy();
    try r.campaign();

    const RunState = struct {
        raftor: *Raftor,
        err: ?raft.Error = null,
        fn run(self: *@This()) void {
            self.raftor.run() catch |err| {
                self.err = err;
            };
        }
    };
    var run_state = RunState{ .raftor = r };
    const run_thread = try std.Thread.spawn(.{}, RunState.run, .{&run_state});
    var run_joined = false;
    defer if (!run_joined) {
        r.stop();
        run_thread.join();
    };
    while (!r.isRunning()) std.atomic.spinLoopHint();

    var completed = std.atomic.Value(usize).init(0);
    var callback_error = std.atomic.Value(bool).init(false);
    const Callback = struct {
        completed: *std.atomic.Value(usize),
        callback_error: *std.atomic.Value(bool),
        fn invoke(ctx: *anyopaque, result: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .err) self.callback_error.store(true, .release);
            _ = self.completed.fetchAdd(1, .release);
        }
    };
    var callback = Callback{ .completed = &completed, .callback_error = &callback_error };

    var reader_done = std.atomic.Value(bool).init(false);
    const Reader = struct {
        raftor: *Raftor,
        done: *std.atomic.Value(bool),
        invalid: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        fn run(self: *@This()) void {
            var previous = self.raftor.getStatus();
            while (!self.done.load(.acquire)) {
                const status = self.raftor.getStatus();
                if (status.id != 1 or
                    status.term < previous.term or
                    status.commit_index < previous.commit_index or
                    status.applied_index < previous.applied_index or
                    status.applied_index > status.commit_index or
                    (status.role == .leader and status.leader_id != status.id) or
                    (status.queued_proposals == 0 and status.queued_proposal_bytes != 0) or
                    (status.queued_read_indexes == 0 and status.queued_read_index_bytes != 0))
                {
                    self.invalid.store(true, .release);
                }
                previous = status;
            }
        }
    };
    var reader = Reader{ .raftor = r, .done = &reader_done };
    const reader_thread = try std.Thread.spawn(.{}, Reader.run, .{&reader});
    var reader_joined = false;
    defer if (!reader_joined) {
        reader_done.store(true, .release);
        reader_thread.join();
    };

    for (0..proposal_count) |_| {
        try r.propose("value", .{ .ctx = &callback, .function = Callback.invoke });
    }
    while (completed.load(.acquire) != proposal_count) std.atomic.spinLoopHint();
    reader_done.store(true, .release);
    reader_thread.join();
    reader_joined = true;
    r.stop();
    run_thread.join();
    run_joined = true;

    try std.testing.expect(!reader.invalid.load(.acquire));
    try std.testing.expect(!callback_error.load(.acquire));
    try std.testing.expect(run_state.err == null);
}

test "raftor: manual takeSnapshot compacts storage" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const config = makeConfig(1);
    var r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    try r.campaign();
    try std.testing.expect(r.isLeader());

    // Propose a few entries so there's something to snapshot.
    var tester = ProposalTester{};
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        try r.propose("data", .{ .ctx = &tester, .function = ProposalTester.cb });
    }
    i = 0;
    while (i < 10) : (i += 1) _ = try r.tick();

    // Manual snapshot.
    try r.takeSnapshot();
    try std.testing.expectEqual(@as(usize, 1), sm.snapshot_count);
    try std.testing.expect(sm.last_snapshot_index >= 2);
}

test "raftor: snapshot triggers at entries threshold" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    var config = makeConfig(1);
    config.snapshot_entries_threshold = 3;
    config.snapshot_retry_min_ticks = 0;
    var r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    try r.campaign();

    // Propose 4 entries (threshold=3 → snapshot should fire after 3+ applied).
    var tester = ProposalTester{};
    var i: usize = 0;
    while (i < 4) : (i += 1) {
        try r.propose("x", .{ .ctx = &tester, .function = ProposalTester.cb });
    }
    i = 0;
    while (i < 20) : (i += 1) _ = try r.tick();

    // At least one snapshot should have been triggered.
    try std.testing.expect(sm.snapshot_count >= 1);
}

test "raftor: snapshot threshold accumulates entries across ticks" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    var config = makeConfig(1);
    config.snapshot_entries_threshold = 3;
    config.snapshot_retry_min_ticks = 0;
    var r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    try r.campaign();
    for (0..3) |_| {
        var tester = ProposalTester{};
        try r.propose("x", tester.callback());
        for (0..10) |_| {
            _ = try r.tick();
            if (tester.applied) break;
        }
        try std.testing.expect(tester.applied);
    }

    try std.testing.expect(sm.snapshot_count >= 1);
}

test "raftor: snapshot triggers at interval" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    var config = makeConfig(1);
    config.snapshot_entries_threshold = 0;
    config.snapshot_interval_ticks = 5;
    config.snapshot_retry_min_ticks = 0;
    var r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    try r.campaign();

    // Propose something so applied_index > 0.
    var tester = ProposalTester{};
    try r.propose("y", .{ .ctx = &tester, .function = ProposalTester.cb });
    var i: usize = 0;
    while (i < 5) : (i += 1) _ = try r.tick();

    // Tick past the interval threshold.
    i = 0;
    while (i < 10) : (i += 1) _ = try r.tick();

    try std.testing.expect(sm.snapshot_count >= 1);
}

test "raftor: snapshot disabled when all thresholds zero" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    var config = makeConfig(1);
    config.snapshot_entries_threshold = 0;
    config.snapshot_interval_ticks = 0;
    var r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    try r.campaign();
    var tester = ProposalTester{};
    try r.propose("z", .{ .ctx = &tester, .function = ProposalTester.cb });
    var i: usize = 0;
    while (i < 20) : (i += 1) _ = try r.tick();

    try std.testing.expectEqual(@as(usize, 0), sm.snapshot_count);
}

test "raftor: automatic snapshot failure does not fail the tick" {
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    var failing = FailingStateMachine{
        .inner = &machine,
        .fail_data = "not-proposed",
        .fail_snapshot = true,
    };
    var config = makeConfig(1);
    config.snapshot_entries_threshold = 1;
    config.snapshot_retry_min_ticks = 0;
    const r = try Raftor.create(allocator, config, failing.stateMachine());
    defer r.destroy();
    try r.campaign();

    var proposal = ProposalTester{};
    try r.propose("snapshot", proposal.callback());
    for (0..10) |_| {
        _ = try r.tick();
        if (proposal.applied) break;
    }
    try std.testing.expect(proposal.applied);
    try std.testing.expectEqual(@as(usize, 1), failing.snapshot_attempts);
    try std.testing.expectEqual(@as(usize, 0), machine.snapshot_count);
}

test "raftor: snapshot rate-limits retries" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    var config = makeConfig(1);
    config.snapshot_entries_threshold = 1;
    config.snapshot_retry_min_ticks = 100;
    var r = try Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    try r.campaign();
    var tester = ProposalTester{};
    try r.propose("a", .{ .ctx = &tester, .function = ProposalTester.cb });

    // Tick a few times — snapshot fires once, then rate-limited.
    var i: usize = 0;
    while (i < 5) : (i += 1) _ = try r.tick();

    const count_after_first_burst = sm.snapshot_count;

    // More ticks — rate limit prevents additional snapshots.
    i = 0;
    while (i < 5) : (i += 1) _ = try r.tick();

    // Count should NOT increase significantly (at most +1 from interval).
    try std.testing.expect(sm.snapshot_count <= count_after_first_burst + 1);
}

test "raftor: injected dependencies are borrowed" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    {
        const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = sm.stateMachine(),
        });
        defer r.destroy();
        try r.campaign();
        try std.testing.expect(r.isLeader());
    }

    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{1}, state.conf_state.voters);
    try transport.transport().send(&.{.{ .msg_type = .heartbeat, .to = 2 }});
}

test "raftor: implicit joint configuration automatically leaves" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();

    var changes = [_]raft.ConfChangeSingle{
        .{ .change_type = .add_learner_node, .node_id = 2 },
    };
    try r.getRawNode().proposeConfChange("", .{
        .transition = .implicit,
        .changes = &changes,
    });
    for (0..16) |_| _ = try r.tick();

    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{1}, state.conf_state.voters);
    try std.testing.expectEqualSlices(u64, &.{2}, state.conf_state.learners);
    try std.testing.expectEqual(@as(usize, 0), state.conf_state.voters_outgoing.len);
    try std.testing.expectEqual(@as(usize, 0), state.conf_state.learners_next.len);
    try std.testing.expect(!state.conf_state.auto_leave);

    var proposal = ProposalTester{};
    try r.propose("after-auto-leave", proposal.callback());
    for (0..16) |_| _ = try r.tick();
    try std.testing.expect(proposal.applied);
}

test "raftor: startup mode validates and reloads storage" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const dependencies = raft.RaftorDependencies{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    };
    try std.testing.expectError(
        error.IncompatibleStorage,
        Raftor.createWithDependencies(allocator, makeConfig(1), .restart, dependencies),
    );

    try storage.setRaftState(allocator, .{
        .hard_state = .{ .term = 7, .vote = 1 },
        .conf_state = .{ .voters = @constCast(&[_]u64{1}) },
    });
    try std.testing.expectError(
        error.IncompatibleStorage,
        Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, dependencies),
    );

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, dependencies);
    defer r.destroy();
    try std.testing.expectEqual(@as(u64, 7), r.getStatus().term);
    try std.testing.expectEqual(@as(u64, 1), r.getRawNode().raftConst().vote);
}

test "raftor: Ready persistence resumes at the failed phase" {
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    const failing_allocator = failing.allocator();
    var sm = MockStateMachine.init(failing_allocator);
    defer sm.deinit();

    const r = try Raftor.create(failing_allocator, makeConfig(1), sm.stateMachine());
    defer r.destroy();
    try r.getRawNode().campaign();

    try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.validate, r.getReadyPhase().?);
    try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.persist_snapshot, r.getReadyPhase().?);
    try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.persist_entries, r.getReadyPhase().?);

    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.persist_entries, r.getReadyPhase().?);

    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expect(try r.tick());
    try std.testing.expectEqual(@as(?raft.ReadyPhase, null), r.getReadyPhase());
    try std.testing.expect(r.isLeader());
}

test "raftor: Ready persists snapshot before its suffix and HardState" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try stageSnapshotAndSuffix(r);
    try processOneReady(r);

    var snapshot = (try storage.localSnapshot(allocator)).?;
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 10), snapshot.metadata.index);
    try std.testing.expectEqual(@as(u64, 13), try storage.lastIndex());
    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 12), state.hard_state.commit);
    try std.testing.expectEqual(@as(usize, 1), machine.restore_count);
    try std.testing.expect(!r.getRawNode().hasReady());
}

test "raftor: WAL recovers snapshot Ready suffix and HardState" {
    var fixture = try raft.FsTestFixture.init(allocator, .real);
    defer fixture.deinit();

    {
        var storage = try raft.WALStorage.openWithFs(allocator, fixture.walDir(), fixture.fs());
        defer storage.deinit();
        var transport = raft.NoopTransport.init(allocator);
        defer transport.deinit();
        var machine = DurableStateMachine.init(allocator);
        defer machine.deinit();
        const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        });
        defer r.destroy();
        try stageSnapshotAndSuffix(r);
        try processOneReady(r);
        try std.testing.expectEqual(@as(usize, 1), machine.restore_count);
    }

    var recovered = try raft.WALStorage.openWithFs(allocator, fixture.walDir(), fixture.fs());
    defer recovered.deinit();
    const storage = recovered.asWritableStorage();
    var snapshot = (try storage.localSnapshot(allocator)).?;
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 10), snapshot.metadata.index);
    try std.testing.expectEqual(@as(u64, 11), try storage.firstIndex());
    try std.testing.expectEqual(@as(u64, 13), try storage.lastIndex());
    const entries = try storage.entries(allocator, 11, 14, null, .{ .empty = .{ .can_async = false } });
    defer {
        for (entries) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 3), entries.len);
    for (entries, 11..) |entry, index| try std.testing.expectEqual(index, entry.index);
    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 12), state.hard_state.commit);
}

test "raftor: snapshot sync failure does not restore application state" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try stageSnapshotAndSuffix(r);
    failing_storage.fail_sync = true;

    try std.testing.expect(try r.processReadyStep());
    while (r.getReadyPhase() != raft.ReadyPhase.sync) {
        try std.testing.expect(try r.processReadyStep());
    }
    try std.testing.expectError(error.WalSyncFailed, r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.sync, r.getReadyPhase().?);
    try std.testing.expectEqual(@as(usize, 0), machine.restore_count);
    try std.testing.expectEqual(@as(u64, 0), r.getStatus().applied_index);

    failing_storage.fail_sync = false;
    try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.restore_snapshot, r.getReadyPhase().?);
    try std.testing.expectEqual(@as(usize, 0), machine.restore_count);
    while (r.getReadyPhase() != null) try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(@as(usize, 1), machine.restore_count);
    try std.testing.expectEqual(@as(u64, 12), r.getStatus().applied_index);
}

test "raftor: bootstrap sync failure aborts creation" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var failing_storage = SyncFailingStorage{
        .inner = storage.asWritableStorage(),
        .fail_sync = true,
    };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    try std.testing.expectError(
        error.WalSyncFailed,
        Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
            .storage = failing_storage.writableStorage(),
            .transport = transport.transport(),
            .state_machine = sm.stateMachine(),
        }),
    );
}

test "raftor: incarnation reservation failure aborts creation" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var failing_storage = SyncFailingStorage{
        .inner = storage.asWritableStorage(),
        .fail_incarnation = true,
    };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    try std.testing.expectError(
        error.WalSyncFailed,
        Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
            .storage = failing_storage.writableStorage(),
            .transport = transport.transport(),
            .state_machine = sm.stateMachine(),
        }),
    );
    try std.testing.expectEqual(@as(u64, 0), storage.incarnation);
}

test "raftor: Ready sync failure blocks send and apply" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();
    failing_storage.fail_sync = true;
    try r.getRawNode().campaign();

    while (r.getReadyPhase() != raft.ReadyPhase.sync) {
        try std.testing.expect(try r.processReadyStep());
    }
    try std.testing.expectError(error.WalSyncFailed, r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.sync, r.getReadyPhase().?);
    try std.testing.expectEqual(@as(usize, 0), sm.applied.items.len);

    failing_storage.fail_sync = false;
    while (r.getReadyPhase() != null) {
        try std.testing.expect(try r.processReadyStep());
    }
    try std.testing.expectEqual(@as(usize, 1), sm.applied.items.len);
}

test "raftor: committed apply failure is terminal" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var failing_sm = FailingStateMachine{ .inner = &sm, .fail_data = "fail" };

    const r = try Raftor.create(allocator, makeConfig(1), failing_sm.stateMachine());
    defer r.destroy();
    try r.campaign();

    var failed = ErrorTester{};
    var after = ErrorTester{};
    try r.propose("fail", failed.proposalCallback());
    try r.propose("after", after.proposalCallback());

    try std.testing.expectError(error.OutOfMemory, r.tick());
    try std.testing.expectEqual(@as(u64, 1), r.getStatus().applied_index);
    try std.testing.expectEqual(@as(usize, 1), sm.applied.items.len);
    try std.testing.expectEqual(error.OutOfMemory, failed.err.?);
    try std.testing.expectEqual(error.OutOfMemory, after.err.?);
    try std.testing.expectError(error.OutOfMemory, r.tick());

    var rejected = ErrorTester{};
    try std.testing.expectError(error.ShuttingDown, r.propose("new", rejected.proposalCallback()));
    try std.testing.expect(!rejected.completed);
}

test "raftor: terminal failure drains queued requests" {
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();
    var failing_sm = FailingStateMachine{ .inner = &sm, .fail_data = "fail" };

    const r = try Raftor.create(allocator, makeConfig(1), failing_sm.stateMachine());
    defer r.destroy();
    try r.campaign();
    try r.getRawNode().propose("", "fail");

    var proposal = ErrorTester{};
    var read = ErrorTester{};
    try r.propose("queued", proposal.proposalCallback());
    try r.readIndex("queued-read", read.readCallback());

    var terminal_error: ?raft.Error = null;
    for (0..32) |_| {
        _ = r.processReadyStep() catch |err| {
            terminal_error = err;
            break;
        };
    }
    try std.testing.expectEqual(error.OutOfMemory, terminal_error.?);
    try std.testing.expectEqual(error.OutOfMemory, proposal.err.?);
    try std.testing.expectEqual(error.OutOfMemory, read.err.?);
}

test "raftor: configuration persistence failure is terminal" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();
    failing_storage.fail_conf_state = true;

    try r.addNode(2, "peer-2");
    try std.testing.expectError(error.WalWriteFailed, r.tick());
    try std.testing.expectEqual(@as(u64, 1), r.getStatus().applied_index);
    try std.testing.expectError(error.WalWriteFailed, r.tick());
}

test "raftor: configuration sync failure is terminal before apply advances" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();
    try r.addNode(2, "peer-2");

    try std.testing.expect(try r.processReadyStep());
    while (r.getReadyPhase() != raft.ReadyPhase.apply_advanced_committed) {
        try std.testing.expect(try r.processReadyStep());
    }
    failing_storage.fail_sync = true;
    try std.testing.expectError(error.WalSyncFailed, r.processReadyStep());
    try std.testing.expectEqual(@as(u64, 1), r.getStatus().applied_index);
    try std.testing.expectError(error.WalSyncFailed, r.tick());
}

test "raftor: WAL recovers membership immediately after configuration apply" {
    var fixture = try raft.FsTestFixture.init(allocator, .real);
    defer fixture.deinit();

    {
        var storage = try raft.WALStorage.openWithFs(allocator, fixture.walDir(), fixture.fs());
        defer storage.deinit();
        var transport = raft.NoopTransport.init(allocator);
        defer transport.deinit();
        var machine = MockStateMachine.init(allocator);
        defer machine.deinit();
        const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        });
        defer r.destroy();
        try r.campaign();
        try r.addNode(2, "peer-2");
        try processOneReady(r);
    }

    var recovered = try raft.WALStorage.openWithFs(allocator, fixture.walDir(), fixture.fs());
    defer recovered.deinit();
    var state = try recovered.asWritableStorage().initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, state.conf_state.voters);
}

test "raftor: advanced commit survives restart" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    const dependencies = raft.RaftorDependencies{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = sm.stateMachine(),
    };
    {
        const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, dependencies);
        defer r.destroy();
        try r.campaign();
    }

    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqual(sm.last_applied_index, state.hard_state.commit);

    var config = makeConfig(1);
    config.raft.applied = sm.last_applied_index;
    var restart_transport = raft.NoopTransport.init(allocator);
    defer restart_transport.deinit();
    const restarted = try Raftor.createWithDependencies(allocator, config, .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = restart_transport.transport(),
        .state_machine = sm.stateMachine(),
    });
    defer restarted.destroy();
    try std.testing.expectEqual(sm.last_applied_index, restarted.getStatus().applied_index);
}

test "raftor: state machine without durable cursor keeps snapshot recovery" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedRecoveryStorage(&storage);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    machine.durable_applied = .{ .index = 7, .term = 3 };
    try machine.state.appendSlice(allocator, "durable");

    const state_machine = machine.stateMachine();
    try std.testing.expect((try state_machine.durableApplied()) == null);
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = state_machine,
    });
    defer r.destroy();

    try std.testing.expectEqual(@as(usize, 1), machine.restore_count);
    try std.testing.expectEqualStrings("snapshot-5", machine.state.items);
    try std.testing.expectEqual(@as(u64, 5), r.getStatus().applied_index);
}

test "raftor: durable cursor behind snapshot restores snapshot" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedRecoveryStorage(&storage);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    try machine.state.appendSlice(allocator, "durable");

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.durableStateMachine(),
    });
    defer r.destroy();

    try std.testing.expectEqual(@as(usize, 1), machine.restore_count);
    try std.testing.expectEqualStrings("snapshot-5", machine.state.items);
    try std.testing.expectEqual(raft.DurableApplied{ .index = 5, .term = 2 }, machine.durable_applied);
    try std.testing.expectEqual(@as(u64, 5), r.getStatus().applied_index);
}

test "raftor: durable cursor at snapshot skips restore" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedRecoveryStorage(&storage);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    machine.durable_applied = .{ .index = 5, .term = 2 };
    try machine.state.appendSlice(allocator, "durable");

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.durableStateMachine(),
    });
    defer r.destroy();

    try std.testing.expectEqual(@as(usize, 0), machine.restore_count);
    try std.testing.expectEqualStrings("durable", machine.state.items);
    try std.testing.expectEqual(@as(u64, 5), r.getStatus().applied_index);
}

test "raftor: durable cursor ahead of snapshot resumes from cursor" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedRecoveryStorage(&storage);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    machine.durable_applied = .{ .index = 7, .term = 3 };
    try machine.state.appendSlice(allocator, "durable");

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.durableStateMachine(),
    });
    defer r.destroy();

    try std.testing.expectEqual(@as(usize, 0), machine.restore_count);
    try std.testing.expectEqual(@as(u64, 7), r.getStatus().applied_index);
    try std.testing.expectEqual(@as(u64, 7), r.getRawNode().raftConst().raft_log.applied);
    _ = try r.tick();
    try std.testing.expectEqualStrings("durableentry", machine.state.items);
    try std.testing.expectEqual(raft.DurableApplied{ .index = 8, .term = 3 }, machine.durable_applied);
    try std.testing.expectEqual(@as(u64, 8), r.getStatus().applied_index);
}

test "raftor: durable cursor beyond commit is rejected" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedRecoveryStorage(&storage);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    machine.durable_applied = .{ .index = 9, .term = 3 };

    try std.testing.expectError(error.IncompatibleStorage, Raftor.createWithDependencies(
        allocator,
        makeConfig(1),
        .restart,
        .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.durableStateMachine(),
        },
    ));
}

test "raftor: durable cursor term mismatch is rejected" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedRecoveryStorage(&storage);
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    machine.durable_applied = .{ .index = 7, .term = 2 };

    try std.testing.expectError(error.IncompatibleStorage, Raftor.createWithDependencies(
        allocator,
        makeConfig(1),
        .restart,
        .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.durableStateMachine(),
        },
    ));
}

test "raftor: durable apply syncs commit-only Ready before apply" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedCommitOnlyStorage(&storage);
    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.durableStateMachine(),
    });
    defer r.destroy();
    try stageCommitOnlyReady(r);

    while (r.getReadyPhase() != raft.ReadyPhase.sync) {
        try std.testing.expect(try r.processReadyStep());
    }
    failing_storage.fail_sync = true;
    try std.testing.expectError(error.WalSyncFailed, r.processReadyStep());
    try std.testing.expectEqual(@as(u64, 0), machine.durable_applied.index);

    failing_storage.fail_sync = false;
    while (r.getReadyPhase() != null) try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(@as(usize, 1), failing_storage.successful_syncs);
    try std.testing.expectEqual(raft.DurableApplied{ .index = 1, .term = 1 }, machine.durable_applied);
}

test "raftor: non-durable commit-only Ready does not add a sync" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try seedCommitOnlyStorage(&storage);
    var failing_storage = SyncFailingStorage{
        .inner = storage.asWritableStorage(),
        .fail_sync = true,
    };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try stageCommitOnlyReady(r);

    try processOneReady(r);
    try std.testing.expectEqual(@as(usize, 0), failing_storage.successful_syncs);
    try std.testing.expectEqual(@as(u64, 1), machine.last_applied_index);
}

test "raftor: configured filesystem is used for WAL storage" {
    var fixture = try raft.FsTestFixture.init(allocator, .real);
    defer fixture.deinit();
    var backend = fault.FaultFs.init(fixture.fs());
    backend.inject(.{ .operation = .make_dir, .occurrence = 1, .effect = .fail_before });
    var config = makeConfig(1);
    config.data_dir = fixture.walDir();
    config.file_system = backend.fs();
    var sm = MockStateMachine.init(allocator);
    defer sm.deinit();

    try std.testing.expectError(
        error.WalCreateDirectoryFailed,
        Raftor.create(allocator, config, sm.stateMachine()),
    );
    try backend.assertConsumed();
}

test "raftor: WAL restart restores snapshot before replaying its suffix" {
    var fixture = try raft.FsTestFixture.init(allocator, .real);
    defer fixture.deinit();
    var config = makeConfig(1);
    config.data_dir = fixture.walDir();
    config.file_system = fixture.fs();
    config.snapshot_entries_threshold = 0;

    var snapshot_index: u64 = 0;
    var first_incarnation: u64 = 0;
    {
        var machine = DurableStateMachine.init(allocator);
        defer machine.deinit();
        const r = try Raftor.create(allocator, config, machine.stateMachine());
        defer r.destroy();
        first_incarnation = r.getStatus().incarnation;
        try std.testing.expectEqual(@as(u64, 1), first_incarnation);
        try r.campaign();

        var alpha = ProposalTester{};
        try r.propose("alpha", alpha.callback());
        for (0..16) |_| _ = try r.tick();
        try std.testing.expect(alpha.applied);
        try r.takeSnapshot();
        snapshot_index = r.getStatus().applied_index;
        try std.testing.expectEqualStrings("alpha", machine.state.items);

        var beta = ProposalTester{};
        try r.propose("beta", beta.callback());
        for (0..16) |_| _ = try r.tick();
        try std.testing.expect(beta.applied);
        try std.testing.expectEqualStrings("alphabeta", machine.state.items);
        try std.testing.expect(r.getStatus().applied_index > snapshot_index);
    }

    {
        var failing_machine = DurableStateMachine.init(allocator);
        defer failing_machine.deinit();
        failing_machine.fail_restore = true;
        try std.testing.expectError(error.OutOfMemory, Raftor.create(allocator, config, failing_machine.stateMachine()));
        try std.testing.expectEqual(@as(usize, 0), failing_machine.state.items.len);
    }

    var restored_incarnation: u64 = 0;
    {
        var restored_machine = DurableStateMachine.init(allocator);
        defer restored_machine.deinit();
        config.raft.applied = std.math.maxInt(u64);
        const r = try Raftor.create(allocator, config, restored_machine.stateMachine());
        defer r.destroy();
        restored_incarnation = r.getStatus().incarnation;
        try std.testing.expectEqual(first_incarnation + 2, restored_incarnation);
        try std.testing.expectEqual(@as(usize, 1), restored_machine.restore_count);
        try std.testing.expectEqual(snapshot_index, r.getStatus().applied_index);
        try std.testing.expectEqualStrings("alpha", restored_machine.state.items);

        for (0..16) |_| _ = try r.tick();
        try std.testing.expectEqualStrings("alphabeta", restored_machine.state.items);
        try std.testing.expect(r.getStatus().applied_index > snapshot_index);

        var gamma = ProposalTester{};
        try r.campaign();
        try r.propose("gamma", gamma.callback());
        for (0..16) |_| _ = try r.tick();
        try std.testing.expect(gamma.applied);
    }

    {
        var storage = try raft.WALStorage.openWithFs(allocator, fixture.walDir(), fixture.fs());
        defer storage.deinit();
        const iface = storage.asWritableStorage();
        const first = try iface.firstIndex();
        const last = try iface.lastIndex();
        const entries = try iface.entries(allocator, first, last + 1, null, .{ .empty = .{ .can_async = false } });
        defer {
            for (entries) |*entry| entry.deinit(allocator);
            allocator.free(entries);
        }
        var saw_beta = false;
        var saw_gamma = false;
        for (entries) |entry| {
            const header = raft.request_context.decode(entry.context) orelse continue;
            if (std.mem.eql(u8, entry.data, "beta")) {
                saw_beta = true;
                try std.testing.expectEqual(first_incarnation, header.incarnation);
            }
            if (std.mem.eql(u8, entry.data, "gamma")) {
                saw_gamma = true;
                try std.testing.expectEqual(restored_incarnation, header.incarnation);
            }
            try std.testing.expectEqual(@as(u64, 1), header.node_id);
            try std.testing.expectEqual(raft.request_context.Kind.proposal, header.kind);
        }
        try std.testing.expect(saw_beta);
        try std.testing.expect(saw_gamma);
    }
}

test "raftor: durable membership add syncs before transport add" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{1}) }, &peers, &.{}, 0, .{});

    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    transport.sync_counter = &failing_storage.successful_syncs;
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();
    transport.clear();
    const syncs_before = failing_storage.successful_syncs;

    try r.addNode(2, "node-2");
    try std.testing.expectEqual(@as(usize, 0), transport.events.items.len);
    for (0..16) |_| _ = try r.tick();

    try std.testing.expectEqual(@as(usize, 1), transport.events.items.len);
    const event = transport.events.items[0];
    try std.testing.expectEqual(RecordingTransport.EventKind.add, event.kind);
    try std.testing.expectEqual(@as(u64, 2), event.node_id);
    try std.testing.expectEqualStrings("node-2", event.addressSlice());
    try std.testing.expect(event.successful_syncs > syncs_before);
    try std.testing.expectEqual(r.getMembershipIndex(), storage.core.raft_state.membership_index);
    try std.testing.expectEqualStrings("node-2", r.getClusterMembership().?.addressOf(2).?);
}

test "raftor: membership sync failure leaves runtime and transport unchanged" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{1}) }, &peers, &.{}, 0, .{});

    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();
    transport.clear();
    try r.addNode(2, "node-2");

    try std.testing.expect(try r.processReadyStep());
    while (r.getReadyPhase() != raft.ReadyPhase.apply_advanced_committed) {
        try std.testing.expect(try r.processReadyStep());
    }
    failing_storage.fail_sync = true;
    try std.testing.expectError(error.WalSyncFailed, r.processReadyStep());
    try std.testing.expectEqual(@as(u64, 0), r.getMembershipIndex());
    try std.testing.expect(r.getClusterMembership().?.addressOf(2) == null);
    try std.testing.expectEqual(@as(usize, 0), transport.events.items.len);
}

test "raftor: durable transport failure is terminal after membership install" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{1}) }, &peers, &.{}, 0, .{});
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();
    transport.fail_add = true;

    try r.addNode(2, "node-2");
    try std.testing.expectError(error.ConnectionClosed, r.tick());
    try std.testing.expectEqualStrings("node-2", r.getClusterMembership().?.addressOf(2).?);
    try std.testing.expectEqual(r.getMembershipIndex(), storage.core.raft_state.membership_index);
    try std.testing.expectError(error.ConnectionClosed, r.tick());
}

test "raftor: joint removal retains transport endpoint until leave joint" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2") },
    };
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{ 1, 2 }) }, &peers, &.{}, 0, .{});
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    transport.clear();

    var remove = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 2 }};
    try stageCommittedConfChange(r, 3, 1, .{ .transition = .explicit, .changes = &remove });
    try processOneReady(r);
    try std.testing.expectEqualStrings("node-2", r.getClusterMembership().?.addressOf(2).?);
    try std.testing.expectEqual(@as(usize, 0), transport.events.items.len);

    try stageCommittedConfChange(r, 3, 2, .{});
    try processOneReady(r);
    try std.testing.expect(r.getClusterMembership().?.addressOf(2) == null);
    try std.testing.expectEqual(@as(usize, 1), transport.events.items.len);
    try std.testing.expectEqual(RecordingTransport.EventKind.remove, transport.events.items[0].kind);
    try std.testing.expectEqual(@as(u64, 2), transport.events.items[0].node_id);
}

test "raftor: membership address update removes then adds transport peer" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2-old") },
    };
    try seedMembership(&storage, .{
        .voters = @constCast(&[_]u64{1}),
        .learners = @constCast(&[_]u64{2}),
    }, &peers, &.{}, 0, .{});
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();
    transport.clear();

    try r.updateNodeAddress(2, "node-2-new");
    try std.testing.expectEqual(@as(usize, 0), transport.events.items.len);
    const unstable = r.getRawNode().raftConst().raft_log.unstable.entries.items;
    var cc = try raft.core.util.decodeConfChangeV2(allocator, unstable[unstable.len - 1].data);
    defer cc.deinit(allocator);
    var context = try raft.decodeMembershipContext(allocator, cc.context);
    defer context.deinit(allocator);
    try std.testing.expectEqualStrings("node-2-new", context.endpoints[0].address);
    for (0..16) |_| _ = try r.tick();

    try std.testing.expectEqual(@as(usize, 2), transport.events.items.len);
    try std.testing.expectEqual(RecordingTransport.EventKind.remove, transport.events.items[0].kind);
    try std.testing.expectEqual(RecordingTransport.EventKind.add, transport.events.items[1].kind);
    try std.testing.expectEqualStrings("node-2-new", transport.events.items[1].addressSlice());
    try std.testing.expectEqualStrings("node-2-new", r.getClusterMembership().?.addressOf(2).?);
}

test "raftor: membership index skips already durable joint entries on restart" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    try seedMembership(
        &storage,
        .{ .voters = @constCast(&[_]u64{1}) },
        &peers,
        @constCast(&[_]u64{2}),
        2,
        .{ .term = 3, .commit = 2 },
    );
    var remove = [_]raft.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 2 }};
    var entries = [_]raft.Entry{
        .{
            .entry_type = .conf_change_v2,
            .term = 3,
            .index = 1,
            .data = try raft.core.util.encodeConfChangeV2(allocator, .{ .transition = .explicit, .changes = &remove }),
        },
        .{
            .entry_type = .conf_change_v2,
            .term = 3,
            .index = 2,
            .data = try raft.core.util.encodeConfChangeV2(allocator, .{}),
        },
    };
    defer for (&entries) |*entry| entry.deinit(allocator);
    try storage.setEntries(allocator, &entries);

    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try processOneReady(r);
    try std.testing.expectEqual(@as(u64, 2), r.getStatus().applied_index);
    try std.testing.expectEqual(@as(u64, 2), r.getMembershipIndex());
    try std.testing.expectEqual(@as(usize, 0), transport.events.items.len);
}

test "raftor: incoming snapshot syncs before membership reconcile" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var initial_peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{1}) }, &initial_peers, &.{}, 0, .{});
    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    transport.sync_counter = &failing_storage.successful_syncs;
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    transport.clear();
    failing_storage.successful_syncs = 0;

    var snapshot_peers = [_]raft.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2") },
    };
    const membership = try (raft.ClusterMembership{
        .cluster_id = .{1} ++ .{0} ** 15,
        .peers = &snapshot_peers,
    }).encode(allocator);
    const voters = try allocator.dupe(u64, &.{1});
    const learners = try allocator.dupe(u64, &.{2});
    try r.getRawNode().step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 4,
        .snapshot = .{
            .membership = membership,
            .data = try allocator.dupe(u8, "snapshot"),
            .metadata = .{
                .index = 10,
                .term = 4,
                .conf_state = .{ .voters = voters, .learners = learners },
            },
        },
    });

    try std.testing.expect(try r.processReadyStep());
    while (r.getReadyPhase() != raft.ReadyPhase.sync) try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(@as(usize, 0), transport.events.items.len);
    try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(@as(usize, 1), transport.events.items.len);
    try std.testing.expectEqual(@as(usize, 1), transport.events.items[0].successful_syncs);
    try std.testing.expectEqual(@as(usize, 0), machine.restore_count);
    while (r.getReadyPhase() != null) try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(@as(usize, 1), machine.restore_count);
    try std.testing.expectEqual(@as(u64, 10), r.getMembershipIndex());
}

test "raftor: incoming snapshot requires membership after migration" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{1}) }, &peers, &.{}, 3, .{});
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = DurableStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try r.getRawNode().step(.{
        .msg_type = .snapshot,
        .from = 2,
        .to = 1,
        .term = 4,
        .snapshot = .{
            .data = try allocator.dupe(u8, "snapshot"),
            .metadata = .{
                .index = 10,
                .term = 4,
                .conf_state = .{ .voters = try allocator.dupe(u64, &.{1}) },
            },
        },
    });

    try std.testing.expect(try r.processReadyStep());
    try std.testing.expect(try r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.persist_snapshot, r.getReadyPhase().?);
    try std.testing.expectError(error.MissingClusterMembership, r.processReadyStep());
    try std.testing.expectEqual(@as(u64, 3), r.getMembershipIndex());
    try std.testing.expectEqual(@as(usize, 0), machine.restore_count);
}

test "raftor: local snapshot injects membership and restores it on restart" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    try seedMembership(
        &storage,
        .{ .voters = @constCast(&[_]u64{1}) },
        &peers,
        &.{},
        0,
        .{ .term = 1, .commit = 1 },
    );
    try storage.setEntries(allocator, &.{.{ .term = 1, .index = 1 }});
    var config = makeConfig(1);
    config.raft.applied = 1;

    var first_transport = RecordingTransport.init(allocator);
    defer first_transport.deinit();
    var first_machine = DurableStateMachine.init(allocator);
    defer first_machine.deinit();
    {
        const r = try Raftor.createWithDependencies(allocator, config, .restart, .{
            .storage = storage.asWritableStorage(),
            .transport = first_transport.transport(),
            .state_machine = first_machine.stateMachine(),
        });
        defer r.destroy();
        try r.takeSnapshot();
    }

    var snapshot = (try storage.localSnapshot(allocator)).?;
    defer snapshot.deinit(allocator);
    try std.testing.expect(snapshot.membership.len > 0);
    var decoded = try raft.decodeClusterMembership(allocator, snapshot.membership);
    defer decoded.deinit(allocator);
    try std.testing.expectEqualStrings("node-1", decoded.addressOf(1).?);

    var second_transport = RecordingTransport.init(allocator);
    defer second_transport.deinit();
    var second_machine = DurableStateMachine.init(allocator);
    defer second_machine.deinit();
    const restarted = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = second_transport.transport(),
        .state_machine = second_machine.stateMachine(),
    });
    defer restarted.destroy();
    try std.testing.expectEqual(@as(u64, 1), restarted.getMembershipIndex());
    try std.testing.expectEqualStrings("node-1", restarted.getClusterMembership().?.addressOf(1).?);
}

test "raftor: restart hydrates persisted nonlocal transport peers" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2") },
    };
    try seedMembership(&storage, .{
        .voters = @constCast(&[_]u64{1}),
        .learners = @constCast(&[_]u64{2}),
    }, &peers, &.{}, 7, .{});
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();

    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try std.testing.expectEqual(@as(usize, 1), transport.events.items.len);
    try std.testing.expectEqual(RecordingTransport.EventKind.add, transport.events.items[0].kind);
    try std.testing.expectEqual(@as(u64, 2), transport.events.items[0].node_id);
    try std.testing.expectEqualStrings("node-2", transport.events.items[0].addressSlice());
    try std.testing.expectEqual(@as(usize, 1), transport.start_count);
    try std.testing.expectEqual(@as(u64, 7), r.getMembershipIndex());
}

test "raftor: durable bootstrap persists sorted membership and validates restart cluster" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const dependencies = raft.RaftorDependencies{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    };
    const peers = [_]raft.Peer{
        .{ .id = 2, .context = "node-2" },
        .{ .id = 1, .context = "node-1" },
    };
    var config = makeDurableConfig(1, "ignored-local-address");
    config.initial_peers = &peers;

    {
        const r = try Raftor.createWithDependencies(allocator, config, .bootstrap, dependencies);
        defer r.destroy();
        try std.testing.expectEqualStrings("node-1", r.getClusterMembership().?.addressOf(1).?);
        try std.testing.expectEqualStrings("node-2", r.getClusterMembership().?.addressOf(2).?);
    }

    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, state.conf_state.voters);
    try std.testing.expect(state.cluster_membership != null);
    try std.testing.expectEqual(durable_cluster_id, state.cluster_membership.?.cluster_id);
    try std.testing.expectEqual(@as(u64, 0), state.membership_index);

    {
        const restarted = try Raftor.createWithDependencies(allocator, config, .restart, dependencies);
        defer restarted.destroy();
        try std.testing.expectEqualStrings("node-2", restarted.getClusterMembership().?.addressOf(2).?);
    }

    var wrong_config = config;
    wrong_config.cluster_id = .{2} ++ .{0} ** 15;
    try std.testing.expectError(
        error.ClusterIdMismatch,
        Raftor.createWithDependencies(allocator, wrong_config, .restart, dependencies),
    );
}

test "raftor: fresh modes reject nonzero durable cursors before storage mutation" {
    const bootstrap_peers = [_]raft.Peer{.{ .id = 1, .context = "node-1" }};
    const join_peers = [_]raft.Peer{.{ .id = 2, .context = "node-2" }};
    const cursors = [_]raft.DurableApplied{
        .{ .index = 1, .term = 1 },
        .{ .index = 1, .term = 0 },
        .{ .index = 0, .term = 1 },
    };

    for ([_]raft.StartupMode{ .bootstrap, .join }) |startup_mode| {
        for (cursors) |cursor| {
            var storage = raft.MemoryStorage.init();
            defer storage.deinit(allocator);
            var transport = raft.NoopTransport.init(allocator);
            defer transport.deinit();
            var machine = DurableStateMachine.init(allocator);
            defer machine.deinit();
            machine.durable_applied = cursor;
            var config = makeDurableConfig(1, "node-1");
            config.initial_peers = if (startup_mode == .bootstrap) &bootstrap_peers else &join_peers;

            try std.testing.expectError(error.IncompatibleStorage, Raftor.createWithDependencies(
                allocator,
                config,
                startup_mode,
                .{
                    .storage = storage.asWritableStorage(),
                    .transport = transport.transport(),
                    .state_machine = machine.durableStateMachine(),
                },
            ));
            try expectUnmodifiedFreshStorage(&storage);
        }
    }
}

test "raftor: fresh modes accept zero durable cursor" {
    const bootstrap_peers = [_]raft.Peer{.{ .id = 1, .context = "node-1" }};
    const join_peers = [_]raft.Peer{.{ .id = 2, .context = "node-2" }};

    for ([_]raft.StartupMode{ .bootstrap, .join }) |startup_mode| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        var transport = raft.NoopTransport.init(allocator);
        defer transport.deinit();
        var machine = DurableStateMachine.init(allocator);
        defer machine.deinit();
        var config = makeDurableConfig(1, "node-1");
        config.initial_peers = if (startup_mode == .bootstrap) &bootstrap_peers else &join_peers;

        const r = try Raftor.createWithDependencies(allocator, config, startup_mode, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.durableStateMachine(),
        });
        defer r.destroy();
        try std.testing.expect(r.getClusterMembership() != null);
    }
}

test "raftor: transport identity mismatch fails before transport start" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const mismatched_identity = raft.TransportIdentity{
        .cluster_id = .{9} ** 16,
        .node_id = 1,
    };

    try std.testing.expectError(
        error.TransportIdentityMismatch,
        Raftor.createWithDependencies(allocator, makeDurableConfig(1, "node-1"), .bootstrap, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transportWithIdentity(mismatched_identity),
            .state_machine = machine.stateMachine(),
        }),
    );
    try std.testing.expectEqual(@as(usize, 0), transport.start_count);
}

test "raftor: durable bootstrap validates peer addresses and IDs" {
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();

    const missing_address = [_]raft.Peer{.{ .id = 1 }};
    var missing_config = makeDurableConfig(1, "node-1");
    missing_config.initial_peers = &missing_address;
    var missing_storage = raft.MemoryStorage.init();
    defer missing_storage.deinit(allocator);
    try std.testing.expectError(error.PeerAddressMissing, Raftor.createWithDependencies(
        allocator,
        missing_config,
        .bootstrap,
        .{
            .storage = missing_storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        },
    ));

    const duplicate_peers = [_]raft.Peer{
        .{ .id = 1, .context = "node-1-a" },
        .{ .id = 1, .context = "node-1-b" },
    };
    var duplicate_config = makeDurableConfig(1, "node-1");
    duplicate_config.initial_peers = &duplicate_peers;
    var duplicate_storage = raft.MemoryStorage.init();
    defer duplicate_storage.deinit(allocator);
    try std.testing.expectError(error.DuplicatePeerId, Raftor.createWithDependencies(
        allocator,
        duplicate_config,
        .bootstrap,
        .{
            .storage = duplicate_storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        },
    ));
}

test "raftor: durable restart rejects retired local node and legacy membership" {
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();

    var retired_storage = raft.MemoryStorage.init();
    defer retired_storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{.{ .node_id = 2, .address = @constCast("node-2") }};
    try seedMembership(
        &retired_storage,
        .{ .voters = @constCast(&[_]u64{2}) },
        &peers,
        @constCast(&[_]u64{1}),
        4,
        .{},
    );
    try std.testing.expectError(error.NodeRetired, Raftor.createWithDependencies(
        allocator,
        makeDurableConfig(1, "node-1"),
        .restart,
        .{
            .storage = retired_storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        },
    ));

    var legacy_storage = raft.MemoryStorage.init();
    defer legacy_storage.deinit(allocator);
    try legacy_storage.setConfState(allocator, .{ .voters = @constCast(&[_]u64{1}) });
    try std.testing.expectError(error.LegacyMembershipMigrationRequired, Raftor.createWithDependencies(
        allocator,
        makeDurableConfig(1, "node-1"),
        .restart,
        .{
            .storage = legacy_storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        },
    ));
}

test "raftor: explicit legacy membership migration hydrates and rejects stale config" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try storage.setConfState(allocator, .{ .voters = @constCast(&[_]u64{ 1, 2 }) });
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const peers = [_]raft.Peer{
        .{ .id = 2, .context = "node-2" },
        .{ .id = 1, .context = "node-1" },
    };
    var config = makeDurableConfig(1, "unused");
    config.legacy_membership_migration = .{
        .peers = &peers,
        .retired_node_ids = &.{ 4, 3 },
        .membership_index = 0,
    };
    const dependencies = raft.RaftorDependencies{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    };

    {
        const migrated = try Raftor.createWithDependencies(allocator, config, .restart, dependencies);
        defer migrated.destroy();
        try std.testing.expectEqualStrings("node-2", migrated.getClusterMembership().?.addressOf(2).?);
        try std.testing.expectEqualSlices(u64, &.{ 3, 4 }, migrated.getClusterMembership().?.retired_node_ids);
        try std.testing.expectEqual(@as(usize, 1), transport.events.items.len);
        try std.testing.expectEqual(@as(u64, 2), transport.events.items[0].node_id);
    }

    try std.testing.expectError(
        error.InvalidConfig,
        Raftor.createWithDependencies(allocator, config, .restart, dependencies),
    );
}

test "raftor: legacy snapshot migration requires explicit historical membership" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try storage.append(allocator, &.{.{ .index = 1, .term = 1 }});
    try storage.applyLocalSnapshot(allocator, .{
        .data = @constCast("legacy-state"),
        .metadata = .{
            .index = 1,
            .term = 1,
            .conf_state = .{ .voters = @constCast(&[_]u64{1}) },
        },
    });
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const peers = [_]raft.Peer{.{ .id = 1, .context = "node-1" }};
    var config = makeDurableConfig(1, "node-1");
    config.legacy_membership_migration = .{ .peers = &peers, .membership_index = 1 };

    try std.testing.expectError(
        error.LegacySnapshotMigrationRequired,
        Raftor.createWithDependencies(allocator, config, .restart, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        }),
    );
    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expect(state.cluster_membership == null);
    var snapshot = (try storage.localSnapshot(allocator)).?;
    defer snapshot.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), snapshot.membership.len);

    config.legacy_membership_migration.?.snapshot = .{ .peers = &peers };
    const migrated = try Raftor.createWithDependencies(allocator, config, .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer migrated.destroy();
    try std.testing.expectEqual(@as(u64, 1), migrated.getMembershipIndex());
    try std.testing.expectEqualStrings("node-1", migrated.getClusterMembership().?.addressOf(1).?);
}

test "raftor: fresh join persists seed membership and restarts non-promotable" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const seeds = [_]raft.Peer{
        .{ .id = 2, .context = "seed-2" },
        .{ .id = 1, .context = "seed-1" },
    };
    var config = makeDurableConfig(3, "join-3");
    config.join = true;
    config.initial_peers = &seeds;
    const dependencies = raft.RaftorDependencies{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    };

    {
        const joining = try Raftor.createWithDependencies(allocator, config, .join, dependencies);
        defer joining.destroy();
        try std.testing.expect(!joining.getRawNode().raftConst().promotable);
        try std.testing.expect(joining.getClusterMembership().?.addressOf(3) == null);
    }
    var state = try storage.initialState(allocator);
    defer state.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, state.conf_state.voters);
    try std.testing.expectEqualStrings("seed-1", state.cluster_membership.?.addressOf(1).?);
    try std.testing.expectEqual(@as(u64, 0), state.membership_index);

    const restarted = try Raftor.createWithDependencies(allocator, config, .restart, dependencies);
    defer restarted.destroy();
    try std.testing.expect(!restarted.getRawNode().raftConst().promotable);
    try std.testing.expect(restarted.getClusterMembership().?.addressOf(3) == null);
}

test "raftor: join snapshot installs local membership and survives restart" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var first_transport = RecordingTransport.init(allocator);
    defer first_transport.deinit();
    var first_machine = MockStateMachine.init(allocator);
    defer first_machine.deinit();
    const seeds = [_]raft.Peer{.{ .id = 1, .context = "seed-1" }};
    var config = makeDurableConfig(3, "join-3");
    config.initial_peers = &seeds;
    const joining = try Raftor.createWithDependencies(allocator, config, .join, .{
        .storage = storage.asWritableStorage(),
        .transport = first_transport.transport(),
        .state_machine = first_machine.stateMachine(),
    });
    try std.testing.expect(!joining.getRawNode().raftConst().promotable);

    var snapshot_peers = [_]raft.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("seed-1") },
        .{ .node_id = 3, .address = @constCast("join-3") },
    };
    const encoded_membership = try (raft.ClusterMembership{
        .cluster_id = durable_cluster_id,
        .peers = &snapshot_peers,
    }).encode(allocator);
    try joining.getRawNode().step(.{
        .msg_type = .snapshot,
        .from = 1,
        .to = 3,
        .term = 2,
        .snapshot = .{
            .membership = encoded_membership,
            .data = try allocator.dupe(u8, "joined"),
            .metadata = .{
                .index = 10,
                .term = 2,
                .conf_state = .{ .voters = try allocator.dupe(u64, &.{ 1, 3 }) },
            },
        },
    });
    try processOneReady(joining);
    try std.testing.expect(joining.getRawNode().raftConst().promotable);
    try std.testing.expectEqualStrings("join-3", joining.getClusterMembership().?.addressOf(3).?);
    try std.testing.expectEqual(@as(u64, 10), joining.getMembershipIndex());
    joining.destroy();

    var second_transport = RecordingTransport.init(allocator);
    defer second_transport.deinit();
    var second_machine = MockStateMachine.init(allocator);
    defer second_machine.deinit();
    const restarted = try Raftor.createWithDependencies(allocator, config, .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = second_transport.transport(),
        .state_machine = second_machine.stateMachine(),
    });
    defer restarted.destroy();
    try std.testing.expect(restarted.getRawNode().raftConst().promotable);
    try std.testing.expectEqualStrings("join-3", restarted.getClusterMembership().?.addressOf(3).?);
    try std.testing.expectEqual(@as(u64, 10), restarted.getMembershipIndex());
}

test "raftor: durable learner proposal uses RMC1 and reconciles only after commit" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeDurableConfig(1, "node-1"), .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();
    transport.clear();

    try r.addLearner(2, "node-2");
    try std.testing.expectEqual(@as(usize, 0), transport.events.items.len);
    const unstable = r.getRawNode().raftConst().raft_log.unstable.entries.items;
    var cc = try raft.core.util.decodeConfChangeV2(allocator, unstable[unstable.len - 1].data);
    defer cc.deinit(allocator);
    try std.testing.expectEqual(raft.ConfChangeType.add_learner_node, cc.changes[0].change_type);
    var context = try raft.decodeMembershipContext(allocator, cc.context);
    defer context.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 2), context.endpoints[0].node_id);
    try std.testing.expectEqualStrings("node-2", context.endpoints[0].address);

    for (0..16) |_| _ = try r.tick();
    try std.testing.expectEqual(@as(usize, 1), transport.events.items.len);
    try std.testing.expectEqualStrings("node-2", r.getClusterMembership().?.addressOf(2).?);

    try r.addNode(2, "node-2");
    const promotion_entries = r.getRawNode().raftConst().raft_log.unstable.entries.items;
    var promotion = try raft.core.util.decodeConfChangeV2(allocator, promotion_entries[promotion_entries.len - 1].data);
    defer promotion.deinit(allocator);
    var promotion_context = try raft.decodeMembershipContext(allocator, promotion.context);
    defer promotion_context.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), promotion_context.endpoints.len);
}

test "raftor: durable membership APIs reject retired IDs before proposal" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{1}) }, &peers, @constCast(&[_]u64{2}), 0, .{});
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeDurableConfig(1, "node-1"), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();

    try std.testing.expectError(error.NodeRetired, r.addNode(2, "node-2"));
    try std.testing.expectError(error.NodeRetired, r.addLearner(2, "node-2"));
    try std.testing.expectError(error.NodeRetired, r.updateNodeAddress(2, "node-2"));
    try std.testing.expectError(error.NodeRetired, r.removeNode(2));
    try std.testing.expectEqual(@as(usize, 0), r.getRawNode().raftConst().raft_log.unstable.entries.items.len);
}

test "raftor: auto-detects fresh join from config" {
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const seeds = [_]raft.Peer{.{ .id = 1, .context = "seed-1" }};
    var config = makeDurableConfig(3, "join-3");
    config.join = true;
    config.initial_peers = &seeds;

    const r = try Raftor.create(allocator, config, machine.stateMachine());
    defer r.destroy();
    try std.testing.expect(!r.getRawNode().raftConst().promotable);
    try std.testing.expectEqualStrings("seed-1", r.getClusterMembership().?.addressOf(1).?);
    try std.testing.expect(r.getClusterMembership().?.addressOf(3) == null);
}

test "raftor: legacy add mutates transport only after commit" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();
    transport.clear();

    try r.addNode(2, "node-2");
    try std.testing.expectEqual(@as(usize, 0), transport.events.items.len);
    for (0..16) |_| _ = try r.tick();
    try std.testing.expectEqual(@as(usize, 1), transport.events.items.len);
    try std.testing.expectEqualStrings("node-2", transport.events.items[0].addressSlice());
}

test "raftor: checksum mismatch is terminal" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    var config = makeConfig(1);
    config.checksum_enabled = true;
    const r = try Raftor.createWithDependencies(allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    try r.campaign();

    try r.getRawNode().propose("corrupt-context", "corrupt");
    const unstable = r.getRawNode().raftPtr().raft_log.unstable.entries.items;
    unstable[unstable.len - 1].checksum = 1;

    try std.testing.expectError(error.ChecksumMismatch, r.run());
    try std.testing.expectError(error.ChecksumMismatch, r.run());
}

test "raftor: incoming snapshot rejects malformed and inconsistent membership" {
    const Case = struct {
        membership: []const u8,
        expected: raft.Error,
    };
    const cases = [_]Case{
        .{ .membership = "not-a-membership", .expected = error.InvalidClusterMembership },
        .{ .membership = "valid-but-inconsistent", .expected = error.InvalidClusterMembership },
    };

    for (cases, 0..) |case, case_index| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        var peers = [_]raft.PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
        try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{1}) }, &peers, &.{}, 0, .{});
        var transport = RecordingTransport.init(allocator);
        defer transport.deinit();
        var machine = DurableStateMachine.init(allocator);
        defer machine.deinit();
        const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        });
        defer r.destroy();

        const membership = if (case_index == 0)
            try allocator.dupe(u8, case.membership)
        else blk: {
            var snapshot_peers = [_]raft.PeerEndpoint{
                .{ .node_id = 1, .address = @constCast("node-1") },
                .{ .node_id = 2, .address = @constCast("node-2") },
            };
            break :blk try (raft.ClusterMembership{
                .cluster_id = durable_cluster_id,
                .peers = &snapshot_peers,
            }).encode(allocator);
        };
        try r.getRawNode().step(.{
            .msg_type = .snapshot,
            .from = 2,
            .to = 1,
            .term = 4,
            .snapshot = .{
                .membership = membership,
                .data = try allocator.dupe(u8, "snapshot"),
                .metadata = .{
                    .index = 10,
                    .term = 4,
                    .conf_state = .{ .voters = try allocator.dupe(u64, &.{1}) },
                },
            },
        });
        try std.testing.expect(try r.processReadyStep());
        try std.testing.expect(try r.processReadyStep());
        try std.testing.expectError(case.expected, r.processReadyStep());
        try std.testing.expectEqual(@as(usize, 0), machine.restore_count);
    }
}

test "raftor: legacy transport add and remove failures become terminal after Ready" {
    const Change = struct {
        change_type: raft.ConfChangeType,
        fail_add: bool,
        fail_remove: bool,
    };
    const cases = [_]Change{
        .{ .change_type = .add_node, .fail_add = true, .fail_remove = false },
        .{ .change_type = .remove_node, .fail_add = false, .fail_remove = true },
    };
    for (cases) |case| {
        var storage = raft.MemoryStorage.init();
        defer storage.deinit(allocator);
        try storage.setConfState(allocator, .{ .voters = @constCast(&[_]u64{ 1, 2 }) });
        var transport = RecordingTransport.init(allocator);
        defer transport.deinit();
        transport.fail_add = case.fail_add;
        transport.fail_remove = case.fail_remove;
        var machine = MockStateMachine.init(allocator);
        defer machine.deinit();
        const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        });
        defer r.destroy();

        var change = [_]raft.ConfChangeSingle{.{ .change_type = case.change_type, .node_id = 2 }};
        try stageCommittedConfChange(r, 3, 1, .{ .changes = &change, .context = @constCast("node-2") });
        var terminal_error: ?raft.Error = null;
        for (0..32) |_| {
            if (r.processReadyStep()) |_| {} else |err| {
                terminal_error = err;
                break;
            }
        }
        try std.testing.expectEqual(error.ConnectionClosed, terminal_error.?);
        try std.testing.expectError(error.ConnectionClosed, r.processReadyStep());
    }
}

test "raftor: run rejects a concurrent runner and exits cleanly on callback stop" {
    const thread_allocator = std.heap.smp_allocator;
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(thread_allocator);
    var transport = RecordingTransport.init(thread_allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(thread_allocator);
    defer machine.deinit();
    var config = makeConfig(1);
    config.tick_interval_ms = 1;
    const r = try Raftor.createWithDependencies(thread_allocator, config, .bootstrap, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    const StopGate = struct {
        raftor: *Raftor,
        entered: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        release: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        fn stop(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
            self.raftor.stop();
        }
    };
    var stop_gate = StopGate{ .raftor = r };
    transport.before_message_ctx = &stop_gate;
    transport.before_message = StopGate.stop;
    try transport.queueMessage(.{ .msg_type = .heartbeat, .from = 2, .to = 1 });

    const RunState = struct {
        raftor: *Raftor,
        error_value: ?raft.Error = null,
        fn run(self: *@This()) void {
            self.raftor.run() catch |err| {
                self.error_value = err;
            };
        }
    };
    var run_state = RunState{ .raftor = r };
    const thread = try std.Thread.spawn(.{}, RunState.run, .{&run_state});
    errdefer {
        stop_gate.release.store(true, .release);
        thread.join();
    }
    while (!stop_gate.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(r.isRunning());
    try std.testing.expectError(error.AlreadyStarted, r.run());
    stop_gate.release.store(true, .release);
    thread.join();
    try std.testing.expect(run_state.error_value == null);
}

test "raftor: stopped drain breaks before later queued requests" {
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    var config = makeConfig(1);
    config.raft.disable_proposal_forwarding = true;
    const r = try Raftor.create(allocator, config, machine.stateMachine());
    defer r.destroy();

    const StopCallback = struct {
        raftor: *Raftor,
        calls: usize = 0,
        fn proposal(ctx: *anyopaque, _: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            self.raftor.stop();
        }
    };
    var first = StopCallback{ .raftor = r };
    var second = ErrorTester{};
    try r.propose("first", .{ .ctx = &first, .function = StopCallback.proposal });
    try r.propose("second", second.proposalCallback());
    _ = try r.tick();
    try std.testing.expectEqual(@as(usize, 1), first.calls);
    try std.testing.expectEqual(error.ShuttingDown, second.err.?);
}

test "raftor: tracking allocation failures complete proposal and read callbacks" {
    const Kind = enum { proposal, read };
    for ([_]Kind{ .proposal, .read }) |kind| {
        var failing = std.testing.FailingAllocator.init(allocator, .{});
        const failing_allocator = failing.allocator();
        var machine = MockStateMachine.init(failing_allocator);
        defer machine.deinit();
        const r = try Raftor.create(failing_allocator, makeConfig(1), machine.stateMachine());
        defer r.destroy();
        var callback = ErrorTester{};
        switch (kind) {
            .proposal => try r.propose("queued", callback.proposalCallback()),
            .read => try r.readIndex("queued", callback.readCallback()),
        }
        failing.fail_index = failing.alloc_index;
        _ = try r.tick();
        try std.testing.expectEqual(error.OutOfMemory, callback.err.?);
        failing.fail_index = std.math.maxInt(usize);
    }
}

test "raftor: read construction failure removes the tracked callback" {
    var saw_read_failure = false;
    for (0..64) |failure_offset| {
        var failing = std.testing.FailingAllocator.init(allocator, .{});
        const failing_allocator = failing.allocator();
        var machine = MockStateMachine.init(failing_allocator);
        const r = try Raftor.create(failing_allocator, makeConfig(1), machine.stateMachine());
        var callback = ErrorTester{};
        try r.readIndex("queued", callback.readCallback());
        failing.fail_index = failing.alloc_index + failure_offset;

        if (r.tick()) |_| {} else |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            try std.testing.expectEqual(error.OutOfMemory, callback.err.?);
            saw_read_failure = true;
        }

        failing.fail_index = std.math.maxInt(usize);
        r.destroy();
        machine.deinit();
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        if (saw_read_failure) break;
    }
    try std.testing.expect(saw_read_failure);
}

test "raftor: read drain stops after tracking failure callback shuts down" {
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    const failing_allocator = failing.allocator();
    var machine = MockStateMachine.init(failing_allocator);
    defer machine.deinit();
    const r = try Raftor.create(failing_allocator, makeConfig(1), machine.stateMachine());
    defer r.destroy();

    const StopCallback = struct {
        raftor: *Raftor,
        error_value: ?raft.Error = null,
        fn read(ctx: *anyopaque, result: raft.ReadIndexResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (result == .err) self.error_value = result.err;
            self.raftor.stop();
        }
    };
    var first = StopCallback{ .raftor = r };
    var second = ErrorTester{};
    try r.readIndex("first", .{ .ctx = &first, .function = StopCallback.read });
    try r.readIndex("second", second.readCallback());
    failing.fail_index = failing.alloc_index;
    _ = try r.tick();
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(error.OutOfMemory, first.error_value.?);
    try std.testing.expectEqual(error.ShuttingDown, second.err.?);
}

test "raftor: fresh join rejects persisted state" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try storage.setHardState(.{ .term = 1 });
    var transport = RecordingTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const seeds = [_]raft.Peer{.{ .id = 2, .context = "seed-2" }};
    var config = makeDurableConfig(1, "node-1");
    config.initial_peers = &seeds;
    try std.testing.expectError(error.IncompatibleStorage, Raftor.createWithDependencies(allocator, config, .join, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    }));
}

test "raftor: durable address conflicts and remove allocation failure do not propose" {
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    const failing_allocator = failing.allocator();
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(failing_allocator);
    var peers = [_]raft.PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2") },
    };
    var retired_node_ids = [_]u64{ 3, 4 };
    try seedMembership(&storage, .{ .voters = @constCast(&[_]u64{ 1, 2 }) }, &peers, &retired_node_ids, 0, .{});
    var transport = RecordingTransport.init(failing_allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(failing_allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(failing_allocator, makeDurableConfig(1, "node-1"), .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();

    try std.testing.expectError(error.ConflictingPeerAddress, r.addNode(2, "other-address"));
    try std.testing.expectError(error.ProposalDropped, r.addNode(5, "node-5"));
    try r.campaign();
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, r.removeNode(2));
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expectError(error.ProposalDropped, r.removeNode(2));
}

test "raftor: legacy membership construction cleans up every allocation failure" {
    const peers = [_]raft.Peer{
        .{ .id = 1, .context = "node-1" },
        .{ .id = 2, .context = "node-2" },
    };
    var saw_oom = false;
    var reached_success = false;
    for (0..128) |failure_offset| {
        var failing = std.testing.FailingAllocator.init(allocator, .{});
        const failing_allocator = failing.allocator();
        var storage = raft.MemoryStorage.init();
        try storage.setConfState(failing_allocator, .{ .voters = @constCast(&[_]u64{ 1, 2 }) });
        var transport = raft.NoopTransport.init(allocator);
        defer transport.deinit();
        var machine = MockStateMachine.init(allocator);
        defer machine.deinit();
        failing.fail_index = failing.alloc_index + failure_offset;
        var config = makeDurableConfig(1, "node-1");
        config.legacy_membership_migration = .{
            .peers = &peers,
            .retired_node_ids = &.{ 3, 4 },
            .membership_index = 0,
        };
        if (Raftor.createWithDependencies(failing.allocator(), config, .restart, .{
            .storage = storage.asWritableStorage(),
            .transport = transport.transport(),
            .state_machine = machine.stateMachine(),
        })) |r| {
            r.destroy();
            reached_success = true;
        } else |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            saw_oom = true;
        }
        storage.deinit(failing_allocator);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        if (reached_success) break;
    }
    try std.testing.expect(saw_oom);
    try std.testing.expect(reached_success);
}

test "raftor: invalid retired legacy member cleans up construction" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try storage.setConfState(allocator, .{ .voters = @constCast(&[_]u64{ 1, 2 }) });
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const peers = [_]raft.Peer{
        .{ .id = 1, .context = "node-1" },
        .{ .id = 2, .context = "node-2" },
    };
    var config = makeDurableConfig(1, "node-1");
    config.legacy_membership_migration = .{
        .peers = &peers,
        .retired_node_ids = &.{ 3, 3 },
        .membership_index = 0,
    };
    try std.testing.expectError(error.InvalidClusterMembership, Raftor.createWithDependencies(allocator, config, .restart, .{
        .storage = storage.asWritableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    }));
}

test "raftor: advance allocation failure is terminal" {
    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    try storage.setConfState(allocator, .{ .voters = @constCast(&[_]u64{ 1, 2 }) });
    var failing_storage = SyncFailingStorage{ .inner = storage.asWritableStorage() };
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    const r = try Raftor.createWithDependencies(allocator, makeConfig(1), .restart, .{
        .storage = failing_storage.writableStorage(),
        .transport = transport.transport(),
        .state_machine = machine.stateMachine(),
    });
    defer r.destroy();
    const raft_state = r.getRawNode().raftPtr();
    raft_state.becomeCandidate();
    try raft_state.becomeLeader();
    try r.getRawNode().step(.{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .term = raft_state.term,
        .index = raft_state.raft_log.lastIndex(),
    });
    while (try r.processReadyStep()) {}
    try r.getRawNode().propose("context", "payload");

    try std.testing.expect(try r.processReadyStep());
    try r.getRawNode().step(.{
        .msg_type = .append_response,
        .from = 2,
        .to = 1,
        .term = raft_state.term,
        .index = raft_state.raft_log.lastIndex(),
    });
    while (r.getReadyPhase() != raft.ReadyPhase.advance) {
        try std.testing.expect(try r.processReadyStep());
    }
    failing_storage.fail_entries = true;
    try std.testing.expectError(error.OutOfMemory, r.processReadyStep());
    try std.testing.expectError(error.OutOfMemory, r.tick());
}

test "raftor: createWithTransport unwinds owned storage on initialization OOM" {
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var transport = raft.NoopTransport.init(allocator);
    defer transport.deinit();
    var machine = MockStateMachine.init(allocator);
    defer machine.deinit();
    failing.fail_index = 1;
    try std.testing.expectError(error.OutOfMemory, Raftor.createWithTransport(
        failing.allocator(),
        makeConfig(1),
        machine.stateMachine(),
        transport.transport(),
    ));
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}
