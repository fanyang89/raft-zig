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
    successful_syncs: usize = 0,

    fn cast(ctx: *anyopaque) *SyncFailingStorage {
        return @ptrCast(@alignCast(ctx));
    }

    fn initialState(ctx: *anyopaque, alloc: std.mem.Allocator) raft.Error!raft.RaftState {
        return cast(ctx).inner.initialState(alloc);
    }

    fn entries(ctx: *anyopaque, alloc: std.mem.Allocator, low: u64, high: u64, max_size: ?u64, request_ctx: raft.GetEntriesContext) raft.Error![]raft.Entry {
        return cast(ctx).inner.entries(alloc, low, high, max_size, request_ctx);
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

    fn cast(ctx: *anyopaque) *FailingStateMachine {
        return @ptrCast(@alignCast(ctx));
    }

    fn apply(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self = cast(ctx);
        if (std.mem.eql(u8, entry.data, self.fail_data)) return error.OutOfMemory;
        return MockStateMachine.applyImpl(self.inner, entry);
    }

    fn takeSnapshot(ctx: *anyopaque, alloc: std.mem.Allocator, applied_index: u64, applied_term: u64, conf_state: raft.ConfState) raft.Error!raft.Snapshot {
        return MockStateMachine.takeSnapshotImpl(cast(ctx).inner, alloc, applied_index, applied_term, conf_state);
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
        self.restore_count += 1;
    }

    fn stateMachine(self: *DurableStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }

    const vtable: raft.StateMachine.VTable = .{
        .apply = apply,
        .take_snapshot = takeSnapshot,
        .restore_snapshot = restoreSnapshot,
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
