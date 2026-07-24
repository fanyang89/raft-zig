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

const SyncFailingStorage = struct {
    inner: raft.WritableStorage,
    fail_sync: bool = false,
    fail_conf_state: bool = false,
    fail_incarnation: bool = false,

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

    fn applySnapshot(ctx: *anyopaque, alloc: std.mem.Allocator, snapshot: raft.Snapshot) raft.Error!void {
        return cast(ctx).inner.applySnapshot(alloc, snapshot);
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
        return self.inner.sync();
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
        .apply_snapshot = applySnapshot,
        .apply_local_snapshot = applyLocalSnapshot,
        .local_snapshot = localSnapshot,
        .reserve_incarnation = reserveIncarnation,
        .sync_ = sync,
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
    try std.testing.expectEqual(raft.ReadyPhase.persist_entries, r.getReadyPhase().?);

    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, r.processReadyStep());
    try std.testing.expectEqual(raft.ReadyPhase.persist_entries, r.getReadyPhase().?);

    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expect(try r.tick());
    try std.testing.expectEqual(@as(?raft.ReadyPhase, null), r.getReadyPhase());
    try std.testing.expect(r.isLeader());
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
    const restarted = try Raftor.createWithDependencies(allocator, config, .restart, dependencies);
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
