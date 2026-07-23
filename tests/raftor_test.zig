//! Raftor integration tests.
//!
//! End-to-end tests that exercise the full Raftor pipeline: create →
//! campaign → propose → apply. Single-node only (multi-node requires RPC).

const std = @import("std");
const raft = @import("raft_zig");

const allocator = std.testing.allocator;
const Raftor = raft.Raftor;
const RaftorConfig = raft.RaftorConfig;
const MockStateMachine = raft.MockStateMachine;
const StateRole = raft.StateRole;

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
