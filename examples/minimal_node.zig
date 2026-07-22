//! Single-node Raft demo.
//!
//! Creates a Raftor with an in-memory state machine, campaigns to become
//! leader, proposes data, and prints the applied result. Demonstrates the
//! full lifecycle: init → election → propose → commit → apply → snapshot.

const std = @import("std");
const raft = @import("raft_zig");

const CounterStateMachine = struct {
    count: u64 = 0,
    applied: std.ArrayList([]u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) CounterStateMachine {
        return .{ .applied = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *CounterStateMachine) void {
        for (self.applied.items) |d| self.allocator.free(d);
        self.applied.deinit(self.allocator);
    }

    fn applyImpl(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        _ = entry;
        const self: *CounterStateMachine = @ptrCast(@alignCast(ctx));
        self.count += 1;
        const resp = try self.allocator.dupe(u8, "ok");
        return .{ .response = resp };
    }

    fn takeSnapshotImpl(ctx: *anyopaque, allocator: std.mem.Allocator, idx: u64, term: u64, cs: raft.ConfState) raft.Error!raft.Snapshot {
        const self: *CounterStateMachine = @ptrCast(@alignCast(ctx));
        _ = self;
        return .{
            .data = try allocator.dupe(u8, ""),
            .metadata = .{ .index = idx, .term = term, .conf_state = try raft.cloneConfState(allocator, cs) },
        };
    }

    fn restoreSnapshotImpl(_: *anyopaque, _: raft.SnapshotMetadata, _: raft.SnapshotReader) raft.Error!void {}

    pub const vtable: raft.StateMachine.VTable = .{
        .apply = applyImpl,
        .take_snapshot = takeSnapshotImpl,
        .restore_snapshot = restoreSnapshotImpl,
    };

    pub fn stateMachine(self: *CounterStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var sm = CounterStateMachine.init(allocator);
    defer sm.deinit();

    // Configure single-node cluster.
    var config = raft.RaftorConfig{};
    config.raft.id = 1;
    config.raft.election_tick = 10;
    config.raft.heartbeat_tick = 1;
    config.raft.election_timeout_seed = 42;
    config.snapshot_entries_threshold = 5;

    const r = try raft.Raftor.create(allocator, config, sm.stateMachine());
    defer r.destroy();

    std.log.info("raft-zig {s}: starting node {}", .{ raft.version, config.raft.id });

    // Campaign to become leader.
    try r.campaign();
    if (r.isLeader()) {
        std.log.info("became leader at term {}", .{r.getStatus().term});
    } else {
        std.log.warn("failed to become leader", .{});
        return;
    }

    // Propose some data.
    const ProposeTester = struct {
        done: bool = false,
        fn cb(ctx: *anyopaque, result: raft.ProposalResult) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.done = switch (result) {
                .ok => |resp| blk: {
                    std.log.info("proposal applied: response={s}", .{resp});
                    break :blk true;
                },
                .err => |e| blk: {
                    std.log.warn("proposal failed: {s}", .{@errorName(e)});
                    break :blk false;
                },
            };
        }
    };
    var tester = ProposeTester{};

    try r.propose("hello world", .{ .ctx = &tester, .function = ProposeTester.cb });

    // Drive the event loop.
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        _ = try r.tick();
    }

    const status = r.getStatus();
    std.log.info(
        "final state: role={s} term={} commit={} applied={} pending={} snapshot_count={}",
        .{ raft.roleName(status.role), status.term, status.commit_index, status.applied_index, status.pending_proposals, sm.count },
    );

    if (tester.done) {
        std.log.info("demo completed successfully", .{});
    } else {
        std.log.warn("proposal did not complete in time", .{});
    }
}
