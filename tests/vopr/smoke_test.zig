const std = @import("std");
const mar = @import("marionette");
const raft = @import("raft_zig");
const raft_adapter = @import("raft_adapter.zig");

const Payload = struct {
    value: u64,
};

const App = struct {
    sim: mar.Sim,
    endpoints: [2]mar.Endpoint(Payload),
    received: ?u64 = null,
};

const Case = mar.SimCase(App);

fn processRestart(_: *anyopaque, env: mar.Env) anyerror!void {
    try env.record("raft_vopr.process restart=ok", .{});
}

fn init(sim: mar.Sim) !App {
    try sim.registerProcess(0, .{
        .ptr = sim.control.world,
        .restart = processRestart,
    });
    return .{
        .sim = sim,
        .endpoints = try sim.endpoints(Payload, 2, 0),
    };
}

fn scenario(case: *Case) !void {
    const left = [_]mar.NodeId{0};
    const right = [_]mar.NodeId{1};
    try case.control().network.partition(&left, &right);
    try case.app.endpoints[0].send(1, .{ .value = 41 });
    try std.testing.expectEqual(@as(?mar.Endpoint(Payload).Envelope, null), try case.app.endpoints[1].receive());

    try case.control().network.heal();
    try case.app.endpoints[0].send(1, .{ .value = 42 });
    const envelope = (try case.app.endpoints[1].receive()) orelse return error.MessageNotDelivered;
    case.app.received = envelope.message.value;

    try case.app.sim.killProcess(0);
    try case.app.sim.restartProcess(0);
}

fn check(case: *const Case) !void {
    try std.testing.expectEqual(@as(?u64, 42), case.app.received);
}

const checks = [_]mar.StateCheck(Case){
    .{ .name = "healed network delivers and process restarts", .check = check },
};

test "Marionette adapter boundary is deterministic" {
    try mar.expectSimPass(.{
        .allocator = std.testing.allocator,
        .seed = 0xC0FFEE,
        .tick_ns = mar.default_tick_ns,
        .simulate = mar.World.SimulateOptions{
            .network = .{
                .nodes = 2,
                .service_nodes = 2,
                .path_capacity = 8,
            },
        },
        .init = init,
        .scenario = scenario,
        .checks = &checks,
    });
}

const RaftApp = struct {
    sim: mar.Sim,
    pool: *raft_adapter.PacketPool,
    node: *raft_adapter.NodeProcess,
    restarted_term: u64 = 0,
    initial_incarnation: u64 = 0,
    restarted_incarnation: u64 = 0,

    pub fn deinit(self: *RaftApp) void {
        const allocator = self.sim.env.allocator();
        self.node.destroy();
        self.pool.deinit();
        allocator.destroy(self.pool);
    }
};

const RaftCase = mar.SimCase(RaftApp);

fn initRaft(sim: mar.Sim) !RaftApp {
    const allocator = sim.env.allocator();
    const pool = try allocator.create(raft_adapter.PacketPool);
    pool.* = raft_adapter.PacketPool.init(allocator);
    errdefer {
        pool.deinit();
        allocator.destroy(pool);
    }

    const endpoints = try sim.endpoints(raft_adapter.PacketRef, 1, 0);
    var config = raft.RaftorConfig{};
    config.raft.id = 1;
    config.raft.election_tick = 10;
    config.raft.heartbeat_tick = 1;
    config.raft.election_timeout_seed = 1;
    const node = try raft_adapter.NodeProcess.create(allocator, config, endpoints[0], pool);
    errdefer node.destroy();
    try sim.registerProcess(0, node.lifecycle());
    return .{ .sim = sim, .pool = pool, .node = node };
}

fn raftScenario(case: *RaftCase) !void {
    try case.app.node.raftor.?.campaign();
    const term = case.app.node.raftor.?.getStatus().term;
    case.app.initial_incarnation = case.app.node.raftor.?.getStatus().incarnation;
    try std.testing.expect(term > 0);
    try std.testing.expect(case.app.node.state_machine.last_applied_index > 0);

    try case.app.sim.killProcess(0);
    try std.testing.expectEqual(@as(?*raft.Raftor, null), case.app.node.raftor);
    try case.app.sim.restartProcess(0);
    case.app.restarted_term = case.app.node.raftor.?.getStatus().term;
    case.app.restarted_incarnation = case.app.node.raftor.?.getStatus().incarnation;
}

fn checkRaftRestart(case: *const RaftCase) !void {
    try std.testing.expect(case.app.node.raftor != null);
    try std.testing.expect(case.app.restarted_term > 0);
    try std.testing.expectEqual(case.app.initial_incarnation + 1, case.app.restarted_incarnation);
    try std.testing.expectEqual(
        case.app.node.state_machine.last_applied_index,
        case.app.node.raftor.?.getStatus().applied_index,
    );
}

const raft_checks = [_]mar.StateCheck(RaftCase){
    .{ .name = "Raftor restarts from durable semantic state", .check = checkRaftRestart },
};

test "Marionette Raftor process restart preserves state" {
    try mar.expectSimPass(.{
        .allocator = std.testing.allocator,
        .seed = 0x5EED,
        .tick_ns = mar.default_tick_ns,
        .simulate = mar.World.SimulateOptions{
            .network = .{
                .nodes = 1,
                .service_nodes = 1,
                .path_capacity = 8,
            },
        },
        .init = initRaft,
        .scenario = raftScenario,
        .checks = &raft_checks,
    });
}

const Receiver = struct {
    message: ?raft.Message = null,
    allocator: std.mem.Allocator,

    fn callback(ctx: *anyopaque, message: raft.Message) raft.Error!void {
        const self: *Receiver = @ptrCast(@alignCast(ctx));
        if (self.message) |*previous| previous.deinit(self.allocator);
        self.message = message;
    }

    fn deinit(self: *Receiver) void {
        if (self.message) |*message| message.deinit(self.allocator);
    }
};

const TransportApp = struct {
    sim: mar.Sim,
    pool: *raft_adapter.PacketPool,
    transports: [2]raft_adapter.SimTransport,
    receiver: *Receiver,

    pub fn deinit(self: *TransportApp) void {
        const allocator = self.sim.env.allocator();
        self.receiver.deinit();
        allocator.destroy(self.receiver);
        self.pool.deinit();
        allocator.destroy(self.pool);
    }
};

const TransportCase = mar.SimCase(TransportApp);

fn initTransport(sim: mar.Sim) !TransportApp {
    const allocator = sim.env.allocator();
    const pool = try allocator.create(raft_adapter.PacketPool);
    pool.* = raft_adapter.PacketPool.init(allocator);
    errdefer {
        pool.deinit();
        allocator.destroy(pool);
    }
    const receiver = try allocator.create(Receiver);
    receiver.* = .{ .allocator = allocator };
    errdefer allocator.destroy(receiver);
    const endpoints = try sim.endpoints(raft_adapter.PacketRef, 2, 0);
    return .{
        .sim = sim,
        .pool = pool,
        .transports = .{
            raft_adapter.SimTransport.init(endpoints[0], pool),
            raft_adapter.SimTransport.init(endpoints[1], pool),
        },
        .receiver = receiver,
    };
}

fn transportScenario(case: *TransportCase) !void {
    case.app.transports[1].transport().setMessageCallback(.{
        .ctx = case.app.receiver,
        .function = Receiver.callback,
    });
    var entries = [_]raft.Entry{.{ .data = @constCast("entry") }};
    const message = raft.Message{
        .msg_type = .append,
        .from = 1,
        .to = 2,
        .entries = &entries,
        .context = @constCast("context"),
        .snapshot = .{ .data = @constCast("snapshot") },
    };
    try case.app.transports[0].transport().send(&.{message});
    try std.testing.expect(try case.app.transports[1].transport().pollOne());

    const left = [_]mar.NodeId{0};
    const right = [_]mar.NodeId{1};
    try case.control().network.partition(&left, &right);
    try case.app.transports[0].transport().send(&.{message});
    try std.testing.expect(!(try case.app.transports[1].transport().pollOne()));
}

fn checkTransport(case: *const TransportCase) !void {
    const message = case.app.receiver.message orelse return error.MessageNotDelivered;
    try std.testing.expectEqualStrings("entry", message.entries[0].data);
    try std.testing.expectEqualStrings("context", message.context);
    try std.testing.expectEqualStrings("snapshot", message.snapshot.?.data);
}

const transport_checks = [_]mar.StateCheck(TransportCase){
    .{ .name = "PacketRef transport owns nested messages", .check = checkTransport },
};

test "Marionette PacketRef transport preserves ownership" {
    try mar.expectSimPass(.{
        .allocator = std.testing.allocator,
        .seed = 0xC10E,
        .tick_ns = mar.default_tick_ns,
        .simulate = mar.World.SimulateOptions{
            .network = .{
                .nodes = 2,
                .service_nodes = 2,
                .path_capacity = 8,
            },
        },
        .init = initTransport,
        .scenario = transportScenario,
        .checks = &transport_checks,
    });
}

test {
    _ = @import("cluster_test.zig");
    _ = @import("wal_fs_adapter.zig");
}
