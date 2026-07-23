const std = @import("std");
const mar = @import("marionette");

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
