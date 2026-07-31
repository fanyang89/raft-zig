const std = @import("std");

const grpc = @import("grpc_lite");
const pb = @import("database_proto");
const client_mod = @import("client.zig");
const config_mod = @import("config.zig");
const runtime_mod = @import("runtime.zig");

const runtime_allocator = std.heap.smp_allocator;

fn testOptions() runtime_mod.Options {
    return .{
        .tick_interval_ms = 10,
        .election_tick = 10,
        .heartbeat_tick = 1,
        .proposal_timeout_ticks = 200,
        .read_index_timeout_ticks = 200,
        .snapshot_entries_threshold = 2,
        .api_reactor_count = 2,
    };
}

fn waitForLeader(runtime: *runtime_mod.Runtime) !void {
    for (0..500) |_| {
        if (runtime.status().role == .leader) return;
        if (runtime.driverExited()) return error.DriverExited;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.LeaderTimeout;
}

fn endpoint(allocator: std.mem.Allocator, runtime: *runtime_mod.Runtime) ![]u8 {
    const address = try runtime.apiAddress();
    return std.fmt.allocPrint(allocator, "{s}:{}", .{ address.host, address.port });
}

test "single-node gRPC executes and linearly queries SQL" {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", runtime_allocator);
    defer runtime_allocator.free(root_path);
    const ports = try reserveUniquePorts(2);
    const api_address = try std.fmt.allocPrint(runtime_allocator, "127.0.0.1:{}", .{ports[0]});
    defer runtime_allocator.free(api_address);
    const raft_address = try std.fmt.allocPrint(runtime_allocator, "127.0.0.1:{}", .{ports[1]});
    defer runtime_allocator.free(raft_address);
    const arguments = [_][]const u8{
        "--node-id",     "1",
        "--cluster-id",  "0198f54d-5c2a-7000-8000-000000000001",
        "--api-listen",  api_address,
        "--raft-listen", raft_address,
        "--data-dir",    root_path,
    };
    var config = try config_mod.parseServer(runtime_allocator, &arguments);
    defer config.deinit();
    const runtime = try runtime_mod.Runtime.create(runtime_allocator, &config, testOptions());
    defer {
        if (runtime.running) runtime.shutdown() catch {};
        runtime.deinit();
    }
    try waitForLeader(runtime);

    const target = try endpoint(runtime_allocator, runtime);
    defer runtime_allocator.free(target);
    var client = try client_mod.Client.init(runtime_allocator, target);
    defer client.deinit();
    var arena: std.heap.ArenaAllocator = .init(runtime_allocator);
    defer arena.deinit();
    var execute_request: pb.ExecuteRequest = .{ .request_id = "grpc-execute-1" };
    try execute_request.statements.append(arena.allocator(), .{
        .sql = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL) STRICT",
    });
    var insert: pb.Statement = .{ .sql = "INSERT INTO items VALUES (?1, ?2)" };
    try insert.parameters.append(arena.allocator(), .{ .kind = .{ .integer_value = 1 } });
    try insert.parameters.append(arena.allocator(), .{ .kind = .{ .text_value = "first" } });
    try execute_request.statements.append(arena.allocator(), insert);
    var execute_result = try client.execute(runtime_allocator, execute_request);
    defer execute_result.deinit();
    try std.testing.expect(execute_result.raw.status.isOk());
    try std.testing.expectEqual(pb.ExecuteCode.EXECUTE_CODE_OK, execute_result.response.?.code);

    var query_result = try client.query(runtime_allocator, .{ .sql = "SELECT id, name FROM items" });
    defer query_result.deinit();
    try std.testing.expect(query_result.raw.status.isOk());
    try std.testing.expectEqual(@as(usize, 1), query_result.response.?.rows.items.len);
    try std.testing.expectEqualStrings("first", query_result.response.?.rows.items[0].values.items[1].kind.?.text_value);

    var boundary_result = try client.query(runtime_allocator, .{ .sql = "SELECT zeroblob(4194304)" });
    defer boundary_result.deinit();
    try std.testing.expect(boundary_result.raw.status.isOk());
    try std.testing.expectEqual(@as(usize, 4 * 1024 * 1024), boundary_result.response.?.rows.items[0].values.items[0].kind.?.blob_value.len);

    var limited_result = try client.query(runtime_allocator, .{
        .sql = "WITH RECURSIVE values_table(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM values_table) SELECT max(value) FROM values_table",
    });
    defer limited_result.deinit();
    try std.testing.expectEqual(grpc.StatusCode.resource_exhausted, limited_result.raw.status.code);

    var status_result = try client.status(runtime_allocator);
    defer status_result.deinit();
    try std.testing.expect(status_result.raw.status.isOk());
    try std.testing.expectEqual(@as(u64, 1), status_result.response.?.node_id);
    try std.testing.expectEqualStrings("leader", status_result.response.?.role);
}

test "three-node cluster preserves SQL across leader failover and restart" {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", runtime_allocator);
    defer runtime_allocator.free(root_path);
    const ports = try reserveUniquePorts(6);
    var arena: std.heap.ArenaAllocator = .init(runtime_allocator);
    defer arena.deinit();
    const scratch = arena.allocator();
    var api_addresses: [3][]const u8 = undefined;
    var raft_addresses: [3][]const u8 = undefined;
    var data_directories: [3][]const u8 = undefined;
    for (0..3) |index| {
        api_addresses[index] = try std.fmt.allocPrint(scratch, "127.0.0.1:{}", .{ports[index * 2]});
        raft_addresses[index] = try std.fmt.allocPrint(scratch, "127.0.0.1:{}", .{ports[index * 2 + 1]});
        data_directories[index] = try std.fmt.allocPrint(scratch, "{s}/node-{}", .{ root_path, index + 1 });
    }

    var configs: [3]config_mod.ServerConfig = undefined;
    var config_count: usize = 0;
    defer for (configs[0..config_count]) |*config| config.deinit();
    for (0..3) |index| {
        configs[index] = try makeClusterConfig(
            runtime_allocator,
            index + 1,
            api_addresses[index],
            raft_addresses[index],
            data_directories[index],
            raft_addresses,
        );
        config_count += 1;
    }

    var runtimes: [3]?*runtime_mod.Runtime = .{ null, null, null };
    defer for (&runtimes) |*maybe_runtime| {
        if (maybe_runtime.*) |runtime| {
            if (runtime.running) runtime.shutdown() catch {};
            runtime.deinit();
            maybe_runtime.* = null;
        }
    };
    for (0..3) |index| {
        runtimes[index] = try runtime_mod.Runtime.create(runtime_allocator, &configs[index], testOptions());
    }

    const first_leader = try waitForClusterLeader(&runtimes);
    try executeSql(runtimes[first_leader].?, "cluster-schema", &.{
        "CREATE TABLE items (id INTEGER PRIMARY KEY) STRICT",
        "INSERT INTO items VALUES (1)",
    });
    try expectCount(runtimes[first_leader].?, 1);

    const stopped = runtimes[first_leader].?;
    try stopped.shutdown();
    stopped.deinit();
    runtimes[first_leader] = null;

    const second_leader = try waitForClusterLeader(&runtimes);
    try expectCount(runtimes[second_leader].?, 1);
    try executeSql(runtimes[second_leader].?, "cluster-insert-2", &.{"INSERT INTO items VALUES (2)"});
    try expectCount(runtimes[second_leader].?, 2);
    const target_index = runtimes[second_leader].?.status().applied_index;

    runtimes[first_leader] = try runtime_mod.Runtime.create(runtime_allocator, &configs[first_leader], testOptions());
    for (0..1000) |_| {
        if (runtimes[first_leader].?.status().applied_index >= target_index) break;
        if (runtimes[first_leader].?.driverExited()) return error.DriverExited;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    var response = try runtimes[first_leader].?.machine.query(runtime_allocator, .{ .sql = "SELECT count(*) FROM items" });
    defer response.deinit(runtime_allocator);
    try std.testing.expectEqual(@as(i64, 2), response.rows.items[0].values.items[0].kind.?.integer_value);
}

fn makeClusterConfig(
    allocator: std.mem.Allocator,
    node_id: usize,
    api_address: []const u8,
    raft_address: []const u8,
    data_directory: []const u8,
    raft_addresses: [3][]const u8,
) !config_mod.ServerConfig {
    var node_buffer: [16]u8 = undefined;
    const node_text = try std.fmt.bufPrint(&node_buffer, "{}", .{node_id});
    const peer_1 = try std.fmt.allocPrint(allocator, "1={s}", .{raft_addresses[0]});
    defer allocator.free(peer_1);
    const peer_2 = try std.fmt.allocPrint(allocator, "2={s}", .{raft_addresses[1]});
    defer allocator.free(peer_2);
    const peer_3 = try std.fmt.allocPrint(allocator, "3={s}", .{raft_addresses[2]});
    defer allocator.free(peer_3);
    const arguments = [_][]const u8{
        "--node-id",     node_text,
        "--cluster-id",  "0198f54d-5c2a-7000-8000-000000000002",
        "--api-listen",  api_address,
        "--raft-listen", raft_address,
        "--data-dir",    data_directory,
        "--peer",        peer_1,
        "--peer",        peer_2,
        "--peer",        peer_3,
    };
    return config_mod.parseServer(allocator, &arguments);
}

fn waitForClusterLeader(runtimes: *[3]?*runtime_mod.Runtime) !usize {
    for (0..1000) |_| {
        var leader: ?usize = null;
        var leaders: usize = 0;
        for (runtimes, 0..) |maybe_runtime, index| {
            const runtime = maybe_runtime orelse continue;
            if (runtime.driverExited()) return error.DriverExited;
            if (runtime.status().role == .leader) {
                leader = index;
                leaders += 1;
            }
        }
        if (leaders == 1) return leader.?;
        try std.testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.LeaderTimeout;
}

fn executeSql(runtime: *runtime_mod.Runtime, request_id: []const u8, statements: []const []const u8) !void {
    const target = try endpoint(runtime_allocator, runtime);
    defer runtime_allocator.free(target);
    var client = try client_mod.Client.init(runtime_allocator, target);
    defer client.deinit();
    var arena: std.heap.ArenaAllocator = .init(runtime_allocator);
    defer arena.deinit();
    var request: pb.ExecuteRequest = .{ .request_id = request_id };
    for (statements) |sql| try request.statements.append(arena.allocator(), .{ .sql = sql });
    var result = try client.execute(runtime_allocator, request);
    defer result.deinit();
    try std.testing.expect(result.raw.status.isOk());
    try std.testing.expectEqual(pb.ExecuteCode.EXECUTE_CODE_OK, result.response.?.code);
}

fn expectCount(runtime: *runtime_mod.Runtime, expected: i64) !void {
    const target = try endpoint(runtime_allocator, runtime);
    defer runtime_allocator.free(target);
    var client = try client_mod.Client.init(runtime_allocator, target);
    defer client.deinit();
    var result = try client.query(runtime_allocator, .{ .sql = "SELECT count(*) FROM items" });
    defer result.deinit();
    try std.testing.expect(result.raw.status.isOk());
    try std.testing.expectEqual(expected, result.response.?.rows.items[0].values.items[0].kind.?.integer_value);
}

fn reserveUniquePorts(comptime count: usize) ![count]u16 {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listeners: [count]std.Io.net.Server = undefined;
    var listener_count: usize = 0;
    defer for (listeners[0..listener_count]) |*listener| listener.deinit(std.testing.io);

    var ports: [count]u16 = undefined;
    for (&listeners, 0..) |*listener, index| {
        listener.* = try address.listen(std.testing.io, .{});
        listener_count += 1;
        var local_address: std.posix.sockaddr.in = undefined;
        var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
        if (std.posix.errno(std.posix.system.getsockname(
            listener.socket.handle,
            @ptrCast(&local_address),
            &address_length,
        )) != .SUCCESS) return error.AddressQueryFailed;
        ports[index] = std.mem.bigToNative(u16, local_address.port);
    }
    return ports;
}
