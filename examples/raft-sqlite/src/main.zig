const std = @import("std");
const database = @import("raft_sqlite");

var stop_requested = std.atomic.Value(bool).init(false);

pub fn main(init: std.process.Init) !void {
    try database.raft.log.initGlobal(init.gpa, init.io, false);
    defer database.raft.log.deinitGlobal(init.gpa);
    const arguments = try init.minimal.args.toSlice(init.arena.allocator());
    if (arguments.len < 2 or std.mem.eql(u8, arguments[1], "help") or std.mem.eql(u8, arguments[1], "--help")) {
        try writeStdout(init.io, usage);
        return;
    }
    if (std.mem.eql(u8, arguments[1], "serve")) return serveMain(init, arguments[2..]);
    if (std.mem.eql(u8, arguments[1], "exec")) return executeMain(init, arguments[2..]);
    if (std.mem.eql(u8, arguments[1], "query")) return queryMain(init, arguments[2..]);
    if (std.mem.eql(u8, arguments[1], "status")) return statusMain(init, arguments[2..]);
    try writeStderr(init.io, "unknown command\n\n");
    try writeStderr(init.io, usage);
    return error.UnknownCommand;
}

fn serveMain(init: std.process.Init, arguments: []const []const u8) !void {
    var config = database.config.parseServer(init.gpa, arguments) catch |err| {
        var buffer: [256]u8 = undefined;
        const message = try std.fmt.bufPrint(&buffer, "invalid server configuration: {s}\n", .{@errorName(err)});
        try writeStderr(init.io, message);
        return err;
    };
    defer config.deinit();

    var old_interrupt: std.posix.Sigaction = undefined;
    var old_terminate: std.posix.Sigaction = undefined;
    const action: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.INT, &action, &old_interrupt);
    defer std.posix.sigaction(.INT, &old_interrupt, null);
    std.posix.sigaction(.TERM, &action, &old_terminate);
    defer std.posix.sigaction(.TERM, &old_terminate, null);

    const runtime = try database.Runtime.create(std.heap.smp_allocator, &config, .{});
    defer {
        if (runtime.running) runtime.shutdown() catch {};
        runtime.deinit();
    }
    while (!stop_requested.load(.acquire) and !runtime.driverExited()) {
        try init.io.sleep(.fromMilliseconds(100), .awake);
    }
    try runtime.shutdown();
}

fn executeMain(init: std.process.Init, arguments: []const []const u8) !void {
    if (arguments.len < 3) {
        try writeStderr(init.io, "usage: raft-sqlite exec ENDPOINT REQUEST_ID SQL [PARAM ...]\n");
        return error.InvalidArguments;
    }
    var request: database.pb.ExecuteRequest = .{ .request_id = arguments[1] };
    var statement: database.pb.Statement = .{ .sql = arguments[2] };
    for (arguments[3..]) |argument| {
        try statement.parameters.append(init.arena.allocator(), try parseParameter(init.arena.allocator(), argument));
    }
    try request.statements.append(init.arena.allocator(), statement);
    var client = try database.Client.init(init.gpa, arguments[0]);
    defer client.deinit();
    var result = try client.execute(init.gpa, request);
    defer result.deinit();
    if (!result.raw.status.isOk()) return reportRpcFailure(init.io, result.raw.status);
    try writeJson(init, result.response.?);
}

fn queryMain(init: std.process.Init, arguments: []const []const u8) !void {
    if (arguments.len < 2) {
        try writeStderr(init.io, "usage: raft-sqlite query ENDPOINT SQL [PARAM ...]\n");
        return error.InvalidArguments;
    }
    var request: database.pb.QueryRequest = .{ .sql = arguments[1] };
    for (arguments[2..]) |argument| {
        try request.parameters.append(init.arena.allocator(), try parseParameter(init.arena.allocator(), argument));
    }
    var client = try database.Client.init(init.gpa, arguments[0]);
    defer client.deinit();
    var result = try client.query(init.gpa, request);
    defer result.deinit();
    if (!result.raw.status.isOk()) return reportRpcFailure(init.io, result.raw.status);
    try writeJson(init, result.response.?);
}

fn statusMain(init: std.process.Init, arguments: []const []const u8) !void {
    if (arguments.len != 1) {
        try writeStderr(init.io, "usage: raft-sqlite status ENDPOINT\n");
        return error.InvalidArguments;
    }
    var client = try database.Client.init(init.gpa, arguments[0]);
    defer client.deinit();
    var result = try client.status(init.gpa);
    defer result.deinit();
    if (!result.raw.status.isOk()) return reportRpcFailure(init.io, result.raw.status);
    try writeJson(init, result.response.?);
}

fn parseParameter(allocator: std.mem.Allocator, argument: []const u8) !database.pb.Value {
    if (std.mem.eql(u8, argument, "null")) return .{ .kind = .{ .null_value = .NULL_VALUE } };
    const separator = std.mem.indexOfScalar(u8, argument, ':') orelse return error.InvalidParameter;
    const kind = argument[0..separator];
    const value = argument[separator + 1 ..];
    if (std.mem.eql(u8, kind, "int")) {
        return .{ .kind = .{ .integer_value = try std.fmt.parseInt(i64, value, 10) } };
    }
    if (std.mem.eql(u8, kind, "real")) {
        return .{ .kind = .{ .real_value = try std.fmt.parseFloat(f64, value) } };
    }
    if (std.mem.eql(u8, kind, "text")) return .{ .kind = .{ .text_value = value } };
    if (std.mem.eql(u8, kind, "blob")) {
        if (value.len % 2 != 0) return error.InvalidParameter;
        const bytes = try allocator.alloc(u8, value.len / 2);
        _ = std.fmt.hexToBytes(bytes, value) catch return error.InvalidParameter;
        return .{ .kind = .{ .blob_value = bytes } };
    }
    return error.InvalidParameter;
}

fn writeJson(init: std.process.Init, value: anytype) !void {
    const encoded = try value.jsonEncode(.{}, .{}, init.gpa);
    defer init.gpa.free(encoded);
    try writeStdout(init.io, encoded);
    try writeStdout(init.io, "\n");
}

fn reportRpcFailure(io: std.Io, status: database.grpc.Status) error{RpcFailed} {
    var buffer: [512]u8 = undefined;
    const message = std.fmt.bufPrint(&buffer, "RPC failed: {s}: {s}\n", .{ @tagName(status.code), status.message }) catch "RPC failed\n";
    writeStderr(io, message) catch {};
    return error.RpcFailed;
}

fn writeStdout(io: std.Io, text: []const u8) !void {
    try std.Io.File.writeStreamingAll(.stdout(), io, text);
}

fn writeStderr(io: std.Io, text: []const u8) !void {
    try std.Io.File.writeStreamingAll(.stderr(), io, text);
}

fn signalHandler(_: std.posix.SIG) callconv(.c) void {
    stop_requested.store(true, .release);
}

const usage =
    \\raft-sqlite commands:
    \\  serve --node-id ID --cluster-id UUID --api-listen IPv4:PORT
    \\        --raft-listen IPv4:PORT --data-dir PATH [--raft-advertise IPv4:PORT]
    \\        [--peer ID=IPv4:PORT ...]
    \\  exec ENDPOINT REQUEST_ID SQL [PARAM ...]
    \\  query ENDPOINT SQL [PARAM ...]
    \\  status ENDPOINT
    \\
    \\Parameters: null, int:42, real:3.14, text:value, blob:00ff
;
