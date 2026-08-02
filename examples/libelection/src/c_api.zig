const std = @import("std");
const options = @import("election_options");
const api = @import("api_types.zig");
const Node = @import("node.zig").Node;
const mapError = @import("node.zig").mapError;

pub const Error = api.Error;
pub const NodeHandle = api.NodeHandle;
pub const NodeOptions = api.NodeOptions;
pub const Callbacks = api.Callbacks;
pub const Status = api.Status;

const allocator = std.heap.c_allocator;

pub fn election_abi_version() callconv(.c) u32 {
    return (@as(u32, api.abi_major) << 16) | api.abi_minor;
}

pub fn election_library_version() callconv(.c) [*:0]const u8 {
    return options.version ++ "\x00";
}

pub fn election_error_string(error_code: i32) callconv(.c) [*:0]const u8 {
    const value = std.enums.fromInt(Error, error_code) orelse return "unknown error";
    return switch (value) {
        .ok => "ok",
        .invalid_argument => "invalid argument",
        .invalid_state => "invalid state",
        .out_of_memory => "out of memory",
        .io => "I/O error",
        .unavailable => "unavailable",
        .corrupt_storage => "corrupt storage",
        .incompatible_storage => "incompatible storage",
        .closed => "closed",
        .internal => "internal error",
    };
}

pub fn election_node_create(
    node_options: ?*const NodeOptions,
    callbacks: ?*const Callbacks,
    out_node: ?*?*NodeHandle,
) callconv(.c) Error {
    const output = out_node orelse return .invalid_argument;
    output.* = null;
    const input = node_options orelse return .invalid_argument;
    if (input.struct_size < @sizeOf(NodeOptions)) return .invalid_argument;
    const callback_value = if (callbacks) |value| blk: {
        if (value.struct_size < @sizeOf(Callbacks)) return .invalid_argument;
        break :blk value.*;
    } else Callbacks{};
    const node = Node.create(allocator, input.*, callback_value) catch |err| return mapError(err);
    output.* = @ptrCast(node);
    return .ok;
}

pub fn election_node_start(handle: ?*NodeHandle) callconv(.c) Error {
    const node = unwrap(handle) orelse return .invalid_argument;
    node.start() catch |err| return mapError(err);
    return .ok;
}

pub fn election_node_poll(handle: ?*NodeHandle, out_had_work: ?*u32) callconv(.c) Error {
    const node = unwrap(handle) orelse return .invalid_argument;
    const had_work = node.poll() catch |err| return mapError(err);
    if (out_had_work) |output| output.* = @intFromBool(had_work);
    return .ok;
}

pub fn election_node_tick(handle: ?*NodeHandle, out_had_work: ?*u32) callconv(.c) Error {
    const node = unwrap(handle) orelse return .invalid_argument;
    const had_work = node.tick() catch |err| return mapError(err);
    if (out_had_work) |output| output.* = @intFromBool(had_work);
    return .ok;
}

pub fn election_node_get_status(handle: ?*const NodeHandle, out_status: ?*Status) callconv(.c) Error {
    const node = unwrapConst(handle) orelse return .invalid_argument;
    const output = out_status orelse return .invalid_argument;
    if (output.struct_size < @sizeOf(Status)) return .invalid_argument;
    output.* = node.getStatus();
    return .ok;
}

pub fn election_node_shutdown(handle: ?*NodeHandle) callconv(.c) Error {
    const node = unwrap(handle) orelse return .invalid_argument;
    node.shutdown() catch |err| return mapError(err);
    return .ok;
}

pub fn election_node_destroy(handle: ?*NodeHandle) callconv(.c) void {
    const node = unwrap(handle) orelse return;
    node.destroy();
}

fn unwrap(handle: ?*NodeHandle) ?*Node {
    return if (handle) |value| @ptrCast(@alignCast(value)) else null;
}

fn unwrapConst(handle: ?*const NodeHandle) ?*const Node {
    return if (handle) |value| @ptrCast(@alignCast(value)) else null;
}

test "version and error strings are stable" {
    try std.testing.expectEqual(@as(u32, 0x0001_0000), election_abi_version());
    try std.testing.expectEqualStrings("ok", std.mem.span(election_error_string(0)));
    try std.testing.expectEqualStrings("unknown error", std.mem.span(election_error_string(-1)));
}
