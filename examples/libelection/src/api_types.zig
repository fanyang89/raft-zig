const std = @import("std");

pub const abi_major: u16 = 1;
pub const abi_minor: u16 = 0;

pub const Error = enum(i32) {
    ok = 0,
    invalid_argument = 1,
    invalid_state = 2,
    out_of_memory = 3,
    io = 4,
    unavailable = 5,
    corrupt_storage = 6,
    incompatible_storage = 7,
    closed = 8,
    internal = 255,
};

pub const DriveMode = enum(u32) {
    managed = 0,
    external = 1,
};

pub const NodeState = enum(u32) {
    created = 0,
    running = 1,
    stopped = 2,
    failed = 3,
};

pub const Role = enum(u32) {
    follower = 0,
    candidate = 1,
    leader = 2,
    pre_candidate = 3,
};

pub const EventType = enum(u32) {
    leadership_acquired = 1,
    leadership_lost = 2,
    failed = 3,
};

pub const BytesView = extern struct {
    data: ?[*]const u8 = null,
    size: usize = 0,

    pub fn slice(self: BytesView) ![]const u8 {
        if (self.size == 0) return "";
        const data = self.data orelse return error.InvalidArgument;
        return data[0..self.size];
    }
};

pub const Peer = extern struct {
    id: u64 = 0,
    address: BytesView = .{},
};

pub const NodeOptions = extern struct {
    struct_size: usize = @sizeOf(NodeOptions),
    drive_mode: u32 = @intFromEnum(DriveMode.managed),
    node_id: u64 = 0,
    cluster_id: [16]u8 = @splat(0),
    listen_address: BytesView = .{},
    data_dir: BytesView = .{},
    peers: ?[*]const Peer = null,
    peer_count: usize = 0,
    tick_interval_ms: u64 = 100,
    heartbeat_ticks: u32 = 2,
    election_ticks: u32 = 20,
};

pub const Status = extern struct {
    struct_size: usize = @sizeOf(Status),
    state: u32 = @intFromEnum(NodeState.created),
    role: u32 = @intFromEnum(Role.follower),
    leader_active: u32 = 0,
    reserved: u32 = 0,
    node_id: u64 = 0,
    term: u64 = 0,
    leader_id: u64 = 0,
    commit_index: u64 = 0,
    applied_index: u64 = 0,
    last_error: i32 = @intFromEnum(Error.ok),
};

pub const Event = extern struct {
    struct_size: usize = @sizeOf(Event),
    event_type: u32,
    status: Status,
};

pub const EventCallback = *const fn (?*anyopaque, *const Event) callconv(.c) void;

pub const Callbacks = extern struct {
    struct_size: usize = @sizeOf(Callbacks),
    user_data: ?*anyopaque = null,
    on_event: ?EventCallback = null,
};

pub const NodeHandle = opaque {};

test "C ABI structures have stable initializers" {
    const options: NodeOptions = .{};
    const callbacks: Callbacks = .{};
    const status: Status = .{};
    try std.testing.expectEqual(@sizeOf(NodeOptions), options.struct_size);
    try std.testing.expectEqual(@sizeOf(Callbacks), callbacks.struct_size);
    try std.testing.expectEqual(@sizeOf(Status), status.struct_size);
}
