const std = @import("std");
const mar = @import("marionette");
const raft = @import("raft_zig");
const MarionetteWalFs = @import("marionette_fs.zig").MarionetteFs;

pub const PacketRef = struct {
    id: u64,
};

pub const PacketPool = struct {
    messages: std.AutoHashMap(u64, raft.Message),
    next_id: u64 = 0,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) PacketPool {
        return .{ .messages = std.AutoHashMap(u64, raft.Message).init(allocator), .allocator = allocator };
    }

    pub fn deinit(self: *PacketPool) void {
        var iterator = self.messages.valueIterator();
        while (iterator.next()) |message| message.deinit(self.allocator);
        self.messages.deinit();
    }

    pub fn put(self: *PacketPool, message: raft.Message) !PacketRef {
        const start = self.next_id;
        while (self.messages.contains(self.next_id)) {
            self.next_id +%= 1;
            if (self.next_id == start) return error.OutOfMemory;
        }
        const id = self.next_id;
        self.next_id +%= 1;
        const cloned = try raft.cloneMessage(self.allocator, message);
        errdefer {
            var owned = cloned;
            owned.deinit(self.allocator);
        }
        try self.messages.put(id, cloned);
        return .{ .id = id };
    }

    pub fn take(self: *PacketPool, packet: PacketRef) raft.Error!raft.Message {
        const entry = self.messages.fetchRemove(packet.id) orelse return error.PayloadParseFailed;
        return entry.value;
    }

    pub fn discard(self: *PacketPool, packet: PacketRef) void {
        if (self.messages.fetchRemove(packet.id)) |entry| {
            var message = entry.value;
            message.deinit(self.allocator);
        }
    }
};

pub const SimTransport = struct {
    endpoint: mar.Endpoint(PacketRef),
    pool: *PacketPool,
    callback: ?raft.MessageCallback = null,

    pub fn init(endpoint: mar.Endpoint(PacketRef), pool: *PacketPool) SimTransport {
        return .{ .endpoint = endpoint, .pool = pool };
    }

    pub fn transport(self: *SimTransport) raft.Transport {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn start(_: *anyopaque) raft.Error!void {}
    fn stop(_: *anyopaque) void {}
    fn addPeer(_: *anyopaque, _: u64, _: []const u8) raft.Error!bool {
        return true;
    }
    fn removePeer(_: *anyopaque, _: u64) raft.Error!void {}

    fn send(ctx: *anyopaque, messages: []const raft.Message) raft.Error!void {
        const self: *SimTransport = @ptrCast(@alignCast(ctx));
        for (messages) |message| {
            if (message.to == 0) continue;
            const packet = self.pool.put(message) catch return error.OutOfMemory;
            self.endpoint.send(raftIdToNode(message.to), packet) catch {
                self.pool.discard(packet);
                return error.ConnectionClosed;
            };
        }
    }

    fn setMessageCallback(ctx: *anyopaque, callback: ?raft.MessageCallback) void {
        const self: *SimTransport = @ptrCast(@alignCast(ctx));
        self.callback = callback;
    }

    fn pollOne(ctx: *anyopaque) raft.Error!bool {
        const self: *SimTransport = @ptrCast(@alignCast(ctx));
        const callback = self.callback orelse return false;
        const envelope = (self.endpoint.receive() catch return error.ConnectionClosed) orelse return false;
        const message = try self.pool.take(envelope.message);
        try callback.invoke(message);
        return true;
    }

    const vtable: raft.Transport.VTable = .{
        .start = start,
        .stop = stop,
        .add_peer = addPeer,
        .remove_peer = removePeer,
        .send = send,
        .set_message_callback = setMessageCallback,
        .poll_one = pollOne,
    };
};

pub const NodeProcess = struct {
    allocator: std.mem.Allocator,
    config: raft.RaftorConfig,
    wal_fs: MarionetteWalFs,
    storage: ?*raft.WALStorage = null,
    state_machine: raft.MockStateMachine,
    transport: SimTransport,
    raftor: ?*raft.Raftor = null,

    pub fn create(
        allocator: std.mem.Allocator,
        env: mar.Env,
        config: raft.RaftorConfig,
        endpoint: mar.Endpoint(PacketRef),
        pool: *PacketPool,
    ) !*NodeProcess {
        const self = try allocator.create(NodeProcess);
        self.* = .{
            .allocator = allocator,
            .config = config,
            .wal_fs = MarionetteWalFs.init(env.io(), env.disk),
            .state_machine = raft.MockStateMachine.init(allocator),
            .transport = SimTransport.init(endpoint, pool),
        };
        errdefer {
            self.state_machine.deinit();
            allocator.destroy(self);
        }
        try self.start(.bootstrap);
        return self;
    }

    pub fn destroy(self: *NodeProcess) void {
        self.kill();
        self.state_machine.deinit();
        self.allocator.destroy(self);
    }

    pub fn lifecycle(self: *NodeProcess) mar.ProcessLifecycle {
        return .{ .ptr = self, .on_kill = onKill, .restart = restart };
    }

    pub fn start(self: *NodeProcess, startup_mode: raft.StartupMode) !void {
        if (self.raftor != null or self.storage != null) return error.AlreadyStarted;
        var dir_buffer: [64]u8 = undefined;
        const dir = try std.fmt.bufPrintZ(&dir_buffer, "raft-vopr-node-{}", .{self.config.nodeId()});
        const storage = try raft.WALStorage.openWithFs(self.allocator, dir, self.wal_fs.fileSystem());
        self.storage = storage;
        errdefer {
            storage.deinit();
            self.storage = null;
        }
        var config = self.config;
        config.raft.applied = self.state_machine.last_applied_index;
        self.raftor = try raft.Raftor.createWithDependencies(self.allocator, config, startup_mode, .{
            .storage = storage.asWritableStorage(),
            .transport = self.transport.transport(),
            .state_machine = self.state_machine.stateMachine(),
        });
    }

    pub fn kill(self: *NodeProcess) void {
        if (self.raftor) |node| {
            node.destroy();
            self.raftor = null;
        }
        if (self.storage) |storage| storage.deinit();
        self.storage = null;
        self.state_machine.deinit();
        self.state_machine = raft.MockStateMachine.init(self.allocator);
    }

    fn onKill(ctx: *anyopaque) void {
        const self: *NodeProcess = @ptrCast(@alignCast(ctx));
        self.kill();
    }

    fn restart(ctx: *anyopaque, env: mar.Env) anyerror!void {
        const self: *NodeProcess = @ptrCast(@alignCast(ctx));
        self.wal_fs = MarionetteWalFs.init(env.io(), env.disk);
        try self.start(.restart);
        try env.record("raft_vopr.node restart={} applied={}", .{ self.config.nodeId(), self.state_machine.last_applied_index });
    }
};

fn raftIdToNode(id: u64) mar.NodeId {
    std.debug.assert(id > 0 and id <= @as(u64, std.math.maxInt(mar.NodeId)) + 1);
    return @intCast(id - 1);
}
