//! Persistent grpc-lite raw streaming transport.
//!
//! The allocator passed to `create` must be thread-safe. Server callbacks and
//! peer workers encode, decode, and queue data concurrently.

const std = @import("std");
const grpc = @import("grpc_lite");
const Error = @import("../core/error.zig").Error;
const types = @import("../core/types.zig");
const raw_node = @import("../raw_node.zig");
const transport_mod = @import("../transport.zig");
const codec = @import("../codec.zig");
const inbound_mailbox = @import("inbound_mailbox.zig");
const peer_event_queue = @import("peer_event_queue.zig");
const peer_manager = @import("peer_manager.zig");

const Message = types.Message;
const Transport = transport_mod.Transport;
const TransportIdentity = transport_mod.TransportIdentity;
const MessageCallback = transport_mod.MessageCallback;
const PeerEventCallback = transport_mod.PeerEventCallback;
const InboundMailbox = inbound_mailbox.InboundMailbox;
const PeerEventQueue = peer_event_queue.PeerEventQueue;
const PeerManager = peer_manager.PeerManager;

const log = grpc.log;

pub const Config = struct {
    identity: TransportIdentity,
    listen_addr: []const u8,
    stream_limits: grpc.StreamBufferLimits = .{},
    mailbox_max_messages: usize = 4096,
    mailbox_max_bytes: usize = 64 * 1024 * 1024,
    reconnect_initial_delay_ns: u64 = 20 * std.time.ns_per_ms,
    reconnect_max_delay_ns: u64 = 2 * std.time.ns_per_s,
    graceful_shutdown_timeout_ns: u64 = 5 * std.time.ns_per_s,
    runtime: ?*grpc.Runtime = null,

    pub fn validate(self: Config) !void {
        if (self.identity.node_id == 0) return error.InvalidNodeId;
        if (std.mem.allEqual(u8, &self.identity.cluster_id, 0)) return error.ClusterIdRequired;
        if (self.listen_addr.len == 0) return error.ListenAddressEmpty;
        try self.stream_limits.validate();
        try (InboundMailbox.Limits{
            .max_messages = self.mailbox_max_messages,
            .max_bytes = self.mailbox_max_bytes,
        }).validate();
        if (self.reconnect_initial_delay_ns == 0 or
            self.reconnect_max_delay_ns < self.reconnect_initial_delay_ns or
            self.graceful_shutdown_timeout_ns == 0)
        {
            return error.InvalidConfig;
        }
    }
};

pub const GrpcLiteTransport = struct {
    const State = enum { initialized, starting, started, stopping, stopped };

    allocator: std.mem.Allocator,
    config: Config,
    listen_addr: []u8,
    server: grpc.Server,
    peer_manager: PeerManager,
    mailbox: InboundMailbox,
    peer_events: PeerEventQueue,
    callback: ?MessageCallback = null,
    peer_event_callback: ?PeerEventCallback = null,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    state: State = .initialized,
    accepting: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn create(allocator: std.mem.Allocator, config: Config) !*GrpcLiteTransport {
        try config.validate();
        const parsed = try parseAddress(config.listen_addr);
        const listen_addr = try allocator.dupe(u8, config.listen_addr);
        errdefer allocator.free(listen_addr);
        const self = try allocator.create(GrpcLiteTransport);
        errdefer allocator.destroy(self);

        var mailbox = try InboundMailbox.init(allocator, .{
            .max_messages = config.mailbox_max_messages,
            .max_bytes = config.mailbox_max_bytes,
        });
        errdefer mailbox.deinit();
        var events = try PeerEventQueue.init(allocator, config.mailbox_max_messages);
        errdefer events.deinit();
        var server = try grpc.Server.init(allocator, .{
            .host = parsed.host,
            .port = parsed.port,
            .max_request_size = config.stream_limits.max_message_size,
            .stream_limits = config.stream_limits,
        });
        errdefer server.deinit();

        var owned_config = config;
        owned_config.listen_addr = listen_addr;
        self.* = .{
            .allocator = allocator,
            .config = owned_config,
            .listen_addr = listen_addr,
            .server = server,
            .peer_manager = undefined,
            .mailbox = mailbox,
            .peer_events = events,
        };
        self.peer_manager = PeerManager.init(allocator, .{
            .identity = config.identity,
            .stream_limits = config.stream_limits,
            .reconnect_initial_delay_ns = config.reconnect_initial_delay_ns,
            .reconnect_max_delay_ns = config.reconnect_max_delay_ns,
            .runtime = config.runtime,
            .event_sink = .{ .ctx = self, .function = queuePeerEvent },
        });
        errdefer self.peer_manager.deinit();
        try self.server.registerStream(peer_manager.stream_method_path, .{
            .context = self,
            .on_start = onStreamStart,
            .on_message = onStreamMessage,
            .on_remote_end = onStreamRemoteEnd,
            .on_cancel = onStreamCancel,
        });
        return self;
    }

    pub fn destroy(self: *GrpcLiteTransport) void {
        self.stop();
        self.callback = null;
        self.peer_event_callback = null;
        self.peer_manager.deinit();
        self.server.deinit();
        self.mailbox.deinit();
        self.peer_events.deinit();
        self.allocator.free(self.listen_addr);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn start(self: *GrpcLiteTransport) Error!void {
        return startImpl(self);
    }

    pub fn stop(self: *GrpcLiteTransport) void {
        stopImpl(self);
    }

    pub fn localAddress(self: *const GrpcLiteTransport) !grpc.ServerLocalAddress {
        return self.server.localAddress();
    }

    pub fn port(self: *const GrpcLiteTransport) !u16 {
        return self.server.port();
    }

    pub fn peerOpenCount(self: *GrpcLiteTransport, id: u64) u64 {
        return self.peer_manager.openCount(id);
    }

    pub fn peerState(self: *GrpcLiteTransport, id: u64) ?peer_manager.LifecycleState {
        return self.peer_manager.peerState(id);
    }

    pub fn pollOne(self: *GrpcLiteTransport) Error!bool {
        if (!self.accepting.load(.acquire)) return false;
        if (self.callback) |callback| {
            if (self.mailbox.pop()) |message| {
                try callback.invoke(message);
                return true;
            }
        }
        if (self.peer_event_callback) |callback| {
            if (self.peer_events.pop()) |event| {
                try callback.invoke(event);
                return true;
            }
        }
        return false;
    }

    fn startImpl(context: *anyopaque) Error!void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        lock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state != .initialized) return error.AlreadyStarted;
        self.state = .starting;
        peer_manager.lockTsanLifecycle();
        const start_result = self.server.start();
        peer_manager.unlockTsanLifecycle();
        start_result catch |err| {
            self.state = .stopped;
            return mapStartError(err);
        };
        self.accepting.store(true, .release);
        self.peer_manager.startAll() catch |err| {
            self.accepting.store(false, .release);
            self.peer_manager.stopAll();
            self.server.shutdown();
            self.server.wait();
            self.state = .stopped;
            return err;
        };
        self.state = .started;
        const address = self.server.localAddress() catch return;
        log.info(@src(), "grpc transport listening on {s}:{}", .{ address.host, address.port });
    }

    fn stopImpl(context: *anyopaque) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        while (true) {
            lock(&self.lifecycle_mutex);
            switch (self.state) {
                .stopped => {
                    self.lifecycle_mutex.unlock();
                    return;
                },
                .stopping => {
                    self.lifecycle_mutex.unlock();
                    std.atomic.spinLoopHint();
                    continue;
                },
                .initialized, .starting, .started => {
                    self.state = .stopping;
                    self.accepting.store(false, .release);
                    self.lifecycle_mutex.unlock();
                    break;
                },
            }
        }

        self.server.shutdownGracefully(self.config.graceful_shutdown_timeout_ns);
        self.peer_manager.stopAll();
        peer_manager.lockTsanLifecycle();
        self.server.wait();
        peer_manager.unlockTsanLifecycle();
        self.mailbox.clear();
        self.peer_events.clear();

        lock(&self.lifecycle_mutex);
        self.state = .stopped;
        self.lifecycle_mutex.unlock();
    }

    fn addPeerImpl(context: *anyopaque, id: u64, addr: []const u8) Error!bool {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        lock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state == .stopping or self.state == .stopped) return error.ConnectionClosed;
        return self.peer_manager.addPeer(id, addr);
    }

    fn removePeerImpl(context: *anyopaque, id: u64) Error!void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        lock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state == .stopping or self.state == .stopped) return error.ConnectionClosed;
        try self.peer_manager.removePeer(id);
    }

    fn sendImpl(context: *anyopaque, messages: []const Message) Error!void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        lock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state != .started) return error.ConnectionClosed;
        for (messages) |message| {
            if (message.to == 0 or message.to == self.config.identity.node_id) return error.MessageDestinationMismatch;
            if (raw_node.isLocalMessage(message.msg_type)) return error.LocalMessageOnTransport;
            if (message.msg_type == .transfer_leader) {
                if (message.from == 0 or
                    (message.from != self.config.identity.node_id and !self.peer_manager.hasPeer(message.from)))
                {
                    return error.MessageSourceMismatch;
                }
            } else if (message.from != self.config.identity.node_id) {
                return error.MessageSourceMismatch;
            }
            const payload = codec.encodeMessage(self.allocator, message) catch |err| return mapCodecError(err);
            defer self.allocator.free(payload);
            try self.peer_manager.send(message.to, payload, message.msg_type == .snapshot);
        }
    }

    fn setMessageCallbackImpl(context: *anyopaque, callback: ?MessageCallback) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        self.callback = callback;
    }

    fn setPeerEventCallbackImpl(context: *anyopaque, callback: ?PeerEventCallback) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        self.peer_event_callback = callback;
    }

    fn pollOneImpl(context: *anyopaque) Error!bool {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        return self.pollOne();
    }

    fn identityImpl(context: *anyopaque) TransportIdentity {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
        return self.config.identity;
    }

    pub const vtable: Transport.VTable = .{
        .start = startImpl,
        .stop = stopImpl,
        .add_peer = addPeerImpl,
        .remove_peer = removePeerImpl,
        .send = sendImpl,
        .set_message_callback = setMessageCallbackImpl,
        .set_peer_event_callback = setPeerEventCallbackImpl,
        .poll_one = pollOneImpl,
        .identity = identityImpl,
    };

    pub fn transport(self: *GrpcLiteTransport) Transport {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

fn onStreamStart(
    context: ?*anyopaque,
    stream: grpc.ServerStream,
    server_context: *grpc.ServerContext,
) !void {
    const self: *GrpcLiteTransport = @ptrCast(@alignCast(context.?));
    const identity = validateInboundIdentity(self, &server_context.request_metadata) catch |err| {
        finishIdentityError(stream, err);
        return;
    };
    var source_bytes: [8]u8 = undefined;
    var target_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &source_bytes, self.config.identity.node_id, .little);
    std.mem.writeInt(u64, &target_bytes, identity.source_node, .little);
    try server_context.addInitialMetadata(peer_manager.protocol_version_key, "1");
    try server_context.addInitialMetadata(peer_manager.cluster_id_key, &self.config.identity.cluster_id);
    try server_context.addInitialMetadata(peer_manager.source_node_key, &source_bytes);
    try server_context.addInitialMetadata(peer_manager.target_node_key, &target_bytes);
}

fn onStreamMessage(
    context: ?*anyopaque,
    stream: grpc.ServerStream,
    server_context: *grpc.ServerContext,
    payload: []const u8,
    _: grpc.Compression,
) !grpc.StreamReceiveAction {
    const self: *GrpcLiteTransport = @ptrCast(@alignCast(context.?));
    if (!self.accepting.load(.acquire)) {
        return error.TransportStopping;
    }
    const identity = validateInboundIdentity(self, &server_context.request_metadata) catch |err| {
        finishIdentityError(stream, err);
        return .continue_receiving;
    };
    if (!self.mailbox.canAccept(payload.len)) return error.InboundMailboxFull;
    var message = codec.decodeMessage(self.allocator, payload) catch {
        return error.InvalidRaftMessage;
    };
    if (!validRoute(self, identity.source_node, message)) {
        message.deinit(self.allocator);
        return error.InvalidRaftMessageRoute;
    }
    if (message.msg_type == .append_response) self.peer_manager.acknowledgeSnapshot(identity.source_node);
    self.mailbox.push(message, payload.len) catch |err| {
        message.deinit(self.allocator);
        return if (err == error.TransportBackpressure) error.InboundMailboxFull else error.InboundMailboxFailed;
    };
    return .continue_receiving;
}

fn onStreamRemoteEnd(
    context: ?*anyopaque,
    stream: grpc.ServerStream,
    server_context: *grpc.ServerContext,
) !void {
    const self: *GrpcLiteTransport = @ptrCast(@alignCast(context.?));
    _ = validateInboundIdentity(self, &server_context.request_metadata) catch |err| {
        finishIdentityError(stream, err);
        return;
    };
    tryFinish(stream, grpc.Status.ok);
}

fn onStreamCancel(_: ?*anyopaque, _: grpc.ServerStream, _: *grpc.ServerContext) void {}

fn validateInboundIdentity(self: *GrpcLiteTransport, metadata: *const grpc.Metadata) !peer_manager.StreamIdentity {
    const identity = peer_manager.parseStreamIdentity(metadata) catch return error.InvalidIdentityMetadata;
    if (!self.accepting.load(.acquire) or
        !std.mem.eql(u8, &identity.cluster_id, &self.config.identity.cluster_id) or
        identity.target_node != self.config.identity.node_id or
        !self.peer_manager.hasPeer(identity.source_node))
    {
        return error.IdentityRejected;
    }
    return identity;
}

fn validRoute(self: *GrpcLiteTransport, source: u64, message: Message) bool {
    if (message.to != self.config.identity.node_id or raw_node.isLocalMessage(message.msg_type)) return false;
    if (message.msg_type != .transfer_leader) return message.from == source;
    return message.from != 0 and
        (message.from == self.config.identity.node_id or self.peer_manager.hasPeer(message.from));
}

fn finishIdentityError(stream: grpc.ServerStream, err: anyerror) void {
    tryFinish(stream, if (err == error.InvalidIdentityMetadata)
        .init(.invalid_argument, "invalid raft stream identity")
    else
        .init(.failed_precondition, "raft stream identity rejected"));
}

fn tryFinish(stream: grpc.ServerStream, status: grpc.Status) void {
    stream.finish(status) catch {};
}

fn queuePeerEvent(context: *anyopaque, event: transport_mod.PeerEvent, generation: u64) void {
    const self: *GrpcLiteTransport = @ptrCast(@alignCast(context));
    if (!self.accepting.load(.acquire)) return;
    self.peer_events.push(event, generation);
}

const ParsedAddress = struct { host: []const u8, port: u16 };

fn parseAddress(address: []const u8) !ParsedAddress {
    const colon = std.mem.lastIndexOfScalar(u8, address, ':') orelse return error.AddressPortMissing;
    if (colon == 0 or colon + 1 == address.len) return error.AddressPortInvalid;
    const port = std.fmt.parseInt(u16, address[colon + 1 ..], 10) catch return error.AddressPortInvalid;
    return .{ .host = address[0..colon], .port = port };
}

fn mapStartError(err: anyerror) Error {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.BindFailed => error.BindFailed,
        error.ListenFailed => error.ListenFailed,
        else => error.ConnectionClosed,
    };
}

fn mapCodecError(err: anyerror) Error {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.MessageTooLarge => error.MessageTooLarge,
        else => error.PayloadParseFailed,
    };
}

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

// KCOV_EXCL_START
const TestEventSink = struct {
    fn emit(_: *anyopaque, _: transport_mod.PeerEvent, _: u64) void {}
};

fn validTestConfig() Config {
    return .{
        .identity = .{ .cluster_id = [_]u8{1} ** 16, .node_id = 2 },
        .listen_addr = "127.0.0.1:0",
    };
}

test "grpc transport config validation rejects invalid fields" {
    var config = validTestConfig();
    try config.validate();

    config.identity.node_id = 0;
    try std.testing.expectError(error.InvalidNodeId, config.validate());
    config = validTestConfig();
    config.identity.cluster_id = [_]u8{0} ** 16;
    try std.testing.expectError(error.ClusterIdRequired, config.validate());
    config = validTestConfig();
    config.listen_addr = "";
    try std.testing.expectError(error.ListenAddressEmpty, config.validate());
    config = validTestConfig();
    config.stream_limits.max_message_size = 0;
    try std.testing.expectError(error.InvalidMaxMessageSize, config.validate());
    config = validTestConfig();
    config.mailbox_max_messages = 0;
    try std.testing.expectError(error.InvalidConfig, config.validate());
    config = validTestConfig();
    config.mailbox_max_bytes = 0;
    try std.testing.expectError(error.InvalidConfig, config.validate());
    config = validTestConfig();
    config.reconnect_initial_delay_ns = 0;
    try std.testing.expectError(error.InvalidConfig, config.validate());
    config = validTestConfig();
    config.reconnect_max_delay_ns = config.reconnect_initial_delay_ns - 1;
    try std.testing.expectError(error.InvalidConfig, config.validate());
    config = validTestConfig();
    config.graceful_shutdown_timeout_ns = 0;
    try std.testing.expectError(error.InvalidConfig, config.validate());
}

test "grpc transport validates inbound message routes" {
    const allocator = std.testing.allocator;
    var self: GrpcLiteTransport = undefined;
    self.config = validTestConfig();
    self.peer_manager = PeerManager.init(allocator, .{
        .identity = self.config.identity,
        .stream_limits = self.config.stream_limits,
        .reconnect_initial_delay_ns = 1,
        .reconnect_max_delay_ns = 2,
        .runtime = null,
        .event_sink = .{ .ctx = undefined, .function = TestEventSink.emit },
    });
    defer self.peer_manager.deinit();
    try std.testing.expect(try self.peer_manager.addPeer(1, "source"));
    try std.testing.expect(try self.peer_manager.addPeer(3, "transferee"));

    try std.testing.expect(validRoute(&self, 1, .{ .msg_type = .append, .from = 1, .to = 2 }));
    try std.testing.expect(!validRoute(&self, 1, .{ .msg_type = .append, .from = 3, .to = 2 }));
    try std.testing.expect(!validRoute(&self, 1, .{ .msg_type = .append, .from = 1, .to = 3 }));
    try std.testing.expect(!validRoute(&self, 1, .{ .msg_type = .hup, .from = 1, .to = 2 }));
    try std.testing.expect(validRoute(&self, 1, .{ .msg_type = .transfer_leader, .from = 2, .to = 2 }));
    try std.testing.expect(validRoute(&self, 1, .{ .msg_type = .transfer_leader, .from = 3, .to = 2 }));
    try std.testing.expect(!validRoute(&self, 1, .{ .msg_type = .transfer_leader, .from = 0, .to = 2 }));
    try std.testing.expect(!validRoute(&self, 1, .{ .msg_type = .transfer_leader, .from = 4, .to = 2 }));
}

test "grpc transport error mapping is stable" {
    const start_cases = [_]struct { input: anyerror, expected: Error }{
        .{ .input = error.OutOfMemory, .expected = error.OutOfMemory },
        .{ .input = error.BindFailed, .expected = error.BindFailed },
        .{ .input = error.ListenFailed, .expected = error.ListenFailed },
        .{ .input = error.Unexpected, .expected = error.ConnectionClosed },
    };
    for (start_cases) |case| try std.testing.expectEqual(case.expected, mapStartError(case.input));

    const codec_cases = [_]struct { input: anyerror, expected: Error }{
        .{ .input = error.OutOfMemory, .expected = error.OutOfMemory },
        .{ .input = error.MessageTooLarge, .expected = error.MessageTooLarge },
        .{ .input = error.Unexpected, .expected = error.PayloadParseFailed },
    };
    for (codec_cases) |case| try std.testing.expectEqual(case.expected, mapCodecError(case.input));
}
// KCOV_EXCL_STOP
