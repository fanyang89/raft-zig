//! grpc-lite backed Transport for real network RPC.
//!
//! Implements the `Transport` vtable using grpc-lite's Server (inbound)
//! and Channel (outbound). Each Raft message is sent as one unary RPC
//! (fire-and-forget, empty response). Inbound messages are decoded and
//! pushed to a thread-safe `InboundMailbox`; `poll()` drains the mailbox
//! and invokes the registered callback.

const std = @import("std");
const grpc = @import("grpc_lite");

const error_model = @import("../core/error.zig");
const types = @import("../core/types.zig");
const transport_mod = @import("../transport.zig");
const codec_mod = @import("../codec.zig");
const inbound_mailbox_mod = @import("inbound_mailbox.zig");
const peer_manager_mod = @import("peer_manager.zig");

const Error = error_model.Error;
const Message = types.Message;
const Transport = transport_mod.Transport;
const MessageCallback = transport_mod.MessageCallback;
const PeerEventCallback = transport_mod.PeerEventCallback;
const InboundMailbox = inbound_mailbox_mod.InboundMailbox;
const PeerManager = peer_manager_mod.PeerManager;

const log = std.log.scoped(.raft_zig_grpc_transport);

const raft_method_path = "/raft.Raft/SendMessage";

/// grpc-lite backed transport. Must be heap-allocated (via `create`) because
/// the Server's handler captures a pointer to the internal mailbox.
pub const GrpcLiteTransport = struct {
    const State = enum { initialized, starting, started, stopping, stopped };

    allocator: std.mem.Allocator,
    server: ?*grpc.Server = null,
    peer_manager: PeerManager,
    mailbox: InboundMailbox,
    callback: ?MessageCallback = null,
    peer_event_callback: ?PeerEventCallback = null,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    state: State = .initialized,
    accepting: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    listen_addr: []const u8,
    addr_copy: []u8,

    /// Create a transport configured to listen on `host:port` after start().
    pub fn create(
        allocator: std.mem.Allocator,
        listen_addr: []const u8,
    ) !*GrpcLiteTransport {
        const self = try allocator.create(GrpcLiteTransport);
        errdefer allocator.destroy(self);

        const addr_copy = try allocator.dupe(u8, listen_addr);
        errdefer allocator.free(addr_copy);

        self.* = .{
            .allocator = allocator,
            .peer_manager = PeerManager.init(allocator),
            .mailbox = InboundMailbox.init(allocator),
            .listen_addr = addr_copy,
            .addr_copy = addr_copy,
        };
        errdefer {
            self.peer_manager.deinit();
            self.mailbox.deinit();
        }

        // Parse host and port from listen_addr (e.g. "127.0.0.1:9000").
        const colon = std.mem.lastIndexOfScalar(u8, listen_addr, ':') orelse return error.AddressPortInvalid;
        const host = listen_addr[0..colon];
        const port_str = listen_addr[colon + 1 ..];
        const parsed_port = std.fmt.parseInt(u16, port_str, 10) catch return error.AddressPortInvalid;

        // Initialize the grpc server. Binding is deferred until start().
        const srv = try allocator.create(grpc.Server);
        errdefer allocator.destroy(srv);
        srv.* = try grpc.Server.init(allocator, .{
            .host = host,
            .port = parsed_port,
        });
        errdefer srv.deinit();
        self.server = srv;

        try srv.registerUnary(raft_method_path, .{
            .context = self,
            .invoke_fn = handleMessage,
        });
        return self;
    }

    pub fn destroy(self: *GrpcLiteTransport) void {
        self.stop();
        self.callback = null;
        self.peer_event_callback = null;
        if (self.server) |srv| {
            srv.deinit();
            self.allocator.destroy(srv);
        }
        self.peer_manager.deinit();
        self.mailbox.deinit();
        self.allocator.free(self.addr_copy);
        self.allocator.destroy(self);
    }

    pub fn start(self: *GrpcLiteTransport) Error!void {
        return startImpl(self);
    }

    pub fn stop(self: *GrpcLiteTransport) void {
        stopImpl(self);
    }

    pub fn localAddress(self: *const GrpcLiteTransport) !grpc.ServerLocalAddress {
        return self.server.?.localAddress();
    }

    pub fn port(self: *const GrpcLiteTransport) !u16 {
        return self.server.?.port();
    }

    /// grpc handler: decode the unary payload and push to the mailbox.
    /// Runs on grpc-lite's event-loop thread — must not touch raft state.
    fn handleMessage(
        context: ?*anyopaque,
        response_allocator: std.mem.Allocator,
        _: *grpc.ServerContext,
        request: []const u8,
    ) !grpc.UnaryResponse {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context.?));
        if (!self.accepting.load(.acquire)) return grpc.UnaryResponse.ok(response_allocator, &.{});
        var msg = codec_mod.decodeMessage(self.allocator, request) catch {
            log.warn("failed to decode inbound message ({} bytes)", .{request.len});
            return grpc.UnaryResponse.ok(response_allocator, &.{});
        };
        if (!self.accepting.load(.acquire)) {
            msg.deinit(self.allocator);
            return grpc.UnaryResponse.ok(response_allocator, &.{});
        }
        self.mailbox.push(msg) catch |err| {
            msg.deinit(self.allocator);
            return err;
        };
        return grpc.UnaryResponse.ok(response_allocator, &.{});
    }

    /// Drain inbound messages and deliver to the callback.
    pub fn pollOne(self: *GrpcLiteTransport) Error!bool {
        if (!self.accepting.load(.acquire)) return false;
        const cb = self.callback orelse return false;
        const msg = self.mailbox.pop() orelse return false;
        try cb.invoke(msg);
        return true;
    }

    // ---- Transport vtable impl ----

    fn startImpl(ctx: *anyopaque) Error!void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state != .initialized) return error.AlreadyStarted;
        self.state = .starting;
        self.server.?.start() catch |err| return mapStartError(err);
        self.state = .started;
        self.accepting.store(true, .release);
        const address = self.server.?.localAddress() catch {
            log.info("grpc transport listening on {s}", .{self.listen_addr});
            return;
        };
        log.info("grpc transport listening on {s}:{}", .{ address.host, address.port });
    }

    fn stopImpl(ctx: *anyopaque) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        while (true) {
            spinLock(&self.lifecycle_mutex);
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

        self.server.?.shutdown();
        self.server.?.wait();
        self.peer_manager.closeAll();
        self.mailbox.clear();

        spinLock(&self.lifecycle_mutex);
        self.state = .stopped;
        self.lifecycle_mutex.unlock();
    }

    fn addPeerImpl(ctx: *anyopaque, id: u64, addr: []const u8) Error!bool {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state == .stopping or self.state == .stopped) return error.ConnectionClosed;
        return self.peer_manager.addPeer(id, addr);
    }

    fn removePeerImpl(ctx: *anyopaque, id: u64) Error!void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state == .stopping or self.state == .stopped) return error.ConnectionClosed;
        self.peer_manager.removePeer(id);
    }

    fn sendImpl(ctx: *anyopaque, messages: []const Message) Error!void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        spinLock(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.state != .started) return error.ConnectionClosed;
        for (messages) |msg| {
            if (msg.to == 0) continue;
            const bytes = codec_mod.encodeMessage(self.allocator, msg) catch |err| return mapCodecError(err);
            defer self.allocator.free(bytes);
            try self.peer_manager.send(msg.to, raft_method_path, bytes);
        }
    }

    fn setMessageCallbackImpl(ctx: *anyopaque, cb: ?MessageCallback) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        self.callback = cb;
    }

    fn setPeerEventCallbackImpl(ctx: *anyopaque, cb: ?PeerEventCallback) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        self.peer_event_callback = cb;
    }

    fn pollOneImpl(ctx: *anyopaque) Error!bool {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        return self.pollOne();
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
    };

    pub fn transport(self: *GrpcLiteTransport) Transport {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
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
