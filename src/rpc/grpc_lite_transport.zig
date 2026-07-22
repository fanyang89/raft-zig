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
const InboundMailbox = inbound_mailbox_mod.InboundMailbox;
const PeerManager = peer_manager_mod.PeerManager;

const log = std.log.scoped(.raft_zig_grpc_transport);

const raft_method_path = "/raft.Raft/SendMessage";

/// grpc-lite backed transport. Must be heap-allocated (via `create`) because
/// the Server's handler captures a pointer to the internal mailbox.
pub const GrpcLiteTransport = struct {
    allocator: std.mem.Allocator,
    server: ?*grpc.Server = null,
    peer_manager: PeerManager,
    mailbox: InboundMailbox,
    callback: ?MessageCallback = null,
    listen_addr: []const u8,
    addr_copy: []u8,

    /// Create a transport that listens on `host:port`.
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
        const port = std.fmt.parseInt(u16, port_str, 10) catch return error.AddressPortInvalid;

        // Create and start the grpc server.
        const srv = try allocator.create(grpc.Server);
        errdefer allocator.destroy(srv);
        srv.* = try grpc.Server.init(allocator, .{
            .host = host,
            .port = port,
        });
        errdefer srv.deinit();
        self.server = srv;

        try srv.registerUnary(raft_method_path, .{
            .context = self,
            .invoke_fn = handleMessage,
        });
        try srv.start();

        log.info("grpc transport listening on {s}", .{listen_addr});
        return self;
    }

    pub fn destroy(self: *GrpcLiteTransport) void {
        if (self.server) |srv| {
            srv.deinit();
            self.allocator.destroy(srv);
        }
        self.peer_manager.deinit();
        self.mailbox.deinit();
        self.allocator.free(self.addr_copy);
        self.allocator.destroy(self);
    }

    /// grpc handler: decode the unary payload and push to the mailbox.
    /// Runs on grpc-lite's libuv thread — must not touch raft state.
    fn handleMessage(
        context: ?*anyopaque,
        allocator: std.mem.Allocator,
        _: *grpc.ServerContext,
        request: []const u8,
    ) !grpc.UnaryResponse {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(context.?));
        const msg = codec_mod.decodeMessage(allocator, request) catch {
            log.warn("failed to decode inbound message ({} bytes)", .{request.len});
            return grpc.UnaryResponse.ok(allocator, &.{});
        };
        self.mailbox.push(msg);
        return grpc.UnaryResponse.ok(allocator, &.{});
    }

    /// Drain inbound messages and deliver to the callback.
    pub fn poll(self: *GrpcLiteTransport) void {
        if (self.mailbox.empty()) return;
        const cb = self.callback orelse return;
        const msgs = self.mailbox.drain() catch return;
        defer self.allocator.free(msgs);
        for (msgs) |msg| {
            cb.invoke(msg);
        }
    }

    // ---- Transport vtable impl ----

    fn startImpl(_: *anyopaque) Error!void {}
    fn stopImpl(_: *anyopaque) void {}

    fn addPeerImpl(ctx: *anyopaque, id: u64, addr: []const u8) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        self.peer_manager.addPeer(id, addr) catch {};
    }

    fn removePeerImpl(ctx: *anyopaque, id: u64) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        self.peer_manager.removePeer(id);
    }

    fn sendImpl(ctx: *anyopaque, messages: []const Message) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        for (messages) |msg| {
            if (msg.to == 0) continue;
            const bytes = codec_mod.encodeMessage(self.allocator, msg) catch continue;
            defer self.allocator.free(bytes);
            self.peer_manager.send(msg.to, raft_method_path, bytes);
        }
    }

    fn setMessageCallbackImpl(ctx: *anyopaque, cb: MessageCallback) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        self.callback = cb;
    }

    fn pollImpl(ctx: *anyopaque) void {
        const self: *GrpcLiteTransport = @ptrCast(@alignCast(ctx));
        self.poll();
    }

    pub const vtable: Transport.VTable = .{
        .start = startImpl,
        .stop = stopImpl,
        .add_peer = addPeerImpl,
        .remove_peer = removePeerImpl,
        .send = sendImpl,
        .set_message_callback = setMessageCallbackImpl,
        .poll = pollImpl,
    };

    pub fn transport(self: *GrpcLiteTransport) Transport {
        return .{ .ctx = self, .vtable = &vtable };
    }
};
