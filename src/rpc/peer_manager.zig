//! Manages peer connections for the grpc-lite transport.
//!
//! Each peer is identified by node ID and has a network address ("host:port").
//! Channels are created lazily on first send and destroyed on removePeer.

const std = @import("std");
const grpc = @import("grpc_lite");
const Error = @import("../core/error.zig").Error;

pub const PeerInfo = struct {
    id: u64,
    addr: []const u8,
    channel: ?*grpc.Channel = null,
    connected: bool = false,
};

pub const PeerManager = struct {
    peers: std.AutoHashMap(u64, PeerInfo),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) PeerManager {
        return .{
            .peers = std.AutoHashMap(u64, PeerInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PeerManager) void {
        var it = self.peers.valueIterator();
        while (it.next()) |info| {
            if (info.channel) |ch| {
                ch.deinit();
                self.allocator.destroy(ch);
            }
            self.allocator.free(info.addr);
        }
        self.peers.deinit();
    }

    pub fn addPeer(self: *PeerManager, id: u64, addr: []const u8) !bool {
        if (self.peers.contains(id)) return false;
        const addr_copy = try self.allocator.dupe(u8, addr);
        errdefer self.allocator.free(addr_copy);
        try self.peers.put(id, .{
            .id = id,
            .addr = addr_copy,
        });
        return true;
    }

    pub fn removePeer(self: *PeerManager, id: u64) void {
        if (self.peers.fetchRemove(id)) |kv| {
            const info = kv.value;
            if (info.channel) |ch| {
                ch.deinit();
                self.allocator.destroy(ch);
            }
            self.allocator.free(info.addr);
        }
    }

    pub fn getPeer(self: *PeerManager, id: u64) ?*PeerInfo {
        return self.peers.getPtr(id);
    }

    pub fn hasPeer(self: PeerManager, id: u64) bool {
        return self.peers.contains(id);
    }

    /// Get or create a Channel for the given peer. Returns null if the peer
    /// is not registered or the connection failed.
    pub fn getOrCreateChannel(self: *PeerManager, id: u64) !?*grpc.Channel {
        const info = self.peers.getPtr(id) orelse return null;
        if (info.channel) |ch| return ch;

        // Create a new Channel.
        const ch = try self.allocator.create(grpc.Channel);
        errdefer self.allocator.destroy(ch);
        ch.* = try grpc.Channel.init(self.allocator, info.addr, .{});
        info.channel = ch;
        info.connected = true;
        return ch;
    }

    /// Send a unary RPC to the peer. Fire-and-forget: the response is
    /// checked for status but the body is discarded.
    pub fn send(self: *PeerManager, id: u64, method: []const u8, payload: []const u8) Error!void {
        const ch = self.getOrCreateChannel(id) catch |err| return mapTransportError(err);
        const channel = ch orelse return error.ConnectionClosed;
        var res = channel.callUnary(self.allocator, method, payload, .{
            .timeout_ns = 200 * std.time.ns_per_ms,
        }) catch |err| return mapTransportError(err);
        defer res.deinit();
        if (!res.status.isOk()) return mapStatus(res.status.code);
    }

    pub fn count(self: PeerManager) usize {
        return self.peers.count();
    }
};

fn mapTransportError(err: anyerror) Error {
    return if (err == error.OutOfMemory) error.OutOfMemory else error.ConnectionClosed;
}

fn mapStatus(code: grpc.StatusCode) Error {
    return switch (code) {
        .deadline_exceeded => error.Timeout,
        .data_loss, .invalid_argument => error.PayloadParseFailed,
        else => error.ConnectionClosed,
    };
}
