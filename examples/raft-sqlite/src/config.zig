const std = @import("std");

const raft = @import("raft_zig");

pub const ServerConfig = struct {
    allocator: std.mem.Allocator,
    node_id: u64,
    cluster_id: raft.ClusterId,
    api_host: []u8,
    api_port: u16,
    raft_listen: []u8,
    raft_advertise: []u8,
    data_dir: []u8,
    peers: []raft.Peer,
    join: bool,

    pub fn deinit(self: *ServerConfig) void {
        for (self.peers) |peer| self.allocator.free(peer.context.?);
        self.allocator.free(self.peers);
        self.allocator.free(self.data_dir);
        self.allocator.free(self.raft_advertise);
        self.allocator.free(self.raft_listen);
        self.allocator.free(self.api_host);
        self.* = undefined;
    }
};

pub fn parseServer(allocator: std.mem.Allocator, arguments: []const []const u8) !ServerConfig {
    var node_id: ?u64 = null;
    var cluster_id: ?raft.ClusterId = null;
    var api_listen: ?[]const u8 = null;
    var raft_listen: ?[]const u8 = null;
    var raft_advertise: ?[]const u8 = null;
    var data_dir: ?[]const u8 = null;
    var join = false;
    var peer_values: std.ArrayList([]const u8) = .empty;
    defer peer_values.deinit(allocator);
    var index: usize = 0;
    while (index < arguments.len) : (index += 1) {
        const argument = arguments[index];
        if (std.mem.eql(u8, argument, "--join")) {
            join = true;
            continue;
        }
        if (index + 1 == arguments.len) return error.MissingOptionValue;
        const value = arguments[index + 1];
        index += 1;
        if (std.mem.eql(u8, argument, "--node-id")) {
            node_id = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, argument, "--cluster-id")) {
            cluster_id = try parseClusterId(value);
        } else if (std.mem.eql(u8, argument, "--api-listen")) {
            api_listen = value;
        } else if (std.mem.eql(u8, argument, "--raft-listen")) {
            raft_listen = value;
        } else if (std.mem.eql(u8, argument, "--raft-advertise")) {
            raft_advertise = value;
        } else if (std.mem.eql(u8, argument, "--data-dir")) {
            data_dir = value;
        } else if (std.mem.eql(u8, argument, "--peer")) {
            try peer_values.append(allocator, value);
        } else {
            return error.UnknownOption;
        }
    }

    const resolved_node_id = node_id orelse return error.NodeIdRequired;
    if (resolved_node_id == 0) return error.InvalidNodeId;
    const resolved_api = try parseEndpoint(api_listen orelse return error.ApiListenRequired);
    const resolved_raft_listen = raft_listen orelse return error.RaftListenRequired;
    _ = try parseEndpoint(resolved_raft_listen);
    const resolved_advertise = raft_advertise orelse resolved_raft_listen;
    _ = try parseEndpoint(resolved_advertise);
    const resolved_data_dir = data_dir orelse return error.DataDirRequired;
    if (resolved_data_dir.len == 0) return error.DataDirRequired;

    const api_host = try allocator.dupe(u8, resolved_api.host);
    errdefer allocator.free(api_host);
    const owned_raft_listen = try allocator.dupe(u8, resolved_raft_listen);
    errdefer allocator.free(owned_raft_listen);
    const owned_advertise = try allocator.dupe(u8, resolved_advertise);
    errdefer allocator.free(owned_advertise);
    const owned_data_dir = try allocator.dupe(u8, resolved_data_dir);
    errdefer allocator.free(owned_data_dir);
    const peers = try allocator.alloc(raft.Peer, peer_values.items.len);
    errdefer allocator.free(peers);
    var initialized: usize = 0;
    errdefer for (peers[0..initialized]) |peer| allocator.free(peer.context.?);
    for (peer_values.items, 0..) |peer_text, peer_index| {
        const parsed = try parsePeer(peer_text);
        if (join and parsed.id == resolved_node_id) return error.JoinPeerMatchesLocalNode;
        if (join and (std.mem.eql(u8, parsed.address, resolved_raft_listen) or
            std.mem.eql(u8, parsed.address, resolved_advertise))) return error.JoinPeerMatchesLocalAddress;
        for (peers[0..initialized]) |existing| {
            if (existing.id == parsed.id) return error.DuplicatePeerId;
            if (std.mem.eql(u8, existing.context.?, parsed.address)) return error.DuplicatePeerAddress;
        }
        peers[peer_index] = .{ .id = parsed.id, .context = try allocator.dupe(u8, parsed.address) };
        initialized += 1;
    }
    if (join and peers.len == 0) return error.JoinPeerRequired;

    return .{
        .allocator = allocator,
        .node_id = resolved_node_id,
        .cluster_id = cluster_id orelse return error.ClusterIdRequired,
        .api_host = api_host,
        .api_port = resolved_api.port,
        .raft_listen = owned_raft_listen,
        .raft_advertise = owned_advertise,
        .data_dir = owned_data_dir,
        .peers = peers,
        .join = join,
    };
}

const Endpoint = struct {
    host: []const u8,
    port: u16,
};

pub fn parseEndpoint(value: []const u8) !Endpoint {
    const separator = std.mem.lastIndexOfScalar(u8, value, ':') orelse return error.InvalidEndpoint;
    if (separator == 0 or separator + 1 == value.len) return error.InvalidEndpoint;
    const port = try std.fmt.parseUnsigned(u16, value[separator + 1 ..], 10);
    if (port == 0) return error.InvalidEndpoint;
    const host = value[0..separator];
    _ = std.Io.net.IpAddress.parseIp4(host, port) catch return error.InvalidEndpoint;
    return .{ .host = host, .port = port };
}

const ParsedPeer = struct {
    id: u64,
    address: []const u8,
};

fn parsePeer(value: []const u8) !ParsedPeer {
    const separator = std.mem.indexOfScalar(u8, value, '=') orelse return error.InvalidPeer;
    if (separator == 0 or separator + 1 == value.len) return error.InvalidPeer;
    const id = try std.fmt.parseUnsigned(u64, value[0..separator], 10);
    if (id == 0) return error.InvalidPeer;
    const address = value[separator + 1 ..];
    _ = try parseEndpoint(address);
    return .{ .id = id, .address = address };
}

fn parseClusterId(value: []const u8) !raft.ClusterId {
    var result: raft.ClusterId = undefined;
    var output_index: usize = 0;
    var high_nibble: ?u8 = null;
    for (value) |byte| {
        if (byte == '-') continue;
        const nibble = std.fmt.charToDigit(byte, 16) catch return error.InvalidClusterId;
        if (high_nibble) |high| {
            if (output_index == result.len) return error.InvalidClusterId;
            result[output_index] = high << 4 | nibble;
            output_index += 1;
            high_nibble = null;
        } else {
            high_nibble = nibble;
        }
    }
    if (output_index != result.len or high_nibble != null or std.mem.allEqual(u8, &result, 0)) {
        return error.InvalidClusterId;
    }
    return result;
}

test "parse a three-node server configuration" {
    const arguments = [_][]const u8{
        "--node-id",     "2",
        "--cluster-id",  "0198f54d-5c2a-7000-8000-000000000001",
        "--api-listen",  "127.0.0.1:8002",
        "--raft-listen", "127.0.0.1:9002",
        "--data-dir",    "/tmp/raft-sqlite-2",
        "--peer",        "1=127.0.0.1:9001",
        "--peer",        "2=127.0.0.1:9002",
        "--peer",        "3=127.0.0.1:9003",
    };
    var parsed = try parseServer(std.testing.allocator, &arguments);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 2), parsed.node_id);
    try std.testing.expectEqual(@as(usize, 3), parsed.peers.len);
    try std.testing.expectEqualStrings("127.0.0.1", parsed.api_host);
    try std.testing.expectEqual(@as(u16, 8002), parsed.api_port);
    try std.testing.expect(!parsed.join);
}

test "parse a joining server configuration" {
    const arguments = [_][]const u8{
        "--join",
        "--node-id",
        "2",
        "--cluster-id",
        "0198f54d-5c2a-7000-8000-000000000001",
        "--api-listen",
        "127.0.0.1:8002",
        "--raft-listen",
        "127.0.0.1:9002",
        "--data-dir",
        "/tmp/raft-sqlite-2",
        "--peer",
        "1=127.0.0.1:9001",
    };
    var parsed = try parseServer(std.testing.allocator, &arguments);
    defer parsed.deinit();
    try std.testing.expect(parsed.join);
    try std.testing.expectEqual(@as(usize, 1), parsed.peers.len);
}

test "joining configuration requires a remote seed" {
    const base = [_][]const u8{
        "--join",
        "--node-id",
        "2",
        "--cluster-id",
        "0198f54d-5c2a-7000-8000-000000000001",
        "--api-listen",
        "127.0.0.1:8002",
        "--raft-listen",
        "127.0.0.1:9002",
        "--data-dir",
        "/tmp/raft-sqlite-2",
    };
    try std.testing.expectError(error.JoinPeerRequired, parseServer(std.testing.allocator, &base));

    const local_seed = base ++ [_][]const u8{ "--peer", "2=127.0.0.1:9002" };
    try std.testing.expectError(error.JoinPeerMatchesLocalNode, parseServer(std.testing.allocator, &local_seed));

    const local_address = base ++ [_][]const u8{ "--peer", "1=127.0.0.1:9002" };
    try std.testing.expectError(error.JoinPeerMatchesLocalAddress, parseServer(std.testing.allocator, &local_address));
}
