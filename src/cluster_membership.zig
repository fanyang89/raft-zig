const std = @import("std");

const ConfState = @import("core/types.zig").ConfState;

pub const ClusterId = [16]u8;

const magic = "RCLS";
const version: u32 = 1;
const header_size = magic.len + @sizeOf(u32) + @sizeOf(ClusterId) + @sizeOf(u32);
const encoded_peer_min_size = @sizeOf(u64) + @sizeOf(u32);

pub const StructuralError = error{
    InvalidClusterId,
    InvalidNodeId,
    EmptyAddress,
    PeersNotSorted,
    DuplicatePeer,
    RetiredNodeIdsNotSorted,
    DuplicateRetiredNodeId,
    ActiveRetiredOverlap,
};

pub const ValidationError = StructuralError || error{ConfStateMismatch};

pub const EncodeError = StructuralError || error{
    MembershipTooLarge,
    OutOfMemory,
};

pub const DecodeError = StructuralError || error{
    InvalidMagic,
    InvalidVersion,
    TruncatedData,
    TrailingData,
    LengthOverflow,
    OutOfMemory,
};

pub const PeerEndpoint = struct {
    node_id: u64,
    address: []u8,

    pub fn init(allocator: std.mem.Allocator, node_id: u64, address: []const u8) !PeerEndpoint {
        return .{
            .node_id = node_id,
            .address = if (address.len == 0) &.{} else try allocator.dupe(u8, address),
        };
    }

    pub fn deinit(self: *PeerEndpoint, allocator: std.mem.Allocator) void {
        if (self.address.len != 0) allocator.free(self.address);
        self.address = &.{};
    }

    pub fn clone(self: PeerEndpoint, allocator: std.mem.Allocator) !PeerEndpoint {
        return init(allocator, self.node_id, self.address);
    }
};

pub const ClusterMembership = struct {
    cluster_id: ClusterId,
    peers: []PeerEndpoint = &.{},
    retired_node_ids: []u64 = &.{},

    pub fn deinit(self: *ClusterMembership, allocator: std.mem.Allocator) void {
        for (self.peers) |*peer| peer.deinit(allocator);
        if (self.peers.len != 0) allocator.free(self.peers);
        if (self.retired_node_ids.len != 0) allocator.free(self.retired_node_ids);
        self.peers = &.{};
        self.retired_node_ids = &.{};
    }

    pub fn clone(self: ClusterMembership, allocator: std.mem.Allocator) !ClusterMembership {
        var peers: []PeerEndpoint = &.{};
        var initialized_peers: usize = 0;
        errdefer {
            for (peers[0..initialized_peers]) |*peer| peer.deinit(allocator);
            if (peers.len != 0) allocator.free(peers);
        }
        if (self.peers.len != 0) {
            peers = try allocator.alloc(PeerEndpoint, self.peers.len);
            for (self.peers) |peer| {
                peers[initialized_peers] = try peer.clone(allocator);
                initialized_peers += 1;
            }
        }

        var retired_node_ids: []u64 = &.{};
        if (self.retired_node_ids.len != 0) {
            retired_node_ids = try allocator.dupe(u64, self.retired_node_ids);
        }
        errdefer if (retired_node_ids.len != 0) allocator.free(retired_node_ids);

        return .{
            .cluster_id = self.cluster_id,
            .peers = peers,
            .retired_node_ids = retired_node_ids,
        };
    }

    pub fn addressOf(self: ClusterMembership, node_id: u64) ?[]const u8 {
        var low: usize = 0;
        var high = self.peers.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const peer = self.peers[mid];
            if (peer.node_id < node_id) {
                low = mid + 1;
            } else if (peer.node_id > node_id) {
                high = mid;
            } else {
                return peer.address;
            }
        }
        return null;
    }

    pub fn validate(self: ClusterMembership, conf_state: ConfState) ValidationError!void {
        try self.validateStructure();

        for (self.peers) |peer| {
            if (!confStateContains(conf_state, peer.node_id)) return error.ConfStateMismatch;
        }
        inline for (.{ conf_state.voters, conf_state.voters_outgoing, conf_state.learners, conf_state.learners_next }) |ids| {
            for (ids) |node_id| {
                if (node_id == 0) return error.InvalidNodeId;
                if (!self.containsPeer(node_id)) return error.ConfStateMismatch;
            }
        }
    }

    pub fn encode(self: ClusterMembership, allocator: std.mem.Allocator) EncodeError![]u8 {
        try self.validateStructure();

        var buffer: std.ArrayList(u8) = .empty;
        errdefer buffer.deinit(allocator);
        try buffer.ensureTotalCapacity(allocator, header_size);
        try buffer.appendSlice(allocator, magic);
        try appendInt(u32, allocator, &buffer, version);
        try buffer.appendSlice(allocator, &self.cluster_id);
        try appendLength(allocator, &buffer, self.peers.len);
        for (self.peers) |peer| {
            try appendInt(u64, allocator, &buffer, peer.node_id);
            try appendLength(allocator, &buffer, peer.address.len);
            try buffer.appendSlice(allocator, peer.address);
        }
        try appendLength(allocator, &buffer, self.retired_node_ids.len);
        for (self.retired_node_ids) |node_id| {
            try appendInt(u64, allocator, &buffer, node_id);
        }
        return buffer.toOwnedSlice(allocator);
    }

    pub fn decode(allocator: std.mem.Allocator, data: []const u8) DecodeError!ClusterMembership {
        return decodeMembership(allocator, data);
    }

    fn validateStructure(self: ClusterMembership) StructuralError!void {
        if (isZeroClusterId(self.cluster_id)) return error.InvalidClusterId;

        for (self.peers, 0..) |peer, index| {
            if (peer.node_id == 0) return error.InvalidNodeId;
            if (peer.address.len == 0) return error.EmptyAddress;
            if (index != 0) {
                const previous = self.peers[index - 1].node_id;
                if (previous == peer.node_id) return error.DuplicatePeer;
                if (previous > peer.node_id) return error.PeersNotSorted;
            }
        }

        for (self.retired_node_ids, 0..) |node_id, index| {
            if (node_id == 0) return error.InvalidNodeId;
            if (index != 0) {
                const previous = self.retired_node_ids[index - 1];
                if (previous == node_id) return error.DuplicateRetiredNodeId;
                if (previous > node_id) return error.RetiredNodeIdsNotSorted;
            }
        }

        var peer_index: usize = 0;
        var retired_index: usize = 0;
        while (peer_index < self.peers.len and retired_index < self.retired_node_ids.len) {
            const active = self.peers[peer_index].node_id;
            const retired = self.retired_node_ids[retired_index];
            if (active < retired) {
                peer_index += 1;
            } else if (active > retired) {
                retired_index += 1;
            } else {
                return error.ActiveRetiredOverlap;
            }
        }
    }

    fn containsPeer(self: ClusterMembership, node_id: u64) bool {
        return self.addressOf(node_id) != null;
    }
};

pub fn decode(allocator: std.mem.Allocator, data: []const u8) DecodeError!ClusterMembership {
    return decodeMembership(allocator, data);
}

fn decodeMembership(allocator: std.mem.Allocator, data: []const u8) DecodeError!ClusterMembership {
    var decoder = Decoder{ .data = data };
    if (!std.mem.eql(u8, try decoder.take(magic.len), magic)) return error.InvalidMagic;
    if (try decoder.readInt(u32) != version) return error.InvalidVersion;

    var cluster_id: ClusterId = undefined;
    @memcpy(&cluster_id, try decoder.take(cluster_id.len));

    const peer_count = try decoder.readInt(u32);
    const min_peer_bytes = std.math.mul(usize, peer_count, encoded_peer_min_size) catch
        return error.LengthOverflow;
    const min_remaining = std.math.add(usize, min_peer_bytes, @sizeOf(u32)) catch
        return error.LengthOverflow;
    if (min_remaining > decoder.remaining()) return error.TruncatedData;

    var peers: []PeerEndpoint = &.{};
    var initialized_peers: usize = 0;
    errdefer {
        for (peers[0..initialized_peers]) |*peer| peer.deinit(allocator);
        if (peers.len != 0) allocator.free(peers);
    }
    if (peer_count != 0) {
        peers = try allocator.alloc(PeerEndpoint, peer_count);
        for (peers) |*peer| {
            const node_id = try decoder.readInt(u64);
            const address = try decoder.readBytes(allocator);
            peer.* = .{ .node_id = node_id, .address = address };
            initialized_peers += 1;
        }
    }

    const retired_count = try decoder.readInt(u32);
    const retired_bytes_len = std.math.mul(usize, retired_count, @sizeOf(u64)) catch
        return error.LengthOverflow;
    const retired_bytes = try decoder.take(retired_bytes_len);
    var retired_node_ids: []u64 = &.{};
    errdefer if (retired_node_ids.len != 0) allocator.free(retired_node_ids);
    if (retired_count != 0) {
        retired_node_ids = try allocator.alloc(u64, retired_count);
        for (retired_node_ids, 0..) |*node_id, index| {
            node_id.* = std.mem.readInt(u64, retired_bytes[index * @sizeOf(u64) ..][0..@sizeOf(u64)], .little);
        }
    }

    if (decoder.remaining() != 0) return error.TrailingData;
    const membership = ClusterMembership{
        .cluster_id = cluster_id,
        .peers = peers,
        .retired_node_ids = retired_node_ids,
    };
    try membership.validateStructure();
    return membership;
}

pub fn collectEffectiveMemberIds(allocator: std.mem.Allocator, conf_state: ConfState) ![]u64 {
    var ids: std.ArrayList(u64) = .empty;
    defer ids.deinit(allocator);

    inline for (.{ conf_state.voters, conf_state.voters_outgoing, conf_state.learners, conf_state.learners_next }) |members| {
        try ids.appendSlice(allocator, members);
    }
    if (ids.items.len == 0) return &.{};

    std.mem.sort(u64, ids.items, {}, std.sort.asc(u64));
    var unique_len: usize = 1;
    for (ids.items[1..]) |node_id| {
        if (node_id != ids.items[unique_len - 1]) {
            ids.items[unique_len] = node_id;
            unique_len += 1;
        }
    }
    ids.items.len = unique_len;
    return ids.toOwnedSlice(allocator);
}

fn isZeroClusterId(cluster_id: ClusterId) bool {
    for (cluster_id) |byte| {
        if (byte != 0) return false;
    }
    return true;
}

fn confStateContains(conf_state: ConfState, node_id: u64) bool {
    inline for (.{ conf_state.voters, conf_state.voters_outgoing, conf_state.learners, conf_state.learners_next }) |ids| {
        for (ids) |member_id| {
            if (member_id == node_id) return true;
        }
    }
    return false;
}

fn appendLength(allocator: std.mem.Allocator, buffer: *std.ArrayList(u8), len: usize) EncodeError!void {
    const encoded = std.math.cast(u32, len) orelse return error.MembershipTooLarge;
    try appendInt(u32, allocator, buffer, encoded);
}

fn appendInt(comptime T: type, allocator: std.mem.Allocator, buffer: *std.ArrayList(u8), value: T) error{OutOfMemory}!void {
    var bytes: [@divExact(@bitSizeOf(T), 8)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try buffer.appendSlice(allocator, &bytes);
}

const Decoder = struct {
    data: []const u8,
    pos: usize = 0,

    fn remaining(self: Decoder) usize {
        return self.data.len - self.pos;
    }

    fn take(self: *Decoder, len: usize) DecodeError![]const u8 {
        const end = std.math.add(usize, self.pos, len) catch return error.LengthOverflow;
        if (end > self.data.len) return error.TruncatedData;
        const bytes = self.data[self.pos..end];
        self.pos = end;
        return bytes;
    }

    fn readInt(self: *Decoder, comptime T: type) DecodeError!T {
        const size = @divExact(@bitSizeOf(T), 8);
        return std.mem.readInt(T, (try self.take(size))[0..size], .little);
    }

    fn readBytes(self: *Decoder, allocator: std.mem.Allocator) DecodeError![]u8 {
        const bytes = try self.take(try self.readInt(u32));
        return if (bytes.len == 0) &.{} else try allocator.dupe(u8, bytes);
    }
};

fn testMembership(allocator: std.mem.Allocator) !ClusterMembership {
    var peers = try allocator.alloc(PeerEndpoint, 3);
    var initialized: usize = 0;
    errdefer allocator.free(peers);
    errdefer for (peers[0..initialized]) |*peer| peer.deinit(allocator);
    peers[0] = try PeerEndpoint.init(allocator, 1, "127.0.0.1:7001");
    initialized += 1;
    peers[1] = try PeerEndpoint.init(allocator, 2, "127.0.0.1:7002");
    initialized += 1;
    peers[2] = try PeerEndpoint.init(allocator, 4, "127.0.0.1:7004");
    initialized += 1;

    return .{
        .cluster_id = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
        .peers = peers,
        .retired_node_ids = try allocator.dupe(u64, &.{3}),
    };
}

fn expectMembershipEqual(expected: ClusterMembership, actual: ClusterMembership) !void {
    try std.testing.expectEqual(expected.cluster_id, actual.cluster_id);
    try std.testing.expectEqual(expected.peers.len, actual.peers.len);
    for (expected.peers, actual.peers) |expected_peer, actual_peer| {
        try std.testing.expectEqual(expected_peer.node_id, actual_peer.node_id);
        try std.testing.expectEqualStrings(expected_peer.address, actual_peer.address);
    }
    try std.testing.expectEqualSlices(u64, expected.retired_node_ids, actual.retired_node_ids);
}

test "cluster membership round trip and address lookup" {
    const allocator = std.testing.allocator;
    var membership = try testMembership(allocator);
    defer membership.deinit(allocator);
    const conf_state = ConfState{ .voters = @constCast(&[_]u64{ 1, 2 }), .learners = @constCast(&[_]u64{4}) };
    try membership.validate(conf_state);

    const encoded = try membership.encode(allocator);
    defer allocator.free(encoded);
    try std.testing.expectEqualStrings(magic, encoded[0..magic.len]);
    try std.testing.expectEqual(version, std.mem.readInt(u32, encoded[magic.len..][0..4], .little));

    var decoded = try decode(allocator, encoded);
    defer decoded.deinit(allocator);
    try expectMembershipEqual(membership, decoded);
    try std.testing.expectEqualStrings("127.0.0.1:7002", decoded.addressOf(2).?);
    try std.testing.expect(decoded.addressOf(3) == null);
}

test "cluster membership clone owns independent data" {
    const allocator = std.testing.allocator;
    var membership = try testMembership(allocator);
    defer membership.deinit(allocator);
    var cloned = try membership.clone(allocator);
    defer cloned.deinit(allocator);

    cloned.peers[0].address[0] = 'X';
    cloned.retired_node_ids[0] = 9;
    try std.testing.expectEqual(@as(u8, '1'), membership.peers[0].address[0]);
    try std.testing.expectEqual(@as(u64, 3), membership.retired_node_ids[0]);
}

test "cluster membership rejects zero cluster ID" {
    var membership = try testMembership(std.testing.allocator);
    defer membership.deinit(std.testing.allocator);
    membership.cluster_id = @splat(0);
    try std.testing.expectError(error.InvalidClusterId, membership.validate(.{}));
    try std.testing.expectError(error.InvalidClusterId, membership.encode(std.testing.allocator));
}

test "cluster membership rejects duplicate and unordered IDs" {
    var membership = try testMembership(std.testing.allocator);
    defer membership.deinit(std.testing.allocator);

    membership.peers[1].node_id = membership.peers[0].node_id;
    try std.testing.expectError(error.DuplicatePeer, membership.validate(.{}));
    membership.peers[0].node_id = 0;
    try std.testing.expectError(error.InvalidNodeId, membership.validate(.{}));
    membership.peers[0].node_id = 1;
    membership.peers[1].node_id = 2;
    membership.peers[2].node_id = 1;
    try std.testing.expectError(error.PeersNotSorted, membership.validate(.{}));
    membership.peers[2].node_id = 4;

    const original_retired = membership.retired_node_ids;
    membership.retired_node_ids = try std.testing.allocator.dupe(u64, &.{ 3, 3 });
    std.testing.allocator.free(original_retired);
    try std.testing.expectError(error.DuplicateRetiredNodeId, membership.validate(.{}));
    membership.retired_node_ids[0] = 5;
    try std.testing.expectError(error.RetiredNodeIdsNotSorted, membership.validate(.{}));
}

test "cluster membership rejects empty address and active retired overlap" {
    var membership = try testMembership(std.testing.allocator);
    defer membership.deinit(std.testing.allocator);

    const address = membership.peers[0].address;
    membership.peers[0].address = &.{};
    try std.testing.expectError(error.EmptyAddress, membership.validate(.{}));
    membership.peers[0].address = address;
    membership.retired_node_ids[0] = 2;
    try std.testing.expectError(error.ActiveRetiredOverlap, membership.validate(.{}));
}

test "effective members include joint voters and staged learners" {
    const allocator = std.testing.allocator;
    const conf_state = ConfState{
        .voters = @constCast(&[_]u64{ 1, 2 }),
        .voters_outgoing = @constCast(&[_]u64{ 2, 4 }),
        .learners = @constCast(&[_]u64{5}),
        .learners_next = @constCast(&[_]u64{ 4, 6 }),
    };
    const ids = try collectEffectiveMemberIds(allocator, conf_state);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 4, 5, 6 }, ids);

    var peers = try allocator.alloc(PeerEndpoint, ids.len);
    var initialized: usize = 0;
    errdefer allocator.free(peers);
    errdefer for (peers[0..initialized]) |*peer| peer.deinit(allocator);
    for (ids, 0..) |node_id, index| {
        peers[index] = try PeerEndpoint.init(allocator, node_id, "node");
        initialized += 1;
    }
    var membership = ClusterMembership{
        .cluster_id = .{1} ++ .{0} ** 15,
        .peers = peers,
    };
    defer membership.deinit(allocator);
    try membership.validate(conf_state);

    membership.peers[4].node_id = 7;
    try std.testing.expectError(error.ConfStateMismatch, membership.validate(conf_state));
}

test "cluster membership decoder rejects malformed truncated and trailing data" {
    const allocator = std.testing.allocator;
    var membership = try testMembership(allocator);
    defer membership.deinit(allocator);
    const encoded = try membership.encode(allocator);
    defer allocator.free(encoded);

    for (0..encoded.len) |len| {
        try std.testing.expectError(error.TruncatedData, decode(allocator, encoded[0..len]));
    }

    var bad_magic = try allocator.dupe(u8, encoded);
    defer allocator.free(bad_magic);
    bad_magic[0] = 'X';
    try std.testing.expectError(error.InvalidMagic, decode(allocator, bad_magic));

    var bad_version = try allocator.dupe(u8, encoded);
    defer allocator.free(bad_version);
    std.mem.writeInt(u32, bad_version[magic.len..][0..4], version + 1, .little);
    try std.testing.expectError(error.InvalidVersion, decode(allocator, bad_version));

    var oversized_count = try allocator.dupe(u8, encoded);
    defer allocator.free(oversized_count);
    std.mem.writeInt(u32, oversized_count[magic.len + 4 + 16 ..][0..4], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.TruncatedData, decode(allocator, oversized_count));

    const trailing = try std.mem.concat(allocator, u8, &.{ encoded, "x" });
    defer allocator.free(trailing);
    try std.testing.expectError(error.TrailingData, decode(allocator, trailing));
}

test "cluster membership allocation failures clean up" {
    const Helper = struct {
        fn run(
            allocator: std.mem.Allocator,
            source: *const ClusterMembership,
            encoded: []const u8,
            conf_state: ConfState,
        ) !void {
            var cloned = try source.clone(allocator);
            defer cloned.deinit(allocator);
            const cloned_encoding = try cloned.encode(allocator);
            defer allocator.free(cloned_encoding);
            var decoded = try decode(allocator, encoded);
            defer decoded.deinit(allocator);
            const ids = try collectEffectiveMemberIds(allocator, conf_state);
            defer allocator.free(ids);
        }
    };

    const allocator = std.testing.allocator;
    var membership = try testMembership(allocator);
    defer membership.deinit(allocator);
    const encoded = try membership.encode(allocator);
    defer allocator.free(encoded);
    const conf_state = ConfState{ .voters = @constCast(&[_]u64{ 1, 2 }), .learners = @constCast(&[_]u64{4}) };
    try std.testing.checkAllAllocationFailures(allocator, Helper.run, .{ &membership, encoded, conf_state });
}
