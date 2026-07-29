const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");

const Error = error_model.Error;
const ConfChangeV2 = types.ConfChangeV2;
const ConfState = types.ConfState;

pub const ClusterId = [16]u8;

const magic = "RCLS";
const version: u32 = 1;
const header_size = magic.len + @sizeOf(u32) + @sizeOf(ClusterId) + @sizeOf(u32);
const encoded_peer_min_size = @sizeOf(u64) + @sizeOf(u32);
const membership_context_magic = "RMC1";
const membership_context_version: u32 = 1;
const membership_context_header_size = membership_context_magic.len + @sizeOf(u32) + @sizeOf(u32);

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

pub const MembershipContextEncodeError = error{
    InvalidNodeId,
    EmptyAddress,
    PeersNotSorted,
    DuplicatePeer,
    MembershipTooLarge,
    OutOfMemory,
};

pub const MembershipContextDecodeError = DecodeError;

pub const MembershipContext = struct {
    endpoints: []PeerEndpoint = &.{},

    pub fn deinit(self: *MembershipContext, allocator: std.mem.Allocator) void {
        for (self.endpoints) |*endpoint| endpoint.deinit(allocator);
        if (self.endpoints.len != 0) allocator.free(self.endpoints);
        self.endpoints = &.{};
    }

    pub fn encode(self: MembershipContext, allocator: std.mem.Allocator) MembershipContextEncodeError![]u8 {
        try validateEndpoints(self.endpoints);

        var buffer: std.ArrayList(u8) = .empty;
        errdefer buffer.deinit(allocator);
        try buffer.ensureTotalCapacity(allocator, membership_context_header_size);
        try buffer.appendSlice(allocator, membership_context_magic);
        try appendInt(u32, allocator, &buffer, membership_context_version);
        try appendMembershipContextLength(allocator, &buffer, self.endpoints.len);
        for (self.endpoints) |endpoint| {
            try appendInt(u64, allocator, &buffer, endpoint.node_id);
            try appendMembershipContextLength(allocator, &buffer, endpoint.address.len);
            try buffer.appendSlice(allocator, endpoint.address);
        }
        return buffer.toOwnedSlice(allocator);
    }

    pub fn decode(allocator: std.mem.Allocator, data: []const u8) MembershipContextDecodeError!MembershipContext {
        return decodeMembershipContext(allocator, data);
    }
};

pub fn decodeMembershipContext(allocator: std.mem.Allocator, data: []const u8) MembershipContextDecodeError!MembershipContext {
    var decoder = Decoder{ .data = data };
    if (!std.mem.eql(u8, try decoder.take(membership_context_magic.len), membership_context_magic)) {
        return error.InvalidMagic;
    }
    if (try decoder.readInt(u32) != membership_context_version) return error.InvalidVersion;

    const endpoint_count = try decoder.readInt(u32);
    const min_endpoint_bytes = std.math.mul(usize, endpoint_count, encoded_peer_min_size) catch
        return error.LengthOverflow;
    if (min_endpoint_bytes > decoder.remaining()) return error.TruncatedData;

    var endpoints: []PeerEndpoint = &.{};
    var initialized_endpoints: usize = 0;
    errdefer {
        for (endpoints[0..initialized_endpoints]) |*endpoint| endpoint.deinit(allocator);
        if (endpoints.len != 0) allocator.free(endpoints);
    }
    if (endpoint_count != 0) {
        endpoints = try allocator.alloc(PeerEndpoint, endpoint_count);
        for (endpoints) |*endpoint| {
            const node_id = try decoder.readInt(u64);
            const address = try decoder.readBytes(allocator);
            endpoint.* = .{ .node_id = node_id, .address = address };
            initialized_endpoints += 1;
        }
    }

    if (decoder.remaining() != 0) return error.TrailingData;
    try validateEndpoints(endpoints);
    return .{ .endpoints = endpoints };
}

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

    pub fn eql(self: ClusterMembership, other: ClusterMembership) bool {
        if (!std.mem.eql(u8, &self.cluster_id, &other.cluster_id) or
            !std.mem.eql(u64, self.retired_node_ids, other.retired_node_ids) or
            self.peers.len != other.peers.len) return false;
        for (self.peers, other.peers) |peer, other_peer| {
            if (peer.node_id != other_peer.node_id or !std.mem.eql(u8, peer.address, other_peer.address)) return false;
        }
        return true;
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

pub fn deriveClusterMembership(
    allocator: std.mem.Allocator,
    current: ClusterMembership,
    previous_conf_state: ConfState,
    new_conf_state: ConfState,
    conf_change: ConfChangeV2,
) Error!ClusterMembership {
    current.validate(previous_conf_state) catch return error.InvalidClusterMembership;

    const previous_ids = collectEffectiveMemberIds(allocator, previous_conf_state) catch return error.OutOfMemory;
    defer if (previous_ids.len != 0) allocator.free(previous_ids);
    const new_ids = collectEffectiveMemberIds(allocator, new_conf_state) catch return error.OutOfMemory;
    defer if (new_ids.len != 0) allocator.free(new_ids);

    for (new_ids) |node_id| {
        if (node_id == 0) return error.InvalidClusterMembership;
        if (containsSorted(current.retired_node_ids, node_id)) return error.RetiredNodeId;
    }
    for (conf_change.changes) |change| {
        if (change.node_id == 0) continue;
        switch (change.change_type) {
            .add_node, .add_learner_node, .update_node => {
                if (containsSorted(current.retired_node_ids, change.node_id)) return error.RetiredNodeId;
            },
            .remove_node => {},
        }
    }

    var decoded_context: ?MembershipContext = null;
    defer if (decoded_context) |*context| context.deinit(allocator);
    var legacy_endpoint: [1]PeerEndpoint = undefined;
    const endpoints: []const PeerEndpoint = if (std.mem.startsWith(u8, conf_change.context, membership_context_magic)) blk: {
        decoded_context = decodeMembershipContext(allocator, conf_change.context) catch |err| return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.DuplicatePeer => error.ConflictingPeerAddress,
            else => error.MalformedMembershipContext,
        };
        break :blk decoded_context.?.endpoints;
    } else if (conf_change.context.len != 0) blk: {
        var add_change: ?types.ConfChangeSingle = null;
        for (conf_change.changes) |change| switch (change.change_type) {
            .add_node, .add_learner_node => {
                if (add_change != null or change.node_id == 0) return error.MalformedMembershipContext;
                add_change = change;
            },
            .remove_node, .update_node => {},
        };
        const change = add_change orelse return error.MalformedMembershipContext;
        legacy_endpoint[0] = .{ .node_id = change.node_id, .address = conf_change.context };
        break :blk legacy_endpoint[0..];
    } else &.{};

    for (conf_change.changes, 0..) |change, change_index| {
        if (change.change_type != .update_node or change.node_id == 0) continue;
        for (conf_change.changes[0..change_index]) |previous_change| {
            if (previous_change.change_type == .update_node and previous_change.node_id == change.node_id) {
                return error.ConflictingPeerAddress;
            }
        }
        if (!containsSorted(previous_ids, change.node_id) or !containsSorted(new_ids, change.node_id)) {
            return error.ConflictingPeerAddress;
        }
        if (addressInEndpoints(endpoints, change.node_id) == null) return error.MissingPeerAddress;
    }

    for (new_ids) |node_id| {
        if (!containsSorted(previous_ids, node_id) and addressInEndpoints(endpoints, node_id) == null) {
            return error.MissingPeerAddress;
        }
    }

    for (endpoints) |endpoint| {
        const newly_effective = !containsSorted(previous_ids, endpoint.node_id) and containsSorted(new_ids, endpoint.node_id);
        const updates_existing = countChanges(conf_change, .update_node, endpoint.node_id) == 1 and
            containsSorted(previous_ids, endpoint.node_id) and containsSorted(new_ids, endpoint.node_id);
        if (!newly_effective and !updates_existing) return error.UnexpectedPeerAddress;
    }

    var peers: []PeerEndpoint = &.{};
    var initialized_peers: usize = 0;
    errdefer {
        for (peers[0..initialized_peers]) |*peer| peer.deinit(allocator);
        if (peers.len != 0) allocator.free(peers);
    }
    if (new_ids.len != 0) {
        peers = try allocator.alloc(PeerEndpoint, new_ids.len);
        for (new_ids) |node_id| {
            const address = if (countChanges(conf_change, .update_node, node_id) == 1)
                addressInEndpoints(endpoints, node_id).?
            else
                current.addressOf(node_id) orelse addressInEndpoints(endpoints, node_id).?;
            peers[initialized_peers] = PeerEndpoint.init(allocator, node_id, address) catch return error.OutOfMemory;
            initialized_peers += 1;
        }
    }

    var retired: std.ArrayList(u64) = .empty;
    defer retired.deinit(allocator);
    retired.appendSlice(allocator, current.retired_node_ids) catch return error.OutOfMemory;
    for (previous_ids) |node_id| {
        if (!containsSorted(new_ids, node_id)) retired.append(allocator, node_id) catch return error.OutOfMemory;
    }
    std.mem.sort(u64, retired.items, {}, std.sort.asc(u64));
    if (retired.items.len != 0) {
        var unique_len: usize = 1;
        for (retired.items[1..]) |node_id| {
            if (node_id != retired.items[unique_len - 1]) {
                retired.items[unique_len] = node_id;
                unique_len += 1;
            }
        }
        retired.items.len = unique_len;
    }
    const retired_node_ids = retired.toOwnedSlice(allocator) catch return error.OutOfMemory;
    errdefer if (retired_node_ids.len != 0) allocator.free(retired_node_ids);

    var result = ClusterMembership{
        .cluster_id = current.cluster_id,
        .peers = peers,
        .retired_node_ids = retired_node_ids,
    };
    result.validate(new_conf_state) catch return error.InvalidClusterMembership;
    return result;
}

fn validateEndpoints(endpoints: []const PeerEndpoint) error{
    InvalidNodeId,
    EmptyAddress,
    PeersNotSorted,
    DuplicatePeer,
}!void {
    for (endpoints, 0..) |endpoint, index| {
        if (endpoint.node_id == 0) return error.InvalidNodeId;
        if (endpoint.address.len == 0) return error.EmptyAddress;
        if (index != 0) {
            const previous = endpoints[index - 1].node_id;
            if (previous == endpoint.node_id) return error.DuplicatePeer;
            if (previous > endpoint.node_id) return error.PeersNotSorted;
        }
    }
}

fn addressInEndpoints(endpoints: []const PeerEndpoint, node_id: u64) ?[]const u8 {
    var low: usize = 0;
    var high = endpoints.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (endpoints[mid].node_id < node_id) {
            low = mid + 1;
        } else if (endpoints[mid].node_id > node_id) {
            high = mid;
        } else {
            return endpoints[mid].address;
        }
    }
    return null;
}

fn containsSorted(ids: []const u64, node_id: u64) bool {
    var low: usize = 0;
    var high = ids.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (ids[mid] < node_id) {
            low = mid + 1;
        } else if (ids[mid] > node_id) {
            high = mid;
        } else {
            return true;
        }
    }
    return false;
}

fn countChanges(conf_change: ConfChangeV2, change_type: types.ConfChangeType, node_id: u64) usize {
    var count: usize = 0;
    for (conf_change.changes) |change| {
        if (change.change_type == change_type and change.node_id == node_id) count += 1;
    }
    return count;
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

fn appendMembershipContextLength(
    allocator: std.mem.Allocator,
    buffer: *std.ArrayList(u8),
    len: usize,
) MembershipContextEncodeError!void {
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

// KCOV_EXCL_START
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

test "membership context codec is deterministic and owned" {
    const allocator = std.testing.allocator;
    var endpoints = [_]PeerEndpoint{
        .{ .node_id = 2, .address = @constCast("node-2") },
        .{ .node_id = 7, .address = @constCast("node-7") },
    };
    const context = MembershipContext{ .endpoints = &endpoints };
    const encoded = try context.encode(allocator);
    defer allocator.free(encoded);

    try std.testing.expectEqualStrings(membership_context_magic, encoded[0..4]);
    try std.testing.expectEqual(membership_context_version, std.mem.readInt(u32, encoded[4..8], .little));
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, encoded[8..12], .little));

    var decoded = try decodeMembershipContext(allocator, encoded);
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), decoded.endpoints.len);
    try std.testing.expectEqualStrings("node-2", decoded.endpoints[0].address);
    decoded.endpoints[0].address[0] = 'N';
    try std.testing.expectEqual(@as(u8, 'n'), endpoints[0].address[0]);

    const reencoded = try decoded.encode(allocator);
    defer allocator.free(reencoded);
    var round_trip = try MembershipContext.decode(allocator, reencoded);
    defer round_trip.deinit(allocator);
    try std.testing.expectEqualStrings(decoded.endpoints[0].address, round_trip.endpoints[0].address);
}

test "membership context codec rejects malformed data" {
    const allocator = std.testing.allocator;
    var endpoints = [_]PeerEndpoint{
        .{ .node_id = 2, .address = @constCast("two") },
        .{ .node_id = 3, .address = @constCast("three") },
    };
    const encoded = try (MembershipContext{ .endpoints = &endpoints }).encode(allocator);
    defer allocator.free(encoded);

    for (0..membership_context_header_size) |len| {
        try std.testing.expectError(error.TruncatedData, decodeMembershipContext(allocator, encoded[0..len]));
    }

    var bad = try allocator.dupe(u8, encoded);
    defer allocator.free(bad);
    bad[0] = 'X';
    try std.testing.expectError(error.InvalidMagic, decodeMembershipContext(allocator, bad));
    bad[0] = 'R';
    std.mem.writeInt(u32, bad[4..8], membership_context_version + 1, .little);
    try std.testing.expectError(error.InvalidVersion, decodeMembershipContext(allocator, bad));
    std.mem.writeInt(u32, bad[4..8], membership_context_version, .little);

    std.mem.writeInt(u64, bad[12..20], 0, .little);
    try std.testing.expectError(error.InvalidNodeId, decodeMembershipContext(allocator, bad));
    std.mem.writeInt(u64, bad[12..20], 2, .little);

    const second_id_offset = 24 + 3;
    std.mem.writeInt(u64, bad[second_id_offset..][0..8], 2, .little);
    try std.testing.expectError(error.DuplicatePeer, decodeMembershipContext(allocator, bad));
    std.mem.writeInt(u64, bad[second_id_offset..][0..8], 1, .little);
    try std.testing.expectError(error.PeersNotSorted, decodeMembershipContext(allocator, bad));

    var empty_address = [_]u8{'\x00'} ** (membership_context_header_size + encoded_peer_min_size);
    @memcpy(empty_address[0..4], membership_context_magic);
    std.mem.writeInt(u32, empty_address[4..8], membership_context_version, .little);
    std.mem.writeInt(u32, empty_address[8..12], 1, .little);
    std.mem.writeInt(u64, empty_address[12..20], 1, .little);
    try std.testing.expectError(error.EmptyAddress, decodeMembershipContext(allocator, &empty_address));

    var huge_count = try allocator.dupe(u8, encoded[0..membership_context_header_size]);
    defer allocator.free(huge_count);
    std.mem.writeInt(u32, huge_count[8..12], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.TruncatedData, decodeMembershipContext(allocator, huge_count));

    const trailing = try std.mem.concat(allocator, u8, &.{ encoded, "x" });
    defer allocator.free(trailing);
    try std.testing.expectError(error.TrailingData, decodeMembershipContext(allocator, trailing));
}

test "membership context codec allocation failures clean up" {
    const Helper = struct {
        fn run(allocator: std.mem.Allocator, endpoints: []PeerEndpoint, encoded: []const u8) !void {
            const bytes = try (MembershipContext{ .endpoints = endpoints }).encode(allocator);
            defer allocator.free(bytes);
            var decoded = try decodeMembershipContext(allocator, encoded);
            defer decoded.deinit(allocator);
        }
    };

    const allocator = std.testing.allocator;
    var endpoints = [_]PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("one") },
        .{ .node_id = 2, .address = @constCast("two") },
    };
    const encoded = try (MembershipContext{ .endpoints = &endpoints }).encode(allocator);
    defer allocator.free(encoded);
    try std.testing.checkAllAllocationFailures(allocator, Helper.run, .{ &endpoints, encoded });
}

test "derive membership adds voters learners and supports legacy single add" {
    const allocator = std.testing.allocator;
    var current_peers = [_]PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    const current = ClusterMembership{ .cluster_id = .{1} ++ .{0} ** 15, .peers = &current_peers };
    const previous = ConfState{ .voters = @constCast(&[_]u64{1}) };
    const next = ConfState{
        .voters = @constCast(&[_]u64{ 1, 2 }),
        .learners = @constCast(&[_]u64{3}),
    };
    var context_endpoints = [_]PeerEndpoint{
        .{ .node_id = 2, .address = @constCast("node-2") },
        .{ .node_id = 3, .address = @constCast("node-3") },
    };
    const encoded = try (MembershipContext{ .endpoints = &context_endpoints }).encode(allocator);
    defer allocator.free(encoded);
    var changes = [_]types.ConfChangeSingle{
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 3 },
    };
    var derived = try deriveClusterMembership(allocator, current, previous, next, .{
        .changes = &changes,
        .context = encoded,
    });
    defer derived.deinit(allocator);
    try std.testing.expectEqual(current.cluster_id, derived.cluster_id);
    try std.testing.expectEqualStrings("node-1", derived.addressOf(1).?);
    try std.testing.expectEqualStrings("node-2", derived.addressOf(2).?);
    try std.testing.expectEqualStrings("node-3", derived.addressOf(3).?);

    const legacy_next = ConfState{ .voters = @constCast(&[_]u64{ 1, 4 }) };
    var legacy_changes = [_]types.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 4 }};
    var legacy = try deriveClusterMembership(allocator, current, previous, legacy_next, .{
        .changes = &legacy_changes,
        .context = @constCast("node-4"),
    });
    defer legacy.deinit(allocator);
    try std.testing.expectEqualStrings("node-4", legacy.addressOf(4).?);
}

test "derive membership updates only effective nodes" {
    const allocator = std.testing.allocator;
    var current_peers = [_]PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("old-1") },
        .{ .node_id = 2, .address = @constCast("old-2") },
    };
    const current = ClusterMembership{ .cluster_id = .{1} ++ .{0} ** 15, .peers = &current_peers };
    const state = ConfState{ .voters = @constCast(&[_]u64{ 1, 2 }) };
    var endpoint = [_]PeerEndpoint{.{ .node_id = 2, .address = @constCast("new-2") }};
    const encoded = try (MembershipContext{ .endpoints = &endpoint }).encode(allocator);
    defer allocator.free(encoded);
    var changes = [_]types.ConfChangeSingle{.{ .change_type = .update_node, .node_id = 2 }};
    var derived = try deriveClusterMembership(allocator, current, state, state, .{
        .changes = &changes,
        .context = encoded,
    });
    defer derived.deinit(allocator);
    try std.testing.expectEqualStrings("old-1", derived.addressOf(1).?);
    try std.testing.expectEqualStrings("new-2", derived.addressOf(2).?);

    try std.testing.expectError(error.MissingPeerAddress, deriveClusterMembership(allocator, current, state, state, .{
        .changes = &changes,
    }));
    const removed = ConfState{ .voters = @constCast(&[_]u64{1}) };
    try std.testing.expectError(error.ConflictingPeerAddress, deriveClusterMembership(allocator, current, state, removed, .{
        .changes = &changes,
        .context = encoded,
    }));
}

test "derive membership retains joint members and retires only after leave" {
    const allocator = std.testing.allocator;
    var current_peers = [_]PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2") },
    };
    const current = ClusterMembership{ .cluster_id = .{1} ++ .{0} ** 15, .peers = &current_peers };
    const previous = ConfState{ .voters = @constCast(&[_]u64{ 1, 2 }) };
    const joint_state = ConfState{
        .voters = @constCast(&[_]u64{1}),
        .voters_outgoing = @constCast(&[_]u64{ 1, 2 }),
    };
    var remove = [_]types.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 2 }};
    var joint_membership = try deriveClusterMembership(allocator, current, previous, joint_state, .{ .changes = &remove });
    defer joint_membership.deinit(allocator);
    try std.testing.expectEqualStrings("node-2", joint_membership.addressOf(2).?);
    try std.testing.expectEqual(@as(usize, 0), joint_membership.retired_node_ids.len);

    const final_state = ConfState{ .voters = @constCast(&[_]u64{1}) };
    var final_membership = try deriveClusterMembership(allocator, joint_membership, joint_state, final_state, .{});
    defer final_membership.deinit(allocator);
    try std.testing.expect(final_membership.addressOf(2) == null);
    try std.testing.expectEqualSlices(u64, &.{2}, final_membership.retired_node_ids);
}

test "derive membership retains learners next and retired remove is idempotent" {
    const allocator = std.testing.allocator;
    var peers = [_]PeerEndpoint{
        .{ .node_id = 1, .address = @constCast("node-1") },
        .{ .node_id = 2, .address = @constCast("node-2") },
    };
    const current = ClusterMembership{
        .cluster_id = .{1} ++ .{0} ** 15,
        .peers = &peers,
        .retired_node_ids = @constCast(&[_]u64{3}),
    };
    const previous = ConfState{ .voters = @constCast(&[_]u64{ 1, 2 }) };
    const joint_state = ConfState{
        .voters = @constCast(&[_]u64{1}),
        .voters_outgoing = @constCast(&[_]u64{ 1, 2 }),
        .learners_next = @constCast(&[_]u64{2}),
    };
    var demote = [_]types.ConfChangeSingle{.{ .change_type = .add_learner_node, .node_id = 2 }};
    var derived = try deriveClusterMembership(allocator, current, previous, joint_state, .{ .changes = &demote });
    defer derived.deinit(allocator);
    try std.testing.expectEqualStrings("node-2", derived.addressOf(2).?);
    try std.testing.expectEqualSlices(u64, &.{3}, derived.retired_node_ids);

    var remove_retired = [_]types.ConfChangeSingle{.{ .change_type = .remove_node, .node_id = 3 }};
    var unchanged = try deriveClusterMembership(allocator, derived, joint_state, joint_state, .{ .changes = &remove_retired });
    defer unchanged.deinit(allocator);
    try std.testing.expectEqualSlices(u64, &.{3}, unchanged.retired_node_ids);
}

test "derive membership rejects retired re-add and invalid endpoint mappings" {
    const allocator = std.testing.allocator;
    var peer = [_]PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    const current = ClusterMembership{
        .cluster_id = .{1} ++ .{0} ** 15,
        .peers = &peer,
        .retired_node_ids = @constCast(&[_]u64{2}),
    };
    const previous = ConfState{ .voters = @constCast(&[_]u64{1}) };
    const added = ConfState{ .voters = @constCast(&[_]u64{ 1, 2 }) };
    var add_endpoint = [_]PeerEndpoint{.{ .node_id = 2, .address = @constCast("node-2") }};
    const add_context = try (MembershipContext{ .endpoints = &add_endpoint }).encode(allocator);
    defer allocator.free(add_context);
    var add = [_]types.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 2 }};
    try std.testing.expectError(error.RetiredNodeId, deriveClusterMembership(allocator, current, previous, added, .{
        .changes = &add,
        .context = add_context,
    }));

    const no_retired = ClusterMembership{ .cluster_id = current.cluster_id, .peers = &peer };
    try std.testing.expectError(error.MissingPeerAddress, deriveClusterMembership(allocator, no_retired, previous, added, .{ .changes = &add }));

    var extra_endpoint = [_]PeerEndpoint{.{ .node_id = 1, .address = @constCast("other-1") }};
    const extra_context = try (MembershipContext{ .endpoints = &extra_endpoint }).encode(allocator);
    defer allocator.free(extra_context);
    try std.testing.expectError(error.UnexpectedPeerAddress, deriveClusterMembership(allocator, no_retired, previous, previous, .{
        .context = extra_context,
    }));

    var duplicate_updates = [_]types.ConfChangeSingle{
        .{ .change_type = .update_node, .node_id = 1 },
        .{ .change_type = .update_node, .node_id = 1 },
    };
    try std.testing.expectError(error.ConflictingPeerAddress, deriveClusterMembership(allocator, no_retired, previous, previous, .{
        .changes = &duplicate_updates,
        .context = extra_context,
    }));
    try std.testing.expectError(error.MalformedMembershipContext, deriveClusterMembership(allocator, no_retired, previous, previous, .{
        .context = @constCast("RMC1bad"),
    }));
    var second_add = [_]types.ConfChangeSingle{
        .{ .change_type = .add_node, .node_id = 2 },
        .{ .change_type = .add_learner_node, .node_id = 4 },
    };
    try std.testing.expectError(error.MalformedMembershipContext, deriveClusterMembership(allocator, no_retired, previous, added, .{
        .changes = &second_add,
        .context = @constCast("legacy-address"),
    }));
}

test "derive membership allocation failures are atomic" {
    const Helper = struct {
        fn run(
            allocator: std.mem.Allocator,
            current: *const ClusterMembership,
            previous: ConfState,
            next: ConfState,
            conf_change: ConfChangeV2,
        ) !void {
            var derived = try deriveClusterMembership(allocator, current.*, previous, next, conf_change);
            defer derived.deinit(allocator);
        }
    };

    const allocator = std.testing.allocator;
    var peer = [_]PeerEndpoint{.{ .node_id = 1, .address = @constCast("node-1") }};
    const current = ClusterMembership{ .cluster_id = .{1} ++ .{0} ** 15, .peers = &peer };
    const previous = ConfState{ .voters = @constCast(&[_]u64{1}) };
    const next = ConfState{ .voters = @constCast(&[_]u64{ 1, 2 }) };
    var endpoint = [_]PeerEndpoint{.{ .node_id = 2, .address = @constCast("node-2") }};
    const encoded = try (MembershipContext{ .endpoints = &endpoint }).encode(allocator);
    defer allocator.free(encoded);
    var changes = [_]types.ConfChangeSingle{.{ .change_type = .add_node, .node_id = 2 }};
    const conf_change = ConfChangeV2{ .changes = &changes, .context = encoded };
    try std.testing.checkAllAllocationFailures(allocator, Helper.run, .{ &current, previous, next, conf_change });
    try std.testing.expectEqualStrings("node-1", current.addressOf(1).?);
    try std.testing.expect(current.addressOf(2) == null);
}
// KCOV_EXCL_STOP
