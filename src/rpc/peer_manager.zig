//! Persistent outbound raw streams, one worker and one logical stream per peer.

const std = @import("std");
const linux = std.os.linux;
const grpc = @import("grpc_lite");
const build_options = @import("raft_zig_options");
const Error = @import("../core/error.zig").Error;
const transport = @import("../transport.zig");

pub const stream_method_path = "/raft.Raft/StreamMessages";
pub const protocol_version_key = "raft-protocol-version";
pub const cluster_id_key = "raft-cluster-id-bin";
pub const source_node_key = "raft-source-node-bin";
pub const target_node_key = "raft-target-node-bin";

pub const LifecycleState = enum {
    disconnected,
    connecting,
    handshaking,
    active,
    backoff,
    stopping,
};

pub const StreamIdentity = struct {
    cluster_id: [16]u8,
    source_node: u64,
    target_node: u64,
};

pub const EventSink = struct {
    ctx: *anyopaque,
    function: *const fn (*anyopaque, transport.PeerEvent, u64) void,

    fn emit(self: EventSink, event: transport.PeerEvent, generation: u64) void {
        self.function(self.ctx, event, generation);
    }
};

pub const Config = struct {
    identity: transport.TransportIdentity,
    stream_limits: grpc.StreamBufferLimits,
    reconnect_initial_delay_ns: u64,
    reconnect_max_delay_ns: u64,
    event_sink: EventSink,
};

const Peer = struct {
    manager: *PeerManager,
    id: u64,
    addr: []u8,
    mutex: std.atomic.Mutex = .unlocked,
    state: LifecycleState = .disconnected,
    generation: u64 = 0,
    open_count: u64 = 0,
    stopping: bool = false,
    terminal: bool = false,
    identity_rejected: bool = false,
    snapshot_queued: bool = false,
    generation_was_active: bool = false,
    channel: ?*grpc.Channel = null,
    stream: ?grpc.ClientStream = null,
    thread: ?std.Thread = null,
};

const CallbackContext = struct {
    peer: *Peer,
    generation: u64,
};

// libxev's io_uring descriptors need an explicit cross-worker happens-before
// edge so TSan does not report kernel-managed descriptor reuse as a race.
var tsan_channel_lifecycle_mutex: std.atomic.Mutex = .unlocked;

pub const PeerManager = struct {
    allocator: std.mem.Allocator,
    config: Config,
    mutex: std.atomic.Mutex = .unlocked,
    peers: std.AutoHashMap(u64, *Peer),
    started: bool = false,
    stopping: bool = false,

    pub fn init(allocator: std.mem.Allocator, config: Config) PeerManager {
        return .{
            .allocator = allocator,
            .config = config,
            .peers = std.AutoHashMap(u64, *Peer).init(allocator),
        };
    }

    pub fn deinit(self: *PeerManager) void {
        self.stopAll();
        var iterator = self.peers.valueIterator();
        while (iterator.next()) |peer| self.destroyPeer(peer.*);
        self.peers.deinit();
        self.* = undefined;
    }

    pub fn startAll(self: *PeerManager) Error!void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.started or self.stopping) return error.AlreadyStarted;
        self.started = true;
        var iterator = self.peers.valueIterator();
        while (iterator.next()) |peer| {
            self.startPeer(peer.*) catch |err| {
                self.started = false;
                return mapWorkerError(err);
            };
        }
    }

    pub fn stopAll(self: *PeerManager) void {
        lock(&self.mutex);
        if (self.stopping) {
            self.mutex.unlock();
            return;
        }
        self.stopping = true;
        var iterator = self.peers.valueIterator();
        while (iterator.next()) |peer| requestStop(peer.*);
        self.mutex.unlock();

        iterator = self.peers.valueIterator();
        while (iterator.next()) |peer| joinPeer(peer.*);
    }

    pub fn addPeer(self: *PeerManager, id: u64, addr: []const u8) Error!bool {
        if (id == 0) return error.InvalidNodeId;
        if (id == self.config.identity.node_id or addr.len == 0) return error.InvalidConfig;
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.stopping) return error.ConnectionClosed;
        if (self.peers.get(id)) |existing| {
            if (!std.mem.eql(u8, existing.addr, addr)) return error.ConflictingPeerAddress;
            return false;
        }

        const peer = try self.allocator.create(Peer);
        errdefer self.allocator.destroy(peer);
        const addr_copy = try self.allocator.dupe(u8, addr);
        errdefer self.allocator.free(addr_copy);
        peer.* = .{ .manager = self, .id = id, .addr = addr_copy };
        try self.peers.put(id, peer);
        errdefer _ = self.peers.remove(id);
        if (self.started) self.startPeer(peer) catch |err| return mapWorkerError(err);
        return true;
    }

    pub fn removePeer(self: *PeerManager, id: u64) Error!void {
        lock(&self.mutex);
        if (self.stopping) {
            self.mutex.unlock();
            return error.ConnectionClosed;
        }
        const removed = self.peers.fetchRemove(id);
        self.mutex.unlock();
        const peer = if (removed) |entry| entry.value else return;
        requestStop(peer);
        joinPeer(peer);
        self.destroyPeer(peer);
    }

    pub fn hasPeer(self: *PeerManager, id: u64) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.peers.contains(id);
    }

    pub fn send(self: *PeerManager, id: u64, payload: []const u8, snapshot: bool) Error!void {
        lock(&self.mutex);
        const peer = self.peers.get(id) orelse {
            self.mutex.unlock();
            return error.ConnectionClosed;
        };
        lock(&peer.mutex);
        self.mutex.unlock();
        defer peer.mutex.unlock();
        if (peer.stopping or peer.state != .active) return error.ConnectionClosed;
        const stream = peer.stream orelse return error.ConnectionClosed;
        stream.send(payload, .{}) catch |err| return mapSendError(err);
        if (snapshot) peer.snapshot_queued = true;
    }

    pub fn count(self: *PeerManager) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.peers.count();
    }

    pub fn openCount(self: *PeerManager, id: u64) u64 {
        lock(&self.mutex);
        const peer = self.peers.get(id) orelse {
            self.mutex.unlock();
            return 0;
        };
        lock(&peer.mutex);
        self.mutex.unlock();
        defer peer.mutex.unlock();
        return peer.open_count;
    }

    pub fn peerState(self: *PeerManager, id: u64) ?LifecycleState {
        lock(&self.mutex);
        const peer = self.peers.get(id) orelse {
            self.mutex.unlock();
            return null;
        };
        lock(&peer.mutex);
        self.mutex.unlock();
        defer peer.mutex.unlock();
        return peer.state;
    }

    pub fn acknowledgeSnapshot(self: *PeerManager, id: u64) void {
        lock(&self.mutex);
        const peer = self.peers.get(id) orelse {
            self.mutex.unlock();
            return;
        };
        lock(&peer.mutex);
        self.mutex.unlock();
        peer.snapshot_queued = false;
        peer.mutex.unlock();
    }

    fn startPeer(_: *PeerManager, peer: *Peer) !void {
        peer.thread = try std.Thread.spawn(.{}, workerMain, .{peer});
    }

    fn destroyPeer(self: *PeerManager, peer: *Peer) void {
        std.debug.assert(peer.thread == null);
        self.allocator.free(peer.addr);
        self.allocator.destroy(peer);
    }
};

fn workerMain(peer: *Peer) void {
    var delay = peer.manager.config.reconnect_initial_delay_ns;
    while (!isStopping(peer)) {
        const generation = beginGeneration(peer);
        var channel = initChannel(peer.manager.allocator, peer.addr) catch {
            emitTerminalEvents(peer, generation, false, false);
            if (!enterBackoff(peer, delay)) break;
            delay = nextDelay(delay, peer.manager.config.reconnect_max_delay_ns);
            continue;
        };

        lock(&peer.mutex);
        if (peer.stopping or peer.generation != generation) {
            peer.mutex.unlock();
            deinitChannel(&channel);
            break;
        }
        peer.channel = &channel;
        peer.state = .handshaking;
        peer.mutex.unlock();

        var source_bytes: [8]u8 = undefined;
        var target_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &source_bytes, peer.manager.config.identity.node_id, .little);
        std.mem.writeInt(u64, &target_bytes, peer.id, .little);
        const metadata = [_]grpc.MetadataEntry{
            .{ .key = protocol_version_key, .value = "1" },
            .{ .key = cluster_id_key, .value = &peer.manager.config.identity.cluster_id },
            .{ .key = source_node_key, .value = &source_bytes },
            .{ .key = target_node_key, .value = &target_bytes },
        };
        var callback_context = CallbackContext{ .peer = peer, .generation = generation };
        const opened = channel.openStream(stream_method_path, .{
            .metadata = &metadata,
            .limits = peer.manager.config.stream_limits,
        }, .{
            .context = &callback_context,
            .on_headers = onHeaders,
            .on_message = onResponseMessage,
            .on_remote_end = onRemoteEnd,
            .on_terminal = onTerminal,
        }) catch null;

        if (opened) |handle| {
            lock(&peer.mutex);
            if (!peer.stopping and peer.generation == generation) {
                peer.stream = handle;
                peer.open_count += 1;
                peer.mutex.unlock();
                waitForTerminal(peer, generation);
            } else {
                peer.mutex.unlock();
                var owned = handle;
                owned.cancel();
                owned.deinit();
            }
        }

        const outcome = detachGeneration(peer, generation);
        if (outcome.stream) |stream| {
            var owned = stream;
            owned.deinit();
        }
        clearChannel(peer, generation);
        deinitChannel(&channel);

        if (outcome.stopping) break;
        emitTerminalEvents(peer, generation, outcome.identity_rejected, outcome.snapshot_queued);
        if (!enterBackoff(peer, delay)) break;
        delay = if (outcome.was_active) peer.manager.config.reconnect_initial_delay_ns else nextDelay(
            delay,
            peer.manager.config.reconnect_max_delay_ns,
        );
    }
    lock(&peer.mutex);
    peer.state = .stopping;
    peer.mutex.unlock();
}

const GenerationOutcome = struct {
    stream: ?grpc.ClientStream,
    stopping: bool,
    identity_rejected: bool,
    snapshot_queued: bool,
    was_active: bool,
};

fn beginGeneration(peer: *Peer) u64 {
    lock(&peer.mutex);
    defer peer.mutex.unlock();
    peer.generation +%= 1;
    if (peer.generation == 0) peer.generation = 1;
    peer.state = .connecting;
    peer.terminal = false;
    peer.identity_rejected = false;
    peer.snapshot_queued = false;
    peer.generation_was_active = false;
    return peer.generation;
}

fn detachGeneration(peer: *Peer, generation: u64) GenerationOutcome {
    lock(&peer.mutex);
    defer peer.mutex.unlock();
    const outcome = GenerationOutcome{
        .stream = peer.stream,
        .stopping = peer.stopping or peer.generation != generation,
        .identity_rejected = peer.identity_rejected,
        .snapshot_queued = peer.snapshot_queued,
        .was_active = peer.generation_was_active,
    };
    peer.stream = null;
    if (!peer.stopping) peer.state = .disconnected;
    return outcome;
}

fn clearChannel(peer: *Peer, generation: u64) void {
    lock(&peer.mutex);
    defer peer.mutex.unlock();
    if (peer.generation == generation) peer.channel = null;
}

fn waitForTerminal(peer: *Peer, generation: u64) void {
    while (true) {
        lock(&peer.mutex);
        const done = peer.stopping or peer.generation != generation or peer.terminal;
        peer.mutex.unlock();
        if (done) return;
        sleepNanoseconds(2 * std.time.ns_per_ms);
    }
}

fn enterBackoff(peer: *Peer, delay: u64) bool {
    lock(&peer.mutex);
    if (peer.stopping) {
        peer.mutex.unlock();
        return false;
    }
    peer.state = .backoff;
    peer.mutex.unlock();
    var remaining = delay;
    while (remaining > 0) {
        if (isStopping(peer)) return false;
        const slice = @min(remaining, 5 * std.time.ns_per_ms);
        sleepNanoseconds(slice);
        remaining -= slice;
    }
    return !isStopping(peer);
}

fn nextDelay(current: u64, maximum: u64) u64 {
    return @min(current *| 2, maximum);
}

fn requestStop(peer: *Peer) void {
    lock(&peer.mutex);
    peer.stopping = true;
    peer.state = .stopping;
    if (peer.stream) |stream| stream.cancel();
    if (peer.channel) |channel| channel.shutdown();
    peer.mutex.unlock();
}

fn joinPeer(peer: *Peer) void {
    const thread = peer.thread orelse return;
    peer.thread = null;
    thread.join();
}

fn isStopping(peer: *Peer) bool {
    lock(&peer.mutex);
    defer peer.mutex.unlock();
    return peer.stopping;
}

fn emitTerminalEvents(peer: *Peer, generation: u64, identity_rejected: bool, snapshot_queued: bool) void {
    if (isStopping(peer)) return;
    const sink = peer.manager.config.event_sink;
    if (identity_rejected) {
        sink.emit(.{ .peer_id = peer.id, .kind = .identity_rejected }, generation);
        return;
    }
    sink.emit(.{ .peer_id = peer.id, .kind = .@"unreachable" }, generation);
    if (snapshot_queued) sink.emit(.{ .peer_id = peer.id, .kind = .snapshot_failure }, generation);
}

fn onHeaders(context: ?*anyopaque, stream: grpc.ClientStream, metadata: *const grpc.Metadata) void {
    const callback: *CallbackContext = @ptrCast(@alignCast(context.?));
    const expected = callback.peer.manager.config.identity;
    const actual = parseStreamIdentity(metadata) catch {
        rejectIdentity(callback, stream);
        return;
    };
    if (!std.mem.eql(u8, &actual.cluster_id, &expected.cluster_id) or
        actual.source_node != callback.peer.id or actual.target_node != expected.node_id)
    {
        rejectIdentity(callback, stream);
        return;
    }

    lock(&callback.peer.mutex);
    defer callback.peer.mutex.unlock();
    if (callback.peer.generation != callback.generation or callback.peer.stopping or callback.peer.terminal) return;
    callback.peer.state = .active;
    callback.peer.generation_was_active = true;
}

fn rejectIdentity(callback: *CallbackContext, stream: grpc.ClientStream) void {
    lock(&callback.peer.mutex);
    if (callback.peer.generation == callback.generation and !callback.peer.stopping) {
        callback.peer.identity_rejected = true;
    }
    callback.peer.mutex.unlock();
    stream.cancel();
}

fn onResponseMessage(
    context: ?*anyopaque,
    stream: grpc.ClientStream,
    _: []const u8,
    _: grpc.Compression,
) grpc.StreamReceiveAction {
    const callback: *CallbackContext = @ptrCast(@alignCast(context.?));
    lock(&callback.peer.mutex);
    if (callback.peer.generation == callback.generation) callback.peer.terminal = true;
    callback.peer.mutex.unlock();
    stream.cancel();
    return .continue_receiving;
}

fn onRemoteEnd(context: ?*anyopaque, stream: grpc.ClientStream) void {
    const callback: *CallbackContext = @ptrCast(@alignCast(context.?));
    lock(&callback.peer.mutex);
    if (callback.peer.generation == callback.generation) callback.peer.terminal = true;
    callback.peer.mutex.unlock();
    stream.cancel();
}

fn onTerminal(
    context: ?*anyopaque,
    _: grpc.ClientStream,
    _: grpc.Status,
    _: *const grpc.Metadata,
) void {
    const callback: *CallbackContext = @ptrCast(@alignCast(context.?));
    lock(&callback.peer.mutex);
    defer callback.peer.mutex.unlock();
    if (callback.peer.generation != callback.generation) return;
    callback.peer.terminal = true;
    if (!callback.peer.stopping) callback.peer.state = .disconnected;
}

pub fn parseStreamIdentity(metadata: *const grpc.Metadata) !StreamIdentity {
    const version = try exactlyOne(metadata, protocol_version_key);
    const cluster_id = try exactlyOne(metadata, cluster_id_key);
    const source = try exactlyOne(metadata, source_node_key);
    const target = try exactlyOne(metadata, target_node_key);
    if (!std.mem.eql(u8, version, "1") or cluster_id.len != 16 or source.len != 8 or target.len != 8) {
        return error.MalformedIdentityMetadata;
    }
    const identity: StreamIdentity = .{
        .cluster_id = cluster_id[0..16].*,
        .source_node = std.mem.readInt(u64, source[0..8], .little),
        .target_node = std.mem.readInt(u64, target[0..8], .little),
    };
    if (std.mem.allEqual(u8, &identity.cluster_id, 0) or identity.source_node == 0 or identity.target_node == 0) {
        return error.MalformedIdentityMetadata;
    }
    return identity;
}

fn exactlyOne(metadata: *const grpc.Metadata, key: []const u8) ![]const u8 {
    var value: ?[]const u8 = null;
    for (metadata.items()) |entry| {
        if (!std.mem.eql(u8, entry.key, key)) continue;
        if (value != null) return error.DuplicateIdentityMetadata;
        value = entry.value;
    }
    return value orelse error.MissingIdentityMetadata;
}

fn mapSendError(err: anyerror) Error {
    return switch (err) {
        error.WouldBlock, error.OutboundBufferLimitExceeded => error.TransportBackpressure,
        error.MessageTooLarge => error.MessageTooLarge,
        error.OutOfMemory => error.OutOfMemory,
        error.StreamClosed, error.SendClosed => error.ConnectionClosed,
        else => error.ConnectionClosed,
    };
}

fn mapWorkerError(err: anyerror) Error {
    return if (err == error.OutOfMemory) error.OutOfMemory else error.ConnectionClosed;
}

fn initChannel(allocator: std.mem.Allocator, address: []const u8) !grpc.Channel {
    if (build_options.sanitize_thread) lock(&tsan_channel_lifecycle_mutex);
    defer if (build_options.sanitize_thread) tsan_channel_lifecycle_mutex.unlock();
    return grpc.Channel.init(allocator, address, .{});
}

fn deinitChannel(channel: *grpc.Channel) void {
    if (build_options.sanitize_thread) lock(&tsan_channel_lifecycle_mutex);
    defer if (build_options.sanitize_thread) tsan_channel_lifecycle_mutex.unlock();
    channel.shutdown();
    channel.wait();
    channel.deinit();
}

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn sleepNanoseconds(nanoseconds: u64) void {
    var request = linux.timespec{
        .sec = std.math.cast(isize, nanoseconds / std.time.ns_per_s) orelse std.math.maxInt(isize),
        .nsec = @intCast(nanoseconds % std.time.ns_per_s),
    };
    var remaining: linux.timespec = undefined;
    while (true) {
        const rc = linux.nanosleep(&request, &remaining);
        switch (linux.errno(rc)) {
            .SUCCESS => return,
            .INTR => request = remaining,
            else => return,
        }
    }
}
