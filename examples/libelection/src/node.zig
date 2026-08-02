const std = @import("std");
const linux = std.os.linux;
const raft = @import("raft_zig");
const api = @import("api_types.zig");

const Lifecycle = enum { created, running, stopping, stopped, failed };
const identity_magic = "LIBELID1";
const identity_version: u32 = 1;
const identity_size = 44;

pub const Node = struct {
    allocator: std.mem.Allocator,
    mode: api.DriveMode,
    tick_interval_ms: u64,
    listen_address: []u8,
    data_dir: []u8,
    data_dir_lock: linux.fd_t,
    peers: []raft.Peer,
    transport: *raft.GrpcLiteTransport,
    raftor: *raft.Raftor,
    callbacks: api.Callbacks,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    lifecycle: Lifecycle = .created,
    stop_requested: std.atomic.Value(bool) = .init(false),
    drive_active: std.atomic.Value(bool) = .init(false),
    callback_thread_id: std.atomic.Value(usize) = .init(0),
    driver_thread: ?std.Thread = null,
    status_mutex: std.atomic.Mutex = .unlocked,
    status: api.Status,
    terminal_error: api.Error = .ok,

    pub fn create(
        allocator: std.mem.Allocator,
        options: api.NodeOptions,
        callbacks: api.Callbacks,
    ) !*Node {
        const mode = std.enums.fromInt(api.DriveMode, options.drive_mode) orelse return error.InvalidArgument;
        if (options.node_id == 0 or std.mem.allEqual(u8, &options.cluster_id, 0)) return error.InvalidArgument;
        if (options.tick_interval_ms == 0 or options.heartbeat_ticks == 0 or
            options.election_ticks <= options.heartbeat_ticks)
        {
            return error.InvalidArgument;
        }
        const listen_address = try options.listen_address.slice();
        const data_dir = try options.data_dir.slice();
        if (listen_address.len == 0 or data_dir.len == 0 or options.peer_count == 0) {
            return error.InvalidArgument;
        }
        const input_peers = if (options.peers) |peers|
            peers[0..options.peer_count]
        else
            return error.InvalidArgument;

        const self = try allocator.create(Node);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .mode = mode,
            .tick_interval_ms = options.tick_interval_ms,
            .listen_address = &.{},
            .data_dir = &.{},
            .data_dir_lock = -1,
            .peers = &.{},
            .transport = undefined,
            .raftor = undefined,
            .callbacks = callbacks,
            .status = .{ .node_id = options.node_id },
        };

        self.listen_address = try allocator.dupe(u8, listen_address);
        errdefer allocator.free(self.listen_address);
        self.data_dir = try allocator.dupe(u8, data_dir);
        errdefer allocator.free(self.data_dir);
        self.peers = try allocator.alloc(raft.Peer, input_peers.len);
        errdefer allocator.free(self.peers);
        var initialized_peers: usize = 0;
        errdefer for (self.peers[0..initialized_peers]) |peer| allocator.free(peer.context.?);

        var local_address: ?[]const u8 = null;
        for (input_peers, 0..) |peer, index| {
            const address = try peer.address.slice();
            if (peer.id == 0 or address.len == 0) return error.InvalidArgument;
            const port = try validateAddress(address);
            if (input_peers.len > 1 and port == 0) return error.AddressPortInvalid;
            for (self.peers[0..initialized_peers]) |existing| {
                if (existing.id == peer.id or std.mem.eql(u8, existing.context.?, address)) {
                    return error.InvalidArgument;
                }
            }
            const owned_address = try allocator.dupe(u8, address);
            self.peers[index] = .{ .id = peer.id, .context = owned_address };
            initialized_peers += 1;
            if (peer.id == options.node_id) local_address = owned_address;
        }
        const advertise_address = local_address orelse return error.InvalidArgument;

        self.data_dir_lock = try acquireDataDirectory(
            allocator,
            self.data_dir,
            options.node_id,
            options.cluster_id,
        );
        errdefer closeDataDirectory(self.data_dir_lock);

        const transport = try raft.GrpcLiteTransport.create(allocator, .{
            .identity = .{ .node_id = options.node_id, .cluster_id = options.cluster_id },
            .listen_addr = self.listen_address,
            .graceful_shutdown_timeout_ns = std.time.ns_per_s,
        });
        errdefer transport.destroy();

        const raftor = try raft.Raftor.createWithTransport(
            allocator,
            .{
                .raft = .{
                    .id = options.node_id,
                    .heartbeat_tick = options.heartbeat_ticks,
                    .election_tick = options.election_ticks,
                    .check_quorum = true,
                    .pre_vote = true,
                    .disable_proposal_forwarding = true,
                },
                .cluster_id = options.cluster_id,
                .listen_addr = self.listen_address,
                .advertise_addr = advertise_address,
                .initial_peers = self.peers,
                .data_dir = self.data_dir,
                .tick_interval_ms = options.tick_interval_ms,
            },
            self.stateMachine(),
            transport.transport(),
        );
        errdefer raftor.destroy();
        try validateMembership(raftor, self.peers);

        self.transport = transport;
        self.raftor = raftor;
        self.refreshStatus();
        return self;
    }

    pub fn start(self: *Node) !void {
        lock(&self.lifecycle_mutex);
        if (self.lifecycle != .created) {
            self.lifecycle_mutex.unlock();
            return error.InvalidState;
        }
        self.stop_requested.store(false, .release);
        self.lifecycle = .running;
        lock(&self.status_mutex);
        self.status.state = @intFromEnum(api.NodeState.running);
        self.status.last_error = @intFromEnum(api.Error.ok);
        self.status_mutex.unlock();
        if (self.mode == .managed) {
            const thread = std.Thread.spawn(.{}, runDriver, .{self}) catch |err| {
                self.lifecycle = .created;
                lock(&self.status_mutex);
                self.status.state = @intFromEnum(api.NodeState.created);
                self.status_mutex.unlock();
                self.lifecycle_mutex.unlock();
                return err;
            };
            self.driver_thread = thread;
        }
        self.lifecycle_mutex.unlock();
    }

    pub fn poll(self: *Node) !bool {
        return self.externalDrive(false);
    }

    pub fn tick(self: *Node) !bool {
        return self.externalDrive(true);
    }

    pub fn getStatus(self: *const Node) api.Status {
        const mutex = @constCast(&self.status_mutex);
        lock(mutex);
        defer mutex.unlock();
        return self.status;
    }

    pub fn shutdown(self: *Node) !void {
        if (self.callback_thread_id.load(.acquire) == currentThreadId()) return error.InvalidState;

        lock(&self.lifecycle_mutex);
        switch (self.lifecycle) {
            .stopped => {
                self.lifecycle_mutex.unlock();
                return;
            },
            .stopping => {
                self.lifecycle_mutex.unlock();
                return error.InvalidState;
            },
            .created, .running => self.lifecycle = .stopping,
            .failed => {},
        }
        self.stop_requested.store(true, .release);
        const thread = self.driver_thread;
        self.driver_thread = null;
        self.lifecycle_mutex.unlock();

        self.raftor.stop();
        if (thread) |driver| driver.join();
        if (self.mode == .external) waitForExternalDrive(self);

        lock(&self.lifecycle_mutex);
        if (self.lifecycle != .failed) self.lifecycle = .stopped;
        self.lifecycle_mutex.unlock();
        self.refreshStatus();
    }

    pub fn destroy(self: *Node) void {
        self.shutdown() catch return;
        self.raftor.destroy();
        self.transport.destroy();
        closeDataDirectory(self.data_dir_lock);
        for (self.peers) |peer| self.allocator.free(peer.context.?);
        self.allocator.free(self.peers);
        self.allocator.free(self.data_dir);
        self.allocator.free(self.listen_address);
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    fn externalDrive(self: *Node, advance_clock: bool) !bool {
        if (self.mode != .external) return error.InvalidState;
        if (self.drive_active.cmpxchgStrong(false, true, .acquire, .monotonic) != null) {
            return error.InvalidState;
        }
        defer self.drive_active.store(false, .release);

        lock(&self.lifecycle_mutex);
        const running = self.lifecycle == .running;
        self.lifecycle_mutex.unlock();
        if (!running) return error.InvalidState;
        return self.driveOnce(advance_clock);
    }

    fn driveOnce(self: *Node, advance_clock: bool) !bool {
        const had_work = if (advance_clock)
            self.raftor.tick()
        else
            self.raftor.poll();
        const result = had_work catch |err| {
            if (self.stop_requested.load(.acquire) and err == error.ShuttingDown) return false;
            self.fail(err);
            return err;
        };
        self.refreshStatus();
        return result;
    }

    fn runDriver(self: *Node) void {
        while (!self.stop_requested.load(.acquire)) {
            _ = self.driveOnce(true) catch return;
            sleepUntilNextTick(self);
        }
        self.refreshStatus();
    }

    fn fail(self: *Node, err: anyerror) void {
        lock(&self.lifecycle_mutex);
        if (self.lifecycle == .stopping or self.lifecycle == .stopped) {
            self.lifecycle_mutex.unlock();
            return;
        }
        self.lifecycle = .failed;
        self.terminal_error = mapError(err);
        self.stop_requested.store(true, .release);
        self.lifecycle_mutex.unlock();
        self.raftor.stop();
        self.refreshStatus();
    }

    fn refreshStatus(self: *Node) void {
        const core = self.raftor.getStatus();
        lock(&self.lifecycle_mutex);
        const lifecycle = self.lifecycle;
        const failure = self.terminal_error;
        self.lifecycle_mutex.unlock();
        const state: api.NodeState = switch (lifecycle) {
            .created => .created,
            .running => .running,
            .stopping, .stopped => .stopped,
            .failed => .failed,
        };
        const leader_active = state == .running and core.role == .leader and
            self.raftor.getRawNode().raftConst().commitToCurrentTerm();
        const next = api.Status{
            .state = @intFromEnum(state),
            .role = roleValue(core.role),
            .leader_active = @intFromBool(leader_active),
            .node_id = core.id,
            .term = core.term,
            .leader_id = core.leader_id,
            .commit_index = core.commit_index,
            .applied_index = core.applied_index,
            .last_error = @intFromEnum(failure),
        };

        lock(&self.status_mutex);
        const was_active = self.status.leader_active != 0;
        const was_failed = self.status.state == @intFromEnum(api.NodeState.failed);
        self.status = next;
        self.status_mutex.unlock();

        if (was_active and !leader_active) self.invoke(.leadership_lost, next);
        if (!was_active and leader_active) self.invoke(.leadership_acquired, next);
        if (!was_failed and state == .failed) self.invoke(.failed, next);
    }

    fn invoke(self: *Node, event_type: api.EventType, status: api.Status) void {
        const callback = self.callbacks.on_event orelse return;
        const thread_id = currentThreadId();
        if (self.callback_thread_id.cmpxchgStrong(0, thread_id, .acq_rel, .acquire) != null) return;
        defer self.callback_thread_id.store(0, .release);
        const event = api.Event{
            .event_type = @intFromEnum(event_type),
            .status = status,
        };
        callback(self.callbacks.user_data, &event);
    }

    fn stateMachine(self: *Node) raft.StateMachine {
        return .{ .ctx = self, .vtable = &state_machine_vtable };
    }

    fn apply(_: *anyopaque, _: raft.Entry) raft.Error!raft.ApplyResult {
        return .{};
    }

    fn takeSnapshot(
        _: *anyopaque,
        allocator: std.mem.Allocator,
        applied_index: u64,
        applied_term: u64,
        conf_state: raft.ConfState,
    ) raft.Error!raft.Snapshot {
        const data = try allocator.dupe(u8, "");
        errdefer allocator.free(data);
        const owned_conf_state = try raft.cloneConfState(allocator, conf_state);
        return .{
            .data = data,
            .metadata = .{
                .index = applied_index,
                .term = applied_term,
                .conf_state = owned_conf_state,
            },
        };
    }

    fn restoreSnapshot(_: *anyopaque, _: raft.SnapshotMetadata, reader: raft.SnapshotReader) raft.Error!void {
        var byte: [1]u8 = undefined;
        if (try reader.read(&byte) != 0) return error.IncompatibleStorage;
    }

    const state_machine_vtable: raft.StateMachine.VTable = .{
        .apply = apply,
        .take_snapshot = takeSnapshot,
        .restore_snapshot = restoreSnapshot,
    };
};

fn validateMembership(raftor: *const raft.Raftor, peers: []const raft.Peer) !void {
    const membership = raftor.getClusterMembership() orelse return error.IncompatibleStorage;
    if (membership.peers.len != peers.len) return error.IncompatibleStorage;
    for (peers) |expected| {
        var found = false;
        for (membership.peers) |actual| {
            if (actual.node_id == expected.id and std.mem.eql(u8, actual.address, expected.context.?)) {
                found = true;
                break;
            }
        }
        if (!found) return error.IncompatibleStorage;
    }
}

fn roleValue(role: raft.StateRole) u32 {
    return @intFromEnum(switch (role) {
        .follower => api.Role.follower,
        .candidate => api.Role.candidate,
        .leader => api.Role.leader,
        .pre_candidate => api.Role.pre_candidate,
    });
}

pub fn mapError(err: anyerror) api.Error {
    return switch (err) {
        error.InvalidArgument,
        error.InvalidConfig,
        error.InvalidNodeId,
        error.HeartbeatTickTooSmall,
        error.ElectionTickTooSmall,
        error.MaxInflightMessagesTooSmall,
        error.LeaseBasedReadRequiresCheckQuorum,
        error.ListenAddressEmpty,
        error.DataDirectoryEmpty,
        error.AddressPortMissing,
        error.AddressPortInvalid,
        error.AddressPortOutOfRange,
        error.NodeIdNotInInitialPeers,
        error.ClusterIdRequired,
        error.PeerAddressMissing,
        error.DuplicatePeerId,
        error.MissingPeerAddress,
        error.ConflictingPeerAddress,
        error.UnexpectedPeerAddress,
        error.MalformedMembershipContext,
        => .invalid_argument,
        error.InvalidState, error.AlreadyStarted, error.EventLoopBusy => .invalid_state,
        error.ShuttingDown => .closed,
        error.OutOfMemory => .out_of_memory,
        error.BindFailed,
        error.ListenFailed,
        error.UdpBindFailed,
        error.UdpRecvStartFailed,
        error.ConnectionClosed,
        error.TransportBackpressure,
        error.Timeout,
        error.DataDirectoryInUse,
        error.Unavailable,
        error.LogTemporarilyUnavailable,
        error.SnapshotTemporarilyUnavailable,
        => .unavailable,
        error.MetadataFileTooSmall,
        error.InvalidMetadataHeader,
        error.MetadataCrcMismatch,
        error.HardStateParseError,
        error.ConfStateParseError,
        error.ClusterMembershipParseError,
        error.InvalidClusterMembership,
        error.InvalidMembershipIndex,
        error.PeerAddressBookParseError,
        error.CurrentSegmentNotFound,
        error.SegmentNotOpen,
        error.InvalidSegmentHeader,
        error.CorruptEntryRecord,
        error.EntryParseError,
        error.WalMetadataCorrupt,
        error.IdentityCorrupt,
        => .corrupt_storage,
        error.WalOpenFailed,
        error.WalReadFailed,
        error.WalWriteFailed,
        error.WalSyncFailed,
        error.WalTruncateFailed,
        error.WalDeleteFailed,
        error.WalStatFailed,
        error.WalCreateDirectoryFailed,
        error.WalRenameFailed,
        error.WalCloseFailed,
        error.IdentityOpenFailed,
        error.IdentityReadFailed,
        error.IdentityWriteFailed,
        error.IdentitySyncFailed,
        error.DataDirectoryCreateFailed,
        => .io,
        error.IncompatibleStorage,
        error.ClusterIdMismatch,
        error.TransportIdentityMismatch,
        error.MissingClusterMembership,
        error.NodeRetired,
        error.RetiredNodeId,
        error.LegacyMembershipMigrationRequired,
        error.LegacySnapshotMigrationRequired,
        error.IdentityMismatch,
        error.IdentityMissing,
        => .incompatible_storage,
        else => .internal,
    };
}

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn currentThreadId() usize {
    return @intCast(std.Thread.getCurrentId());
}

fn sleepUntilNextTick(node: *const Node) void {
    var remaining = node.tick_interval_ms *| std.time.ns_per_ms;
    while (remaining > 0 and !node.stop_requested.load(.acquire)) {
        const duration = @min(remaining, 10 * std.time.ns_per_ms);
        sleepNanoseconds(duration);
        remaining -= duration;
    }
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

fn waitForExternalDrive(node: *const Node) void {
    while (node.drive_active.load(.acquire)) {
        std.Thread.yield() catch std.atomic.spinLoopHint();
    }
}

fn validateAddress(address: []const u8) !u16 {
    const colon = std.mem.lastIndexOfScalar(u8, address, ':') orelse return error.AddressPortMissing;
    if (colon == 0 or colon + 1 == address.len) return error.AddressPortInvalid;
    return std.fmt.parseInt(u16, address[colon + 1 ..], 10) catch |err| return switch (err) {
        error.Overflow => error.AddressPortOutOfRange,
        else => error.AddressPortInvalid,
    };
}

fn acquireDataDirectory(
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    node_id: u64,
    cluster_id: [16]u8,
) !linux.fd_t {
    try makeDirectories(allocator, data_dir);
    const lock_path = try std.fmt.allocPrintSentinel(allocator, "{s}/libelection.lock", .{data_dir}, 0);
    defer allocator.free(lock_path);
    const flags: linux.O = .{
        .ACCMODE = .RDWR,
        .CREAT = true,
        .CLOEXEC = true,
        .NOFOLLOW = true,
    };
    const open_result = linux.open(lock_path.ptr, flags, 0o600);
    const fd: linux.fd_t = switch (linux.errno(open_result)) {
        .SUCCESS => @intCast(open_result),
        else => return error.IdentityOpenFailed,
    };
    errdefer closeDataDirectory(fd);

    while (true) {
        const lock_result = linux.flock(fd, std.posix.LOCK.EX | std.posix.LOCK.NB);
        switch (linux.errno(lock_result)) {
            .SUCCESS => break,
            .INTR => continue,
            .AGAIN => return error.DataDirectoryInUse,
            else => return error.IdentityOpenFailed,
        }
    }

    const directory = try allocator.dupeSentinel(u8, data_dir, 0);
    defer allocator.free(directory);
    const identity_path = try std.fmt.allocPrintSentinel(allocator, "{s}/libelection.identity", .{data_dir}, 0);
    defer allocator.free(identity_path);
    const identity_tmp_path = try std.fmt.allocPrintSentinel(allocator, "{s}/libelection.identity.tmp", .{data_dir}, 0);
    defer allocator.free(identity_tmp_path);
    const fs = raft.realFileSystem();

    const identity_handle = fs.open(identity_path, .read_only) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return error.IdentityOpenFailed,
    };
    if (identity_handle) |handle| {
        defer fs.close(handle) catch {};
        const size = fs.fileSize(handle) catch return error.IdentityReadFailed;
        if (size != identity_size) {
            return error.IdentityCorrupt;
        }
        var stored: [identity_size]u8 = undefined;
        const stored_len = fs.preadAll(handle, &stored, 0) catch return error.IdentityReadFailed;
        if (stored_len != identity_size) {
            return error.IdentityCorrupt;
        }
        try validateIdentity(&stored, node_id, cluster_id);
        fs.syncDir(directory) catch return error.IdentitySyncFailed;
    } else {
        var listing = fs.listDir(allocator, directory) catch return error.IdentityReadFailed;
        defer listing.deinit();
        for (listing.entries.items) |entry| {
            if (std.mem.eql(u8, entry.name, "libelection.lock") or
                std.mem.eql(u8, entry.name, "libelection.identity.tmp"))
            {
                continue;
            }
            return error.IdentityMissing;
        }

        const encoded = encodeIdentity(node_id, cluster_id);
        const handle = fs.open(identity_tmp_path, .write_truncate) catch return error.IdentityOpenFailed;
        var handle_open = true;
        defer if (handle_open) fs.close(handle) catch {};
        fs.pwriteAll(handle, &encoded, 0) catch return error.IdentityWriteFailed;
        fs.syncFile(handle) catch return error.IdentitySyncFailed;
        const close_result = fs.close(handle);
        handle_open = false;
        close_result catch return error.IdentitySyncFailed;
        fs.rename(identity_tmp_path, identity_path) catch return error.IdentityWriteFailed;
        fs.syncDir(directory) catch return error.IdentitySyncFailed;
    }
    return fd;
}

fn closeDataDirectory(fd: linux.fd_t) void {
    if (fd >= 0) _ = linux.close(fd);
}

fn makeDirectories(allocator: std.mem.Allocator, data_dir: []const u8) !void {
    const path = try allocator.dupeSentinel(u8, data_dir, 0);
    defer allocator.free(path);
    const fs = raft.realFileSystem();
    for (path[0..path.len], 0..) |byte, index| {
        if (byte != '/' or index == 0 or path[index - 1] == '/') continue;
        path[index] = 0;
        const component = path[0..index :0];
        const created = fs.makeDir(component) catch return error.DataDirectoryCreateFailed;
        if (created) syncParentDirectory(allocator, component) catch return error.IdentitySyncFailed;
        path[index] = '/';
    }
    const created = fs.makeDir(path) catch return error.DataDirectoryCreateFailed;
    if (created) syncParentDirectory(allocator, path) catch return error.IdentitySyncFailed;
}

fn syncParentDirectory(allocator: std.mem.Allocator, path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse ".";
    const parent_z = try allocator.dupeSentinel(u8, parent, 0);
    defer allocator.free(parent_z);
    try raft.realFileSystem().syncDir(parent_z);
}

fn encodeIdentity(node_id: u64, cluster_id: [16]u8) [identity_size]u8 {
    var encoded: [identity_size]u8 = @splat(0);
    @memcpy(encoded[0..identity_magic.len], identity_magic);
    std.mem.writeInt(u32, encoded[8..12], identity_version, .little);
    std.mem.writeInt(u64, encoded[16..24], node_id, .little);
    @memcpy(encoded[24..40], &cluster_id);
    std.mem.writeInt(u32, encoded[40..44], std.hash.crc.Crc32Iscsi.hash(encoded[0..40]), .little);
    return encoded;
}

fn validateIdentity(encoded: []const u8, node_id: u64, cluster_id: [16]u8) !void {
    if (!std.mem.eql(u8, encoded[0..8], identity_magic) or
        std.mem.readInt(u32, encoded[8..12], .little) != identity_version or
        std.mem.readInt(u32, encoded[40..44], .little) != std.hash.crc.Crc32Iscsi.hash(encoded[0..40]))
    {
        return error.IdentityCorrupt;
    }
    if (std.mem.readInt(u64, encoded[16..24], .little) != node_id or
        !std.mem.eql(u8, encoded[24..40], &cluster_id))
    {
        return error.IdentityMismatch;
    }
}
