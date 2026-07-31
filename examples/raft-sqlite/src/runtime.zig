const std = @import("std");

const grpc = @import("grpc_lite");
const raft = @import("raft_zig");
const config_mod = @import("config.zig");
const service_mod = @import("service.zig");
const sqlite = @import("sqlite.zig");
const state_machine = @import("state_machine.zig");

pub const Options = struct {
    tick_interval_ms: u64 = 100,
    election_tick: usize = 20,
    heartbeat_tick: usize = 2,
    proposal_timeout_ticks: u64 = 100,
    read_index_timeout_ticks: u64 = 100,
    snapshot_entries_threshold: u64 = 10_000,
    api_reactor_count: usize = 4,
    graceful_timeout_ns: u64 = 5 * std.time.ns_per_s,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    options: Options,
    machine: state_machine.SqliteStateMachine,
    transport: *raft.GrpcLiteTransport,
    raftor: *raft.Raftor,
    service: service_mod.DatabaseService,
    registration: service_mod.Registration,
    api_server: grpc.Server,
    driver_thread: std.Thread,
    driver_exited: std.atomic.Value(bool) = .init(false),
    driver_failed: std.atomic.Value(bool) = .init(false),
    running: bool = false,

    /// The allocator must support concurrent use by Raft and gRPC threads.
    pub fn create(
        allocator: std.mem.Allocator,
        config: *const config_mod.ServerConfig,
        options: Options,
    ) !*Runtime {
        if (options.api_reactor_count == 0 or options.graceful_timeout_ns == 0) return error.InvalidConfig;
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.options = options;
        self.driver_exited = .init(false);
        self.driver_failed = .init(false);
        self.running = false;

        const data_dir = try std.fmt.allocPrintSentinel(allocator, "{s}", .{config.data_dir}, 0);
        defer allocator.free(data_dir);
        const fs = raft.realFileSystem();
        _ = try fs.makeDir(data_dir);
        const database_path = try std.fmt.allocPrintSentinel(allocator, "{s}/state.sqlite3", .{config.data_dir}, 0);
        defer allocator.free(database_path);
        self.machine = try state_machine.SqliteStateMachine.initFile(allocator, database_path, config.cluster_id);
        errdefer self.machine.deinit();
        try fs.syncDir(data_dir);

        const max_transport_message = state_machine.max_snapshot_bytes + 1024 * 1024;
        const stream_buffer_bytes = max_transport_message + 5;
        self.transport = try raft.GrpcLiteTransport.create(allocator, .{
            .identity = .{ .cluster_id = config.cluster_id, .node_id = config.node_id },
            .listen_addr = config.raft_listen,
            .stream_limits = .{
                .max_message_size = max_transport_message,
                .max_inbound_buffer_size = stream_buffer_bytes,
                .max_outbound_buffer_size = stream_buffer_bytes,
            },
            .mailbox_max_bytes = max_transport_message * 2,
        });
        errdefer self.transport.destroy();

        var raft_config: raft.RaftorConfig = .{};
        raft_config.raft.id = config.node_id;
        raft_config.raft.election_tick = options.election_tick;
        raft_config.raft.heartbeat_tick = options.heartbeat_tick;
        raft_config.raft.check_quorum = true;
        raft_config.raft.pre_vote = true;
        raft_config.raft.disable_proposal_forwarding = true;
        raft_config.cluster_id = config.cluster_id;
        raft_config.listen_addr = config.raft_listen;
        raft_config.advertise_addr = config.raft_advertise;
        raft_config.initial_peers = config.peers;
        raft_config.data_dir = config.data_dir;
        raft_config.tick_interval_ms = options.tick_interval_ms;
        raft_config.proposal_timeout_ticks = options.proposal_timeout_ticks;
        raft_config.read_index_timeout_ticks = options.read_index_timeout_ticks;
        raft_config.snapshot_entries_threshold = options.snapshot_entries_threshold;
        raft_config.checksum_enabled = true;
        self.raftor = try raft.Raftor.createWithTransport(
            allocator,
            raft_config,
            self.machine.stateMachine(),
            self.transport.transport(),
        );
        errdefer self.raftor.destroy();

        self.service = try service_mod.DatabaseService.init(allocator, self.raftor, &self.machine);
        self.registration = self.service.registration();
        errdefer self.registration.deinit();
        self.api_server = try grpc.Server.init(allocator, .{
            .host = config.api_host,
            .port = config.api_port,
            .reactor_count = options.api_reactor_count,
            .max_request_size = state_machine.max_command_bytes,
            .stream_limits = .{
                .max_message_size = sqlite.max_api_response_bytes,
                .max_inbound_buffer_size = sqlite.max_api_response_bytes + 5,
                .max_outbound_buffer_size = sqlite.max_api_response_bytes + 5,
            },
        });
        errdefer self.api_server.deinit();
        try self.registration.register(&self.api_server);

        self.driver_thread = try std.Thread.spawn(.{}, runDriver, .{self});
        errdefer {
            self.raftor.stop();
            self.driver_thread.join();
        }
        try self.api_server.start();
        self.running = true;
        return self;
    }

    pub fn shutdown(self: *Runtime) !void {
        if (!self.running) return;
        self.api_server.shutdownGracefully(self.options.graceful_timeout_ns);
        self.raftor.stop();
        self.api_server.wait();
        self.driver_thread.join();
        self.running = false;
        if (self.driver_failed.load(.acquire)) return error.RaftDriverFailed;
    }

    pub fn deinit(self: *Runtime) void {
        std.debug.assert(!self.running);
        self.api_server.deinit();
        self.registration.deinit();
        self.raftor.destroy();
        self.transport.destroy();
        self.machine.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn driverExited(self: *const Runtime) bool {
        return self.driver_exited.load(.acquire);
    }

    pub fn apiAddress(self: *const Runtime) !grpc.ServerLocalAddress {
        return self.api_server.localAddress();
    }

    pub fn status(self: *const Runtime) raft.NodeStatus {
        return self.raftor.getStatus();
    }

    fn runDriver(self: *Runtime) void {
        self.raftor.run() catch |err| {
            self.driver_failed.store(true, .release);
            raft.log.err(@src(), "Raft driver stopped: {s}", .{@errorName(err)});
        };
        self.driver_exited.store(true, .release);
    }
};
