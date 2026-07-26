const std = @import("std");
const raft = @import("raft_zig");

const BenchmarkOptions = struct {
    proposals: usize = 1_000_000,
    batch_size: usize = 256,
    payload_size: usize = 32,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const options = try parseOptions(init);
    const payload = try allocator.alloc(u8, options.payload_size);
    defer allocator.free(payload);
    @memset(payload, 0x5a);

    var storage = raft.MemoryStorage.init();
    defer storage.deinit(allocator);
    var conf_state = raft.ConfState{ .voters = try allocator.dupe(u64, &.{1}) };
    defer conf_state.deinit(allocator);
    try storage.setRaftState(allocator, .{ .conf_state = conf_state });

    var config = raft.defaultConfig();
    config.id = 1;
    config.election_tick = 10;
    config.heartbeat_tick = 1;
    config.election_timeout_seed = 42;
    var node = try raft.RawNode.init(allocator, config, storage.asStorage());
    defer node.deinit();

    var last_applied: u64 = 0;
    try node.campaign();
    try drainReady(allocator, &node, &storage, &last_applied);
    if (node.raftConst().state != .leader) return error.CampaignFailed;
    const baseline = last_applied;

    const start = std.Io.Clock.awake.now(init.io).nanoseconds;
    var proposed: usize = 0;
    while (proposed < options.proposals) {
        const batch_end = proposed + @min(options.batch_size, options.proposals - proposed);
        while (proposed < batch_end) : (proposed += 1) {
            try node.propose("", payload);
        }
        try drainReady(allocator, &node, &storage, &last_applied);
    }
    const elapsed_ns: u64 = @intCast(std.Io.Clock.awake.now(init.io).nanoseconds - start);

    if (last_applied != baseline + @as(u64, @intCast(options.proposals))) return error.CommitMismatch;
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;
    const proposals_per_second: u64 = @intFromFloat(@as(f64, @floatFromInt(options.proposals)) / elapsed_s);

    var stdout_buffer: [256]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print(
        "proposals={} batch={} payload={} elapsed_ns={} proposals_per_second={} ns_per_proposal={}\n",
        .{
            options.proposals,
            options.batch_size,
            options.payload_size,
            elapsed_ns,
            proposals_per_second,
            elapsed_ns / options.proposals,
        },
    );
    try stdout.flush();
}

fn parseOptions(init: std.process.Init) !BenchmarkOptions {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    var options = BenchmarkOptions{};
    var index: usize = 1;
    while (index < args.len) : (index += 2) {
        const arg = args[index];
        if (index + 1 >= args.len) return error.MissingArgumentValue;
        const value = try std.fmt.parseUnsigned(usize, args[index + 1], 10);
        if (std.mem.eql(u8, arg, "--proposals")) {
            options.proposals = value;
        } else if (std.mem.eql(u8, arg, "--batch")) {
            options.batch_size = value;
        } else if (std.mem.eql(u8, arg, "--payload")) {
            options.payload_size = value;
        } else {
            return error.UnknownArgument;
        }
    }
    if (options.proposals == 0 or options.batch_size == 0) return error.InvalidArgument;
    return options;
}

fn drainReady(
    allocator: std.mem.Allocator,
    node: *raft.RawNode,
    storage: *raft.MemoryStorage,
    last_applied: *u64,
) !void {
    while (node.hasReady()) {
        var ready = try node.getReady();
        defer ready.deinit(allocator);

        if (ready.snapshot) |snapshot| try storage.applySnapshot(allocator, snapshot);
        if (ready.entries.len != 0) try storage.append(allocator, ready.entries);
        if (ready.hs) |hard_state| try storage.setHardState(hard_state);
        if (ready.must_sync) try storage.sync_();
        recordApplied(ready.light.committed_entries, last_applied);

        var light = try node.advance(ready);
        defer light.deinit(allocator);
        if (light.commit_index != null) {
            try storage.setHardState(node.raftConst().hardState());
            try storage.sync_();
        }
        recordApplied(light.committed_entries, last_applied);
        node.advanceApply();
    }
}

fn recordApplied(entries: []const raft.Entry, last_applied: *u64) void {
    if (entries.len != 0) last_applied.* = entries[entries.len - 1].index;
}
