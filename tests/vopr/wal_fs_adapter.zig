const std = @import("std");
const mar = @import("marionette");
const raft = @import("raft_zig");
const MarionetteFs = @import("marionette_fs.zig").MarionetteFs;
const MarionetteWalFs = MarionetteFs;
const fault_witness = @import("fault_witness.zig");

fn noopRestart(_: *anyopaque, _: mar.Env) anyerror!void {}

fn restartAfterCrash(sim: mar.Sim) !void {
    try sim.control.disk.restart();
    try sim.control.process.restart(0);
}

fn finishCrash(sim: mar.Sim, completed: bool) !void {
    if (sim.control.disk.crash()) {
        if (!completed) return error.VictimFailedWithoutCrash;
    } else |err| switch (err) {
        error.DiskCrashed => {},
        else => |other| return other,
    }
}

const SimWalFixture = struct {
    allocator: std.mem.Allocator,
    world: *mar.World,
    sim: mar.Sim,

    fn deinit(self: *SimWalFixture) void {
        self.world.deinit();
        self.allocator.destroy(self.world);
        self.* = undefined;
    }
};

fn initSimWal(allocator: std.mem.Allocator, seed: u64, sector_size: u64) !SimWalFixture {
    const world = try allocator.create(mar.World);
    errdefer allocator.destroy(world);
    world.* = try mar.World.init(allocator, .{ .seed = seed, .tick_ns = 1 });
    errdefer world.deinit();
    const sim = try world.simulate(.{ .disk = .{ .sector_size = sector_size, .min_latency_ns = 1 } });
    try sim.registerProcess(0, .{ .ptr = sim.control.world, .restart = noopRestart });
    return .{ .allocator = allocator, .world = world, .sim = sim };
}

const IncarnationVictim = struct {
    wal: *raft.WAL,
    completed: bool = false,

    fn run(self: *IncarnationVictim, _: std.Io) void {
        _ = self.wal.reserveIncarnation() catch return;
        self.completed = true;
    }
};

const LocalSnapshotVictim = struct {
    wal: *raft.WAL,
    completed: bool = false,

    fn run(self: *LocalSnapshotVictim, _: std.Io) void {
        var voters = [_]u64{1};
        self.wal.applyLocalSnapshot(.{
            .data = @constCast("snapshot-state"),
            .metadata = .{
                .index = 2,
                .term = 1,
                .conf_state = .{ .voters = &voters },
            },
        }) catch return;
        self.completed = true;
    }
};

const CompactionVictim = struct {
    wal: *raft.WAL,
    completed: bool = false,

    fn run(self: *CompactionVictim, _: std.Io) void {
        self.wal.compact(4) catch return;
        self.completed = true;
    }
};

const SuffixOverwriteVictim = struct {
    wal: *raft.WAL,
    completed: bool = false,

    fn run(self: *SuffixOverwriteVictim, _: std.Io) void {
        self.wal.append(&.{
            .{ .index = 3, .term = 2, .data = @constCast("new-3") },
            .{ .index = 4, .term = 2, .data = @constCast("new-4") },
            .{ .index = 5, .term = 2, .data = @constCast("new-5") },
        }) catch return;
        self.wal.sync() catch return;
        self.completed = true;
    }
};

fn runIncarnationCrashPoint(allocator: std.mem.Allocator, crash_after_ops: u64) !bool {
    var fixture = try initSimWal(allocator, 0x57414D00 + crash_after_ops, 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    try std.testing.expectEqual(@as(u64, 1), try wal.reserveIncarnation());
    try fixture.sim.control.disk.setFaults(.{ .crash_lost_metadata_rate = .always() });
    try fixture.sim.control.disk.crashAfterOps(crash_after_ops);

    var victim = IncarnationVictim{ .wal = &wal };
    var future = try std.Io.concurrent(fixture.sim.env.io(), IncarnationVictim.run, .{ &victim, fixture.sim.env.io() });
    future.await(fixture.sim.env.io());
    try finishCrash(fixture.sim, victim.completed);
    wal.deinit();
    try restartAfterCrash(fixture.sim);

    var recovered = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    defer recovered.deinit();
    if (victim.completed) {
        try std.testing.expectEqual(@as(u64, 2), recovered.incarnation);
    } else {
        try std.testing.expect(recovered.incarnation == 1 or recovered.incarnation == 2);
    }
    const previous = recovered.incarnation;
    try std.testing.expectEqual(previous + 1, try recovered.reserveIncarnation());
    return !victim.completed;
}

fn runLocalSnapshotCrashPoint(allocator: std.mem.Allocator, crash_after_ops: u64) !bool {
    var fixture = try initSimWal(allocator, 0x57414E00 + crash_after_ops, 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    try wal.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
        .{ .index = 3, .term = 1 },
    });
    try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 3 });
    try wal.sync();
    try fixture.sim.control.disk.setFaults(.{
        .crash_lost_write_rate = .always(),
        .crash_lost_metadata_rate = .always(),
    });
    try fixture.sim.control.disk.crashAfterOps(crash_after_ops);

    var victim = LocalSnapshotVictim{ .wal = &wal };
    var future = try std.Io.concurrent(fixture.sim.env.io(), LocalSnapshotVictim.run, .{ &victim, fixture.sim.env.io() });
    future.await(fixture.sim.env.io());
    try finishCrash(fixture.sim, victim.completed);
    wal.deinit();
    try restartAfterCrash(fixture.sim);

    var recovered = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 3), recovered.lastIndex());
    try std.testing.expectEqual(@as(u64, 3), recovered.hard_state.commit);
    if (recovered.snapshot) |snapshot| {
        try std.testing.expectEqual(@as(u64, 2), snapshot.metadata.index);
        try std.testing.expectEqualStrings("snapshot-state", snapshot.data);
        try std.testing.expectEqual(@as(u64, 3), recovered.firstIndex());
    } else {
        try std.testing.expect(!victim.completed);
        try std.testing.expectEqual(@as(u64, 1), recovered.firstIndex());
    }
    if (victim.completed) try std.testing.expect(recovered.snapshot != null);
    return !victim.completed;
}

fn runCompactionCrashPoint(allocator: std.mem.Allocator, crash_after_ops: u64) !bool {
    var fixture = try initSimWal(allocator, 0x57414F00 + crash_after_ops, 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{
        .dir = "wal",
        .segment_size = 96,
        .fs = backend.fileSystem(),
    });
    try wal.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
        .{ .index = 3, .term = 1 },
        .{ .index = 4, .term = 1 },
        .{ .index = 5, .term = 1 },
        .{ .index = 6, .term = 1 },
    });
    try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 6 });
    try wal.sync();
    try fixture.sim.control.disk.setFaults(.{ .crash_lost_metadata_rate = .always() });
    try fixture.sim.control.disk.crashAfterOps(crash_after_ops);

    var victim = CompactionVictim{ .wal = &wal };
    var future = try std.Io.concurrent(fixture.sim.env.io(), CompactionVictim.run, .{ &victim, fixture.sim.env.io() });
    future.await(fixture.sim.env.io());
    try finishCrash(fixture.sim, victim.completed);
    wal.deinit();
    try restartAfterCrash(fixture.sim);

    var recovered = try raft.WAL.open(allocator, .{
        .dir = "wal",
        .segment_size = 96,
        .fs = backend.fileSystem(),
    });
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 6), recovered.lastIndex());
    try std.testing.expectEqual(@as(u64, 6), recovered.hard_state.commit);
    try std.testing.expect(recovered.firstIndex() == 1 or recovered.firstIndex() == 4);
    if (victim.completed) try std.testing.expectEqual(@as(u64, 4), recovered.firstIndex());
    return !victim.completed;
}

fn runSuffixOverwriteCrashPoint(allocator: std.mem.Allocator, crash_after_ops: u64) !bool {
    var fixture = try initSimWal(allocator, 0x57415000 + crash_after_ops, 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    try wal.append(&.{
        .{ .index = 1, .term = 1, .data = @constCast("old-1") },
        .{ .index = 2, .term = 1, .data = @constCast("old-2") },
        .{ .index = 3, .term = 1, .data = @constCast("old-3") },
        .{ .index = 4, .term = 1, .data = @constCast("old-4") },
        .{ .index = 5, .term = 1, .data = @constCast("old-5") },
    });
    try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 2 });
    try wal.sync();
    try fixture.sim.control.disk.setFaults(.{
        .crash_lost_write_rate = .always(),
        .crash_lost_metadata_rate = .always(),
    });
    try fixture.sim.control.disk.crashAfterOps(crash_after_ops);

    var victim = SuffixOverwriteVictim{ .wal = &wal };
    var future = try std.Io.concurrent(fixture.sim.env.io(), SuffixOverwriteVictim.run, .{ &victim, fixture.sim.env.io() });
    future.await(fixture.sim.env.io());
    try finishCrash(fixture.sim, victim.completed);
    wal.deinit();
    try restartAfterCrash(fixture.sim);

    var recovered = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 1), recovered.firstIndex());
    try std.testing.expect(recovered.lastIndex() >= 2 and recovered.lastIndex() <= 5);
    try std.testing.expectEqual(@as(u64, 2), recovered.hard_state.commit);
    try std.testing.expectEqual(@as(u64, 1), try recovered.term(1));
    try std.testing.expectEqual(@as(u64, 1), try recovered.term(2));
    if (recovered.lastIndex() > 2) {
        const suffix_term = try recovered.term(3);
        try std.testing.expect(suffix_term == 1 or suffix_term == 2);
        var index: u64 = 3;
        while (index <= recovered.lastIndex()) : (index += 1) {
            try std.testing.expectEqual(suffix_term, try recovered.term(index));
        }
        if (suffix_term == 1) try std.testing.expectEqual(@as(u64, 5), recovered.lastIndex());
    }
    if (victim.completed) {
        try std.testing.expectEqual(@as(u64, 5), recovered.lastIndex());
        try std.testing.expectEqual(@as(u64, 2), try recovered.term(3));
    }
    return !victim.completed;
}

test "MarionetteFs satisfies the filesystem contract" {
    const allocator = std.testing.allocator;
    var world = try mar.World.init(allocator, .{ .seed = 0x4653, .tick_ns = 1 });
    defer world.deinit();
    const sim = try world.simulate(.{ .disk = .{ .sector_size = 512, .min_latency_ns = 1 } });
    try sim.registerProcess(0, .{ .ptr = sim.control.world, .restart = noopRestart });
    var backend = MarionetteFs.init(sim.env.io(), sim.env.disk);
    const fs = backend.fs();
    try std.testing.expect(try fs.makeDir("contract"));
    try std.testing.expect(!try fs.makeDir("contract"));

    const handle = try fs.open("contract/data", .create_exclusive);
    var handle_open = true;
    defer if (handle_open) fs.close(handle) catch {};
    try std.testing.expectError(error.OpenFailed, fs.open("contract/data", .create_exclusive));
    try fs.pwriteAll(handle, "abcdef", 0);
    try std.testing.expectEqual(@as(u64, 6), try fs.fileSize(handle));
    try fs.truncate(handle, 4);
    try fs.syncFile(handle);
    handle_open = false;
    try fs.close(handle);

    const read_handle = try fs.open("contract/data", .read_only);
    defer fs.close(read_handle) catch {};
    var data: [4]u8 = undefined;
    try std.testing.expectEqual(data.len, try fs.preadAll(read_handle, &data, 0));
    try std.testing.expectEqualStrings("abcd", &data);
    var listing = try fs.listDir(allocator, "contract");
    defer listing.deinit();
    try std.testing.expectEqual(@as(usize, 1), listing.entries.items.len);
    try std.testing.expectEqualStrings("data", listing.entries.items[0].name);
    try fs.rename("contract/data", "contract/renamed");
    try fs.syncDir("contract");
    try std.testing.expectError(error.FileNotFound, fs.open("contract/data", .read_only));
    try fs.unlink("contract/renamed");
    try fs.unlink("contract/renamed");
    try fs.syncDir("contract");
}

test "MarionetteFs reopens the production WAL format" {
    const allocator = std.testing.allocator;
    var world = try mar.World.init(allocator, .{ .seed = 0x57414C, .tick_ns = 1 });
    defer world.deinit();
    const sim = try world.simulate(.{ .disk = .{ .sector_size = 512, .min_latency_ns = 1 } });
    try sim.registerProcess(0, .{ .ptr = sim.control.world, .restart = noopRestart });
    var backend = MarionetteWalFs.init(sim.env.io(), sim.env.disk);

    {
        var storage = try raft.WALStorage.openWithFs(allocator, "wal", backend.fileSystem());
        defer storage.deinit();
        const writable = storage.asWritableStorage();
        try writable.append(allocator, &.{
            .{ .index = 1, .term = 1, .data = @constCast("one") },
            .{ .index = 2, .term = 1, .data = @constCast("two") },
        });
        try writable.setHardState(.{ .term = 1, .vote = 1, .commit = 2 });
        try writable.sync();
    }

    {
        var storage = try raft.WALStorage.openWithFs(allocator, "wal", backend.fileSystem());
        defer storage.deinit();
        const writable = storage.asWritableStorage();
        try std.testing.expectEqual(@as(u64, 1), try writable.firstIndex());
        try std.testing.expectEqual(@as(u64, 2), try writable.lastIndex());
        var state = try writable.initialState(allocator);
        defer state.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 2), state.hard_state.commit);
    }
}

test "WAL metadata reservation survives every structural crash point" {
    const allocator = std.testing.allocator;
    var crash_after_ops: u64 = 0;
    var windows: usize = 0;
    while (crash_after_ops < 64) : (crash_after_ops += 1) {
        if (try runIncarnationCrashPoint(allocator, crash_after_ops)) {
            windows += 1;
        } else {
            break;
        }
    }
    try std.testing.expect(windows > 0);
    try std.testing.expect(crash_after_ops < 64);
}

test "WAL local snapshot survives every structural crash point" {
    const allocator = std.testing.allocator;
    var crash_after_ops: u64 = 0;
    var windows: usize = 0;
    while (crash_after_ops < 96) : (crash_after_ops += 1) {
        if (try runLocalSnapshotCrashPoint(allocator, crash_after_ops)) {
            windows += 1;
        } else {
            break;
        }
    }
    try std.testing.expect(windows > 0);
    try std.testing.expect(crash_after_ops < 96);
}

test "WAL compaction survives every structural crash point" {
    const allocator = std.testing.allocator;
    var crash_after_ops: u64 = 0;
    var windows: usize = 0;
    while (crash_after_ops < 96) : (crash_after_ops += 1) {
        if (try runCompactionCrashPoint(allocator, crash_after_ops)) {
            windows += 1;
        } else {
            break;
        }
    }
    try std.testing.expect(windows > 0);
    try std.testing.expect(crash_after_ops < 96);
}

test "WAL suffix overwrite survives every structural crash point" {
    const allocator = std.testing.allocator;
    var crash_after_ops: u64 = 0;
    var windows: usize = 0;
    while (crash_after_ops < 96) : (crash_after_ops += 1) {
        if (try runSuffixOverwriteCrashPoint(allocator, crash_after_ops)) {
            windows += 1;
        } else {
            break;
        }
    }
    try std.testing.expect(windows > 0);
    try std.testing.expect(crash_after_ops < 96);
}

test "WAL crash recovery loses only the unsynced suffix" {
    const allocator = std.testing.allocator;
    var fixture = try initSimWal(allocator, 0x57414C01, 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    try wal.append(&.{
        .{ .index = 1, .term = 1, .data = @constCast("one") },
        .{ .index = 2, .term = 1, .data = @constCast("two") },
    });
    try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 2 });
    try wal.sync();
    try fixture.sim.control.disk.setFaults(.{ .crash_lost_write_rate = .always() });
    try wal.append(&.{.{ .index = 3, .term = 1, .data = @constCast("volatile") }});
    try fixture.sim.control.disk.crash();
    wal.deinit();
    try restartAfterCrash(fixture.sim);

    var recovered = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 2), recovered.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), recovered.hard_state.commit);
}

test "WAL crash recovery repairs a sector-torn active tail" {
    const allocator = std.testing.allocator;
    var fixture = try initSimWal(allocator, 0x57414C02, 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    try wal.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
    });
    try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 2 });
    try wal.sync();
    var payload: [2048]u8 = @splat(0x5a);
    try fixture.sim.control.disk.setFaults(.{ .crash_torn_write_rate = .always() });
    try wal.append(&.{.{ .index = 3, .term = 2, .data = &payload }});
    try fixture.sim.control.disk.crash();
    wal.deinit();
    try restartAfterCrash(fixture.sim);

    var recovered = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 2), recovered.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), recovered.hard_state.commit);
}

test "WAL crash recovery never accepts a reordered suffix with gaps" {
    const allocator = std.testing.allocator;
    var fixture = try initSimWal(allocator, 0x57414C03, 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    try wal.append(&.{
        .{ .index = 1, .term = 1, .data = @constCast("one") },
        .{ .index = 2, .term = 1, .data = @constCast("two") },
    });
    try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 2 });
    try wal.sync();
    try fixture.sim.control.disk.setFaults(.{ .crash_reordered_write_rate = .always() });
    try wal.append(&.{
        .{ .index = 3, .term = 2, .data = @constCast("three") },
        .{ .index = 4, .term = 2, .data = @constCast("four") },
    });
    try fixture.sim.control.disk.crash();

    // Fault witness: the reorder fault must have observably fired, otherwise
    // the recovery assertions below would pass even with the fault silently
    // dropped. Two unsynced entries produce two pending writes that the disk
    // shuffles at crash time.
    const witness = fault_witness.collectCrashWitness(fixture.world.traceBytes());
    try std.testing.expect(witness.pending_writes > 0);
    try std.testing.expect(witness.reordered > 0);

    wal.deinit();
    try restartAfterCrash(fixture.sim);

    var recovered = try raft.WAL.open(allocator, .{ .dir = "wal", .fs = backend.fileSystem() });
    defer recovered.deinit();
    try std.testing.expect(recovered.lastIndex() >= 2 and recovered.lastIndex() <= 4);
    const entries = try recovered.readEntries(allocator, 1, recovered.lastIndex() + 1, null);
    defer {
        for (entries) |*entry| entry.deinit(allocator);
        allocator.free(entries);
    }
    for (entries, 1..) |entry, expected_index| {
        try std.testing.expectEqual(@as(u64, @intCast(expected_index)), entry.index);
    }

    // Committed-prefix anchor: the synced entries survive the crash intact.
    try std.testing.expectEqual(@as(usize, 2), @min(entries.len, 2));
    try std.testing.expectEqualStrings("one", entries[0].data);
    try std.testing.expectEqualStrings("two", entries[1].data);
}

test "WAL crash recovery ignores a segment whose directory entry was lost" {
    const allocator = std.testing.allocator;
    var fixture = try initSimWal(allocator, 0x57414C04, 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{
        .dir = "wal",
        .segment_size = 96,
        .fs = backend.fileSystem(),
    });
    try wal.append(&.{
        .{ .index = 1, .term = 1, .data = @constCast("one") },
        .{ .index = 2, .term = 1, .data = @constCast("two") },
    });
    try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 2 });
    try wal.sync();
    try fixture.sim.control.disk.setFaults(.{ .crash_lost_metadata_rate = .always() });
    try wal.append(&.{.{ .index = 3, .term = 2, .data = @constCast("volatile") }});
    try fixture.sim.control.disk.crash();
    wal.deinit();
    try restartAfterCrash(fixture.sim);

    var recovered = try raft.WAL.open(allocator, .{
        .dir = "wal",
        .segment_size = 96,
        .fs = backend.fileSystem(),
    });
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 2), recovered.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), recovered.hard_state.commit);
}

test "fuzz: WAL crash recovery preserves the committed prefix" {
    try std.testing.fuzz({}, fuzzWalCrashRecovery, .{ .corpus = &.{
        "",
        "lost-write",
        "torn-reordered-suffix",
    } });
}

fn fuzzWalCrashRecovery(_: void, smith: *std.testing.Smith) !void {
    const allocator = std.testing.allocator;
    var fixture = try initSimWal(allocator, smith.value(u64), 512);
    defer fixture.deinit();
    var backend = MarionetteWalFs.init(fixture.sim.env.io(), fixture.sim.env.disk);
    var wal = try raft.WAL.open(allocator, .{
        .dir = "wal",
        .segment_size = 1024,
        .fs = backend.fileSystem(),
    });
    var wal_is_open = true;
    defer if (wal_is_open) wal.deinit();

    var committed: u64 = 0;
    const rounds = smith.valueRangeAtMost(u8, 1, 6);
    for (0..rounds) |round| {
        const stable_index = committed + 1;
        const stable_term: u64 = @intCast(round + 1);
        var stable_data: [16]u8 = undefined;
        smith.bytes(&stable_data);
        try wal.append(&.{.{ .index = stable_index, .term = stable_term, .data = &stable_data }});
        try wal.saveHardState(.{ .term = stable_term, .vote = 1, .commit = stable_index });
        try wal.sync();
        committed = stable_index;

        if (smith.value(bool)) _ = try wal.reserveIncarnation();
        if (smith.value(bool) and wal.firstIndex() < committed) {
            const compact_index = smith.valueRangeAtMost(u64, wal.firstIndex(), committed);
            try wal.compact(compact_index);
        }
        if (smith.value(bool)) {
            const snapshot_term = try wal.term(committed);
            var voters = [_]u64{1};
            try wal.applyLocalSnapshot(.{
                .data = @constCast("fuzz-snapshot"),
                .metadata = .{
                    .index = committed,
                    .term = snapshot_term,
                    .conf_state = .{ .voters = &voters },
                },
            });
        }

        var volatile_data: [2048]u8 = undefined;
        smith.bytes(&volatile_data);
        try wal.append(&.{.{
            .index = committed + 1,
            .term = stable_term + 1,
            .data = &volatile_data,
        }});
        switch (smith.valueRangeAtMost(u8, 0, 2)) {
            0 => try fixture.sim.control.disk.setFaults(.{
                .crash_lost_write_rate = .always(),
                .crash_lost_metadata_rate = .always(),
            }),
            1 => try fixture.sim.control.disk.setFaults(.{
                .crash_torn_write_rate = .always(),
                .crash_lost_metadata_rate = .always(),
            }),
            else => try fixture.sim.control.disk.setFaults(.{
                .crash_reordered_write_rate = .always(),
                .crash_lost_metadata_rate = .always(),
            }),
        }
        try fixture.sim.control.disk.crash();
        wal.deinit();
        wal_is_open = false;
        try restartAfterCrash(fixture.sim);
        try fixture.sim.control.disk.setFaults(.{});
        wal = try raft.WAL.open(allocator, .{
            .dir = "wal",
            .segment_size = 1024,
            .fs = backend.fileSystem(),
        });
        wal_is_open = true;
        try std.testing.expectEqual(committed, wal.hard_state.commit);
        try std.testing.expect(wal.firstIndex() <= committed + 1);
        try std.testing.expect(wal.lastIndex() >= committed);
        if (wal.firstIndex() <= committed) {
            _ = try wal.term(committed);
        } else {
            try std.testing.expectEqual(committed, wal.snapshot_metadata.index);
            _ = try wal.term(committed);
        }

        // Committed-prefix data anchor: the stable entry recorded this round
        // must still carry its exact bytes after recovery (when it has not
        // been compacted below firstIndex).
        if (wal.firstIndex() <= committed) {
            const anchor = try wal.readEntries(allocator, committed, committed + 1, null);
            defer {
                for (anchor) |*entry| entry.deinit(allocator);
                allocator.free(anchor);
            }
            try std.testing.expectEqual(@as(usize, 1), anchor.len);
            try std.testing.expectEqualStrings(&stable_data, anchor[0].data);
        }
    }

    // Fault witness: across all rounds the crash faults must have observably
    // fired at least once, otherwise the recovery loop would pass even with
    // every pending write silently landing in order.
    const witness = fault_witness.collectCrashWitness(fixture.world.traceBytes());
    try std.testing.expect(witness.fired());
}
