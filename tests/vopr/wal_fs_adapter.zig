const std = @import("std");
const mar = @import("marionette");
const raft = @import("raft_zig");

const Fs = raft.WalFileSystem;
const FsError = raft.WalFileSystemError;

pub const MarionetteWalFs = struct {
    io: std.Io,
    disk: mar.Disk,
    root: std.Io.Dir = .cwd(),

    pub fn init(io: std.Io, disk: mar.Disk) MarionetteWalFs {
        return .{ .io = io, .disk = disk };
    }

    pub fn fileSystem(self: *MarionetteWalFs) Fs {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn cast(ctx: *anyopaque) *MarionetteWalFs {
        return @ptrCast(@alignCast(ctx));
    }

    fn file(raw_handle: raft.WalFileHandle) std.Io.File {
        return .{ .handle = @intCast(raw_handle), .flags = .{ .nonblocking = false } };
    }

    fn handle(opened: std.Io.File) raft.WalFileHandle {
        return @intCast(opened.handle);
    }

    fn makeDir(ctx: *anyopaque, path: [:0]const u8) FsError!bool {
        const self = cast(ctx);
        self.root.createDir(self.io, path, .default_dir) catch |err| return switch (err) {
            error.PathAlreadyExists => false,
            else => error.MkdirFailed,
        };
        return true;
    }

    fn listDir(ctx: *anyopaque, allocator: std.mem.Allocator, path: [:0]const u8) FsError!raft.WalDirListing {
        const self = cast(ctx);
        var dir = self.root.openDir(self.io, path, .{ .iterate = true }) catch return error.OpenFailed;
        defer dir.close(self.io);
        var result = raft.WalDirListing{ .allocator = allocator };
        errdefer result.deinit();
        var iterator = dir.iterate();
        while (iterator.next(self.io) catch return error.ReadFailed) |entry| {
            const name = try allocator.dupe(u8, entry.name);
            errdefer allocator.free(name);
            try result.entries.append(allocator, .{
                .name = name,
                .kind = switch (entry.kind) {
                    .file => .file,
                    .directory => .directory,
                    else => .unknown,
                },
            });
        }
        return result;
    }

    fn open(ctx: *anyopaque, path: [:0]const u8, mode: raft.WalOpenMode) FsError!raft.WalFileHandle {
        const self = cast(ctx);
        const opened = switch (mode) {
            .read_only => self.root.openFile(self.io, path, .{}) catch |err| return mapOpenError(err),
            .read_write => self.root.openFile(self.io, path, .{ .mode = .read_write }) catch |err| return mapOpenError(err),
            .create_exclusive => self.root.createFile(self.io, path, .{
                .read = true,
                .truncate = false,
                .exclusive = true,
            }) catch |err| return mapOpenError(err),
            .write_truncate => self.root.createFile(self.io, path, .{
                .read = true,
                .truncate = true,
            }) catch |err| return mapOpenError(err),
        };
        return handle(opened);
    }

    fn mapOpenError(err: anyerror) FsError {
        return switch (err) {
            error.FileNotFound => error.FileNotFound,
            else => error.OpenFailed,
        };
    }

    fn pread(ctx: *anyopaque, file_handle: raft.WalFileHandle, buffer: []u8, offset: u64) FsError!usize {
        const self = cast(ctx);
        return file(file_handle).readPositional(self.io, &.{buffer}, offset) catch error.ReadFailed;
    }

    fn pwrite(ctx: *anyopaque, file_handle: raft.WalFileHandle, data: []const u8, offset: u64) FsError!usize {
        const self = cast(ctx);
        return file(file_handle).writePositional(self.io, &.{data}, offset) catch error.WriteFailed;
    }

    fn fileSize(ctx: *anyopaque, file_handle: raft.WalFileHandle) FsError!u64 {
        const self = cast(ctx);
        const stat = file(file_handle).stat(self.io) catch return error.StatFailed;
        return stat.size;
    }

    fn truncate(ctx: *anyopaque, file_handle: raft.WalFileHandle, len: u64) FsError!void {
        const self = cast(ctx);
        file(file_handle).setLength(self.io, len) catch return error.TruncateFailed;
    }

    fn syncFile(ctx: *anyopaque, file_handle: raft.WalFileHandle) FsError!void {
        const self = cast(ctx);
        file(file_handle).sync(self.io) catch return error.SyncFailed;
    }

    fn close(ctx: *anyopaque, file_handle: raft.WalFileHandle) FsError!void {
        const self = cast(ctx);
        file(file_handle).close(self.io);
    }

    fn rename(ctx: *anyopaque, old_path: [:0]const u8, new_path: [:0]const u8) FsError!void {
        const self = cast(ctx);
        self.root.rename(old_path, self.root, new_path, self.io) catch return error.RenameFailed;
    }

    fn unlink(ctx: *anyopaque, path: [:0]const u8) FsError!void {
        const self = cast(ctx);
        self.root.deleteFile(self.io, path) catch |err| return switch (err) {
            error.FileNotFound => {},
            else => error.UnlinkFailed,
        };
    }

    fn syncDir(ctx: *anyopaque, path: [:0]const u8) FsError!void {
        const self = cast(ctx);
        self.disk.syncDir(.{ .path = path }) catch return error.DirectorySyncFailed;
    }

    const vtable: Fs.VTable = .{
        .make_dir = makeDir,
        .list_dir = listDir,
        .open = open,
        .pread = pread,
        .pwrite = pwrite,
        .file_size = fileSize,
        .truncate = truncate,
        .sync_file = syncFile,
        .close = close,
        .rename = rename,
        .unlink = unlink,
        .sync_dir = syncDir,
    };
};

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

test "Marionette WalFs reopens the production WAL format" {
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
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
    });
    try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 2 });
    try wal.sync();
    try fixture.sim.control.disk.setFaults(.{ .crash_reordered_write_rate = .always() });
    try wal.append(&.{
        .{ .index = 3, .term = 2, .data = @constCast("three") },
        .{ .index = 4, .term = 2, .data = @constCast("four") },
    });
    try fixture.sim.control.disk.crash();
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
