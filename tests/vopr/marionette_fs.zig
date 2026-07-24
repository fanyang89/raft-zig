const std = @import("std");
const mar = @import("marionette");
const raft = @import("raft_zig");

const Fs = raft.Fs;
const FsError = raft.FsError;

pub const MarionetteFs = struct {
    io: std.Io,
    disk: mar.Disk,
    root: std.Io.Dir = .cwd(),

    pub fn init(io: std.Io, disk: mar.Disk) MarionetteFs {
        return .{ .io = io, .disk = disk };
    }

    pub fn fs(self: *MarionetteFs) Fs {
        return .{ .ctx = self, .vtable = &vtable };
    }

    pub fn fileSystem(self: *MarionetteFs) Fs {
        return self.fs();
    }

    fn cast(ctx: *anyopaque) *MarionetteFs {
        return @ptrCast(@alignCast(ctx));
    }

    fn file(raw_handle: raft.FileHandle) std.Io.File {
        return .{ .handle = @intCast(raw_handle), .flags = .{ .nonblocking = false } };
    }

    fn handle(opened: std.Io.File) raft.FileHandle {
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

    fn open(ctx: *anyopaque, path: [:0]const u8, mode: raft.FsOpenMode) FsError!raft.FileHandle {
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

    fn pread(ctx: *anyopaque, file_handle: raft.FileHandle, buffer: []u8, offset: u64) FsError!usize {
        const self = cast(ctx);
        return file(file_handle).readPositional(self.io, &.{buffer}, offset) catch error.ReadFailed;
    }

    fn pwrite(ctx: *anyopaque, file_handle: raft.FileHandle, data: []const u8, offset: u64) FsError!usize {
        const self = cast(ctx);
        return file(file_handle).writePositional(self.io, &.{data}, offset) catch error.WriteFailed;
    }

    fn fileSize(ctx: *anyopaque, file_handle: raft.FileHandle) FsError!u64 {
        const self = cast(ctx);
        const stat = file(file_handle).stat(self.io) catch return error.StatFailed;
        return stat.size;
    }

    fn truncate(ctx: *anyopaque, file_handle: raft.FileHandle, len: u64) FsError!void {
        const self = cast(ctx);
        file(file_handle).setLength(self.io, len) catch return error.TruncateFailed;
    }

    fn syncFile(ctx: *anyopaque, file_handle: raft.FileHandle) FsError!void {
        const self = cast(ctx);
        file(file_handle).sync(self.io) catch return error.SyncFailed;
    }

    fn close(ctx: *anyopaque, file_handle: raft.FileHandle) FsError!void {
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

pub const MarionetteWalFs = MarionetteFs;
