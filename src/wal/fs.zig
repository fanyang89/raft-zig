const std = @import("std");
const linux = std.os.linux;

pub const Error = error{
    OutOfMemory,
    Interrupted,
    FileNotFound,
    OpenFailed,
    ReadFailed,
    WriteFailed,
    SyncFailed,
    DirectorySyncFailed,
    TruncateFailed,
    StatFailed,
    CloseFailed,
    RenameFailed,
    UnlinkFailed,
    MkdirFailed,
};

pub const Handle = u64;

pub const OpenMode = enum {
    read_only,
    read_write,
    create_exclusive,
    write_truncate,
};

pub const EntryKind = enum {
    file,
    directory,
    unknown,
};

pub const DirEntry = struct {
    name: []u8,
    kind: EntryKind,
};

pub const DirListing = struct {
    entries: std.ArrayList(DirEntry) = .empty,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DirListing) void {
        for (self.entries.items) |entry| self.allocator.free(entry.name);
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }
};

pub const FileSystem = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        make_dir: *const fn (*anyopaque, [:0]const u8) Error!bool,
        list_dir: *const fn (*anyopaque, std.mem.Allocator, [:0]const u8) Error!DirListing,
        open: *const fn (*anyopaque, [:0]const u8, OpenMode) Error!Handle,
        pread: *const fn (*anyopaque, Handle, []u8, u64) Error!usize,
        pwrite: *const fn (*anyopaque, Handle, []const u8, u64) Error!usize,
        file_size: *const fn (*anyopaque, Handle) Error!u64,
        truncate: *const fn (*anyopaque, Handle, u64) Error!void,
        sync_file: *const fn (*anyopaque, Handle) Error!void,
        close: *const fn (*anyopaque, Handle) Error!void,
        rename: *const fn (*anyopaque, [:0]const u8, [:0]const u8) Error!void,
        unlink: *const fn (*anyopaque, [:0]const u8) Error!void,
        sync_dir: *const fn (*anyopaque, [:0]const u8) Error!void,
    };

    pub fn makeDir(self: FileSystem, path: [:0]const u8) Error!bool {
        while (true) return self.vtable.make_dir(self.ctx, path) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }

    pub fn listDir(self: FileSystem, allocator: std.mem.Allocator, path: [:0]const u8) Error!DirListing {
        while (true) return self.vtable.list_dir(self.ctx, allocator, path) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }

    pub fn open(self: FileSystem, path: [:0]const u8, mode: OpenMode) Error!Handle {
        while (true) return self.vtable.open(self.ctx, path, mode) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }

    pub fn preadAll(self: FileSystem, handle: Handle, buffer: []u8, offset: u64) Error!usize {
        var read_len: usize = 0;
        while (read_len < buffer.len) {
            const current_offset = std.math.add(u64, offset, @intCast(read_len)) catch return error.ReadFailed;
            const count = self.vtable.pread(self.ctx, handle, buffer[read_len..], current_offset) catch |err| switch (err) {
                error.Interrupted => continue,
                else => return err,
            };
            if (count > buffer.len - read_len) return error.ReadFailed;
            if (count == 0) break;
            read_len += count;
        }
        return read_len;
    }

    pub fn pwriteAll(self: FileSystem, handle: Handle, data: []const u8, offset: u64) Error!void {
        var written: usize = 0;
        while (written < data.len) {
            const current_offset = std.math.add(u64, offset, @intCast(written)) catch return error.WriteFailed;
            const count = self.vtable.pwrite(self.ctx, handle, data[written..], current_offset) catch |err| switch (err) {
                error.Interrupted => continue,
                else => return err,
            };
            if (count == 0 or count > data.len - written) return error.WriteFailed;
            written += count;
        }
    }

    pub fn fileSize(self: FileSystem, handle: Handle) Error!u64 {
        while (true) return self.vtable.file_size(self.ctx, handle) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }

    pub fn truncate(self: FileSystem, handle: Handle, len: u64) Error!void {
        while (true) return self.vtable.truncate(self.ctx, handle, len) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }

    pub fn syncFile(self: FileSystem, handle: Handle) Error!void {
        while (true) return self.vtable.sync_file(self.ctx, handle) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }

    pub fn close(self: FileSystem, handle: Handle) Error!void {
        return self.vtable.close(self.ctx, handle);
    }

    pub fn rename(self: FileSystem, old_path: [:0]const u8, new_path: [:0]const u8) Error!void {
        while (true) return self.vtable.rename(self.ctx, old_path, new_path) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }

    pub fn unlink(self: FileSystem, path: [:0]const u8) Error!void {
        while (true) return self.vtable.unlink(self.ctx, path) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }

    pub fn syncDir(self: FileSystem, path: [:0]const u8) Error!void {
        while (true) return self.vtable.sync_dir(self.ctx, path) catch |err| switch (err) {
            error.Interrupted => continue,
            else => return err,
        };
    }
};

var linux_context: u8 = 0;

pub fn linuxFileSystem() FileSystem {
    return .{ .ctx = &linux_context, .vtable = &linux_vtable };
}

fn linuxMakeDir(_: *anyopaque, path: [:0]const u8) Error!bool {
    const rc = linux.mkdir(path.ptr, 0o755);
    return switch (linux.errno(rc)) {
        .SUCCESS => true,
        .EXIST => false,
        .INTR => error.Interrupted,
        else => error.MkdirFailed,
    };
}

fn linuxListDir(_: *anyopaque, allocator: std.mem.Allocator, path: [:0]const u8) Error!DirListing {
    const flags: linux.O = .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true };
    const rc = linux.open(path.ptr, flags, 0);
    const fd: linux.fd_t = switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .INTR => return error.Interrupted,
        else => return error.OpenFailed,
    };
    defer _ = linux.close(fd);

    var result = DirListing{ .allocator = allocator };
    errdefer result.deinit();
    var buffer: [4096]u8 = undefined;
    while (true) {
        const count_rc = linux.getdents64(fd, &buffer, buffer.len);
        const count = switch (linux.errno(count_rc)) {
            .SUCCESS => count_rc,
            .INTR => continue,
            else => return error.ReadFailed,
        };
        if (count == 0) break;
        var offset: usize = 0;
        while (offset < count) {
            const entry: *align(1) linux.dirent64 = @ptrCast(&buffer[offset]);
            const name_offset = @offsetOf(linux.dirent64, "name");
            if (entry.reclen <= name_offset or entry.reclen > count - offset) return error.ReadFailed;
            const name_ptr: [*]const u8 = &entry.name;
            const padded_name = name_ptr[0 .. entry.reclen - name_offset];
            const name_len = std.mem.findScalar(u8, padded_name, 0) orelse return error.ReadFailed;
            const name = name_ptr[0..name_len];
            if (!std.mem.eql(u8, name, ".") and !std.mem.eql(u8, name, "..")) {
                const owned_name = try allocator.dupe(u8, name);
                errdefer allocator.free(owned_name);
                try result.entries.append(allocator, .{
                    .name = owned_name,
                    .kind = switch (entry.type) {
                        linux.DT.REG => .file,
                        linux.DT.DIR => .directory,
                        else => .unknown,
                    },
                });
            }
            offset += entry.reclen;
        }
    }
    return result;
}

fn linuxOpen(_: *anyopaque, path: [:0]const u8, mode: OpenMode) Error!Handle {
    const flags: linux.O = switch (mode) {
        .read_only => .{ .ACCMODE = .RDONLY, .CLOEXEC = true },
        .read_write => .{ .ACCMODE = .RDWR, .CLOEXEC = true },
        .create_exclusive => .{ .ACCMODE = .WRONLY, .CREAT = true, .EXCL = true, .CLOEXEC = true },
        .write_truncate => .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true },
    };
    const rc = linux.open(path.ptr, flags, 0o644);
    return switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .INTR => error.Interrupted,
        .NOENT => error.FileNotFound,
        else => error.OpenFailed,
    };
}

fn linuxPread(_: *anyopaque, handle: Handle, buffer: []u8, offset: u64) Error!usize {
    const signed_offset = std.math.cast(i64, offset) orelse return error.ReadFailed;
    const rc = linux.pread(@intCast(handle), buffer.ptr, buffer.len, signed_offset);
    return switch (linux.errno(rc)) {
        .SUCCESS => rc,
        .INTR => error.Interrupted,
        else => error.ReadFailed,
    };
}

fn linuxPwrite(_: *anyopaque, handle: Handle, data: []const u8, offset: u64) Error!usize {
    const signed_offset = std.math.cast(i64, offset) orelse return error.WriteFailed;
    const rc = linux.pwrite(@intCast(handle), data.ptr, data.len, signed_offset);
    return switch (linux.errno(rc)) {
        .SUCCESS => rc,
        .INTR => error.Interrupted,
        else => error.WriteFailed,
    };
}

fn linuxFileSize(_: *anyopaque, handle: Handle) Error!u64 {
    const fd: linux.fd_t = @intCast(handle);
    const current = linux.lseek(fd, 0, 1);
    if (linux.errno(current) != .SUCCESS) return error.StatFailed;
    const end = linux.lseek(fd, 0, 2);
    if (linux.errno(end) != .SUCCESS) return error.StatFailed;
    _ = linux.lseek(fd, @intCast(current), 0);
    return @intCast(end);
}

fn linuxTruncate(_: *anyopaque, handle: Handle, len: u64) Error!void {
    const signed_len = std.math.cast(i64, len) orelse return error.TruncateFailed;
    const rc = linux.ftruncate(@intCast(handle), signed_len);
    return switch (linux.errno(rc)) {
        .SUCCESS => {},
        .INTR => error.Interrupted,
        else => error.TruncateFailed,
    };
}

fn linuxSyncFile(_: *anyopaque, handle: Handle) Error!void {
    const rc = linux.fsync(@intCast(handle));
    return switch (linux.errno(rc)) {
        .SUCCESS => {},
        .INTR => error.Interrupted,
        else => error.SyncFailed,
    };
}

fn linuxClose(_: *anyopaque, handle: Handle) Error!void {
    if (linux.errno(linux.close(@intCast(handle))) != .SUCCESS) return error.CloseFailed;
}

fn linuxRename(_: *anyopaque, old_path: [:0]const u8, new_path: [:0]const u8) Error!void {
    const rc = linux.rename(old_path.ptr, new_path.ptr);
    return switch (linux.errno(rc)) {
        .SUCCESS => {},
        .INTR => error.Interrupted,
        else => error.RenameFailed,
    };
}

fn linuxUnlink(_: *anyopaque, path: [:0]const u8) Error!void {
    const rc = linux.unlink(path.ptr);
    return switch (linux.errno(rc)) {
        .SUCCESS, .NOENT => {},
        .INTR => error.Interrupted,
        else => error.UnlinkFailed,
    };
}

fn linuxSyncDir(ctx: *anyopaque, path: [:0]const u8) Error!void {
    const flags: linux.O = .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true };
    const rc = linux.open(path.ptr, flags, 0);
    const handle: Handle = switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .INTR => return error.Interrupted,
        else => return error.DirectorySyncFailed,
    };
    defer linuxClose(ctx, handle) catch {};
    linuxSyncFile(ctx, handle) catch |err| return switch (err) {
        error.Interrupted => error.Interrupted,
        else => error.DirectorySyncFailed,
    };
}

const linux_vtable: FileSystem.VTable = .{
    .make_dir = linuxMakeDir,
    .list_dir = linuxListDir,
    .open = linuxOpen,
    .pread = linuxPread,
    .pwrite = linuxPwrite,
    .file_size = linuxFileSize,
    .truncate = linuxTruncate,
    .sync_file = linuxSyncFile,
    .close = linuxClose,
    .rename = linuxRename,
    .unlink = linuxUnlink,
    .sync_dir = linuxSyncDir,
};

test "Linux WalFs round-trips files and directory listings" {
    const allocator = std.testing.allocator;
    const path = "/tmp/raft-zig-wal-fs-test";
    const fs = linuxFileSystem();
    _ = fs.makeDir(path) catch {};
    defer {
        fs.unlink("/tmp/raft-zig-wal-fs-test/data") catch {};
        _ = linux.rmdir("/tmp/raft-zig-wal-fs-test");
    }
    const handle = try fs.open("/tmp/raft-zig-wal-fs-test/data", .write_truncate);
    try fs.pwriteAll(handle, "data", 0);
    try fs.syncFile(handle);
    try fs.close(handle);
    const read_handle = try fs.open("/tmp/raft-zig-wal-fs-test/data", .read_only);
    defer fs.close(read_handle) catch {};
    var data: [4]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 4), try fs.preadAll(read_handle, &data, 0));
    try std.testing.expectEqualStrings("data", &data);
    var listing = try fs.listDir(allocator, path);
    defer listing.deinit();
    try std.testing.expectEqual(@as(usize, 1), listing.entries.items.len);
}
