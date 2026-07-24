const std = @import("std");
const raft = @import("raft_zig");

const Fs = raft.Fs;
const FsError = raft.FsError;

pub const Operation = enum {
    make_dir,
    list_dir,
    open,
    pread,
    pwrite,
    file_size,
    truncate,
    sync_file,
    close,
    rename,
    unlink,
    sync_dir,
};

pub const Effect = union(enum) {
    interrupted,
    fail_before,
    fail_after,
    short: usize,
    zero,
};

pub const Fault = struct {
    operation: Operation,
    occurrence: u32,
    effect: Effect,
};

pub const FaultFs = struct {
    const max_faults = 32;

    inner: Fs,
    faults: [max_faults]Fault = undefined,
    fault_count: usize = 0,
    occurrences: [@typeInfo(Operation).@"enum".fields.len]u32 = @splat(0),

    pub fn init(inner: Fs) FaultFs {
        return .{ .inner = inner };
    }

    pub fn inject(self: *FaultFs, fault: Fault) void {
        self.setScript(&.{fault});
    }

    pub fn setScript(self: *FaultFs, faults: []const Fault) void {
        std.debug.assert(faults.len <= max_faults);
        @memcpy(self.faults[0..faults.len], faults);
        self.fault_count = faults.len;
        self.occurrences = @splat(0);
    }

    pub fn fs(self: *FaultFs) Fs {
        return .{ .ctx = self, .vtable = &vtable };
    }

    pub fn fileSystem(self: *FaultFs) Fs {
        return self.fs();
    }

    pub fn assertConsumed(self: *const FaultFs) error{FaultNotConsumed}!void {
        for (self.faults[0..self.fault_count]) |fault| {
            if (self.occurrences[@intFromEnum(fault.operation)] < fault.occurrence) return error.FaultNotConsumed;
        }
    }

    fn cast(ctx: *anyopaque) *FaultFs {
        return @ptrCast(@alignCast(ctx));
    }

    fn takeEffect(self: *FaultFs, operation: Operation) ?Effect {
        const index = @intFromEnum(operation);
        self.occurrences[index] += 1;
        for (self.faults[0..self.fault_count]) |fault| {
            if (fault.operation == operation and fault.occurrence == self.occurrences[index]) return fault.effect;
        }
        return null;
    }

    fn makeDir(ctx: *anyopaque, path: [:0]const u8) FsError!bool {
        const self = cast(ctx);
        const effect = self.takeEffect(.make_dir) orelse return self.inner.makeDir(path);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before => error.MkdirFailed,
            .fail_after => result: {
                _ = try self.inner.makeDir(path);
                break :result error.MkdirFailed;
            },
            .short, .zero => error.MkdirFailed,
        };
    }

    fn listDir(ctx: *anyopaque, allocator: std.mem.Allocator, path: [:0]const u8) FsError!raft.WalDirListing {
        const self = cast(ctx);
        const effect = self.takeEffect(.list_dir) orelse return self.inner.listDir(allocator, path);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.ReadFailed,
            .fail_after => result: {
                var listing = try self.inner.listDir(allocator, path);
                listing.deinit();
                break :result error.ReadFailed;
            },
        };
    }

    fn open(ctx: *anyopaque, path: [:0]const u8, mode: raft.FsOpenMode) FsError!raft.FileHandle {
        const self = cast(ctx);
        const effect = self.takeEffect(.open) orelse return self.inner.open(path, mode);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.OpenFailed,
            .fail_after => result: {
                const handle = try self.inner.open(path, mode);
                self.inner.close(handle) catch {};
                break :result error.OpenFailed;
            },
        };
    }

    fn pread(ctx: *anyopaque, handle: raft.FileHandle, buffer: []u8, offset: u64) FsError!usize {
        const self = cast(ctx);
        const effect = self.takeEffect(.pread) orelse return self.inner.vtable.pread(self.inner.ctx, handle, buffer, offset);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before => error.ReadFailed,
            .fail_after => result: {
                _ = try self.inner.vtable.pread(self.inner.ctx, handle, buffer, offset);
                break :result error.ReadFailed;
            },
            .short => |limit| self.inner.vtable.pread(self.inner.ctx, handle, buffer[0..@min(limit, buffer.len)], offset),
            .zero => 0,
        };
    }

    fn pwrite(ctx: *anyopaque, handle: raft.FileHandle, data: []const u8, offset: u64) FsError!usize {
        const self = cast(ctx);
        const effect = self.takeEffect(.pwrite) orelse return self.inner.vtable.pwrite(self.inner.ctx, handle, data, offset);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before => error.WriteFailed,
            .fail_after => result: {
                _ = try self.inner.vtable.pwrite(self.inner.ctx, handle, data, offset);
                break :result error.WriteFailed;
            },
            .short => |limit| self.inner.vtable.pwrite(self.inner.ctx, handle, data[0..@min(limit, data.len)], offset),
            .zero => 0,
        };
    }

    fn fileSize(ctx: *anyopaque, handle: raft.FileHandle) FsError!u64 {
        const self = cast(ctx);
        const effect = self.takeEffect(.file_size) orelse return self.inner.fileSize(handle);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.StatFailed,
            .fail_after => result: {
                _ = try self.inner.fileSize(handle);
                break :result error.StatFailed;
            },
        };
    }

    fn truncate(ctx: *anyopaque, handle: raft.FileHandle, len: u64) FsError!void {
        const self = cast(ctx);
        const effect = self.takeEffect(.truncate) orelse return self.inner.truncate(handle, len);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.TruncateFailed,
            .fail_after => result: {
                try self.inner.truncate(handle, len);
                break :result error.TruncateFailed;
            },
        };
    }

    fn syncFile(ctx: *anyopaque, handle: raft.FileHandle) FsError!void {
        const self = cast(ctx);
        const effect = self.takeEffect(.sync_file) orelse return self.inner.syncFile(handle);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.SyncFailed,
            .fail_after => result: {
                try self.inner.syncFile(handle);
                break :result error.SyncFailed;
            },
        };
    }

    fn close(ctx: *anyopaque, handle: raft.FileHandle) FsError!void {
        const self = cast(ctx);
        const effect = self.takeEffect(.close) orelse return self.inner.close(handle);
        try self.inner.close(handle);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.CloseFailed,
            .fail_after => error.CloseFailed,
        };
    }

    fn rename(ctx: *anyopaque, old_path: [:0]const u8, new_path: [:0]const u8) FsError!void {
        const self = cast(ctx);
        const effect = self.takeEffect(.rename) orelse return self.inner.rename(old_path, new_path);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.RenameFailed,
            .fail_after => result: {
                try self.inner.rename(old_path, new_path);
                break :result error.RenameFailed;
            },
        };
    }

    fn unlink(ctx: *anyopaque, path: [:0]const u8) FsError!void {
        const self = cast(ctx);
        const effect = self.takeEffect(.unlink) orelse return self.inner.unlink(path);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.UnlinkFailed,
            .fail_after => result: {
                try self.inner.unlink(path);
                break :result error.UnlinkFailed;
            },
        };
    }

    fn syncDir(ctx: *anyopaque, path: [:0]const u8) FsError!void {
        const self = cast(ctx);
        const effect = self.takeEffect(.sync_dir) orelse return self.inner.syncDir(path);
        return switch (effect) {
            .interrupted => error.Interrupted,
            .fail_before, .short, .zero => error.DirectorySyncFailed,
            .fail_after => result: {
                try self.inner.syncDir(path);
                break :result error.DirectorySyncFailed;
            },
        };
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

pub const ScriptedWalFs = FaultFs;
