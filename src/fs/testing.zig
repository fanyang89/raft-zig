const std = @import("std");
const builtin = @import("builtin");
const fs_mod = @import("../fs.zig");

comptime {
    std.debug.assert(builtin.is_test);
}

pub const Backend = enum {
    /// Host filesystem rooted at a unique std.testing.tmpDir directory.
    real,
    /// Same RealFs implementation, rooted at a unique /dev/shm directory.
    tmpfs,
};

pub const FsFixture = struct {
    allocator: std.mem.Allocator,
    backend: Backend,
    tmp_dir: ?std.testing.TmpDir = null,
    root_path: [:0]u8,
    wal_path: [:0]u8,

    pub fn init(allocator: std.mem.Allocator, backend: Backend) !FsFixture {
        return switch (backend) {
            .real => initReal(allocator),
            .tmpfs => initTmpfs(allocator),
        };
    }

    pub fn deinit(self: *FsFixture) void {
        switch (self.backend) {
            .real => if (self.tmp_dir) |*tmp_dir| tmp_dir.cleanup(),
            .tmpfs => std.Io.Dir.cwd().deleteTree(std.testing.io, self.root_path) catch {},
        }
        self.allocator.free(self.wal_path);
        self.allocator.free(self.root_path);
        self.* = undefined;
    }

    pub fn fs(_: *const FsFixture) fs_mod.Fs {
        return fs_mod.realFileSystem();
    }

    pub fn root(self: *const FsFixture) [:0]const u8 {
        return self.root_path;
    }

    pub fn walDir(self: *const FsFixture) [:0]const u8 {
        return self.wal_path;
    }

    fn initReal(allocator: std.mem.Allocator) !FsFixture {
        var tmp_dir = std.testing.tmpDir(.{ .iterate = true });
        errdefer tmp_dir.cleanup();
        const root_path = try tmp_dir.dir.realPathFileAlloc(std.testing.io, ".", allocator);
        errdefer allocator.free(root_path);
        const wal_path = try std.fmt.allocPrintSentinel(allocator, "{s}/wal", .{root_path}, 0);
        return .{
            .allocator = allocator,
            .backend = .real,
            .tmp_dir = tmp_dir,
            .root_path = root_path,
            .wal_path = wal_path,
        };
    }

    fn initTmpfs(allocator: std.mem.Allocator) !FsFixture {
        var random_bytes: [12]u8 = undefined;
        std.testing.io.random(&random_bytes);
        var encoded: [std.base64.url_safe.Encoder.calcSize(random_bytes.len)]u8 = undefined;
        _ = std.base64.url_safe.Encoder.encode(&encoded, &random_bytes);
        const root_path = try std.fmt.allocPrintSentinel(allocator, "/dev/shm/raft-zig-{s}", .{encoded}, 0);
        errdefer allocator.free(root_path);
        std.Io.Dir.cwd().createDir(std.testing.io, root_path, .default_dir) catch |err| return switch (err) {
            error.FileNotFound => error.SkipZigTest,
            else => |other| other,
        };
        errdefer std.Io.Dir.cwd().deleteTree(std.testing.io, root_path) catch {};
        const wal_path = try std.fmt.allocPrintSentinel(allocator, "{s}/wal", .{root_path}, 0);
        return .{
            .allocator = allocator,
            .backend = .tmpfs,
            .root_path = root_path,
            .wal_path = wal_path,
        };
    }
};
