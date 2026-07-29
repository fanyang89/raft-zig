const std = @import("std");

pub const Runtime = struct {};

pub const log = struct {
    fn discard(comptime src: std.builtin.SourceLocation, comptime format: []const u8, args: anytype) void {
        _ = src;
        _ = format;
        _ = args;
    }

    pub fn err(comptime src: std.builtin.SourceLocation, comptime format: []const u8, args: anytype) void {
        discard(src, format, args);
    }

    pub fn warn(comptime src: std.builtin.SourceLocation, comptime format: []const u8, args: anytype) void {
        discard(src, format, args);
    }

    pub fn info(comptime src: std.builtin.SourceLocation, comptime format: []const u8, args: anytype) void {
        discard(src, format, args);
    }

    pub fn debug(comptime src: std.builtin.SourceLocation, comptime format: []const u8, args: anytype) void {
        discard(src, format, args);
    }

    pub fn trace(comptime src: std.builtin.SourceLocation, comptime format: []const u8, args: anytype) void {
        discard(src, format, args);
    }
};
