const std = @import("std");

const error_model = @import("core/error.zig");

const Error = error_model.Error;

pub const header_size: usize = 32;
pub const magic = "RZCX";
pub const version: u8 = 1;

pub const Kind = enum(u8) {
    proposal = 1,
    read_index = 2,
};

pub const Header = struct {
    kind: Kind,
    node_id: u64,
    incarnation: u64,
    sequence: u64,
};

pub const Generator = struct {
    node_id: u64,
    incarnation: u64,
    sequence: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn init(node_id: u64, incarnation: u64) Generator {
        return .{ .node_id = node_id, .incarnation = incarnation };
    }

    pub fn next(self: *Generator, allocator: std.mem.Allocator, kind: Kind, context_suffix: []const u8) Error![]u8 {
        const sequence = try self.nextSequence();
        const total = std.math.add(usize, header_size, context_suffix.len) catch return error.OutOfMemory;
        const context = try allocator.alloc(u8, total);
        @memcpy(context[0..4], magic);
        context[4] = version;
        context[5] = @intFromEnum(kind);
        @memset(context[6..8], 0);
        std.mem.writeInt(u64, context[8..16], self.node_id, .little);
        std.mem.writeInt(u64, context[16..24], self.incarnation, .little);
        std.mem.writeInt(u64, context[24..32], sequence, .little);
        @memcpy(context[header_size..], context_suffix);
        return context;
    }

    fn nextSequence(self: *Generator) Error!u64 {
        var current = self.sequence.load(.monotonic);
        while (true) {
            if (current == std.math.maxInt(u64)) return error.ContextSequenceExhausted;
            if (self.sequence.cmpxchgWeak(current, current + 1, .monotonic, .monotonic)) |actual| {
                current = actual;
            } else {
                return current;
            }
        }
    }
};

pub fn decode(context: []const u8) ?Header {
    if (context.len < header_size) return null;
    if (!std.mem.eql(u8, context[0..4], magic)) return null;
    if (context[4] != version or context[6] != 0 or context[7] != 0) return null;
    const kind: Kind = switch (context[5]) {
        1 => .proposal,
        2 => .read_index,
        else => return null,
    };
    return .{
        .kind = kind,
        .node_id = std.mem.readInt(u64, context[8..16], .little),
        .incarnation = std.mem.readInt(u64, context[16..24], .little),
        .sequence = std.mem.readInt(u64, context[24..32], .little),
    };
}

pub fn suffix(context: []const u8) ?[]const u8 {
    _ = decode(context) orelse return null;
    return context[header_size..];
}

test "request context has stable versioned encoding" {
    const allocator = std.testing.allocator;
    var generator = Generator.init(0x0102030405060708, 0x1112131415161718);
    const context = try generator.next(allocator, .read_index, "user");
    defer allocator.free(context);

    try std.testing.expectEqualSlices(u8, &.{
        'R',  'Z',  'C',  'X',  1,    2,    0,    0,
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11,
        0,    0,    0,    0,    0,    0,    0,    0,
        'u',  's',  'e',  'r',
    }, context);
    const header = decode(context).?;
    try std.testing.expectEqual(Kind.read_index, header.kind);
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), header.node_id);
    try std.testing.expectEqual(@as(u64, 0x1112131415161718), header.incarnation);
    try std.testing.expectEqual(@as(u64, 0), header.sequence);
    try std.testing.expectEqualStrings("user", suffix(context).?);
}

test "request context domains and sequence are unique" {
    const allocator = std.testing.allocator;
    var generator = Generator.init(1, 2);
    const proposal = try generator.next(allocator, .proposal, "");
    defer allocator.free(proposal);
    const read = try generator.next(allocator, .read_index, "same");
    defer allocator.free(read);
    try std.testing.expect(!std.mem.eql(u8, proposal, read));
    try std.testing.expectEqual(@as(u64, 0), decode(proposal).?.sequence);
    try std.testing.expectEqual(@as(u64, 1), decode(read).?.sequence);

    var other_node = Generator.init(2, 2);
    const remote = try other_node.next(allocator, .proposal, "");
    defer allocator.free(remote);
    try std.testing.expect(!std.mem.eql(u8, proposal, remote));
}

test "request context sequence fails before wrapping" {
    var generator = Generator.init(1, 1);
    generator.sequence.store(std.math.maxInt(u64), .monotonic);
    try std.testing.expectError(
        error.ContextSequenceExhausted,
        generator.next(std.testing.allocator, .proposal, ""),
    );
}
