const std = @import("std");

pub const Status = enum {
    adapted,
    reimplemented,
    covered_elsewhere,
    excluded,
    blocked,
    planned,
};

pub const Case = struct {
    id: []const u8,
    path: []const u8,
    category: []const u8,
    status: Status,
    rationale: []const u8,
};

pub const Source = struct {
    name: []const u8,
    repository: []const u8,
    revision: []const u8,
    license: []const u8,
    policy: []const u8,
    cases: []const Case,
};

pub fn audit(source: Source) !void {
    try std.testing.expect(source.name.len != 0);
    try std.testing.expect(std.mem.startsWith(u8, source.repository, "https://"));
    try std.testing.expectEqual(@as(usize, 40), source.revision.len);
    try std.testing.expect(source.license.len != 0);
    try std.testing.expect(source.policy.len != 0);

    for (source.cases, 0..) |case, index| {
        try std.testing.expect(case.id.len != 0);
        try std.testing.expect(case.path.len != 0);
        try std.testing.expect(case.category.len != 0);
        try std.testing.expect(case.rationale.len != 0);
        for (source.cases[0..index]) |previous| {
            try std.testing.expect(!std.mem.eql(u8, previous.id, case.id));
        }
    }
}
