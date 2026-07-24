const std = @import("std");

pub const Status = enum {
    adapted,
    reimplemented,
    covered_elsewhere,
    excluded,
    blocked,
    planned,
};

pub const StatusCounts = struct {
    adapted: usize = 0,
    reimplemented: usize = 0,
    covered_elsewhere: usize = 0,
    excluded: usize = 0,
    blocked: usize = 0,
    planned: usize = 0,

    fn increment(self: *StatusCounts, status: Status) void {
        switch (status) {
            .adapted => self.adapted += 1,
            .reimplemented => self.reimplemented += 1,
            .covered_elsewhere => self.covered_elsewhere += 1,
            .excluded => self.excluded += 1,
            .blocked => self.blocked += 1,
            .planned => self.planned += 1,
        }
    }
};

pub const Case = struct {
    id: []const u8,
    path: []const u8,
    category: []const u8,
    status: Status,
    target: ?[]const u8 = null,
    rationale: []const u8,
};

pub const Source = struct {
    name: []const u8,
    repository: []const u8,
    revision: []const u8,
    license: []const u8,
    policy: []const u8,
    inventory: []const u8,
    expected_case_count: usize,
    expected_status_counts: StatusCounts,
};

pub const AuditError = error{
    EmptyLine,
    InvalidJsonLine,
    EmptyField,
    InvalidRevision,
    InvalidPath,
    InvalidCategory,
    InvalidTarget,
    UnexpectedTarget,
    MissingTarget,
    DuplicateId,
    DuplicateSource,
    DuplicateConsumedTarget,
    UnconsumedTarget,
    UnknownConsumedTarget,
    InventoryNotSorted,
    CaseCountMismatch,
    StatusCountMismatch,
    InvalidStatus,
};

pub fn audit(allocator: std.mem.Allocator, source: Source) !void {
    try auditSourceMetadata(source);

    var ids = std.StringHashMap(void).init(allocator);
    defer {
        var iterator = ids.keyIterator();
        while (iterator.next()) |id| allocator.free(id.*);
        ids.deinit();
    }

    var previous_path: ?[]u8 = null;
    defer if (previous_path) |path| allocator.free(path);
    var previous_id: ?[]const u8 = null;
    var actual_count: usize = 0;
    var actual_status_counts = StatusCounts{};

    var lines = LineIterator.init(source.inventory);
    while (try lines.next()) |line| {
        const parsed = try parseCase(allocator, line);
        defer parsed.deinit();
        const case = try parsed.value.toCase();
        try auditCase(case);

        if (ids.contains(case.id)) return error.DuplicateId;

        if (previous_path) |path| {
            const path_order = std.mem.order(u8, path, case.path);
            if (path_order == .gt or
                (path_order == .eq and std.mem.order(u8, previous_id.?, case.id) != .lt))
            {
                return error.InventoryNotSorted;
            }
        }

        const owned_id = try allocator.dupe(u8, case.id);
        ids.put(owned_id, {}) catch |err| {
            allocator.free(owned_id);
            return err;
        };

        const next_path = try allocator.dupe(u8, case.path);
        if (previous_path) |path| allocator.free(path);
        previous_path = next_path;
        previous_id = owned_id;
        actual_count += 1;
        actual_status_counts.increment(case.status);
    }

    if (actual_count != source.expected_case_count) return error.CaseCountMismatch;
    if (!std.meta.eql(actual_status_counts, source.expected_status_counts)) return error.StatusCountMismatch;
}

pub fn auditAll(allocator: std.mem.Allocator, sources: []const Source) !void {
    for (sources, 0..) |source, index| {
        try audit(allocator, source);
        for (sources[0..index]) |previous| {
            if (std.mem.eql(u8, previous.name, source.name) or
                std.mem.eql(u8, previous.repository, source.repository))
            {
                return error.DuplicateSource;
            }
        }
    }
}

pub fn auditConsumedTargets(
    allocator: std.mem.Allocator,
    source: Source,
    consumed_targets: []const []const u8,
) !void {
    for (consumed_targets, 0..) |target, index| {
        try auditTarget(target);
        for (consumed_targets[0..index]) |previous| {
            if (std.mem.eql(u8, previous, target)) return error.DuplicateConsumedTarget;
        }
    }

    const consumed = try allocator.alloc(bool, consumed_targets.len);
    defer allocator.free(consumed);
    @memset(consumed, false);

    var lines = LineIterator.init(source.inventory);
    while (try lines.next()) |line| {
        const parsed = try parseCase(allocator, line);
        defer parsed.deinit();
        const case = try parsed.value.toCase();
        if (case.status != .adapted and case.status != .reimplemented) continue;

        const target = case.target orelse return error.MissingTarget;
        var found = false;
        for (consumed_targets, 0..) |candidate, index| {
            if (std.mem.eql(u8, target, candidate)) {
                consumed[index] = true;
                found = true;
                break;
            }
        }
        if (!found) return error.UnconsumedTarget;
    }

    for (consumed) |was_consumed| {
        if (!was_consumed) return error.UnknownConsumedTarget;
    }
}

fn auditSourceMetadata(source: Source) !void {
    try auditText(source.name);
    try auditText(source.repository);
    try auditText(source.license);
    try auditText(source.policy);
    if (source.repository.len == "https://".len or !std.mem.startsWith(u8, source.repository, "https://")) {
        return error.InvalidPath;
    }
    if (source.revision.len != 40) return error.InvalidRevision;
    for (source.revision) |byte| {
        if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.InvalidRevision;
    }
}

fn auditCase(case: Case) !void {
    try auditText(case.id);
    try auditPath(case.path);
    try auditCategory(case.category);
    try auditText(case.rationale);

    switch (case.status) {
        .adapted, .reimplemented, .covered_elsewhere => {
            const target = case.target orelse return error.MissingTarget;
            try auditTarget(target);
        },
        .excluded, .blocked, .planned => {
            if (case.target != null) return error.UnexpectedTarget;
        },
    }
}

fn auditText(text: []const u8) !void {
    if (text.len == 0 or !std.mem.eql(u8, text, std.mem.trim(u8, text, " \t\r\n"))) return error.EmptyField;
}

fn auditPath(path: []const u8) !void {
    try auditText(path);
    if (path[0] == '/' or std.mem.indexOfScalar(u8, path, '\\') != null or hasParentSegment(path)) {
        return error.InvalidPath;
    }
}

fn auditCategory(category: []const u8) !void {
    try auditText(category);
    for (category) |byte| switch (byte) {
        'a'...'z', '0'...'9', '-' => {},
        else => return error.InvalidCategory,
    };
}

fn auditTarget(target: []const u8) !void {
    try auditPath(target);
    const in_source_tree = std.mem.startsWith(u8, target, "src/") or std.mem.startsWith(u8, target, "tests/");
    if (!in_source_tree or !std.mem.endsWith(u8, target, ".zig")) {
        return error.InvalidTarget;
    }
}

fn hasParentSegment(path: []const u8) bool {
    var segments = std.mem.splitScalar(u8, path, '/');
    while (segments.next()) |segment| {
        if (std.mem.eql(u8, segment, "..")) return true;
    }
    return false;
}

const JsonCase = struct {
    id: []const u8,
    path: []const u8,
    category: []const u8,
    status: []const u8,
    target: ?[]const u8 = null,
    rationale: []const u8,

    fn toCase(self: JsonCase) !Case {
        return .{
            .id = self.id,
            .path = self.path,
            .category = self.category,
            .status = std.meta.stringToEnum(Status, self.status) orelse return error.InvalidStatus,
            .target = self.target,
            .rationale = self.rationale,
        };
    }
};

fn parseCase(allocator: std.mem.Allocator, line: []const u8) !std.json.Parsed(JsonCase) {
    if (line.len < 2 or line[0] != '{' or line[line.len - 1] != '}') return error.InvalidJsonLine;
    return std.json.parseFromSlice(JsonCase, allocator, line, .{});
}

const LineIterator = struct {
    remaining: []const u8,

    fn init(inventory: []const u8) LineIterator {
        return .{ .remaining = inventory };
    }

    fn next(self: *LineIterator) !?[]const u8 {
        if (self.remaining.len == 0) return null;
        const end = std.mem.indexOfScalar(u8, self.remaining, '\n') orelse self.remaining.len;
        const line = self.remaining[0..end];
        self.remaining = if (end == self.remaining.len) &.{} else self.remaining[end + 1 ..];
        if (line.len == 0) return error.EmptyLine;
        return line;
    }
};

test "JSONL inventory audit accepts a valid source" {
    const inventory =
        \\{"id":"a.go::TestA","path":"a.go","category":"election","status":"adapted","target":"tests/upstream/example/a_test.zig","rationale":"Adapted behavior."}
        \\{"id":"b.go::TestB","path":"b.go","category":"storage","status":"planned","rationale":"Pending implementation."}
    ;
    const source = Source{
        .name = "example",
        .repository = "https://example.com/raft",
        .revision = "0123456789abcdef0123456789abcdef01234567",
        .license = "Apache-2.0",
        .policy = "Example policy.",
        .inventory = inventory,
        .expected_case_count = 2,
        .expected_status_counts = .{ .adapted = 1, .planned = 1 },
    };

    try audit(std.testing.allocator, source);
    try auditConsumedTargets(std.testing.allocator, source, &.{"tests/upstream/example/a_test.zig"});
}

test "JSONL inventory rejects unknown fields" {
    const inventory =
        \\{"id":"a.go::TestA","path":"a.go","category":"election","status":"planned","rationale":"Pending.","unknown":true}
    ;
    const source = Source{
        .name = "example",
        .repository = "https://example.com/raft",
        .revision = "0123456789abcdef0123456789abcdef01234567",
        .license = "Apache-2.0",
        .policy = "Example policy.",
        .inventory = inventory,
        .expected_case_count = 1,
        .expected_status_counts = .{ .planned = 1 },
    };

    try std.testing.expectError(error.UnknownField, audit(std.testing.allocator, source));
}

test "JSONL inventory enforces sorted unique identifiers" {
    const inventory =
        \\{"id":"b.go::TestB","path":"b.go","category":"storage","status":"planned","rationale":"Pending."}
        \\{"id":"a.go::TestA","path":"a.go","category":"election","status":"planned","rationale":"Pending."}
    ;
    const source = Source{
        .name = "example",
        .repository = "https://example.com/raft",
        .revision = "0123456789abcdef0123456789abcdef01234567",
        .license = "Apache-2.0",
        .policy = "Example policy.",
        .inventory = inventory,
        .expected_case_count = 2,
        .expected_status_counts = .{ .planned = 2 },
    };

    try std.testing.expectError(error.InventoryNotSorted, audit(std.testing.allocator, source));
}

test "JSONL inventory rejects non-string status" {
    const inventory =
        \\{"id":"a.go::TestA","path":"a.go","category":"election","status":5,"rationale":"Pending."}
    ;
    const source = Source{
        .name = "example",
        .repository = "https://example.com/raft",
        .revision = "0123456789abcdef0123456789abcdef01234567",
        .license = "Apache-2.0",
        .policy = "Example policy.",
        .inventory = inventory,
        .expected_case_count = 1,
        .expected_status_counts = .{ .planned = 1 },
    };

    try std.testing.expectError(error.UnexpectedToken, audit(std.testing.allocator, source));
}
