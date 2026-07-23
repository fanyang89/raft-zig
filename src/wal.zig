//! Segmented Write-Ahead Log with CRC32C integrity.
//!
//! Ports `ref/raftpp/lib/raftor/wal/` with multi-file segmentation:
//! entries are split across `segment-NNNNNN.wal` files in a directory.
//! Compaction deletes old segment files, reclaiming disk space.
//!
//! Record format matches raftpp exactly (magic "WAL1", CRC32C over
//! type+flags+length+padding+payload). In-memory entries are retained for
//! reads, while WALIndex tracks segment and byte offsets for truncation.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");
const segment_mod = @import("wal/segment.zig");
const segment_manager_mod = @import("wal/segment_manager.zig");
const metadata_store_mod = @import("wal/metadata_store.zig");
const wal_index_mod = @import("wal/wal_index.zig");

const Error = error_model.Error;
const Entry = types.Entry;
const EntryType = types.EntryType;
const HardState = types.HardState;
const ConfState = types.ConfState;
const Snapshot = types.Snapshot;
const SnapshotMetadata = types.SnapshotMetadata;
const RaftState = storage_mod.RaftState;
const GetEntriesContext = storage_mod.GetEntriesContext;
const Storage = storage_mod.Storage;
const WritableStorage = storage_mod.WritableStorage;
const cloneEntry = storage_mod.cloneEntry;
const cloneConfState = storage_mod.cloneConfState;
const cloneSnapshot = storage_mod.cloneSnapshot;

const Crc32Iscsi = std.hash.crc.Crc32Iscsi;

const log = std.log.scoped(.raft_zig_wal);

// ===========================================================================

// Constants and wire format
// ===========================================================================

const segment_magic: u32 = 0x57414C31; // "WAL1"
const format_version: u32 = 1;

const RecordType = enum(u8) {
    entry = 1,
    hard_state = 3,
    conf_state = 4,
    snapshot = 5,
};

const RecordLocation = struct {
    segment_id: u64,
    offset: u64,
    length: u32,
};

const SEGMENT_HEADER_SIZE: usize = 32;
const RECORD_HEADER_SIZE: usize = 16;

/// Encode a SegmentHeader (32 bytes) into `out`.
fn encodeSegmentHeader(out: *[SEGMENT_HEADER_SIZE]u8, segment_id: u64, first_index: u64) void {
    std.mem.writeInt(u32, out[0..4], segment_magic, .little);
    std.mem.writeInt(u32, out[4..8], format_version, .little);
    std.mem.writeInt(u64, out[8..16], segment_id, .little);
    std.mem.writeInt(u64, out[16..24], first_index, .little);
    // bytes 24..32 are reserved (zeroed).
    @memset(out[24..], 0);
}

fn isValidSegmentHeader(buf: []const u8) bool {
    if (buf.len < SEGMENT_HEADER_SIZE) return false;
    const magic = std.mem.readInt(u32, buf[0..4], .little);
    const ver = std.mem.readInt(u32, buf[4..8], .little);
    return magic == segment_magic and ver == format_version;
}

/// Calculate padding to align a record (16-byte header + payload) to 8 bytes.
fn calcPadding(payload_len: u32) u32 {
    const rem = payload_len % 8;
    return if (rem == 0) 0 else @intCast(8 - rem);
}

/// Build a single WAL record (header + payload + padding). Caller owns the
/// returned slice.
fn buildRecord(allocator: std.mem.Allocator, record_type: RecordType, payload: []const u8) ![]u8 {
    const length = std.math.cast(u32, payload.len) orelse return error.RecordTooLarge;
    const padding = calcPadding(length);
    const payload_end = try std.math.add(usize, RECORD_HEADER_SIZE, payload.len);
    const total = try std.math.add(usize, payload_end, padding);
    var out = try allocator.alloc(u8, total);
    @memset(out, 0);

    // CRC covers: type(1) + flags(1) + reserved(2) + length(4) + padding(4) + payload
    var crc = Crc32Iscsi.init();
    crc.update(&[_]u8{@intFromEnum(record_type)});
    crc.update(&[_]u8{0}); // flags
    var reserved: [2]u8 = .{ 0, 0 };
    crc.update(&reserved);
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, length, .little);
    crc.update(&len_bytes);
    var pad_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &pad_bytes, padding, .little);
    crc.update(&pad_bytes);
    crc.update(payload);
    const crc_val = crc.final();

    // Write header.
    std.mem.writeInt(u32, out[0..4], crc_val, .little); // crc
    out[4] = @intFromEnum(record_type); // type
    out[5] = 0; // flags
    @memset(out[6..8], 0); // reserved
    std.mem.writeInt(u32, out[8..12], length, .little); // length
    std.mem.writeInt(u32, out[12..16], padding, .little); // padding

    // Write payload.
    if (payload.len > 0) {
        @memcpy(out[16 .. 16 + payload.len], payload);
    }
    // Padding is already zeroed.
    return out;
}

/// Parse a record header from `data`. Returns the parsed header and whether
/// the CRC is valid. `data` must have at least RECORD_HEADER_SIZE bytes.
const ParsedRecord = struct {
    record_type: RecordType,
    payload: []const u8,
    valid: bool,
};

fn parseRecord(data: []const u8) ParsedRecord {
    if (data.len < RECORD_HEADER_SIZE) return .{ .record_type = .entry, .payload = &.{}, .valid = false };

    const stored_crc = std.mem.readInt(u32, data[0..4], .little);
    const raw_type = data[4];
    const length = std.mem.readInt(u32, data[8..12], .little);
    const padding = std.mem.readInt(u32, data[12..16], .little);

    if (padding > 7 or padding != calcPadding(length)) return .{ .record_type = .entry, .payload = &.{}, .valid = false };
    const payload_end = std.math.add(usize, RECORD_HEADER_SIZE, length) catch return .{ .record_type = .entry, .payload = &.{}, .valid = false };
    const total_needed = std.math.add(usize, payload_end, padding) catch return .{ .record_type = .entry, .payload = &.{}, .valid = false };
    if (data.len < total_needed) return .{ .record_type = .entry, .payload = &.{}, .valid = false };
    const record_type = checkedEnum(RecordType, raw_type) orelse return .{ .record_type = .entry, .payload = &.{}, .valid = false };

    // Verify CRC.
    var crc = Crc32Iscsi.init();
    crc.update(&[_]u8{raw_type});
    crc.update(&[_]u8{data[5]}); // flags
    crc.update(data[6..8]); // reserved
    crc.update(data[8..12]); // length
    crc.update(data[12..16]); // padding
    crc.update(data[16..payload_end]); // payload
    if (crc.final() != stored_crc) return .{ .record_type = .entry, .payload = &.{}, .valid = false };

    return .{
        .record_type = record_type,
        .payload = data[16..payload_end],
        .valid = true,
    };
}

// ===========================================================================
// Entry / HardState / ConfState serialization
// ===========================================================================

/// Serialize an Entry to bytes. Format:
///   entry_type(1) + term(8) + index(8) + checksum(4) + data_len(4) + data + ctx_len(4) + context
fn serializeEntry(allocator: std.mem.Allocator, entry: Entry) ![]u8 {
    const header_size: usize = 1 + 8 + 8 + 4 + 4 + 4;
    const data_len = std.math.cast(u32, entry.data.len) orelse return error.RecordTooLarge;
    const context_len = std.math.cast(u32, entry.context.len) orelse return error.RecordTooLarge;
    const data_end = try std.math.add(usize, header_size, entry.data.len);
    const total = try std.math.add(usize, data_end, entry.context.len);
    var out = try allocator.alloc(u8, total);
    var pos: usize = 0;
    out[pos] = @intFromEnum(entry.entry_type);
    pos += 1;
    std.mem.writeInt(u64, out[pos..][0..8], entry.term, .little);
    pos += 8;
    std.mem.writeInt(u64, out[pos..][0..8], entry.index, .little);
    pos += 8;
    std.mem.writeInt(u32, out[pos..][0..4], entry.checksum, .little);
    pos += 4;
    std.mem.writeInt(u32, out[pos..][0..4], data_len, .little);
    pos += 4;
    @memcpy(out[pos .. pos + entry.data.len], entry.data);
    pos += entry.data.len;
    std.mem.writeInt(u32, out[pos..][0..4], context_len, .little);
    pos += 4;
    @memcpy(out[pos .. pos + entry.context.len], entry.context);
    return out;
}

fn deserializeEntry(allocator: std.mem.Allocator, data: []const u8) !Entry {
    if (data.len < 1 + 8 + 8 + 4 + 4) return error.EntryParseError;
    var pos: usize = 0;
    const entry_type = checkedEnum(EntryType, data[pos]) orelse return error.EntryParseError;
    pos += 1;
    const term = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const index = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const checksum = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const data_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const data_end = std.math.add(usize, pos, data_len) catch return error.EntryParseError;
    const context_header_end = std.math.add(usize, data_end, 4) catch return error.EntryParseError;
    if (data.len < context_header_end) return error.EntryParseError;
    const entry_data: []u8 = if (data_len > 0) try allocator.dupe(u8, data[pos..data_end]) else &.{};
    errdefer if (entry_data.len > 0) allocator.free(entry_data);
    pos += data_len;
    const ctx_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const context_end = std.math.add(usize, pos, ctx_len) catch return error.EntryParseError;
    if (data.len != context_end) return error.EntryParseError;
    const context: []u8 = if (ctx_len > 0) try allocator.dupe(u8, data[pos..context_end]) else &.{};
    return .{
        .entry_type = entry_type,
        .term = term,
        .index = index,
        .checksum = checksum,
        .data = entry_data,
        .context = context,
    };
}

/// Serialize HardState (term + vote + commit = 24 bytes).
fn serializeHardState(hs: HardState) [24]u8 {
    var out: [24]u8 = undefined;
    std.mem.writeInt(u64, out[0..8], hs.term, .little);
    std.mem.writeInt(u64, out[8..16], hs.vote, .little);
    std.mem.writeInt(u64, out[16..24], hs.commit, .little);
    return out;
}

fn deserializeHardState(data: []const u8) !HardState {
    if (data.len != 24) return error.HardStateParseError;
    return .{
        .term = std.mem.readInt(u64, data[0..8], .little),
        .vote = std.mem.readInt(u64, data[8..16], .little),
        .commit = std.mem.readInt(u64, data[16..24], .little),
    };
}

fn deserializeSnapshotMetadata(data: []const u8) !SnapshotMetadata {
    if (data.len != 16) return error.SnapshotParseError;
    return .{
        .index = std.mem.readInt(u64, data[0..8], .little),
        .term = std.mem.readInt(u64, data[8..16], .little),
    };
}

/// Serialize ConfState. Format:
///   voters_len(4) + voters + learners_len(4) + learners +
///   voters_outgoing_len(4) + voters_outgoing + learners_next_len(4) + learners_next +
///   auto_leave(1)
fn serializeConfState(allocator: std.mem.Allocator, cs: ConfState) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    try writeU64Slice(allocator, &buf, cs.voters);
    try writeU64Slice(allocator, &buf, cs.learners);
    try writeU64Slice(allocator, &buf, cs.voters_outgoing);
    try writeU64Slice(allocator, &buf, cs.learners_next);
    try buf.append(allocator, if (cs.auto_leave) 1 else 0);
    return buf.toOwnedSlice(allocator);
}

fn writeU64Slice(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), slice: []const u64) !void {
    var len_bytes: [4]u8 = undefined;
    const len = std.math.cast(u32, slice.len) orelse return error.RecordTooLarge;
    std.mem.writeInt(u32, &len_bytes, len, .little);
    try buf.appendSlice(allocator, &len_bytes);
    for (slice) |v| {
        var v_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &v_bytes, v, .little);
        try buf.appendSlice(allocator, &v_bytes);
    }
}

fn deserializeConfState(allocator: std.mem.Allocator, data: []const u8) !ConfState {
    var pos: usize = 0;
    const voters = try readU64Slice(allocator, data, &pos);
    errdefer allocator.free(voters);
    const learners = try readU64Slice(allocator, data, &pos);
    errdefer allocator.free(learners);
    const voters_outgoing = try readU64Slice(allocator, data, &pos);
    errdefer allocator.free(voters_outgoing);
    const learners_next = try readU64Slice(allocator, data, &pos);
    errdefer allocator.free(learners_next);
    if (pos + 1 != data.len) return error.ConfStateParseError;
    return .{
        .voters = voters,
        .learners = learners,
        .voters_outgoing = voters_outgoing,
        .learners_next = learners_next,
        .auto_leave = data[pos] != 0,
    };
}

fn readU64Slice(allocator: std.mem.Allocator, data: []const u8, pos: *usize) ![]u64 {
    const header_end = std.math.add(usize, pos.*, 4) catch return error.ConfStateParseError;
    if (data.len < header_end) return error.ConfStateParseError;
    const len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    const count: usize = @intCast(len);
    const byte_len = std.math.mul(usize, count, 8) catch return error.ConfStateParseError;
    const end = std.math.add(usize, pos.*, byte_len) catch return error.ConfStateParseError;
    if (data.len < end) return error.ConfStateParseError;
    const out = try allocator.alloc(u64, count);
    for (0..count) |i| {
        out[i] = std.mem.readInt(u64, data[pos.* + i * 8 ..][0..8], .little);
    }
    pos.* += count * 8;
    return out;
}

fn checkedEnum(comptime T: type, value: std.meta.Tag(T)) ?T {
    inline for (std.meta.fields(T)) |field| {
        if (field.value == value) return @enumFromInt(value);
    }
    return null;
}

// ===========================================================================
// ===========================================================================
// WAL — segmented write-ahead log
// ===========================================================================

pub const WALConfig = struct {
    dir: [:0]const u8,
    segment_size: u64 = 64 * 1024 * 1024, // 64 MB default
};

pub const WAL = struct {
    allocator: std.mem.Allocator,
    dir: [:0]u8,
    segment_size: u64,
    segment_manager: segment_manager_mod.SegmentManager,
    metadata_store: metadata_store_mod.MetadataStore,
    metadata_dirty: bool,
    wal_index: wal_index_mod.WALIndex,

    // In-memory state recovered from the log.
    entries: std.ArrayList(Entry),
    hard_state: HardState,
    conf_state: ConfState,
    snapshot_metadata: SnapshotMetadata,
    first_index: u64,

    pub fn open(allocator: std.mem.Allocator, config: WALConfig) !WAL {
        // Create directory if it does not exist.
        try segment_mod.makeDir(config.dir);

        var sm = try segment_manager_mod.SegmentManager.init(allocator, config.dir);
        var owns_sm = true;
        errdefer if (owns_sm) sm.deinit();
        var metadata_store = try metadata_store_mod.MetadataStore.init(allocator, config.dir);
        var owns_metadata_store = true;
        errdefer if (owns_metadata_store) metadata_store.deinit();

        // If no segments exist, create the first one.
        if (sm.getCurrent() == null) {
            _ = try sm.rollToNew(1);
        }

        const dir_copy = try allocator.dupeSentinel(u8, config.dir, 0);

        var wal = WAL{
            .allocator = allocator,
            .dir = dir_copy,
            .segment_size = config.segment_size,
            .segment_manager = sm,
            .metadata_store = metadata_store,
            .metadata_dirty = false,
            .wal_index = wal_index_mod.WALIndex.init(allocator),
            .entries = .empty,
            .hard_state = .{},
            .conf_state = .{},
            .snapshot_metadata = .{},
            .first_index = 1,
        };
        owns_sm = false;
        owns_metadata_store = false;
        errdefer wal.deinit();

        try wal.recover();
        if (wal.metadata_dirty) try wal.sync();

        log.info("WAL opened: dir={s}, segments={}, entries={}, first={}, last={}", .{ wal.dir, wal.segment_manager.count(), wal.entries.items.len, wal.firstIndex(), wal.lastIndex() });
        return wal;
    }

    pub fn deinit(self: *WAL) void {
        self.segment_manager.deinit();
        self.metadata_store.deinit();
        self.wal_index.deinit();
        for (self.entries.items) |*e| e.deinit(self.allocator);
        self.entries.deinit(self.allocator);
        self.conf_state.deinit(self.allocator);
        self.snapshot_metadata.deinit(self.allocator);
        self.allocator.free(self.dir);
        self.* = undefined;
    }

    fn recover(self: *WAL) !void {
        var persisted_metadata = try self.metadata_store.load();
        defer if (persisted_metadata) |*metadata| metadata.deinit(self.allocator);
        const has_metadata = persisted_metadata != null;
        if (persisted_metadata) |metadata| {
            self.first_index = metadata.first_index;
            self.wal_index.setFirstIndex(metadata.first_index);
            self.snapshot_metadata = .{
                .index = metadata.snapshot_index,
                .term = metadata.snapshot_term,
            };
            if (metadata.hard_state.len > 0) self.hard_state = try deserializeHardState(metadata.hard_state);
            if (metadata.conf_state.len > 0) self.conf_state = try deserializeConfState(self.allocator, metadata.conf_state);
        }

        const segs = self.segment_manager.segments.items;
        if (!has_metadata and segs.len > 0) {
            self.first_index = segs[0].segment.first_index;
            self.wal_index.setFirstIndex(self.first_index);
        }
        for (segs) |entry| {
            const seg = entry.segment;
            const body_size = seg.file_size - SEGMENT_HEADER_SIZE;
            if (body_size == 0) continue;

            const data = try self.allocator.alloc(u8, body_size);
            defer self.allocator.free(data);
            const n = try seg.read(data, SEGMENT_HEADER_SIZE);
            if (n < body_size) continue;

            var offset: usize = 0;
            while (offset < n) {
                const remaining = data[offset..];
                const parsed = parseRecord(remaining);
                if (!parsed.valid) break;
                const pad = calcPadding(@intCast(parsed.payload.len));
                const total = RECORD_HEADER_SIZE + parsed.payload.len + pad;
                switch (parsed.record_type) {
                    .entry => {
                        var e = deserializeEntry(self.allocator, parsed.payload) catch break;
                        if (e.index < self.first_index) {
                            e.deinit(self.allocator);
                            offset += total;
                            continue;
                        }
                        try self.entries.ensureUnusedCapacity(self.allocator, 1);
                        try self.wal_index.ensureUnusedCapacity(1);
                        self.wal_index.insertAssumeCapacity(e.index, .{
                            .segment_id = entry.id,
                            .offset = SEGMENT_HEADER_SIZE + offset,
                            .length = @intCast(total),
                            .term = e.term,
                        }) catch |err| {
                            e.deinit(self.allocator);
                            return err;
                        };
                        self.entries.appendAssumeCapacity(e);
                    },
                    .hard_state => {
                        if (!has_metadata) self.hard_state = deserializeHardState(parsed.payload) catch break;
                    },
                    .conf_state => {
                        if (!has_metadata) {
                            self.conf_state.deinit(self.allocator);
                            self.conf_state = deserializeConfState(self.allocator, parsed.payload) catch break;
                        }
                    },
                    .snapshot => {
                        if (!has_metadata) {
                            const metadata = deserializeSnapshotMetadata(parsed.payload) catch break;
                            self.snapshot_metadata.deinit(self.allocator);
                            self.snapshot_metadata = metadata;
                        }
                    },
                }
                offset += total;
            }
        }

        if (!has_metadata and self.entries.items.len > 0) {
            self.first_index = self.entries.items[0].index;
            self.wal_index.setFirstIndex(self.first_index);
        }
        if (!has_metadata) self.metadata_dirty = true;
    }

    fn writeRecord(self: *WAL, record_type: RecordType, payload: []const u8) !RecordLocation {
        const record = try buildRecord(self.allocator, record_type, payload);
        defer self.allocator.free(record);
        const record_length = std.math.cast(u32, record.len) orelse return error.RecordTooLarge;

        // Roll segment if needed.
        const cur = self.segment_manager.getCurrent() orelse
            try self.segment_manager.rollToNew(if (self.entries.items.len > 0)
                self.entries.items[self.entries.items.len - 1].index + 1
            else
                1);

        if (cur.write_offset + record.len > self.segment_size and cur.write_offset > SEGMENT_HEADER_SIZE) {
            const next_idx = if (self.entries.items.len > 0)
                self.entries.items[self.entries.items.len - 1].index + 1
            else
                cur.first_index;
            _ = try self.segment_manager.rollToNew(next_idx);
        }

        const active = self.segment_manager.getCurrent().?;
        const offset = active.write_offset;
        try active.append(record);
        return .{
            .segment_id = active.segment_id,
            .offset = offset,
            .length = record_length,
        };
    }

    pub fn append(self: *WAL, entries: []const Entry) !void {
        if (entries.len == 0) return;
        for (entries[1..], entries[0 .. entries.len - 1]) |entry, previous| {
            if (entry.index != std.math.add(u64, previous.index, 1) catch return error.Fatal) return error.Fatal;
        }
        if (entries[0].index < self.first_index) return error.Fatal;

        const next_index = std.math.add(u64, self.lastIndex(), 1) catch return error.Fatal;
        if (entries[0].index > next_index) return error.Fatal;

        var append_from: usize = 0;
        while (append_from < entries.len and entries[append_from].index <= self.lastIndex()) : (append_from += 1) {
            const existing_offset: usize = @intCast(entries[append_from].index - self.first_index);
            if (existing_offset >= self.entries.items.len) return error.Fatal;
            if (!entryEql(self.entries.items[existing_offset], entries[append_from])) break;
        }
        if (append_from == entries.len) return;

        const first_new = entries[append_from].index;
        if (first_new <= self.lastIndex()) {
            if (first_new <= self.hard_state.commit) return error.Fatal;
            try self.truncateSuffixFrom(first_new);
        }
        if (first_new != std.math.add(u64, self.lastIndex(), 1) catch return error.Fatal) return error.Fatal;

        for (entries[append_from..]) |entry| {
            var cloned = try cloneEntry(self.allocator, entry);
            errdefer cloned.deinit(self.allocator);
            try self.entries.ensureUnusedCapacity(self.allocator, 1);
            try self.wal_index.ensureUnusedCapacity(1);
            const payload = try serializeEntry(self.allocator, entry);
            defer self.allocator.free(payload);
            const location = try self.writeRecord(.entry, payload);
            try self.wal_index.insertAssumeCapacity(entry.index, .{
                .segment_id = location.segment_id,
                .offset = location.offset,
                .length = location.length,
                .term = entry.term,
            });
            self.entries.appendAssumeCapacity(cloned);
        }
    }

    pub fn saveHardState(self: *WAL, hs: HardState) !void {
        const payload = serializeHardState(hs);
        _ = try self.writeRecord(.hard_state, &payload);
        self.hard_state = hs;
        self.metadata_dirty = true;
    }

    pub fn saveConfState(self: *WAL, cs: ConfState) !void {
        var cloned = try cloneConfState(self.allocator, cs);
        errdefer cloned.deinit(self.allocator);
        const payload = try serializeConfState(self.allocator, cs);
        defer self.allocator.free(payload);
        _ = try self.writeRecord(.conf_state, payload);
        self.conf_state.deinit(self.allocator);
        self.conf_state = cloned;
        self.metadata_dirty = true;
    }

    pub fn saveSnapshotMetadata(self: *WAL, meta: SnapshotMetadata) !void {
        var payload: [16]u8 = undefined;
        std.mem.writeInt(u64, payload[0..8], meta.index, .little);
        std.mem.writeInt(u64, payload[8..16], meta.term, .little);
        _ = try self.writeRecord(.snapshot, &payload);
        self.snapshot_metadata.deinit(self.allocator);
        self.snapshot_metadata = .{ .index = meta.index, .term = meta.term };
        self.metadata_dirty = true;
    }

    pub fn sync(self: *WAL) !void {
        try self.segment_manager.syncAll();
        if (self.metadata_dirty) try self.syncMetadata();
    }

    pub fn close(self: *WAL) !void {
        try self.sync();
        self.segment_manager.closeAll();
    }

    pub fn compact(self: *WAL, compact_index: u64) !void {
        if (self.entries.items.len == 0) return;
        if (compact_index <= self.firstIndex()) return;
        if (compact_index > self.lastIndex() + 1) return error.Fatal;

        // Remove entries from memory.
        const drop_count: usize = @intCast(compact_index - self.firstIndex());
        var i: usize = 0;
        while (i < drop_count) : (i += 1) self.entries.items[i].deinit(self.allocator);
        std.mem.copyForwards(Entry, self.entries.items[0..], self.entries.items[drop_count..]);
        self.entries.shrinkRetainingCapacity(self.entries.items.len - drop_count);
        self.wal_index.truncateBefore(compact_index);
        self.first_index = compact_index;
        self.metadata_dirty = true;

        // Delete old segment files.
        // A segment is safe to delete if its first_index < compact_index AND
        // a newer segment exists. We keep the segment that straddles compact_index.
        var min_surviving_id: u64 = std.math.maxInt(u64);
        const segs = self.segment_manager.segments.items;
        for (segs) |entry| {
            const seg = entry.segment;
            if (seg.first_index >= compact_index) {
                if (entry.id < min_surviving_id) min_surviving_id = entry.id;
            }
        }
        if (min_surviving_id != std.math.maxInt(u64) and min_surviving_id > 1) {
            try self.segment_manager.removeSegmentsBefore(min_surviving_id);
        }
    }

    // -----------------------------------------------------------------------
    // Query helpers (unchanged — read from in-memory entries)
    // -----------------------------------------------------------------------

    pub fn firstIndex(self: WAL) u64 {
        return self.first_index;
    }

    pub fn lastIndex(self: WAL) u64 {
        if (self.entries.items.len == 0) return self.snapshot_metadata.index;
        return self.entries.items[self.entries.items.len - 1].index;
    }

    pub fn term(self: WAL, idx: u64) Error!u64 {
        if (idx == self.snapshot_metadata.index and self.snapshot_metadata.index > 0) {
            return self.snapshot_metadata.term;
        }
        if (idx < self.firstIndex()) return error.Compacted;
        return self.wal_index.term(idx) orelse error.Unavailable;
    }

    pub fn readEntries(
        self: WAL,
        allocator: std.mem.Allocator,
        low: u64,
        high: u64,
        max_size: ?u64,
    ) Error![]Entry {
        if (self.entries.items.len == 0) return allocator.alloc(Entry, 0);
        const offset = self.firstIndex();
        if (low < offset) return error.Compacted;
        if (high > self.lastIndex() + 1) return error.Fatal;
        const lo: usize = @intCast(low - offset);
        const hi: usize = @intCast(high - offset);
        var result = try allocator.alloc(Entry, hi - lo);
        var actual: usize = 0;
        for (self.entries.items[lo..hi]) |e| {
            result[actual] = try cloneEntry(allocator, e);
            actual += 1;
        }
        if (max_size) |ms| {
            var view = result[0..actual];
            @import("core/util.zig").limitSize(&view, ms);
            for (view.len..actual) |j| result[j].deinit(allocator);
            actual = view.len;
        }
        return allocator.realloc(result, actual) catch result[0..actual];
    }

    fn syncMetadata(self: *WAL) !void {
        const hard_state = serializeHardState(self.hard_state);
        const conf_state = try serializeConfState(self.allocator, self.conf_state);
        defer self.allocator.free(conf_state);
        try self.metadata_store.save(.{
            .first_index = self.first_index,
            .snapshot_index = self.snapshot_metadata.index,
            .snapshot_term = self.snapshot_metadata.term,
            .hard_state = @constCast(hard_state[0..]),
            .conf_state = conf_state,
        });
        self.metadata_dirty = false;
    }

    fn truncateSuffixFrom(self: *WAL, index: u64) !void {
        const location = self.wal_index.lookup(index) orelse return error.Fatal;
        const segment = self.segment_manager.get(location.segment_id) orelse return error.Fatal;
        try segment.truncate(location.offset);
        try segment.sync();
        try self.segment_manager.removeSegmentsAfter(location.segment_id);
        try self.segment_manager.syncAll();

        const keep_count: usize = @intCast(index - self.first_index);
        for (self.entries.items[keep_count..]) |*entry| entry.deinit(self.allocator);
        self.entries.shrinkRetainingCapacity(keep_count);
        self.wal_index.truncateFrom(index);
    }
};

fn entryEql(a: Entry, b: Entry) bool {
    return a.index == b.index and
        a.term == b.term and
        a.entry_type == b.entry_type and
        a.checksum == b.checksum and
        std.mem.eql(u8, a.data, b.data) and
        std.mem.eql(u8, a.context, b.context);
}

// ===========================================================================
// WALStorage — adapts WAL to the WritableStorage vtable interface
// ===========================================================================

pub const WALStorage = struct {
    wal: WAL,
    allocator: std.mem.Allocator,

    pub fn open(allocator: std.mem.Allocator, dir: [:0]const u8) Error!*WALStorage {
        const self = try allocator.create(WALStorage);
        errdefer allocator.destroy(self);
        self.* = .{
            .wal = WAL.open(allocator, .{ .dir = dir }) catch |err| return mapError(err),
            .allocator = allocator,
        };
        return self;
    }

    pub fn deinit(self: *WALStorage) void {
        self.wal.deinit();
        self.allocator.destroy(self);
    }

    fn initial_state_impl(ctx: *anyopaque, allocator: std.mem.Allocator) Error!RaftState {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        return .{
            .hard_state = self.wal.hard_state,
            .conf_state = try cloneConfState(allocator, self.wal.conf_state),
        };
    }

    fn entries_impl(ctx: *anyopaque, allocator: std.mem.Allocator, low: u64, high: u64, max_size: ?u64, _: GetEntriesContext) Error![]Entry {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        return self.wal.readEntries(allocator, low, high, max_size);
    }

    fn term_impl(ctx: *anyopaque, idx: u64) Error!u64 {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        return self.wal.term(idx);
    }

    fn first_index_impl(ctx: *anyopaque) Error!u64 {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        return self.wal.firstIndex();
    }

    fn last_index_impl(ctx: *anyopaque) Error!u64 {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        return self.wal.lastIndex();
    }

    fn get_snapshot_impl(_: *anyopaque, _: std.mem.Allocator, _: u64, _: u64) Error!Snapshot {
        return error.SnapshotTemporarilyUnavailable;
    }

    fn append_impl(ctx: *anyopaque, allocator: std.mem.Allocator, to_append: []const Entry) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        _ = allocator;
        self.wal.append(to_append) catch |err| return mapError(err);
    }

    fn set_hard_state_impl(ctx: *anyopaque, hs: HardState) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        self.wal.saveHardState(hs) catch |err| return mapError(err);
    }

    fn set_conf_state_impl(ctx: *anyopaque, allocator: std.mem.Allocator, cs: ConfState) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        _ = allocator;
        self.wal.saveConfState(cs) catch |err| return mapError(err);
    }

    fn apply_snapshot_impl(ctx: *anyopaque, allocator: std.mem.Allocator, snap: Snapshot) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        _ = allocator;
        self.wal.saveSnapshotMetadata(snap.metadata) catch |err| return mapError(err);
    }

    fn apply_local_snapshot_impl(ctx: *anyopaque, allocator: std.mem.Allocator, snap: Snapshot) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        _ = allocator;
        self.wal.saveSnapshotMetadata(snap.metadata) catch |err| return mapError(err);
        self.wal.sync() catch |err| return mapError(err);
        self.wal.compact(snap.metadata.index + 1) catch |err| return mapError(err);
        self.wal.sync() catch |err| return mapError(err);
    }

    fn sync_impl(ctx: *anyopaque) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        self.wal.sync() catch |err| return mapError(err);
    }

    pub const writable_vtable: WritableStorage.VTable = .{
        .initial_state = initial_state_impl,
        .entries = entries_impl,
        .term = term_impl,
        .first_index = first_index_impl,
        .last_index = last_index_impl,
        .get_snapshot = get_snapshot_impl,
        .append = append_impl,
        .set_hard_state = set_hard_state_impl,
        .set_conf_state = set_conf_state_impl,
        .apply_snapshot = apply_snapshot_impl,
        .apply_local_snapshot = apply_local_snapshot_impl,
        .sync_ = sync_impl,
    };

    pub fn asWritableStorage(self: *WALStorage) WritableStorage {
        return .{ .ctx = self, .vtable = &writable_vtable };
    }

    pub fn asStorage(self: *WALStorage) Storage {
        return .{ .ctx = self, .vtable = &.{
            .initial_state = initial_state_impl,
            .entries = entries_impl,
            .term = term_impl,
            .first_index = first_index_impl,
            .last_index = last_index_impl,
            .get_snapshot = get_snapshot_impl,
        } };
    }
};

fn mapError(err: anyerror) Error {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.FileNotFound, error.OpenFailed => error.WalOpenFailed,
        error.ReadFailed => error.WalReadFailed,
        error.WriteFailed => error.WalWriteFailed,
        error.SyncFailed, error.DirectorySyncFailed => error.WalSyncFailed,
        error.TruncateFailed => error.WalTruncateFailed,
        error.UnlinkFailed => error.WalDeleteFailed,
        error.StatFailed => error.WalStatFailed,
        error.MkdirFailed => error.WalCreateDirectoryFailed,
        error.RenameFailed => error.WalRenameFailed,
        error.CloseFailed => error.WalCloseFailed,
        error.MetadataCorrupt => error.WalMetadataCorrupt,
        error.InvalidSegmentHeader => error.InvalidSegmentHeader,
        error.SegmentNotOpen => error.SegmentNotOpen,
        error.HardStateParseError => error.HardStateParseError,
        error.ConfStateParseError => error.ConfStateParseError,
        error.EntryParseError => error.EntryParseError,
        error.RecordTooLarge => error.MessageTooLarge,
        error.Fatal => error.Fatal,
        else => error.CorruptEntryRecord,
    };
}

// ===========================================================================
// Tests
// ===========================================================================

test "wal: record build and parse round-trip" {
    const allocator = std.testing.allocator;
    const payload = "hello wal";
    const record = try buildRecord(allocator, .entry, payload);
    defer allocator.free(record);

    const parsed = parseRecord(record);
    try std.testing.expect(parsed.valid);
    try std.testing.expectEqual(RecordType.entry, parsed.record_type);
    try std.testing.expectEqualStrings(payload, parsed.payload);
}

test "wal: record detects corruption" {
    const allocator = std.testing.allocator;
    const payload = "hello wal";
    var record = try buildRecord(allocator, .entry, payload);
    defer allocator.free(record);

    // Corrupt the payload.
    record[16] ^= 0xFF;

    const parsed = parseRecord(record);
    try std.testing.expect(!parsed.valid);
}

test "wal: entry serialize/deserialize round-trip" {
    const allocator = std.testing.allocator;
    const original = Entry{
        .entry_type = .normal,
        .term = 5,
        .index = 10,
        .checksum = 0xDEADBEEF,
        .data = try allocator.dupe(u8, "data bytes"),
        .context = try allocator.dupe(u8, "ctx"),
    };
    defer {
        var e = original;
        e.deinit(allocator);
    }

    const bytes = try serializeEntry(allocator, original);
    defer allocator.free(bytes);

    var decoded = try deserializeEntry(allocator, bytes);
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(original.entry_type, decoded.entry_type);
    try std.testing.expectEqual(original.term, decoded.term);
    try std.testing.expectEqual(original.index, decoded.index);
    try std.testing.expectEqual(original.checksum, decoded.checksum);
    try std.testing.expectEqualStrings(original.data, decoded.data);
    try std.testing.expectEqualStrings(original.context, decoded.context);
}

test "wal: hardstate serialize/deserialize" {
    const original = HardState{ .term = 3, .vote = 7, .commit = 42 };
    const bytes = serializeHardState(original);
    const decoded = try deserializeHardState(&bytes);
    try std.testing.expectEqual(original.term, decoded.term);
    try std.testing.expectEqual(original.vote, decoded.vote);
    try std.testing.expectEqual(original.commit, decoded.commit);
}

test "wal: fixed-size payload decoders reject wrong lengths" {
    try std.testing.expectError(error.HardStateParseError, deserializeHardState(&([_]u8{0} ** 23)));
    try std.testing.expectError(error.HardStateParseError, deserializeHardState(&([_]u8{0} ** 25)));
    try std.testing.expectError(error.SnapshotParseError, deserializeSnapshotMetadata(&([_]u8{0} ** 15)));
    try std.testing.expectError(error.SnapshotParseError, deserializeSnapshotMetadata(&([_]u8{0} ** 17)));
}

test "wal: maximum record length is rejected without overflow" {
    var header = [_]u8{0} ** RECORD_HEADER_SIZE;
    header[4] = @intFromEnum(RecordType.entry);
    std.mem.writeInt(u32, header[8..12], std.math.maxInt(u32), .little);
    std.mem.writeInt(u32, header[12..16], 1, .little);
    try std.testing.expect(!parseRecord(&header).valid);
}

test "wal: confstate serialize/deserialize round-trip" {
    const allocator = std.testing.allocator;
    const voters = try allocator.dupe(u64, &.{ 1, 2, 3 });
    defer allocator.free(voters);
    const learners = try allocator.dupe(u64, &.{4});
    defer allocator.free(learners);

    const original = ConfState{ .voters = voters, .learners = learners, .auto_leave = true };
    const bytes = try serializeConfState(allocator, original);
    defer allocator.free(bytes);

    var decoded = try deserializeConfState(allocator, bytes);
    defer decoded.deinit(allocator);
    try std.testing.expectEqualSlices(u64, original.voters, decoded.voters);
    try std.testing.expectEqualSlices(u64, original.learners, decoded.learners);
    try std.testing.expect(original.auto_leave);
}

const linux = std.os.linux;

fn removeWALDir(allocator: std.mem.Allocator, dir: [:0]const u8) void {
    // Best-effort: scan directory and delete segment files.
    var sm = segment_manager_mod.SegmentManager.init(allocator, dir) catch return;
    sm.removeAllSegments() catch {};
    sm.deinit();
    metadata_store_mod.removeFiles(allocator, dir);
    _ = linux.rmdir(dir.ptr);
}

fn removeFile(path: [:0]const u8) void {
    _ = linux.unlink(path.ptr);
}

fn createEmptyFile(path: [:0]const u8) !void {
    const flags: linux.O = .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true };
    const rc = linux.open(path.ptr, flags, 0o644);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    _ = linux.close(@intCast(rc));
}

test "wal: segment discovery accepts a compacted prefix" {
    const allocator = std.testing.allocator;
    const dir: [:0]const u8 = "/tmp/raft-zig-wal-test-segment-discovery";
    removeWALDir(allocator, dir);
    try segment_mod.makeDir(dir);

    const metadata_path: [:0]const u8 = "/tmp/raft-zig-wal-test-segment-discovery/metadata";
    try createEmptyFile(metadata_path);
    defer removeFile(metadata_path);

    const segment2 = try segment_mod.Segment.create(allocator, dir, 2, 10);
    segment2.destroy();
    const segment3 = try segment_mod.Segment.create(allocator, dir, 3, 20);
    segment3.destroy();

    {
        var manager = try segment_manager_mod.SegmentManager.init(allocator, dir);
        defer manager.deinit();
        try std.testing.expectEqual(@as(usize, 2), manager.count());
        try std.testing.expectEqual(@as(u64, 2), manager.segments.items[0].id);
        try std.testing.expectEqual(@as(u64, 3), manager.segments.items[1].id);
        const next = try manager.rollToNew(30);
        try std.testing.expectEqual(@as(u64, 4), next.segment_id);
        try manager.syncAll();
    }

    removeFile(metadata_path);
    removeWALDir(allocator, dir);
}

test "wal: segment discovery rejects a mismatched header id" {
    const allocator = std.testing.allocator;
    const dir: [:0]const u8 = "/tmp/raft-zig-wal-test-segment-id-mismatch";
    removeWALDir(allocator, dir);
    try segment_mod.makeDir(dir);
    defer _ = linux.rmdir(dir.ptr);

    const path = try segment_mod.makeFilename(allocator, dir, 2);
    defer allocator.free(path);
    defer removeFile(path);

    const segment = try segment_mod.Segment.create(allocator, dir, 2, 10);
    var wrong_id: [8]u8 = undefined;
    std.mem.writeInt(u64, &wrong_id, 9, .little);
    const rc = linux.pwrite(segment.fd.?, &wrong_id, wrong_id.len, 8);
    try std.testing.expectEqual(linux.E.SUCCESS, linux.errno(rc));
    segment.destroy();

    try std.testing.expectError(error.InvalidSegmentHeader, segment_manager_mod.SegmentManager.init(allocator, dir));
}

test "wal: segment rejects truncated header" {
    const allocator = std.testing.allocator;
    const dir: [:0]const u8 = "/tmp/raft-zig-wal-test-truncated-header";
    removeWALDir(allocator, dir);
    try segment_mod.makeDir(dir);
    defer _ = linux.rmdir(dir.ptr);

    const path = try segment_mod.makeFilename(allocator, dir, 1);
    defer allocator.free(path);
    defer removeFile(path);

    const segment = try segment_mod.Segment.create(allocator, dir, 1, 1);
    try segment.truncate(24);
    segment.destroy();

    try std.testing.expectError(error.InvalidSegmentHeader, segment_mod.Segment.open(allocator, path));
}

test "wal: segment close is idempotent" {
    const allocator = std.testing.allocator;
    const dir: [:0]const u8 = "/tmp/raft-zig-wal-test-segment-close";
    removeWALDir(allocator, dir);
    try segment_mod.makeDir(dir);
    defer _ = linux.rmdir(dir.ptr);

    const segment = try segment_mod.Segment.create(allocator, dir, 1, 1);
    defer {
        segment.unlink() catch {};
        segment.destroy();
    }

    segment.close();
    segment.close();
    try std.testing.expectError(error.SegmentNotOpen, segment.sync());
    try std.testing.expectError(error.SegmentNotOpen, segment.append("record"));
    try std.testing.expectError(error.SegmentNotOpen, segment.truncate(0));
    var buf: [1]u8 = undefined;
    try std.testing.expectError(error.SegmentNotOpen, segment.read(&buf, 0));
}

test "wal: storage sync propagates a closed segment" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/raft-zig-wal-test-sync-error";
    removeWALDir(allocator, path);

    {
        const storage = try WALStorage.open(allocator, path);
        defer storage.deinit();
        storage.wal.segment_manager.getCurrent().?.close();
        try std.testing.expectError(error.SegmentNotOpen, storage.asWritableStorage().sync());
    }

    removeWALDir(allocator, path);
}

test "wal: storage preserves I/O error categories" {
    try std.testing.expectEqual(error.WalOpenFailed, mapError(error.OpenFailed));
    try std.testing.expectEqual(error.WalReadFailed, mapError(error.ReadFailed));
    try std.testing.expectEqual(error.WalWriteFailed, mapError(error.WriteFailed));
    try std.testing.expectEqual(error.WalSyncFailed, mapError(error.SyncFailed));
    try std.testing.expectEqual(error.WalSyncFailed, mapError(error.DirectorySyncFailed));
    try std.testing.expectEqual(error.WalTruncateFailed, mapError(error.TruncateFailed));
    try std.testing.expectEqual(error.WalDeleteFailed, mapError(error.UnlinkFailed));
}

test "wal: open, append, recover" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/raft-zig-wal-test-recover";
    removeWALDir(allocator, path);

    {
        var wal = try WAL.open(allocator, .{ .dir = path, .segment_size = 4096 });
        defer wal.deinit();
        var e1 = Entry{ .index = 1, .term = 1, .data = try allocator.dupe(u8, "a") };
        defer e1.deinit(allocator);
        try wal.append(&.{e1});
        var e2 = Entry{ .index = 2, .term = 1, .data = try allocator.dupe(u8, "b") };
        defer e2.deinit(allocator);
        try wal.append(&.{e2});
        try wal.saveHardState(.{ .term = 1, .vote = 1, .commit = 2 });
        try wal.sync();
    }

    metadata_store_mod.removeFiles(allocator, path);

    // Reopen and verify recovery.
    {
        var wal = try WAL.open(allocator, .{ .dir = path, .segment_size = 4096 });
        defer wal.deinit();
        var regenerated_metadata = (try wal.metadata_store.load()).?;
        defer regenerated_metadata.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 2), wal.lastIndex());
        try std.testing.expectEqual(@as(u64, 1), wal.hard_state.term);
        try std.testing.expectEqual(@as(u64, 2), wal.hard_state.commit);

        const ents = try wal.readEntries(allocator, 1, 3, null);
        defer {
            for (ents) |*e| e.deinit(allocator);
            allocator.free(ents);
        }
        try std.testing.expectEqual(@as(usize, 2), ents.len);
        try std.testing.expectEqualStrings("a", ents[0].data);
        try std.testing.expectEqualStrings("b", ents[1].data);
    }

    removeWALDir(allocator, path);
}

test "wal: compact removes old entries" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/raft-zig-wal-test-compact";
    removeWALDir(allocator, path);

    {
        var wal = try WAL.open(allocator, .{ .dir = path, .segment_size = 4096 });
        defer wal.deinit();

        // Append entries 1..5.
        var i: u64 = 1;
        while (i <= 5) : (i += 1) {
            const e = Entry{ .index = i, .term = 1 };
            try wal.append(&.{e});
        }
        try std.testing.expectEqual(@as(u64, 1), wal.firstIndex());
        try std.testing.expectEqual(@as(u64, 5), wal.lastIndex());

        // Compact past index 3: removes entries 1 and 2.
        try wal.compact(3);
        try std.testing.expectEqual(@as(u64, 3), wal.firstIndex());
        try std.testing.expectEqual(@as(u64, 5), wal.lastIndex());

        // Verify entries 3..5 are readable.
        const ents = try wal.readEntries(allocator, 3, 6, null);
        defer {
            for (ents) |*e| e.deinit(allocator);
            allocator.free(ents);
        }
        try std.testing.expectEqual(@as(usize, 3), ents.len);
        try std.testing.expectEqual(@as(u64, 3), ents[0].index);
        try std.testing.expectEqual(@as(u64, 5), ents[2].index);

        // Entries below firstIndex are compacted.
        try std.testing.expectError(error.Compacted, wal.readEntries(allocator, 1, 3, null));
    }

    removeWALDir(allocator, path);
}

test "wal: suffix overwrite is idempotent and restart-safe" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/raft-zig-wal-test-overwrite";
    removeWALDir(allocator, path);

    {
        var wal = try WAL.open(allocator, .{ .dir = path, .segment_size = 80 });
        defer wal.deinit();
        try wal.append(&.{
            .{ .index = 1, .term = 1, .data = @constCast("a") },
            .{ .index = 2, .term = 1, .data = @constCast("b") },
            .{ .index = 3, .term = 1, .data = @constCast("c") },
            .{ .index = 4, .term = 1, .data = @constCast("d") },
        });
        try wal.saveHardState(.{ .term = 5, .vote = 1, .commit = 1 });
        try wal.sync();
        try std.testing.expect(wal.segment_manager.count() >= 4);

        const offset_before_retry = wal.segment_manager.getCurrent().?.write_offset;
        try wal.append(&.{.{ .index = 4, .term = 1, .data = @constCast("d") }});
        try std.testing.expectEqual(offset_before_retry, wal.segment_manager.getCurrent().?.write_offset);

        try std.testing.expectError(error.Fatal, wal.append(&.{.{ .index = 1, .term = 9 }}));
        try std.testing.expectError(error.Fatal, wal.append(&.{.{ .index = 6, .term = 2 }}));

        try wal.append(&.{
            .{ .index = 2, .term = 2, .data = @constCast("new-b") },
            .{ .index = 3, .term = 2, .data = @constCast("new-c") },
        });
        try wal.sync();
        try std.testing.expectEqual(@as(u64, 3), wal.lastIndex());
    }

    {
        var wal = try WAL.open(allocator, .{ .dir = path, .segment_size = 80 });
        defer wal.deinit();
        try std.testing.expectEqual(@as(u64, 5), wal.hard_state.term);
        try std.testing.expectEqual(@as(u64, 1), wal.hard_state.vote);
        try std.testing.expectEqual(@as(u64, 1), wal.hard_state.commit);
        try std.testing.expectEqual(@as(u64, 3), wal.lastIndex());
        const entries = try wal.readEntries(allocator, 1, 4, null);
        defer {
            for (entries) |*entry| entry.deinit(allocator);
            allocator.free(entries);
        }
        try std.testing.expectEqualStrings("a", entries[0].data);
        try std.testing.expectEqualStrings("new-b", entries[1].data);
        try std.testing.expectEqualStrings("new-c", entries[2].data);
        try std.testing.expectEqual(@as(u64, 2), entries[2].term);
    }

    removeWALDir(allocator, path);
}

test "wal: WALStorage applyLocalSnapshot compacts" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/raft-zig-wal-test-snap-compact";
    removeWALDir(allocator, path);

    {
        var ws = try WALStorage.open(allocator, path);
        defer ws.deinit();

        // Append entries 1..4.
        const ws_iface = ws.asWritableStorage();
        try ws_iface.append(allocator, &.{
            .{ .index = 1, .term = 1 },
            .{ .index = 2, .term = 1 },
            .{ .index = 3, .term = 1 },
            .{ .index = 4, .term = 1 },
        });

        // Apply local snapshot at index 2 → should compact entries 1..2.
        const voters = try allocator.dupe(u64, &.{1});
        var snap = Snapshot{
            .metadata = .{ .index = 2, .term = 1, .conf_state = .{ .voters = voters } },
        };
        defer snap.deinit(allocator);

        try ws_iface.applyLocalSnapshot(allocator, snap);

        // After compact, firstIndex should be 3 (2+1).
        try std.testing.expectEqual(@as(u64, 3), try ws_iface.firstIndex());
        try std.testing.expectEqual(@as(u64, 4), try ws_iface.lastIndex());
    }

    removeWALDir(allocator, path);
}

test "wal: restart recovers entries and hardstate via WALStorage" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/raft-zig-wal-test-restart";
    removeWALDir(allocator, path);

    // First session: write entries + hardstate.
    {
        var ws = try WALStorage.open(allocator, path);
        defer ws.deinit();

        const iface = ws.asWritableStorage();
        try iface.append(allocator, &.{
            .{ .index = 1, .term = 1 },
            .{ .index = 2, .term = 2 },
        });
        try iface.setHardState(.{ .term = 2, .vote = 1, .commit = 2 });
        try iface.sync();
    }

    // Second session: reopen and verify recovery.
    {
        var ws = try WALStorage.open(allocator, path);
        defer ws.deinit();

        const iface = ws.asWritableStorage();
        try std.testing.expectEqual(@as(u64, 2), try iface.lastIndex());
        try std.testing.expectEqual(@as(u64, 2), try iface.term(2));

        const rs = try iface.initialState(allocator);
        var rs_copy = rs;
        defer rs_copy.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 2), rs_copy.hard_state.term);
        try std.testing.expectEqual(@as(u64, 1), rs_copy.hard_state.vote);
        try std.testing.expectEqual(@as(u64, 2), rs_copy.hard_state.commit);
    }

    removeWALDir(allocator, path);
}

test "wal: WALStorage vtable dispatches correctly" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/raft-zig-wal-test-vtable";
    removeWALDir(allocator, path);

    {
        var ws = try WALStorage.open(allocator, path);
        defer ws.deinit();

        const ws_interface = ws.asWritableStorage();
        try ws_interface.append(allocator, &.{.{ .index = 1, .term = 1 }});
        try ws_interface.setHardState(.{ .term = 1, .commit = 1 });
        try ws_interface.sync();

        try std.testing.expectEqual(@as(u64, 1), try ws_interface.lastIndex());
        try std.testing.expectEqual(@as(u64, 1), try ws_interface.term(1));
    }

    // Reopen via vtable and verify.
    {
        var ws = try WALStorage.open(allocator, path);
        defer ws.deinit();

        const ws_interface = ws.asWritableStorage();
        try std.testing.expectEqual(@as(u64, 1), try ws_interface.lastIndex());

        const rs = try ws_interface.initialState(allocator);
        var rs_copy = rs;
        defer rs_copy.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), rs_copy.hard_state.commit);
    }

    removeWALDir(allocator, path);
}

test "fuzz: WAL record and payload decoders" {
    try std.testing.fuzz({}, fuzzWalDecoders, .{ .corpus = &.{
        "",
        "WAL1",
        "\xff\xff\xff\xff\xff\xff\xff\xff",
    } });
}

fn fuzzWalDecoders(_: void, smith: *std.testing.Smith) !void {
    const allocator = std.testing.allocator;
    var input_buffer: [4096]u8 = undefined;
    const input_len = smith.valueRangeAtMost(u16, 0, input_buffer.len);
    const input = input_buffer[0..input_len];
    smith.bytes(input);

    checkParsedRecord(allocator, parseRecord(input));

    const record_type = smith.value(RecordType);
    const record = try buildRecord(allocator, record_type, input);
    defer allocator.free(record);
    const parsed = parseRecord(record);
    try std.testing.expect(parsed.valid);
    try std.testing.expectEqual(record_type, parsed.record_type);
    try std.testing.expectEqualSlices(u8, input, parsed.payload);
    checkParsedRecord(allocator, parsed);
}

fn checkParsedRecord(allocator: std.mem.Allocator, parsed: ParsedRecord) void {
    if (!parsed.valid) return;
    switch (parsed.record_type) {
        .entry => {
            if (deserializeEntry(allocator, parsed.payload)) |entry_value| {
                var entry = entry_value;
                defer entry.deinit(allocator);
                const canonical = serializeEntry(allocator, entry) catch return;
                defer allocator.free(canonical);
                var round_trip = deserializeEntry(allocator, canonical) catch return;
                defer round_trip.deinit(allocator);
            } else |_| {}
        },
        .hard_state => _ = deserializeHardState(parsed.payload) catch {},
        .conf_state => {
            if (deserializeConfState(allocator, parsed.payload)) |conf_value| {
                var conf = conf_value;
                defer conf.deinit(allocator);
                const canonical = serializeConfState(allocator, conf) catch return;
                defer allocator.free(canonical);
                var round_trip = deserializeConfState(allocator, canonical) catch return;
                defer round_trip.deinit(allocator);
            } else |_| {}
        },
        .snapshot => _ = deserializeSnapshotMetadata(parsed.payload) catch {},
    }
}
