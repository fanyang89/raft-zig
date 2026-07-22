//! Minimal single-file Write-Ahead Log with CRC32C integrity.
//!
//! Ports a subset of `ref/raftpp/lib/raftor/wal/` — enough to provide a
//! durable `WritableStorage` backend. Simplifications vs raftpp:
//!
//!   * Single file instead of segmented (no SegmentManager).
//!   * No WALIndex (linear scan during recovery).
//!   * No metadata file (HardState/ConfState stored as records).
//!   * No io_uring backend.
//!
//! Record format matches raftpp exactly (magic "WAL1", CRC32C over
//! type+flags+length+padding+payload), so WAL files are forward-compatible.

const std = @import("std");

const error_model = @import("core/error.zig");
const types = @import("core/types.zig");
const storage_mod = @import("storage.zig");

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
// File I/O helpers (Linux syscalls — Zig 0.16 removed std.fs.cwd())
// ===========================================================================

const linux = std.os.linux;

fn errno(rc: usize) i32 {
    const signed: isize = @bitCast(rc);
    if (signed >= 0) return 0;
    return @intCast(-signed);
}

fn walOpen(path: [:0]const u8) !linux.fd_t {
    const flags: linux.O = .{
        .ACCMODE = .RDWR,
        .CREAT = true,
    };
    const rc = linux.open(path.ptr, flags, 0o644);
    if (errno(rc) != 0) return error.OpenFailed;
    return @intCast(rc);
}

fn walWrite(fd: linux.fd_t, data: []const u8) !void {
    var written: usize = 0;
    while (written < data.len) {
        const rc = linux.write(fd, data.ptr + written, data.len - written);
        const e = errno(rc);
        if (e != 0) return error.WriteFailed;
        written += rc;
    }
}

fn walRead(fd: linux.fd_t, buf: []u8) !usize {
    const rc = linux.read(fd, buf.ptr, buf.len);
    const e = errno(rc);
    if (e != 0) return error.ReadFailed;
    return rc;
}

fn walLseek(fd: linux.fd_t, offset: i64) !void {
    const rc = linux.lseek(fd, offset, 0); // SEEK_SET = 0
    if (errno(rc) != 0) return error.SeekFailed;
}

fn walFsync(fd: linux.fd_t) !void {
    const rc = linux.fsync(fd);
    if (errno(rc) != 0) return error.SyncFailed;
}

fn walClose(fd: linux.fd_t) void {
    _ = linux.close(fd);
}

fn walUnlink(path: [:0]const u8) void {
    _ = linux.unlink(path.ptr);
}

fn walFileSize(fd: linux.fd_t) !u64 {
    // Seek to end to get file size, then restore position.
    const cur = linux.lseek(fd, 0, 1); // SEEK_CUR
    if (errno(cur) != 0) return error.StatFailed;
    const end = linux.lseek(fd, 0, 2); // SEEK_END
    if (errno(end) != 0) return error.StatFailed;
    _ = linux.lseek(fd, @intCast(cur), 0); // SEEK_SET restore
    return @intCast(end);
}

/// Simple file wrapper that hides the fd.
const WalFile = struct {
    fd: linux.fd_t,

    fn open(path: [:0]const u8) !WalFile {
        const fd = try walOpen(path);
        return .{ .fd = fd };
    }

    fn close(self: WalFile) void {
        walClose(self.fd);
    }

    fn writeAll(self: WalFile, data: []const u8) !void {
        try walWrite(self.fd, data);
    }

    fn readAt(self: WalFile, buf: []u8, offset: i64) !usize {
        try walLseek(self.fd, offset);
        return walRead(self.fd, buf);
    }

    fn sync(self: WalFile) !void {
        try walFsync(self.fd);
    }

    fn size(self: WalFile) !u64 {
        return walFileSize(self.fd);
    }

    fn seekTo(self: WalFile, offset: i64) !void {
        try walLseek(self.fd, offset);
    }
};

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
    const total = RECORD_HEADER_SIZE + payload_len;
    const rem = total % 8;
    return if (rem == 0) 0 else @intCast(8 - rem);
}

/// Build a single WAL record (header + payload + padding). Caller owns the
/// returned slice.
fn buildRecord(allocator: std.mem.Allocator, record_type: RecordType, payload: []const u8) ![]u8 {
    const length: u32 = @intCast(payload.len);
    const padding = calcPadding(length);
    const total = RECORD_HEADER_SIZE + payload.len + padding;
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

    const total_needed = RECORD_HEADER_SIZE + length + padding;
    if (data.len < total_needed) return .{ .record_type = .entry, .payload = &.{}, .valid = false };

    // Verify CRC.
    var crc = Crc32Iscsi.init();
    crc.update(&[_]u8{raw_type});
    crc.update(&[_]u8{data[5]}); // flags
    crc.update(data[6..8]); // reserved
    crc.update(data[8..12]); // length
    crc.update(data[12..16]); // padding
    crc.update(data[16 .. 16 + length]); // payload
    if (crc.final() != stored_crc) return .{ .record_type = .entry, .payload = &.{}, .valid = false };

    return .{
        .record_type = @enumFromInt(raw_type),
        .payload = data[16 .. 16 + length],
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
    const total = header_size + entry.data.len + entry.context.len;
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
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(entry.data.len), .little);
    pos += 4;
    @memcpy(out[pos .. pos + entry.data.len], entry.data);
    pos += entry.data.len;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(entry.context.len), .little);
    pos += 4;
    @memcpy(out[pos .. pos + entry.context.len], entry.context);
    return out;
}

fn deserializeEntry(allocator: std.mem.Allocator, data: []const u8) !Entry {
    if (data.len < 1 + 8 + 8 + 4 + 4) return error.EntryParseError;
    var pos: usize = 0;
    const entry_type: EntryType = @enumFromInt(data[pos]);
    pos += 1;
    const term = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const index = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const checksum = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const data_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (data.len < pos + data_len + 4) return error.EntryParseError;
    const entry_data: []u8 = if (data_len > 0) try allocator.dupe(u8, data[pos .. pos + data_len]) else &.{};
    pos += data_len;
    const ctx_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (data.len < pos + ctx_len) return error.EntryParseError;
    const context: []u8 = if (ctx_len > 0) try allocator.dupe(u8, data[pos .. pos + ctx_len]) else &.{};
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

fn deserializeHardState(data: []const u8) HardState {
    if (data.len < 24) return .{};
    return .{
        .term = std.mem.readInt(u64, data[0..8], .little),
        .vote = std.mem.readInt(u64, data[8..16], .little),
        .commit = std.mem.readInt(u64, data[16..24], .little),
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
    std.mem.writeInt(u32, &len_bytes, @intCast(slice.len), .little);
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
    if (pos >= data.len) return error.ConfStateParseError;
    return .{
        .voters = voters,
        .learners = learners,
        .voters_outgoing = voters_outgoing,
        .learners_next = learners_next,
        .auto_leave = data[pos] != 0,
    };
}

fn readU64Slice(allocator: std.mem.Allocator, data: []const u8, pos: *usize) ![]u64 {
    if (data.len < pos.* + 4) return error.ConfStateParseError;
    const len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    const count: usize = @intCast(len);
    if (data.len < pos.* + count * 8) return error.ConfStateParseError;
    const out = try allocator.alloc(u64, count);
    for (0..count) |i| {
        out[i] = std.mem.readInt(u64, data[pos.* + i * 8 ..][0..8], .little);
    }
    pos.* += count * 8;
    return out;
}

// ===========================================================================
// WAL — single-file write-ahead log
// ===========================================================================

pub const WAL = struct {
    allocator: std.mem.Allocator,
    file: WalFile,
    path: [:0]u8,

    // In-memory state recovered from the log.
    entries: std.ArrayList(Entry),
    hard_state: HardState,
    conf_state: ConfState,
    snapshot_metadata: SnapshotMetadata,
    first_index: u64,

    /// Open or create a WAL file at `path`. If the file exists and contains
    /// valid records, they are replayed into memory.
    pub fn open(allocator: std.mem.Allocator, path: [:0]const u8) !WAL {
        const path_copy = try allocator.dupeSentinel(u8, path, 0);
        errdefer allocator.free(path_copy);

        const file = try WalFile.open(path);
        errdefer file.close();

        var wal = WAL{
            .allocator = allocator,
            .file = file,
            .path = path_copy,
            .entries = .empty,
            .hard_state = .{},
            .conf_state = .{},
            .snapshot_metadata = .{},
            .first_index = 1,
        };

        const sz = try file.size();
        if (sz == 0) {
            // New file: write segment header.
            var header: [SEGMENT_HEADER_SIZE]u8 = undefined;
            encodeSegmentHeader(&header, 1, 1);
            try file.writeAll(&header);
            try file.sync();
        } else {
            // Existing file: verify header and replay.
            try wal.recover();
        }

        return wal;
    }

    pub fn deinit(self: *WAL) void {
        self.file.close();
        for (self.entries.items) |*e| e.deinit(self.allocator);
        self.entries.deinit(self.allocator);
        self.conf_state.deinit(self.allocator);
        self.snapshot_metadata.deinit(self.allocator);
        self.allocator.free(self.path);
        self.* = undefined;
    }

    fn recover(self: *WAL) !void {
        try self.file.seekTo(0);
        var header_buf: [SEGMENT_HEADER_SIZE]u8 = undefined;
        const n = try self.file.readAt(&header_buf, 0);
        if (n < SEGMENT_HEADER_SIZE or !isValidSegmentHeader(&header_buf)) {
            return error.InvalidSegmentHeader;
        }

        // Read the rest of the file sequentially.
        const sz = try self.file.size();
        const body_size = sz - SEGMENT_HEADER_SIZE;
        const body = try self.allocator.alloc(u8, body_size);
        defer self.allocator.free(body);
        const body_read = try self.file.readAt(body, @intCast(SEGMENT_HEADER_SIZE));
        if (body_read < body_size) return error.CorruptEntryRecord;

        // Parse records sequentially.
        var offset: usize = 0;
        while (offset < body_read) {
            const remaining = body[offset..];
            const parsed = parseRecord(remaining);
            if (!parsed.valid) break; // truncated or corrupt; stop replay
            const total_consumed = RECORD_HEADER_SIZE + parsed.payload.len + calcPadding(@intCast(parsed.payload.len));
            switch (parsed.record_type) {
                .entry => {
                    const e = deserializeEntry(self.allocator, parsed.payload) catch break;
                    try self.entries.append(self.allocator, e);
                },
                .hard_state => {
                    self.hard_state = deserializeHardState(parsed.payload);
                },
                .conf_state => {
                    self.conf_state.deinit(self.allocator);
                    self.conf_state = deserializeConfState(self.allocator, parsed.payload) catch break;
                },
                .snapshot => {
                    if (parsed.payload.len >= 16) {
                        self.snapshot_metadata.deinit(self.allocator);
                        self.snapshot_metadata = .{
                            .index = std.mem.readInt(u64, parsed.payload[0..8], .little),
                            .term = std.mem.readInt(u64, parsed.payload[8..16], .little),
                        };
                    }
                },
            }
            offset += total_consumed;
        }

        if (self.entries.items.len > 0) {
            self.first_index = self.entries.items[0].index;
        }

        log.info("recovered {} entries, first={}, last={}, hs_term={}", .{
            self.entries.items.len, self.firstIndex(), self.lastIndex(), self.hard_state.term,
        });
    }

    pub fn append(self: *WAL, entries: []const Entry) !void {
        for (entries) |entry| {
            const payload = try serializeEntry(self.allocator, entry);
            defer self.allocator.free(payload);
            const record = try buildRecord(self.allocator, .entry, payload);
            defer self.allocator.free(record);
            try self.file.writeAll(record);
            try self.entries.append(self.allocator, try cloneEntry(self.allocator, entry));
        }
    }

    pub fn saveHardState(self: *WAL, hs: HardState) !void {
        const payload = serializeHardState(hs);
        const record = try buildRecord(self.allocator, .hard_state, &payload);
        defer self.allocator.free(record);
        try self.file.writeAll(record);
        self.hard_state = hs;
    }

    pub fn saveConfState(self: *WAL, cs: ConfState) !void {
        const payload = try serializeConfState(self.allocator, cs);
        defer self.allocator.free(payload);
        const record = try buildRecord(self.allocator, .conf_state, payload);
        defer self.allocator.free(record);
        try self.file.writeAll(record);
        self.conf_state.deinit(self.allocator);
        self.conf_state = try cloneConfState(self.allocator, cs);
    }

    pub fn saveSnapshotMetadata(self: *WAL, meta: SnapshotMetadata) !void {
        var payload: [16]u8 = undefined;
        std.mem.writeInt(u64, payload[0..8], meta.index, .little);
        std.mem.writeInt(u64, payload[8..16], meta.term, .little);
        const record = try buildRecord(self.allocator, .snapshot, &payload);
        defer self.allocator.free(record);
        try self.file.writeAll(record);
        self.snapshot_metadata.deinit(self.allocator);
        self.snapshot_metadata = .{
            .index = meta.index,
            .term = meta.term,
        };
    }

    pub fn sync(self: *WAL) !void {
        try self.file.sync();
    }

    pub fn close(self: *WAL) !void {
        try self.sync();
        self.file.close();
    }

    // -----------------------------------------------------------------------
    // Query helpers
    // -----------------------------------------------------------------------

    pub fn firstIndex(self: WAL) u64 {
        if (self.entries.items.len == 0) return self.snapshot_metadata.index + 1;
        return self.entries.items[0].index;
    }

    pub fn lastIndex(self: WAL) u64 {
        if (self.entries.items.len == 0) return self.snapshot_metadata.index;
        return self.entries.items[self.entries.items.len - 1].index;
    }

    pub fn term(self: WAL, idx: u64) Error!u64 {
        if (idx == self.snapshot_metadata.index and self.snapshot_metadata.index > 0) {
            return self.snapshot_metadata.term;
        }
        if (self.entries.items.len == 0) return error.Unavailable;
        const offset = self.firstIndex();
        if (idx < offset) return error.Compacted;
        if (idx > self.lastIndex()) return error.Unavailable;
        const i: usize = @intCast(idx - offset);
        return self.entries.items[i].term;
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
            // Free truncated tail.
            for (view.len..actual) |i| result[i].deinit(allocator);
            actual = view.len;
        }
        return allocator.realloc(result, actual) catch result[0..actual];
    }
};

// ===========================================================================
// WALStorage — adapts WAL to the WritableStorage vtable interface
// ===========================================================================

pub const WALStorage = struct {
    wal: WAL,
    allocator: std.mem.Allocator,

    pub fn open(allocator: std.mem.Allocator, path: [:0]const u8) !*WALStorage {
        const self = try allocator.create(WALStorage);
        errdefer allocator.destroy(self);
        self.* = .{
            .wal = try WAL.open(allocator, path),
            .allocator = allocator,
        };
        return self;
    }

    pub fn deinit(self: *WALStorage) void {
        self.wal.deinit();
        self.allocator.destroy(self);
    }

    // ---- Storage vtable impl ----

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
        self.wal.append(to_append) catch return error.OutOfMemory;
    }

    fn set_hard_state_impl(ctx: *anyopaque, hs: HardState) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        self.wal.saveHardState(hs) catch return error.OutOfMemory;
    }

    fn set_conf_state_impl(ctx: *anyopaque, allocator: std.mem.Allocator, cs: ConfState) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        _ = allocator;
        self.wal.saveConfState(cs) catch return error.OutOfMemory;
    }

    fn apply_snapshot_impl(ctx: *anyopaque, allocator: std.mem.Allocator, snap: Snapshot) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        _ = allocator;
        self.wal.saveSnapshotMetadata(snap.metadata) catch return error.OutOfMemory;
    }

    fn apply_local_snapshot_impl(ctx: *anyopaque, allocator: std.mem.Allocator, snap: Snapshot) Error!void {
        // For WAL, apply_local_snapshot persists the snapshot metadata and
        // compacts entries before it. Currently delegates to apply_snapshot
        // since WAL compact is not yet implemented.
        return apply_snapshot_impl(ctx, allocator, snap);
    }

    fn sync_impl(ctx: *anyopaque) Error!void {
        const self: *WALStorage = @ptrCast(@alignCast(ctx));
        self.wal.sync() catch return error.OutOfMemory;
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
    const decoded = deserializeHardState(&bytes);
    try std.testing.expectEqual(original.term, decoded.term);
    try std.testing.expectEqual(original.vote, decoded.vote);
    try std.testing.expectEqual(original.commit, decoded.commit);
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

test "wal: open, append, recover" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/opencode/wal_test_open_append_recover.wal";
    walUnlink(path);

    {
        var wal = try WAL.open(allocator, path);
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

    // Reopen and verify recovery.
    {
        var wal = try WAL.open(allocator, path);
        defer wal.deinit();
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

    walUnlink(path);
}

test "wal: WALStorage vtable dispatches correctly" {
    const allocator = std.testing.allocator;
    const path: [:0]const u8 = "/tmp/opencode/wal_test_vtable.wal";
    walUnlink(path);

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

    walUnlink(path);
}
