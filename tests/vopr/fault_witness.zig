//! Fault-fired witnesses for crash-fault tests.
//!
//! The post-crash invariants asserted by the WAL and cluster crash tests are
//! wide enough to pass even when the injected disk fault had no observable
//! effect (for example, every pending write happened to land in order before
//! the crash was honored). These helpers parse marionette's `disk.crash`
//! trace events to prove the fault actually fired, complementing the
//! committed-prefix anchors each test records before crashing.

const std = @import("std");

pub const CrashWitness = struct {
    /// Number of aggregate `disk.crash` events recorded.
    crashes: u64 = 0,
    pending_writes: u64 = 0,
    lost: u64 = 0,
    torn: u64 = 0,
    reordered: u64 = 0,
    pending_metadata: u64 = 0,
    metadata_lost: u64 = 0,

    /// True when at least one pending write/metadata was observably lost,
    /// torn, or reordered by a crash.
    pub fn fired(self: CrashWitness) bool {
        return self.lost + self.torn + self.reordered + self.metadata_lost > 0;
    }
};

/// Sum every aggregate `disk.crash` event in the trace. Per-write
/// `disk.crash_write` / `disk.crash_metadata` lines are excluded so only the
/// aggregate counters (which the disk emits once per crash) are counted.
pub fn collectCrashWitness(trace: []const u8) CrashWitness {
    var w = CrashWitness{};
    var lines = std.mem.splitScalar(u8, trace, '\n');
    while (lines.next()) |line| {
        var toks = std.mem.splitScalar(u8, line, ' ');
        const evt = toks.next() orelse continue;
        if (!std.mem.startsWith(u8, evt, "event=")) continue;
        const name = toks.next() orelse continue;
        if (!std.mem.eql(u8, name, "disk.crash")) continue;

        w.crashes += 1;
        while (toks.next()) |tok| {
            const eq = std.mem.indexOfScalar(u8, tok, '=') orelse continue;
            const key = tok[0..eq];
            const val = tok[eq + 1 ..];
            const n = std.fmt.parseInt(u64, val, 10) catch continue;
            if (std.mem.eql(u8, key, "pending_writes")) {
                w.pending_writes += n;
            } else if (std.mem.eql(u8, key, "lost")) {
                w.lost += n;
            } else if (std.mem.eql(u8, key, "torn")) {
                w.torn += n;
            } else if (std.mem.eql(u8, key, "reordered")) {
                w.reordered += n;
            } else if (std.mem.eql(u8, key, "pending_metadata")) {
                w.pending_metadata += n;
            } else if (std.mem.eql(u8, key, "metadata_lost")) {
                w.metadata_lost += n;
            }
        }
    }
    return w;
}
