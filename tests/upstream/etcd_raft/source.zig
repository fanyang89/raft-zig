const manifest = @import("upstream_manifest");

pub const upstream: manifest.Source = .{
    .name = "etcd/raft",
    .repository = "https://github.com/etcd-io/raft",
    .revision = "56e32004b1af3a4cb625fbfe5dbca24fb6023d09",
    .license = "Apache-2.0",
    .policy = "Primary baseline; adapt behavior with attribution.",
    .inventory = @embedFile("cases.jsonl"),
    .expected_case_count = 299,
    .expected_status_counts = .{
        .adapted = 12,
        .covered_elsewhere = 69,
        .excluded = 30,
        .blocked = 2,
        .planned = 186,
    },
};
