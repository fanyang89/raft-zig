const manifest = @import("upstream_manifest");

pub const upstream: manifest.Source = .{
    .name = "raft-rs",
    .repository = "https://github.com/tikv/raft-rs",
    .revision = "ad13f3d90780f53aea2488c6a4b76c0d334bf136",
    .license = "Apache-2.0",
    .policy = "Adapt only deltas from etcd/raft and historical regressions.",
    .inventory = @embedFile("cases.jsonl"),
    .expected_case_count = 263,
    .expected_status_counts = .{
        .adapted = 44,
        .covered_elsewhere = 111,
        .excluded = 99,
        .blocked = 9,
        .planned = 0,
    },
};
