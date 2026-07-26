const manifest = @import("upstream_manifest");

pub const upstream: manifest.Source = .{
    .name = "Dragonboat",
    .repository = "https://github.com/lni/dragonboat",
    .revision = "076c7f6497dcc18880aed6323246d5079661942c",
    .license = "Apache-2.0",
    .policy = "Adapt only Dragonboat core deltas and historical regressions; delegate etcd-derived tests to the primary baseline.",
    .inventory = @embedFile("cases.jsonl"),
    .expected_case_count = 407,
    .expected_status_counts = .{
        .adapted = 13,
        .covered_elsewhere = 83,
        .excluded = 246,
        .blocked = 2,
        .planned = 63,
    },
};
