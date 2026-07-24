const manifest = @import("upstream_manifest");

pub const upstream: manifest.Source = .{
    .name = "OpenRaft",
    .repository = "https://github.com/databendlabs/openraft",
    .revision = "0d15d99844e8245ac917ce76ce2e4598665d0e40",
    .license = "MIT OR Apache-2.0",
    .policy = "Reimplement stateful invariants and non-duplicate behavior.",
    .inventory = "",
    .expected_case_count = 0,
    .expected_status_counts = .{},
};
