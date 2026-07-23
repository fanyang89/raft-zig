const manifest = @import("upstream_manifest");

pub const upstream: manifest.Source = .{
    .name = "HashiCorp Raft",
    .repository = "https://github.com/hashicorp/raft",
    .revision = "dd30865f162c68ee31130c7f8ee1047e9122f2ec",
    .license = "MPL-2.0",
    .policy = "Clean-room reimplementation of externally observable behavior only.",
    .cases = &.{},
};
