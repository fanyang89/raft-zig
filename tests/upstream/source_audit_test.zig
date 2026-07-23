const manifest = @import("upstream_manifest");

test "upstream source manifests are valid" {
    try manifest.audit(@import("etcd_raft/source.zig").upstream);
    try manifest.audit(@import("raft_rs/source.zig").upstream);
    try manifest.audit(@import("openraft/source.zig").upstream);
    try manifest.audit(@import("hashicorp_raft/source.zig").upstream);
}
