//! raft-zig: a Zig implementation of the RAFT consensus algorithm.
//!
//! This module is the public entry point. It re-exports the stable types that
//! applications and integrations depend on. Lower-level modules are also
//! exported for experimentation but may evolve before 1.0.
//!
//! The port mirrors the layered architecture of raftpp:
//!   * `core/`  — plain data types, errors, roles, and status snapshots.
//!   * Future layers (`log`, `progress`, `raft`, `raw_node`, `raftor`,
//!     `wal`, `rpc`) will be added under `src/` as they are ported.

const std = @import("std");

const version_info = @import("version.zig");
const primitives = @import("core/primitives.zig");
const types = @import("core/types.zig");
const error_model = @import("core/error.zig");
const state_role = @import("core/state_role.zig");
const status = @import("core/status.zig");

pub const core = .{
    .primitives = primitives,
    .types = types,
    .error_model = error_model,
    .state_role = state_role,
    .status = status,
};

pub const Error = error_model.Error;
pub const errorName = error_model.name;

pub const EntryType = types.EntryType;
pub const MessageType = types.MessageType;
pub const ConfChangeType = types.ConfChangeType;
pub const ConfChangeTransition = types.ConfChangeTransition;
pub const Entry = types.Entry;
pub const HardState = types.HardState;
pub const ConfState = types.ConfState;
pub const SnapshotMetadata = types.SnapshotMetadata;
pub const Snapshot = types.Snapshot;
pub const ConfChangeSingle = types.ConfChangeSingle;
pub const ConfChangeV2 = types.ConfChangeV2;
pub const ConfChange = types.ConfChange;
pub const Message = types.Message;

pub const invalid_index = primitives.invalid_index;
pub const invalid_id = primitives.invalid_id;

pub const StateRole = state_role.StateRole;
pub const SoftState = state_role.SoftState;
pub const roleName = state_role.roleName;

pub const Status = status.Status;

pub const version = version_info.string;

test "version is parseable" {
    _ = try std.SemanticVersion.parse(version);
}

test "re-exported modules compile" {
    _ = core;
    _ = primitives;
    _ = types;
    _ = error_model;
    _ = state_role;
    _ = status;
    _ = version_info;
}
