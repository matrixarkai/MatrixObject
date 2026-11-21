// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

include!("membership_plan.rs");
include!("membership_receipts.rs");
include!("raft_log_backend.rs");

// The names the chunk state machine and write group are written against (they were imported by the
// metadata state machine file they were included beside, which went with the embedded store).
#[cfg(feature = "matrix-raft")]
use matrixraft::{
    ApplyRequest, ApplyResponse, LogId, RaftError, SnapshotChunk, SnapshotMetadata, StateMachine,
};

#[cfg(feature = "matrix-raft")]
include!("matrixraft_chunk_state_machine.rs");

#[cfg(feature = "matrix-raft")]
include!("matrixraft_chunk_write_group.rs");
