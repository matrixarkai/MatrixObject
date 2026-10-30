// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Chunk lifecycle operations are grouped by mutation and Raft phase.
include!("chunk_discard_prepare.rs");
include!("chunk_discard_propose.rs");
include!("chunk_compaction_prepare.rs");
include!("chunk_compaction_propose.rs");
