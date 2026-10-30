// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
include!("chunk/protocol_receipts.inc.rs");
include!("chunk/service_state.inc.rs");
// Reaching this replica's state through calls instead of through its fields -- the step a
// replica in another process needs, because a field read cannot cross a process boundary.
include!("chunk/page_access.rs");
include!("chunk/configuration_mutation.inc.rs");
include!("chunk/write_read_repair.inc.rs");
include!("chunk/cache_reclaim_snapshot.inc.rs");
include!("chunk/chunk_footprint.rs");
