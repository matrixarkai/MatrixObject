// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Read receipt queries are grouped by consistency and query cardinality.
include!("metadata_record_queries.rs");
include!("linearizable_head_queries.rs");
include!("shard_head_queries.rs");
include!("linearizable_batch_head_queries.rs");
include!("shard_batch_head_queries.rs");
