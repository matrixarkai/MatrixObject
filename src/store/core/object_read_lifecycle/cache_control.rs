// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Cache control is grouped by operation, cardinality, and storage ownership.
include!("cache_prewarm.rs");
include!("cache_batch_prewarm.rs");
include!("cache_invalidation.rs");
include!("cache_batch_invalidation.rs");
include!("cache_batch_execution.rs");
include!("page_cache_administration.rs");
