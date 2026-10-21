// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
include!("chunk_handoff_readiness/state_validation_ec.rs");
// The fragment arithmetic for a cross-node scheme, not yet placed on replicas.
//
// `cfg(test)` rather than `allow(dead_code)`: CI builds `--lib` with
// `-D warnings`, and an item nothing calls is a hard error there for a good
// reason. This is exercised only by
// `any_k_of_k_plus_m_fragments_reconstruct_the_payload` today, so it compiles
// only for tests; the attribute comes off in the change that gives fragments
// somewhere to live.
#[cfg(test)]
include!("chunk_handoff_readiness/fragment_coding.rs");
include!("chunk_handoff_readiness/checksums.rs");
include!("chunk_handoff_readiness/prepared_envelopes.rs");
include!("chunk_handoff_readiness/handoff_validation.rs");
include!("chunk_handoff_readiness/page_readiness_fingerprint.rs");
include!("chunk_handoff_readiness/chunk_readiness_fingerprint.rs");
