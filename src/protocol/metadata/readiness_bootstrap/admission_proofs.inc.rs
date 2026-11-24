// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn object_read_proof_from_readiness(
    readiness: &MetadataRaftReadiness,
) -> ObjectReadProof {
    ObjectReadProof {
        requested_read_index: readiness.min_read_index,
        consensus_read_index: readiness.safe_read_fence,
        metadata_commit_index: readiness.applied_commit_index,
        read_fence_read_index_lag: readiness.read_fence_read_index_lag,
        read_fence_applied_lag: readiness.read_fence_applied_lag,
    }
}
