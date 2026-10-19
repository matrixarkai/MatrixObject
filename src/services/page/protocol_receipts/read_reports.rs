// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkReadStats {
    pub chunk_reads: u64,
    pub cache_hits: u64,
    pub coalesced_chunk_read_count: u64,
    pub physical_chunk_read_count: u64,
    pub metadata_head_batch_requests: u64,
    pub metadata_physical_head_count: u64,
    pub metadata_coalesced_head_count: u64,
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
    pub replica_fallback_reads: u64,
    pub quorum_verified_chunks: u64,
    pub replica_read_repairs: u64,
    pub replica_read_repair_failures: u64,
    pub quorum_read_degraded_chunks: u64,
}

impl ChunkReadStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("chunk_reads", self.chunk_reads),
            ("cache_hits", self.cache_hits),
            (
                "coalesced_chunk_read_count",
                self.coalesced_chunk_read_count,
            ),
            ("physical_chunk_read_count", self.physical_chunk_read_count),
            (
                "metadata_head_batch_requests",
                self.metadata_head_batch_requests,
            ),
            (
                "metadata_physical_head_count",
                self.metadata_physical_head_count,
            ),
            (
                "metadata_coalesced_head_count",
                self.metadata_coalesced_head_count,
            ),
            ("metadata_cache_hits", self.metadata_cache_hits),
            ("metadata_cache_misses", self.metadata_cache_misses),
            ("replica_fallback_reads", self.replica_fallback_reads),
            ("quorum_verified_chunks", self.quorum_verified_chunks),
            ("replica_read_repairs", self.replica_read_repairs),
            (
                "replica_read_repair_failures",
                self.replica_read_repair_failures,
            ),
            (
                "quorum_read_degraded_chunks",
                self.quorum_read_degraded_chunks,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkReadRepairReport {
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub length: u64,
    pub data: Vec<u8>,
    pub payload_checksum: String,
    pub stats: ChunkReadStats,
    pub repaired_replicas: u64,
    pub failed_repairs: u64,
    pub degraded_chunks: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
}
