// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn audit_metadata_route_cache_linearizable(
        &mut self,
        now_ms: u64,
        evict_stale: bool,
    ) -> Result<(MetadataRouteCacheAudit, MetadataRaftReadiness), ObjectError> {
        self.audit_metadata_route_cache_linearizable_fenced_sampled(
            now_ms,
            evict_stale,
            None,
            None,
            None,
            0,
        )
    }

    pub fn audit_metadata_route_cache_with_read_proof_linearizable(
        &mut self,
        now_ms: u64,
        evict_stale: bool,
    ) -> Result<ProvenMetadataRead<MetadataRouteCacheAudit>, ObjectError> {
        self.audit_metadata_route_cache_with_read_proof_linearizable_fenced_sampled(
            now_ms,
            evict_stale,
            None,
            None,
            None,
            0,
        )
    }

    pub fn batch_resolve_metadata_routes_linearizable_fenced(
        &mut self,
        keys: &[String],
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<(MetadataShardRouteBatch, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(commit_index, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata route batch read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let batch = self.store.batch_resolve_metadata_routes_cached(
            keys,
            max_payload_bytes,
            now_ms,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
        )?;
        Ok((batch, readiness))
    }

    pub fn audit_metadata_route_cache_linearizable_fenced(
        &mut self,
        now_ms: u64,
        evict_stale: bool,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<(MetadataRouteCacheAudit, MetadataRaftReadiness), ObjectError> {
        self.audit_metadata_route_cache_linearizable_fenced_sampled(
            now_ms,
            evict_stale,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
            0,
        )
    }

    pub fn audit_metadata_route_cache_with_read_proof_linearizable_fenced_sampled(
        &mut self,
        now_ms: u64,
        evict_stale: bool,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
        max_stale_samples: usize,
    ) -> Result<ProvenMetadataRead<MetadataRouteCacheAudit>, ObjectError> {
        let (audit, readiness) = self.audit_metadata_route_cache_linearizable_fenced_sampled(
            now_ms,
            evict_stale,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
            max_stale_samples,
        )?;
        Ok(proven_metadata_read(audit, &readiness))
    }

    pub fn audit_metadata_route_cache_linearizable_fenced_sampled(
        &mut self,
        now_ms: u64,
        evict_stale: bool,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
        max_stale_samples: usize,
    ) -> Result<(MetadataRouteCacheAudit, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(commit_index, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata route cache audit read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let audit = self.store.audit_metadata_route_cache_fenced_sampled(
            now_ms,
            evict_stale,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
            max_stale_samples,
        )?;
        Ok((audit, readiness))
    }

}
