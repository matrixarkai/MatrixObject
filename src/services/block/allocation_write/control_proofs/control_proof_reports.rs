impl MatrixPageService {
    pub fn route_control_proof(&self) -> PageRouteControlProof {
        let route_qos_state = self.route_qos_state();
        let write_quorum = self.write_quorum as u64;
        let configured_write_quorum = self.write_quorum as u64;
        let read_quorum = self.read_quorum as u64;
        let configured_read_quorum = self.read_quorum as u64;
        let endpoint_material_len = self
            .chunk_endpoints
            .iter()
            .map(|endpoint| encoded_string_len(endpoint) + std::mem::size_of::<u64>() * 2)
            .sum::<usize>();
        let route_material_capacity = encoded_string_len("MOSRSBLOCKROUTE10")
            + endpoint_material_len
            + self.sealed_slabs.len() * std::mem::size_of::<u64>()
            + self.replicas.len() * std::mem::size_of::<u64>() * 13
            + std::mem::size_of::<u64>() * 20;
        let mut material = Vec::with_capacity(route_material_capacity);
        append_string(&mut material, "MOSRSBLOCKROUTE10");
        append_u64(&mut material, self.page_route_control_commit_index);
        append_u64(&mut material, self.page_route_control_next_log_index);
        append_u64(&mut material, write_quorum);
        append_u64(&mut material, configured_write_quorum);
        append_u64(&mut material, read_quorum);
        append_u64(&mut material, configured_read_quorum);
        append_u64(&mut material, self.chunk_endpoints.len() as u64);
        for (index, endpoint) in self.chunk_endpoints.iter().enumerate() {
            append_u64(&mut material, index as u64);
            append_string(&mut material, endpoint);
            append_u64(
                &mut material,
                self.disabled_chunk_endpoints.contains(&index) as u64,
            );
        }
        append_u64(&mut material, self.replicas.len() as u64);
        append_u64(&mut material, self.quarantined_replicas.len() as u64);
        append_page_route_qos_config(&mut material, &self.route_qos_config);
        append_u64(&mut material, self.receipt_retention as u64);
        append_u64(&mut material, self.sealed_slabs.len() as u64);
        for slab in &self.sealed_slabs {
            append_slab_id(&mut material, slab);
        }
        append_u64(
            &mut material,
            self.primary_batch_read_repair_probe_limit as u64,
        );
        for (index, replica) in self.replicas.iter().enumerate() {
            append_u64(&mut material, index as u64);
            append_u64(
                &mut material,
                self.quarantined_replicas.contains(&index) as u64,
            );
            append_u64(
                &mut material,
                self.disabled_chunk_endpoints.contains(&index) as u64,
            );
            append_u64(&mut material, replica.accepting_writes() as u64);
            append_u64(&mut material, replica.is_draining() as u64);
            append_u64(&mut material, replica.is_frozen() as u64);
            append_u64(&mut material, replica.capacity_bytes());
            append_chunk_mutation_qos_config(&mut material, &replica.mutation_qos_config());
            append_u64(&mut material, replica.trim_flow_rate_bytes_per_sec());
            append_u64(&mut material, replica.trim_flow_burst_bytes());
            append_u64(&mut material, replica.trim_flow_rate_ops_per_sec());
            append_u64(&mut material, replica.trim_flow_burst_ops());
        }
        let disabled_replica_count = self
            .replicas
            .iter()
            .enumerate()
            .filter(|(index, replica)| {
                self.quarantined_replicas.contains(index)
                    || self.disabled_chunk_endpoints.contains(index)
                    || replica.is_frozen()
            })
            .count() as u64;
        PageRouteControlProof {
            control_receipt: true,
            state_format: "MOSRSBLOCKROUTE10".to_string(),
            state_bytes: material.len() as u64,
            state_checksum: etag(&material),
            page_route_control_commit_index: self.page_route_control_commit_index,
            page_route_control_next_log_index: self.page_route_control_next_log_index,
            chunk_endpoint_count: self.chunk_endpoints.len() as u64,
            disabled_chunk_endpoint_count: self.disabled_chunk_endpoints.len() as u64,
            write_quorum,
            configured_write_quorum,
            read_quorum,
            configured_read_quorum,
            disabled_replica_count,
            qos_window_ms: self.route_qos_config.window_ms,
            qos_max_read_ops_per_window: self.route_qos_config.max_read_ops_per_window,
            qos_max_read_bytes_per_window: self.route_qos_config.max_read_bytes_per_window,
            qos_max_write_ops_per_window: self.route_qos_config.max_write_ops_per_window,
            qos_max_write_bytes_per_window: self.route_qos_config.max_write_bytes_per_window,
            qos_max_discard_ops_per_window: self.route_qos_config.max_discard_ops_per_window,
            qos_max_discard_bytes_per_window: self.route_qos_config.max_discard_bytes_per_window,
            qos_throttled_ops: route_qos_state.throttled_ops,
            qos_throttled_bytes: route_qos_state.throttled_bytes,
            receipt_retention: self.receipt_retention as u64,
            primary_batch_read_repair_probe_limit: self.primary_batch_read_repair_probe_limit
                as u64,
            cumulative_quorum_write_degraded_chunks: self.cumulative_quorum_write_degraded_chunks,
            cumulative_quorum_read_degraded_chunks: self.cumulative_quorum_read_degraded_chunks,
            cumulative_quorum_discard_degraded_chunks: self
                .cumulative_quorum_discard_degraded_chunks,
            cumulative_replica_fallback_reads: self.cumulative_replica_fallback_reads,
            cumulative_physical_chunk_read_count: self.cumulative_physical_chunk_read_count.get(),
            cumulative_coalesced_chunk_read_count: self.cumulative_coalesced_chunk_read_count.get(),
            cumulative_repair_checked_chunks: self.cumulative_repair_checked_chunks,
            cumulative_repair_degraded_chunks: self.cumulative_repair_degraded_chunks,
            cumulative_repaired_replicas: self.cumulative_repaired_replicas,
            cumulative_failed_repairs: self.cumulative_failed_repairs,
            cumulative_replica_read_repairs: self.cumulative_replica_read_repairs,
            cumulative_replica_read_repair_failures: self.cumulative_replica_read_repair_failures,
        }
    }

    pub fn chunk_control_proof(&self) -> PageChunkControlProof {
        let mut material = Vec::with_capacity(
            encoded_string_len("MOSRSBLOCK-CHUNK-CONTROL-PROOF1")
                + self.replicas.len() * (std::mem::size_of::<u64>() * 7 + 128),
        );
        append_string(&mut material, "MOSRSBLOCK-CHUNK-CONTROL-PROOF1");
        let mut proof_count = 0u64;
        let mut min_raft_commit_index = u64::MAX;
        let mut max_raft_commit_index = 0u64;
        let mut replica_service_state_checksums = BTreeMap::new();
        for (index, replica) in self.replicas.iter().enumerate() {
            if self.quarantined_replicas.contains(&index)
                || self.disabled_chunk_endpoints.contains(&index)
            {
                continue;
            }
            let proof = replica.control_proof();
            if proof.state_checksum.is_empty() {
                continue;
            }
            replica_service_state_checksums.insert(index, proof.state_checksum.clone());
            append_u64(&mut material, index as u64);
            append_string(&mut material, &proof.state_format);
            append_string(&mut material, &proof.state_checksum);
            append_u64(&mut material, proof.state_bytes);
            append_u64(&mut material, proof.chunk_mutation_control_commit_index);
            append_u64(&mut material, proof.chunk_mutation_control_next_log_index);
            append_u64(&mut material, proof.accepting_writes as u64);
            append_u64(&mut material, proof.draining as u64);
            append_u64(&mut material, proof.frozen as u64);
            min_raft_commit_index =
                min_raft_commit_index.min(proof.chunk_mutation_control_commit_index);
            max_raft_commit_index =
                max_raft_commit_index.max(proof.chunk_mutation_control_commit_index);
            proof_count = proof_count.saturating_add(1);
        }
        if proof_count == 0 {
            return PageChunkControlProof::default();
        }
        PageChunkControlProof {
            proof_count,
            proof_checksum: etag(&material),
            min_raft_commit_index,
            max_raft_commit_index,
            replica_service_state_checksums,
        }
    }

    pub(crate) fn check_expected_route_control_checksum(
        &self,
        expected_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        let Some(expected_checksum) = expected_checksum else {
            return Ok(());
        };
        if expected_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "block route control checksum expectation is empty".to_string(),
            ));
        }
        let actual_checksum = self.route_control_proof().state_checksum;
        if expected_checksum != actual_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block route control checksum mismatch: expected {} got {}",
                expected_checksum, actual_checksum
            )));
        }
        Ok(())
    }


}
