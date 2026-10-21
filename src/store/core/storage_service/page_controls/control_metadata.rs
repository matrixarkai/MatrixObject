// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn chunk_read_cache_control_metadata(
        &self,
        target_replica_index: Option<usize>,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        prepared_replica_count: Option<u64>,
        content_type: &str,
        update_time_ms: u64,
    ) -> ObjectMetadata {
        let key = target_replica_index
            .map(|index| index.to_string())
            .unwrap_or_else(|| "all_replicas".to_string());
        metadata_control_metadata(
            "_chunkcache",
            &key,
            max_bytes_per_replica,
            prepared_replica_count.unwrap_or(max_cacheable_read_bytes_per_replica),
            content_type,
            update_time_ms,
        )
    }

    fn chunk_target_key(target_replica_index: Option<usize>) -> String {
        target_replica_index
            .map(|index| index.to_string())
            .unwrap_or_else(|| "all_replicas".to_string())
    }

    fn prepared_chunk_trim_flow_control_metadata(
        &self,
        prepared: &PreparedChunkTrimFlowConfig,
        content_type: &str,
    ) -> ObjectMetadata {
        metadata_control_metadata(
            "_chunktrimflow",
            &Self::chunk_target_key(prepared.target_replica_index),
            prepared.rate_bytes_per_sec,
            prepared.chunk_prepared_replica_count,
            content_type,
            prepared.now_ms,
        )
    }

    fn chunk_trim_flow_control_metadata(
        &self,
        rate_bytes_per_sec: u64,
        target_replica_index: Option<usize>,
        content_type: &str,
        update_time_ms: u64,
    ) -> ObjectMetadata {
        metadata_control_metadata(
            "_chunktrimflow",
            &Self::chunk_target_key(target_replica_index),
            rate_bytes_per_sec,
            target_replica_index.map_or(self.options.replica_count as u64, |_| 1),
            content_type,
            update_time_ms,
        )
    }

    fn prepared_chunk_mutation_qos_control_metadata(
        &self,
        prepared: &PreparedChunkMutationQosConfig,
        content_type: &str,
    ) -> ObjectMetadata {
        metadata_control_metadata(
            "_chunkqos",
            &Self::chunk_target_key(prepared.target_replica_index),
            prepared
                .config
                .max_write_ops_per_window
                .saturating_add(prepared.config.max_discard_ops_per_window),
            prepared.chunk_prepared_replica_count,
            content_type,
            prepared.config.window_ms,
        )
    }

    fn chunk_mutation_qos_control_metadata(
        &self,
        config: &ReplicaMutationQosConfig,
        target_replica_index: Option<usize>,
        content_type: &str,
    ) -> ObjectMetadata {
        metadata_control_metadata(
            "_chunkqos",
            &Self::chunk_target_key(target_replica_index),
            config
                .max_write_ops_per_window
                .saturating_add(config.max_discard_ops_per_window),
            target_replica_index.map_or(self.options.replica_count as u64, |_| 1),
            content_type,
            config.window_ms,
        )
    }

    fn replica_service_mode_control_metadata(
        &self,
        replica_index: usize,
        prepared_replica_count: Option<u64>,
        content_type: &str,
        update_time_ms: u64,
    ) -> ObjectMetadata {
        if replica_index == PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET {
            let accepting_count = self
                .chunk_replica_accepting_writes()
                .into_iter()
                .filter(|accepting| *accepting)
                .count() as u64;
            return metadata_control_metadata(
                "_chunkmode",
                "all_eligible",
                accepting_count,
                prepared_replica_count.unwrap_or(accepting_count),
                content_type,
                update_time_ms,
            );
        }
        metadata_control_metadata(
            "_chunkmode",
            &replica_index.to_string(),
            self.chunk_replica_accepting_writes()
                .get(replica_index)
                .copied()
                .unwrap_or(false) as u64,
            self.draining_chunk_replicas().len() as u64,
            content_type,
            update_time_ms,
        )
    }

}
