impl MatrixObjectStore {
    pub fn quarantined_page_replicas(&self) -> Vec<usize> {
        self.chunk_service.quarantined_replica_indices()
    }

    pub fn frozen_page_replicas(&self) -> Vec<usize> {
        self.chunk_service.frozen_replica_indices()
    }

    pub fn chunk_replica_capacity_bytes(&self) -> Vec<u64> {
        self.chunk_service.chunk_replica_capacity_bytes()
    }

    pub fn chunk_replica_capacity_free_bytes(&self) -> Vec<u64> {
        self.chunk_service.chunk_replica_capacity_free_bytes()
    }

    pub fn chunk_replica_capacity_rejected_writes(&self) -> Vec<u64> {
        self.chunk_service.chunk_replica_capacity_rejected_writes()
    }

    pub fn chunk_replica_physical_bytes(&self) -> Vec<u64> {
        self.chunk_service.chunk_replica_physical_bytes()
    }

    fn chunk_capacity_control_metadata(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
        prepared_replica_count: Option<u64>,
        content_type: &str,
        update_time_ms: u64,
    ) -> ObjectMetadata {
        if replica_index == PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET {
            let affected_count = prepared_replica_count.unwrap_or_else(|| {
                self.chunk_replica_capacity_bytes()
                    .into_iter()
                    .filter(|capacity| *capacity == capacity_bytes)
                    .count() as u64
            });
            return metadata_control_metadata(
                "_chunkcapacity",
                "all_eligible",
                capacity_bytes,
                affected_count,
                content_type,
                update_time_ms,
            );
        }
        metadata_control_metadata(
            "_chunkcapacity",
            &replica_index.to_string(),
            capacity_bytes,
            self.chunk_service
                .chunk_replica_capacity_free_bytes()
                .get(replica_index)
                .copied()
                .unwrap_or_default(),
            content_type,
            update_time_ms,
        )
    }

    pub fn chunk_replica_physical_write_count(&self) -> Vec<u64> {
        self.chunk_service.chunk_replica_physical_write_count()
    }

    pub fn chunk_replica_physical_write_bytes(&self) -> Vec<u64> {
        self.chunk_service.chunk_replica_physical_write_bytes()
    }

    pub fn chunk_replica_physical_discard_ranges(&self) -> Vec<u64> {
        self.chunk_service.chunk_replica_physical_discard_ranges()
    }

    pub fn chunk_replica_physical_discard_bytes(&self) -> Vec<u64> {
        self.chunk_service.chunk_replica_physical_discard_bytes()
    }

    pub fn chunk_physical_write_count(&self) -> u64 {
        self.chunk_service.physical_write_count()
    }

    pub fn chunk_physical_write_bytes(&self) -> u64 {
        self.chunk_service.physical_write_bytes()
    }

    pub fn chunk_physical_discard_ranges(&self) -> u64 {
        self.chunk_service.physical_discard_ranges()
    }

    pub fn chunk_physical_discard_bytes(&self) -> u64 {
        self.chunk_service.physical_discard_bytes()
    }

    pub fn chunk_ec_reconstructed_reads(&self) -> u64 {
        self.chunk_service.ec_reconstructed_reads()
    }

    pub fn chunk_ec_reconstruction_failures(&self) -> u64 {
        self.chunk_service.ec_reconstruction_failures()
    }

    pub fn chunk_replica_accepting_writes(&self) -> Vec<bool> {
        self.chunk_service.chunk_replica_accepting_writes()
    }

    pub fn draining_chunk_replicas(&self) -> Vec<usize> {
        self.chunk_service.draining_chunk_replicas()
    }

    pub fn chunk_replica_service_mode_rejected_mutations(&self) -> Vec<u64> {
        self.chunk_service
            .chunk_replica_service_mode_rejected_mutations()
    }


}
