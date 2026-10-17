impl MatrixChunkService {
    pub fn set_capacity_bytes(&mut self, capacity_bytes: u64) -> Result<(), ObjectError> {
        if capacity_bytes > 0 && self.physical_bytes() > capacity_bytes {
            return Err(ObjectError::InvalidArgument(
                "chunk service capacity is below current physical bytes".to_string(),
            ));
        }
        self.capacity_bytes = capacity_bytes;
        Ok(())
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub fn capacity_rejected_writes(&self) -> u64 {
        self.capacity_rejected_writes
    }

    pub fn metadata_dirty(&self) -> bool {
        self.metadata_dirty
    }

    pub fn metadata_dirty_mutations(&self) -> u64 {
        self.metadata_dirty_mutations
    }

    pub fn metadata_checkpoint_interval(&self) -> u64 {
        self.metadata_checkpoint_interval
    }

    pub fn configure_metadata_checkpoint_interval(
        &mut self,
        interval: u64,
    ) -> Result<(), ObjectError> {
        if interval == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk metadata checkpoint interval must be positive".to_string(),
            ));
        }
        self.metadata_checkpoint_interval = interval;
        Ok(())
    }

    pub fn set_defer_metadata_checkpoints(&mut self, defer: bool) {
        self.defer_metadata_checkpoints = defer;
        if !defer {
            self.deferred_metadata_checkpoint_pending = false;
        }
    }

    pub fn deferred_metadata_checkpoint_pending(&self) -> bool {
        self.deferred_metadata_checkpoint_pending
    }

    pub fn deferred_metadata_checkpoint_requests(&self) -> u64 {
        self.deferred_metadata_checkpoint_requests
    }

    pub fn deferred_metadata_checkpoint_runs(&self) -> u64 {
        self.deferred_metadata_checkpoint_runs
    }

    pub fn checkpoint_metadata(&mut self) -> bool {
        let had_dirty = self.metadata_dirty || self.deferred_metadata_checkpoint_pending;
        self.metadata_dirty = false;
        self.metadata_dirty_mutations = 0;
        self.deferred_metadata_checkpoint_pending = false;
        if had_dirty {
            self.deferred_metadata_checkpoint_runs =
                self.deferred_metadata_checkpoint_runs.saturating_add(1);
        }
        had_dirty
    }

    pub fn run_deferred_metadata_checkpoint(&mut self) -> bool {
        self.checkpoint_metadata()
    }

    fn mark_metadata_mutation(&mut self) {
        self.metadata_dirty = true;
        self.metadata_dirty_mutations = self.metadata_dirty_mutations.saturating_add(1);
        if self.defer_metadata_checkpoints
            || self.metadata_dirty_mutations < self.metadata_checkpoint_interval
        {
            self.deferred_metadata_checkpoint_pending = true;
            self.deferred_metadata_checkpoint_requests =
                self.deferred_metadata_checkpoint_requests.saturating_add(1);
        } else {
            self.checkpoint_metadata();
        }
    }

    pub fn physical_write_count(&self) -> u64 {
        self.physical_write_count
    }

    pub fn physical_write_bytes(&self) -> u64 {
        self.physical_write_bytes
    }

    pub fn physical_discard_ranges(&self) -> u64 {
        self.physical_discard_ranges
    }

    pub fn physical_discard_bytes(&self) -> u64 {
        self.physical_discard_bytes
    }

    pub fn compression_stats(&self) -> (u64, u64) {
        chunk_map_compression_stats(&self.chunks)
    }

    pub fn compressed_chunk_count(&self) -> u64 {
        self.compression_stats().0
    }

    pub fn compression_saved_bytes(&self) -> u64 {
        self.compression_stats().1
    }

    pub fn ec_reconstructed_reads(&self) -> u64 {
        self.ec_reconstructed_reads.get()
    }

    pub fn ec_reconstruction_failures(&self) -> u64 {
        self.ec_reconstruction_failures.get()
    }

}
