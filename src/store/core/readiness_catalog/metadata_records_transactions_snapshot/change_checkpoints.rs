impl MatrixObjectStore {

    pub fn commit_metadata_change_checkpoint(
        &mut self,
        checkpoint: MetadataChangeCheckpoint,
    ) -> Result<MetadataChangeCheckpoint, ObjectError> {
        self.meta_service.commit_change_checkpoint(checkpoint)
    }


    pub fn metadata_change_checkpoint(
        &self,
        consumer_id: &str,
        prefix: &str,
    ) -> Result<MetadataChangeCheckpoint, ObjectError> {
        self.meta_service.get_change_checkpoint(consumer_id, prefix)
    }


    pub fn metadata_change_checkpoints(&self) -> Vec<MetadataChangeCheckpoint> {
        self.meta_service.list_change_checkpoints()
    }


    pub fn page_metadata_change_checkpoints(
        &self,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangeCheckpointPage, ObjectError> {
        self.meta_service
            .page_change_checkpoints(prefix, marker, limit, max_payload_bytes)
    }


    pub fn plan_metadata_change_checkpoint_lag(
        &self,
        prefix: &str,
        marker: Option<&str>,
        max_checkpoints: usize,
        target_change_sequence: Option<u64>,
        target_commit_index: Option<u64>,
        min_change_lag: u64,
        min_commit_lag: u64,
        include_not_behind: bool,
        only_compacted: bool,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangeCheckpointLagPlan, ObjectError> {
        self.meta_service.plan_change_checkpoint_lag(
            prefix,
            marker,
            max_checkpoints,
            target_change_sequence,
            target_commit_index,
            min_change_lag,
            min_commit_lag,
            include_not_behind,
            only_compacted,
            max_payload_bytes,
        )
    }
}
