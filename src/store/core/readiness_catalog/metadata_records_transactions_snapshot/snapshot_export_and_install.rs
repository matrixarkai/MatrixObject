impl MatrixObjectStore {
    pub fn export_metadata_snapshot_chunk(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<MetaSnapshotChunk, ObjectError> {
        self.meta_service
            .export_snapshot_chunk(offset, max_payload_bytes)
    }

    pub fn export_metadata_snapshot_chunks(
        &self,
        max_payload_bytes: usize,
    ) -> Result<Vec<MetaSnapshotChunk>, ObjectError> {
        self.meta_service.export_snapshot_chunks(max_payload_bytes)
    }

    pub fn export_metadata_snapshot_chunk_stream<F>(
        &self,
        max_payload_bytes: usize,
        emit: F,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        F: FnMut(MetaSnapshotChunk) -> Result<(), ObjectError>,
    {
        self.meta_service
            .export_snapshot_chunk_stream(max_payload_bytes, emit)
    }

    pub fn install_raft_metadata_snapshot_bytes(
        &mut self,
        bytes: &[u8],
        min_commit_index: u64,
    ) -> Result<MetaSnapshot, ObjectError> {
        self.meta_service
            .install_raft_snapshot_bytes(bytes, min_commit_index)
    }

    pub fn install_raft_metadata_snapshot_bytes_report(
        &mut self,
        bytes: &[u8],
        min_commit_index: u64,
    ) -> Result<MetadataSnapshotInstallReport, ObjectError> {
        self.meta_service
            .install_raft_snapshot_bytes_report(bytes, min_commit_index)
    }

    pub fn install_raft_metadata_snapshot_chunks<I>(
        &mut self,
        pages: I,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = MetaSnapshotChunk>,
    {
        self.meta_service
            .install_raft_snapshot_chunks(pages, min_commit_index)
    }

    pub fn install_raft_metadata_snapshot_chunks_report<I>(
        &mut self,
        pages: I,
        min_commit_index: u64,
    ) -> Result<MetadataSnapshotInstallReport, ObjectError>
    where
        I: IntoIterator<Item = MetaSnapshotChunk>,
    {
        self.meta_service
            .install_raft_snapshot_chunks_report(pages, min_commit_index)
    }

    pub fn metadata_snapshot_install_progress(&self) -> SnapshotInstallProgress {
        self.meta_service.metadata_snapshot_install_progress()
    }

    pub fn abort_metadata_snapshot_install(
        &mut self,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        self.meta_service
            .abort_metadata_snapshot_install(expected_stream_id)
    }
}
