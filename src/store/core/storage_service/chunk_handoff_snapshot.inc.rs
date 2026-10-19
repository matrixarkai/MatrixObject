impl MatrixObjectStore {
    pub fn prepare_chunk_state_handoffs(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        source_replica_index: usize,
        max_payload_bytes: u64,
    ) -> Result<Vec<ReplicaStateHandoff>, ObjectError> {
        self.chunk_service.prepare_chunk_state_handoffs(
            slab_id,
            offset,
            length,
            source_replica_index,
            max_payload_bytes,
        )
    }

    pub fn apply_chunk_state_handoffs(
        &mut self,
        target_replica_index: usize,
        handoffs: Vec<ReplicaStateHandoff>,
    ) -> Result<RepairStats, ObjectError> {
        self.chunk_service
            .apply_chunk_state_handoffs(target_replica_index, handoffs)
    }

    pub fn export_page_snapshot_chunk(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<ChunkServiceSnapshotChunk, ObjectError> {
        self.chunk_service
            .export_snapshot_chunk(offset, max_payload_bytes)
    }

    pub fn export_page_snapshot_chunk_stream<F>(
        &self,
        max_payload_bytes: usize,
        emit: F,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        F: FnMut(ChunkServiceSnapshotChunk) -> Result<(), ObjectError>,
    {
        self.chunk_service
            .export_snapshot_chunk_stream(max_payload_bytes, emit)
    }

    pub fn export_page_snapshot_chunks(
        &self,
        max_payload_bytes: usize,
    ) -> Result<Vec<ChunkServiceSnapshotChunk>, ObjectError> {
        self.chunk_service.export_snapshot_chunks(max_payload_bytes)
    }

    pub fn install_page_snapshot_chunks<I>(
        &mut self,
        chunks: I,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ChunkServiceSnapshotChunk>,
    {
        self.chunk_service.install_snapshot_chunks(chunks)
    }

    pub fn chunk_snapshot_install_progress(&self) -> SnapshotInstallProgress {
        self.chunk_service.snapshot_install_progress()
    }

    pub fn abort_page_snapshot_install(
        &mut self,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        self.chunk_service
            .abort_snapshot_install(expected_stream_id)
    }

    pub fn export_chunk_replica_snapshot_chunk(
        &self,
        replica_index: usize,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<ReplicaServiceSnapshotChunk, ObjectError> {
        self.chunk_service.export_chunk_replica_snapshot_chunk(
            replica_index,
            offset,
            max_payload_bytes,
        )
    }

    pub fn export_chunk_replica_snapshot_chunk_stream<F>(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        emit: F,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        F: FnMut(ReplicaServiceSnapshotChunk) -> Result<(), ObjectError>,
    {
        self.chunk_service
            .export_chunk_replica_snapshot_chunk_stream(replica_index, max_payload_bytes, emit)
    }

    pub fn export_chunk_replica_snapshot_chunks(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<ReplicaServiceSnapshotChunk>, ObjectError> {
        self.chunk_service
            .export_chunk_replica_snapshot_chunks(replica_index, max_payload_bytes)
    }

    pub fn install_chunk_replica_snapshot_chunks<I>(
        &mut self,
        replica_index: usize,
        chunks: I,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ReplicaServiceSnapshotChunk>,
    {
        self.chunk_service
            .install_chunk_replica_snapshot_chunks(replica_index, chunks)
    }

    pub fn chunk_replica_snapshot_install_progress(
        &self,
        replica_index: usize,
    ) -> Result<SnapshotInstallProgress, ObjectError> {
        self.chunk_service
            .chunk_replica_snapshot_install_progress(replica_index)
    }

    pub fn abort_chunk_replica_snapshot_install(
        &mut self,
        replica_index: usize,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        self.chunk_service
            .abort_chunk_replica_snapshot_install(replica_index, expected_stream_id)
    }

}
