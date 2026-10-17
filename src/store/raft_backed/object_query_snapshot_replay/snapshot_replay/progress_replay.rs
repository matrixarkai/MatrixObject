impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn snapshot_install_progress(&self) -> SnapshotInstallProgress {
        self.store.snapshot_install_progress()
    }

    pub fn abort_snapshot_install(
        &mut self,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        self.store.abort_snapshot_install(expected_stream_id)
    }

    pub fn push_snapshot_chunk(
        &mut self,
        assembler: &mut StoreSnapshotChunkAssembler,
        chunk: StoreSnapshotChunk,
    ) -> Result<SnapshotInstallProgress, ObjectError> {
        if let Some(snapshot_bytes) = assembler.push_chunk(chunk)? {
            self.store.install_snapshot_bytes_in_place(&snapshot_bytes)?;
        }
        Ok(assembler.install_progress())
    }

    pub fn replay_into(&self, target: &mut MatrixObjectStore) -> Result<(), ObjectError> {
        for entry in self
            .raft
            .entries_from(target.last_applied_index().saturating_add(1))
        {
            target.apply_raft_entry(&entry)?;
        }
        Ok(())
    }
}
