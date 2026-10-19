impl MatrixObjectStore {
    pub fn install_snapshot_chunks<I>(
        &mut self,
        pages: I,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = StoreSnapshotChunk>,
    {
        let mut progress = self.snapshot_assembler.install_progress();
        for chunk in pages {
            if let Some(assembled) = self.snapshot_assembler.push_chunk_with_checksum(chunk)? {
                let snapshot_bytes = assembled.bytes.len() as u64;
                let full_checksum = assembled.full_checksum;
                let snapshot = StoreSnapshot::from_bytes(&assembled.bytes)?;
                let last_applied_index = snapshot.last_applied_index;
                let last_applied_term = snapshot.last_applied_term;
                drop(assembled.bytes);
                self.install_snapshot(snapshot)?;
                return Ok(snapshot_install_progress(
                    true,
                    false,
                    snapshot_bytes,
                    snapshot_bytes,
                    full_checksum,
                    last_applied_index,
                    last_applied_term,
                ));
            }
            progress = self.snapshot_assembler.install_progress();
        }
        if progress.stream_active {
            return Ok(progress);
        }
        Err(ObjectError::InvalidArgument(
            "store snapshot chunk stream is empty".to_string(),
        ))
    }

    pub fn snapshot_install_progress(&self) -> SnapshotInstallProgress {
        self.snapshot_assembler.install_progress()
    }

    pub fn abort_snapshot_install(
        &mut self,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        self.snapshot_assembler.abort_install(expected_stream_id)
    }

}
