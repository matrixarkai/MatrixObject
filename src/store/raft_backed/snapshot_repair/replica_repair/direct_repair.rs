impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_scrub_page_replicas(
        &mut self,
        repair: bool,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ScrubPageReplicas { repair })
    }

    pub fn propose_repair_page_range_bounded(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_repair_page_range_bounded_fenced(
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            "",
        )
    }

    pub fn propose_repair_page_range_bounded_fenced(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "bounded block repair",
            expected_page_route_control_checksum,
        )?;
        self.propose(ObjectCommand::RepairPageRangeBounded {
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
        })
    }

    pub fn propose_repair_page_range_bounded_linearizable_fenced(
        &mut self,
        chunk_id: ChunkId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "bounded block repair",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let command = ObjectCommand::RepairPageRangeBounded {
            chunk_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "bounded block repair raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_repair_page_chunk(
        &mut self,
        page_id: PageId,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_repair_page_chunk_fenced(page_id, "")
    }

    pub fn propose_repair_page_chunk_fenced(
        &mut self,
        page_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "exact block chunk repair",
            expected_page_route_control_checksum,
        )?;
        self.propose(ObjectCommand::RepairPageChunk {
            page_id,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
        })
    }

    pub fn propose_repair_page_chunk_linearizable_fenced(
        &mut self,
        page_id: PageId,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "exact block chunk repair",
            expected_page_route_control_checksum.as_ref(),
        )?;
        let command = ObjectCommand::RepairPageChunk {
            page_id,
            expected_route_control_checksum: expected_page_route_control_checksum
                .as_ref()
                .to_string(),
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "exact block chunk repair raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

}
