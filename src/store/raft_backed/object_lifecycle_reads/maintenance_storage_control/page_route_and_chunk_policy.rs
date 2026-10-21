// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_set_page_replica_quarantine(
        &mut self,
        replica_index: usize,
        quarantined: bool,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_set_page_replica_quarantine_fenced(replica_index, quarantined, None)
    }

    pub fn propose_set_page_replica_quarantine_fenced(
        &mut self,
        replica_index: usize,
        quarantined: bool,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::QuarantinePageReplica {
            replica_index,
            quarantined,
        })
    }

    pub fn propose_set_page_replica_quarantine_linearizable_fenced(
        &mut self,
        replica_index: usize,
        quarantined: bool,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "set block replica quarantine",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_set_page_replica_quarantine_fenced(
            replica_index,
            quarantined,
            expected_route_control_checksum,
        )
    }

    pub fn propose_set_page_replica_frozen(
        &mut self,
        replica_index: usize,
        frozen: bool,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_set_page_replica_frozen_fenced(replica_index, frozen, None)
    }

    pub fn propose_set_page_replica_frozen_fenced(
        &mut self,
        replica_index: usize,
        frozen: bool,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::FreezePageReplica {
            replica_index,
            frozen,
        })
    }

    pub fn propose_set_page_replica_frozen_linearizable_fenced(
        &mut self,
        replica_index: usize,
        frozen: bool,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "set block replica frozen",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_set_page_replica_frozen_fenced(
            replica_index,
            frozen,
            expected_route_control_checksum,
        )
    }

    pub fn propose_configure_page_chunk_endpoints(
        &mut self,
        endpoints: Vec<String>,
        disabled_endpoints: BTreeSet<usize>,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_page_chunk_endpoints_fenced(
            endpoints,
            disabled_endpoints,
            write_quorum,
            read_quorum,
            None,
        )
    }

    pub fn propose_configure_page_chunk_endpoints_fenced(
        &mut self,
        endpoints: Vec<String>,
        disabled_endpoints: BTreeSet<usize>,
        write_quorum: usize,
        read_quorum: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigurePageChunkEndpoints {
            endpoints,
            disabled_endpoints,
            write_quorum,
            read_quorum,
        })
    }

    pub fn propose_configure_page_chunk_endpoints_linearizable_fenced(
        &mut self,
        endpoints: Vec<String>,
        disabled_endpoints: BTreeSet<usize>,
        write_quorum: usize,
        read_quorum: usize,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure block chunk endpoints",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_page_chunk_endpoints_fenced(
            endpoints,
            disabled_endpoints,
            write_quorum,
            read_quorum,
            expected_route_control_checksum,
        )
    }

    pub fn propose_seal_page_chunk(
        &mut self,
        chunk_id: ChunkId,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_seal_page_chunk_fenced(chunk_id, None)
    }

    pub fn propose_seal_page_chunk_fenced(
        &mut self,
        chunk_id: ChunkId,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::SealPageChunk {
            chunk_id,
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
        })
    }

    pub fn propose_seal_page_chunk_linearizable_fenced(
        &mut self,
        chunk_id: ChunkId,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "seal block segment",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_seal_page_chunk_fenced(chunk_id, expected_route_control_checksum)
    }

    pub fn list_page_sealed_chunks_linearizable(
        &self,
        marker: Option<&ChunkId>,
        limit: usize,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SealedChunkPage, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "list block sealed segments",
            None,
            min_read_index,
            min_commit_index,
        )?;
        self.store.list_page_sealed_chunks(marker, limit)
    }
}
