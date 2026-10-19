impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_chunk_read_cache(
        &mut self,
        max_bytes_per_replica: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_read_cache_fenced(max_bytes_per_replica, None)
    }

    pub fn propose_configure_chunk_read_cache_fenced(
        &mut self,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_read_cache_fenced_with_stream_bypass(
            max_bytes_per_replica,
            self.store
                .chunk_read_cache_max_cacheable_read_bytes_per_replica(),
            expected_route_control_checksum,
        )
    }

    pub fn propose_configure_chunk_read_cache_with_stream_bypass(
        &mut self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_read_cache_fenced_with_stream_bypass(
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            None,
        )
    }

    pub fn propose_configure_chunk_read_cache_fenced_with_stream_bypass(
        &mut self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkReadCache {
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
        })
    }

    pub fn propose_configure_chunk_read_cache_linearizable_fenced(
        &mut self,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_read_cache_linearizable_fenced_with_stream_bypass(
            max_bytes_per_replica,
            self.store
                .chunk_read_cache_max_cacheable_read_bytes_per_replica(),
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_configure_chunk_read_cache_linearizable_fenced_with_stream_bypass(
        &mut self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk read-cache",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_read_cache_fenced_with_stream_bypass(
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            expected_route_control_checksum,
        )
    }

    pub fn propose_configure_chunk_replica_read_cache(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_read_cache_fenced(
            replica_index,
            max_bytes_per_replica,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_read_cache_fenced(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_read_cache_fenced_with_stream_bypass(
            replica_index,
            max_bytes_per_replica,
            self.store
                .chunk_read_cache_max_cacheable_read_bytes_per_replica(),
            expected_route_control_checksum,
        )
    }

    pub fn propose_configure_chunk_replica_read_cache_with_stream_bypass(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_read_cache_fenced_with_stream_bypass(
            replica_index,
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_read_cache_fenced_with_stream_bypass(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkReplicaReadCache {
            replica_index,
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
        })
    }

    pub fn propose_configure_chunk_replica_read_cache_linearizable_fenced(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_read_cache_linearizable_fenced_with_stream_bypass(
            replica_index,
            max_bytes_per_replica,
            self.store
                .chunk_read_cache_max_cacheable_read_bytes_per_replica(),
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_configure_chunk_replica_read_cache_linearizable_fenced_with_stream_bypass(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk replica read-cache",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_replica_read_cache_fenced_with_stream_bypass(
            replica_index,
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            expected_route_control_checksum,
        )
    }

}
