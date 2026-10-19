impl MatrixObjectStore {
    pub fn can_accept_page_read(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_read_bounded(slab_id, offset, length, max_payload_bytes, 0)
    }

    pub fn can_accept_page_read_bounded(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_slab_read_bounded(
            slab_id,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }

    pub fn can_accept_page_read_fenced(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_read_bounded_fenced(
            slab_id,
            offset,
            length,
            max_payload_bytes,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_read_bounded_fenced(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_slab_read_bounded_fenced(
            slab_id,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_read(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_read_bounded(requests, max_payload_bytes, 0)
    }

    pub fn can_accept_page_batch_read_bounded(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_read_bounded_borrowed(
            &requests,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }

    pub fn can_accept_page_batch_read_bounded_borrowed(
        &self,
        requests: &[ChunkRangeRequest],
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_slab_reads_bounded_borrowed(
            requests,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }

    pub fn can_accept_page_batch_read_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_read_bounded_fenced(
            requests,
            max_payload_bytes,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_read_bounded_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_read_bounded_fenced_borrowed(
            &requests,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_read_bounded_fenced_borrowed(
        &self,
        requests: &[ChunkRangeRequest],
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_slab_reads_bounded_fenced_borrowed(
            requests,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
        )
    }


}
