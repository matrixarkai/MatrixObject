// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn can_accept_page_discard(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_discard_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service
            .can_accept_page_discard(chunk_id, offset, length, max_discard_bytes)
    }

    pub fn can_accept_page_discard_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_page_discard_fenced(
            chunk_id,
            offset,
            length,
            max_discard_bytes,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_discard(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service
            .can_accept_page_discards(requests, max_discard_bytes)
    }

    pub fn can_accept_page_batch_discard_from(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_page_discards_from(
            requests,
            max_discard_bytes,
            discard_start_range_index,
        )
    }

    pub fn can_accept_page_batch_discard_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_chunk_discards_fenced(
            requests,
            max_discard_bytes,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_discard_from_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_chunk_discards_from_fenced(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
        )
    }


}
