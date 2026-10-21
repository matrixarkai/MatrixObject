// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn page_write_quorum(&self) -> usize {
        self.chunk_service.write_quorum()
    }

    pub fn page_read_quorum(&self) -> usize {
        self.chunk_service.read_quorum()
    }

    pub fn can_accept_page_write(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_slab_write(
            chunk_id,
            offset,
            length,
            max_write_bytes,
            max_chunk_write_count,
        )
    }

    pub fn can_accept_page_write_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_slab_write_fenced(
            chunk_id,
            offset,
            length,
            max_write_bytes,
            max_chunk_write_count,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_write(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_from(writes, max_write_bytes, max_chunk_write_count, 0)
    }

    pub fn can_accept_page_batch_write_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_slab_writes_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
        )
    }

    pub fn can_accept_page_batch_write_fenced(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_page_batch_write_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_write_fenced_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.chunk_service.can_accept_chunk_writes_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            expected_route_control_checksum,
        )
    }


}
