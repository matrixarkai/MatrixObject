impl MatrixObjectStore {
    pub fn can_accept_page_discard(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_discard_bytes: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.page_service
            .can_accept_slab_discard(slab_id, offset, length, max_discard_bytes)
    }

    pub fn can_accept_page_discard_fenced(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.page_service.can_accept_slab_discard_fenced(
            slab_id,
            offset,
            length,
            max_discard_bytes,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_discard(
        &self,
        requests: Vec<PageRangeRequest>,
        max_discard_bytes: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.page_service
            .can_accept_slab_discards(requests, max_discard_bytes)
    }

    pub fn can_accept_page_batch_discard_from(
        &self,
        requests: Vec<PageRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.page_service.can_accept_slab_discards_from(
            requests,
            max_discard_bytes,
            discard_start_range_index,
        )
    }

    pub fn can_accept_page_batch_discard_fenced(
        &self,
        requests: Vec<PageRangeRequest>,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.page_service.can_accept_slab_discards_fenced(
            requests,
            max_discard_bytes,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_page_batch_discard_from_fenced(
        &self,
        requests: Vec<PageRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PageRangeAdmission, ObjectError> {
        self.page_service.can_accept_slab_discards_from_fenced(
            requests,
            max_discard_bytes,
            discard_start_range_index,
            expected_route_control_checksum,
        )
    }


}
