impl MatrixObjectStore {
    fn encoded_slab_id_len() -> usize {
        std::mem::size_of::<u64>() * 2
    }

    fn encoded_chunk_id_len() -> usize {
        Self::encoded_slab_id_len() + std::mem::size_of::<u64>()
    }

    fn encoded_data_range_len() -> usize {
        std::mem::size_of::<u64>() * 2
    }

    fn append_page_batch_allocation_journal_record(
        &self,
        lengths: &[u64],
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * (3 + lengths.len())
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 53);
        append_u64(&mut payload, lengths.len() as u64);
        for length in lengths {
            append_u64(&mut payload, *length);
        }
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_page_allocation_release_journal_record(
        &self,
        allocation_id: &str,
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>()
                + encoded_string_len(allocation_id)
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 54);
        append_string(&mut payload, allocation_id);
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_page_write_journal_record(
        &self,
        slab_id: &SlabId,
        offset: u64,
        data: &[u8],
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + Self::encoded_slab_id_len()
                + data.len()
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 55);
        append_slab_id(&mut payload, slab_id);
        append_u64(&mut payload, offset);
        append_u64(&mut payload, data.len() as u64);
        payload.extend_from_slice(data);
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_page_batch_write_journal_record(
        &self,
        writes: &[PageRangeWrite],
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let payload_bytes = writes
            .iter()
            .map(|write| {
                Self::encoded_slab_id_len()
                    + std::mem::size_of::<u64>() * 2
                    + write.data.len()
            })
            .sum::<usize>();
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + payload_bytes
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 56);
        append_u64(&mut payload, writes.len() as u64);
        for write in writes {
            append_slab_id(&mut payload, &write.slab_id);
            append_u64(&mut payload, write.offset);
            append_u64(&mut payload, write.data.len() as u64);
            payload.extend_from_slice(&write.data);
        }
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_page_discard_journal_record(
        &self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + Self::encoded_slab_id_len()
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 57);
        append_slab_id(&mut payload, slab_id);
        append_u64(&mut payload, offset);
        append_u64(&mut payload, length);
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_page_batch_discard_journal_record(
        &self,
        requests: &[PageRangeRequest],
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + requests.len() * (Self::encoded_slab_id_len() + Self::encoded_data_range_len())
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 58);
        append_u64(&mut payload, requests.len() as u64);
        for request in requests {
            append_slab_id(&mut payload, &request.slab_id);
            append_u64(&mut payload, request.offset);
            append_u64(&mut payload, request.length);
        }
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_chunk_write_journal_record(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        offset: u64,
        data: &[u8],
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 4
                + Self::encoded_chunk_id_len()
                + data.len()
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 59);
        append_u64(&mut payload, replica_index as u64);
        append_chunk_id(&mut payload, chunk_id);
        append_u64(&mut payload, offset);
        append_u64(&mut payload, data.len() as u64);
        payload.extend_from_slice(data);
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_chunk_batch_write_journal_record(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        writes: &[(u64, Vec<u8>)],
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let payload_bytes = writes
            .iter()
            .map(|(_, data)| std::mem::size_of::<u64>() * 2 + data.len())
            .sum::<usize>();
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + Self::encoded_chunk_id_len()
                + payload_bytes
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 60);
        append_u64(&mut payload, replica_index as u64);
        append_chunk_id(&mut payload, chunk_id);
        append_u64(&mut payload, writes.len() as u64);
        for (offset, data) in writes {
            append_u64(&mut payload, *offset);
            append_u64(&mut payload, data.len() as u64);
            payload.extend_from_slice(data);
        }
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_chunk_discard_journal_record(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 4
                + Self::encoded_chunk_id_len()
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 61);
        append_u64(&mut payload, replica_index as u64);
        append_chunk_id(&mut payload, chunk_id);
        append_u64(&mut payload, offset);
        append_u64(&mut payload, length);
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_chunk_batch_discard_journal_record(
        &self,
        replica_index: usize,
        chunk_id: &ChunkId,
        ranges: &[DataRange],
        idempotency_key: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + Self::encoded_chunk_id_len()
                + ranges.len() * Self::encoded_data_range_len()
                + encoded_string_len(idempotency_key),
        );
        append_u64(&mut payload, 62);
        append_u64(&mut payload, replica_index as u64);
        append_chunk_id(&mut payload, chunk_id);
        append_u64(&mut payload, ranges.len() as u64);
        for range in ranges {
            append_range(&mut payload, range);
        }
        append_string(&mut payload, idempotency_key);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }
}
