impl MatrixObjectStore {
    /// A line-item resident-byte budget for the store.
    ///
    /// Reports `len` and `capacity` separately for every byte-carrying buffer, so
    /// allocation slack shows up instead of hiding inside a total, and entry counts
    /// for the maps whose cost is per-entry overhead rather than payload. The gap
    /// between the accounted total and process RSS is the map/allocator overhead.
    pub fn footprint_audit(&self) -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> = Vec::new();
        let mut push = |k: &str, v: u64| out.push((k.to_string(), v));

        // ---- the store's own copy of every written byte ----------------------
        // The store no longer holds payload bytes, only a length and a hash per
        // write. `indexed_bytes` is what those lengths add up to - the bytes the
        // page service holds - and `index_capacity` is what this map itself costs.
        let mut payload_len = 0u64;
        let mut payload_cap = 0u64;
        for value in self.payloads.values() {
            payload_len = payload_len.saturating_add(value.len);
            payload_cap = payload_cap
                .saturating_add(std::mem::size_of::<StoredPayload>() as u64)
                .saturating_add(value.etag.capacity() as u64);
        }
        push("payloads.entries", self.payloads.len() as u64);
        push("payloads.indexed_bytes", payload_len);
        push("payloads.index_capacity", payload_cap);

        // ---- metadata maps (cost is per-entry, not payload) -----------------
        push("objects.entries", self.objects.len() as u64);
        push("versions.entries", self.versions.len() as u64);
        let version_rows: u64 = self
            .versions
            .values()
            .map(|rows| rows.len() as u64)
            .fold(0u64, |a, b| a.saturating_add(b));
        push("versions.rows", version_rows);
        push("object_changes.len", self.object_changes.len() as u64);
        push("object_changes.capacity", self.object_changes.capacity() as u64);
        push("segments.entries", self.slabs.len() as u64);
        push("applied_raft_receipts.entries", self.applied_raft_receipts.len() as u64);
        push("multipart_uploads.entries", self.multipart_uploads.len() as u64);
        push("metadata_route_cache.entries", self.metadata_route_cache.len() as u64);

        // ---- meta service ---------------------------------------------------
        push("meta.records", self.meta_service.records.len() as u64);
        push("meta.shards", self.meta_service.shards.len() as u64);
        push("meta.transactions", self.meta_service.transactions.len() as u64);
        push("meta.batch_receipts", self.meta_service.batch_request_receipts.len() as u64);

        // ---- page service: receipt maps ------------------------------------
        let b = &self.page_service;
        push("block.pending_allocations", b.pending_allocations.len() as u64);
        push("block.alloc_receipts", b.allocation_request_receipts.len() as u64);
        push("block.batch_alloc_receipts", b.batch_allocation_request_receipts.len() as u64);
        push("block.release_receipts", b.release_allocation_request_receipts.len() as u64);
        push("block.write_receipts", b.write_request_receipts.len() as u64);
        push("block.batch_write_receipts", b.batch_write_request_receipts.len() as u64);
        push("block.discard_receipts", b.discard_request_receipts.len() as u64);
        push("block.batch_discard_receipts", b.batch_discard_request_receipts.len() as u64);
        push("block.replicas", b.replicas.len() as u64);

        // ---- the replicas: where the bytes actually live ---------------------
        let mut chunks = 0u64;
        let mut data_len = 0u64;
        let mut data_cap = 0u64;
        let mut shard_len = 0u64;
        let mut shard_cap = 0u64;
        let mut shard_count = 0u64;
        let mut checksum_bytes = 0u64;
        let mut discard_rows = 0u64;
        let mut chunk_receipts = 0u64;
        for replica in &b.replicas {
            chunks = chunks.saturating_add(replica.chunks.len() as u64);
            for state in replica.chunks.values() {
                data_len = data_len.saturating_add(state.data.len() as u64);
                data_cap = data_cap.saturating_add(state.data.capacity() as u64);
                checksum_bytes = checksum_bytes.saturating_add(state.checksum.capacity() as u64);
                discard_rows = discard_rows.saturating_add(state.discarded_ranges.len() as u64);
                shard_count = shard_count.saturating_add(1);
                shard_len = shard_len.saturating_add(state.ec_parity_fragment.len() as u64);
                shard_cap = shard_cap.saturating_add(state.ec_parity_fragment.capacity() as u64);
            }
            chunk_receipts = chunk_receipts
                .saturating_add(replica.write_request_receipts.len() as u64)
                .saturating_add(replica.discard_request_receipts.len() as u64)
                .saturating_add(replica.batch_write_request_receipts.len() as u64)
                .saturating_add(replica.batch_discard_request_receipts.len() as u64);
        }
        push("replica.chunks", chunks);
        push("replica.data.len", data_len);
        push("replica.data.capacity", data_cap);
        push("replica.ec_shards.count", shard_count);
        push("replica.ec_shards.len", shard_len);
        push("replica.ec_shards.capacity", shard_cap);
        push("replica.checksum.capacity", checksum_bytes);
        push("replica.discard_rows", discard_rows);
        push("replica.receipts", chunk_receipts);

        // ---- the headline: bytes we can actually name ------------------------
        let accounted = payload_cap
            .saturating_add(data_cap)
            .saturating_add(shard_cap)
            .saturating_add(checksum_bytes);
        push("ACCOUNTED.byte_buffers", accounted);
        push("logical.written", self.logical_written_bytes);
        push("physical.written", self.physical_written_bytes);
        out
    }
}
