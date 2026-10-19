impl ChunkServiceSnapshot {
    fn legacy_checksum_material_hash(&self) -> String {
        let mut material = Vec::with_capacity(self.encoded_size_hint().min(2 * 1024 * 1024));
        append_u64(&mut material, self.page_size);
        append_u64(&mut material, self.write_quorum as u64);
        append_u64(&mut material, self.read_quorum as u64);
        append_u64(&mut material, self.allocator_chunk_size);
        append_u64(&mut material, self.allocator_next_chunk_id);
        append_u64(&mut material, self.allocator_next_offset);
        append_u64(&mut material, self.released_allocation_count);
        append_u64(&mut material, self.pending_allocations.len() as u64);
        for (allocation_id, allocation) in &self.pending_allocations {
            append_string(&mut material, allocation_id);
            append_string(&mut material, &allocation.allocation_id);
            append_slab_id(&mut material, &allocation.chunk_id);
            append_u64(&mut material, allocation.offset);
            append_u64(&mut material, allocation.length);
            append_u64(&mut material, allocation.create_time_ms);
        }
        if !self.quarantined_replicas.is_empty() {
            append_u64(&mut material, self.quarantined_replicas.len() as u64);
            for index in &self.quarantined_replicas {
                append_u64(&mut material, *index as u64);
            }
        }
        append_u64(&mut material, self.replicas.len() as u64);
        for replica in &self.replicas {
            append_string(&mut material, &replica.checksum);
        }
        etag(&material)
    }
}
