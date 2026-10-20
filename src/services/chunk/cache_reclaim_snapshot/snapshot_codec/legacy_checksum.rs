// SPDX-License-Identifier: Apache-2.0
impl ReplicaServiceSnapshot {
    fn legacy_checksum_material_hash(&self) -> String {
        let mut material = Vec::with_capacity(self.legacy_checksum_material_size_hint());
        append_u64(&mut material, self.pages.len() as u64);
        for (page_id, state) in &self.pages {
            append_chunk_id(&mut material, &page_id.chunk_id);
            append_u64(&mut material, page_id.page_index);
            append_u64(&mut material, state.logical_length);
            append_u64(&mut material, state.write_sequence);
            append_string(&mut material, &state.checksum());
            append_u64(&mut material, state.data.len() as u64);
            material.extend_from_slice(&state.data);
            append_u64(&mut material, state.discarded_ranges.len() as u64);
            for range in &state.discarded_ranges {
                append_range(&mut material, range);
            }
        }
        etag(&material)
    }

    fn legacy_checksum_material_size_hint(&self) -> usize {
        const U64: usize = std::mem::size_of::<u64>();
        self.pages.values().fold(U64, |total, state| {
            total
                .saturating_add(U64 * 5)
                .saturating_add(encoded_string_len(&state.checksum()))
                .saturating_add(state.data.len())
                .saturating_add(U64 * 2 * state.discarded_ranges.len())
        })
    }
}
