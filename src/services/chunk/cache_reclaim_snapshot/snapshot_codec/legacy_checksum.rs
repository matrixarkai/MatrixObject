// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl ReplicaServiceSnapshot {
    fn legacy_checksum_material_hash(&self) -> String {
        let mut material = Vec::with_capacity(1024);
        let mut digest = FNV1A64_OFFSET_BASIS;
        append_u64(&mut material, self.pages.len() as u64);
        for (page_id, state) in &self.pages {
            append_chunk_id(&mut material, &page_id.chunk_id);
            append_u64(&mut material, page_id.page_index);
            append_u64(&mut material, state.logical_length);
            append_u64(&mut material, state.write_sequence);
            append_string(&mut material, &state.checksum());
            append_u64(&mut material, state.data.len() as u64);
            digest = fnv1a64_resumed(digest, &material);
            material.clear();
            digest = fnv1a64_resumed(digest, &state.data);
            append_u64(&mut material, state.discarded_ranges.len() as u64);
            for range in &state.discarded_ranges {
                append_range(&mut material, range);
            }
        }
        digest = fnv1a64_resumed(digest, &material);
        let mut checksum = String::with_capacity(24);
        append_etag_of_digest(&mut checksum, digest);
        checksum
    }

}
