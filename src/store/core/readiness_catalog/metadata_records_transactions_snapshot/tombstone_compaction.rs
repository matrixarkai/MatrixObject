// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn plan_metadata_tombstone_compaction(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<MetadataTombstoneCompactionReport, ObjectError> {
        self.meta_service.plan_tombstone_compaction(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
        )
    }


    pub fn compact_metadata_tombstones(
        &mut self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<MetadataTombstoneCompactionReport, ObjectError> {
        self.meta_service
            .compact_tombstones(prefix, after_key, max_records, max_compact_bytes)
    }
}
