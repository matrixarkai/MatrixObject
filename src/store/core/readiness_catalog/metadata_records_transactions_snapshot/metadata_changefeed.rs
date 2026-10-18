impl MatrixObjectStore {
    pub fn metadata_changes_after(&self, sequence: u64, limit: usize) -> Vec<MetadataChange> {
        self.meta_service.list_changes_after(sequence, limit)
    }


    pub fn page_metadata_changes(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangePage, ObjectError> {
        self.meta_service
            .page_changes(prefix, after_sequence, max_changes, max_payload_bytes)
    }


    pub fn watch_metadata_changes(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataWatchPage, ObjectError> {
        self.meta_service
            .watch_changes(prefix, after_sequence, max_changes, max_payload_bytes)
    }


    pub fn compact_metadata_changefeed(
        &mut self,
        prefix: &str,
        max_changes: usize,
    ) -> Result<MetadataChangefeedCompactionReport, ObjectError> {
        self.meta_service.compact_changefeed(prefix, max_changes)
    }
}
