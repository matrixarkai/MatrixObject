impl MatrixObjectStore {
    fn apply_persistent_journal_object_operation(
        &mut self,
        op: u64,
        cursor: &mut SnapshotByteCursor<'_>,
        payload_len: usize,
    ) -> Result<bool, ObjectError> {
        Ok(
            self.apply_persistent_journal_basic_object_operation(op, cursor, payload_len)?
                || self.apply_persistent_journal_multipart_operation(op, cursor)?
                || self.apply_persistent_journal_batch_lifecycle_operation(op, cursor)?,
        )
    }
}

include!("object_operations/basic_object_operations.rs");
include!("object_operations/multipart_operations.rs");
include!("object_operations/batch_lifecycle_operations.rs");
