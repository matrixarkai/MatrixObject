// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_commit_object_change_cursor(
        &mut self,
        consumer_id: &str,
        sequence: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CommitObjectChangeCursor {
            consumer_id: consumer_id.to_string(),
            sequence,
            update_time_ms,
        })
    }

    pub fn propose_compact_object_changefeed(
        &mut self,
        max_records: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CompactObjectChangefeed { max_records })
    }
}
