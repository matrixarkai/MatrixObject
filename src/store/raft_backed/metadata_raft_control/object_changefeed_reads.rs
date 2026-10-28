// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_object_changes_after_linearizable(
        &self,
        after_sequence: u64,
        limit: usize,
        now_ms: u64,
    ) -> Result<(Vec<ObjectChangeRecord>, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "list object changes after linearizable",
            now_ms,
            0,
            |options| {
                self.store.list_object_changes_after_with_metadata_read(
                    after_sequence,
                    limit,
                    options,
                )
            },
        )
    }

    pub fn list_object_changes_page_after_linearizable(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectChangePage, MetadataRaftReadiness), ObjectError> {
        let (mut page, readiness) = self.read_metadata_at_raft_position(
            "list object changes page after linearizable",
            now_ms,
            0,
            |options| {
                self.store
                    .list_object_changes_page_after_with_metadata_read(
                        after_sequence,
                        limit,
                        max_payload_bytes,
                        options,
                    )
            },
        )?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_change_page_proof(
            after_sequence,
            limit,
            "ListObjectChanges",
            &mut page,
            None,
        )?;
        Ok((page, readiness))
    }

    pub fn list_object_changes_page_after_linearizable_change_page_proof_fenced(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_object_change_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectChangePage, MetadataRaftReadiness), ObjectError> {
        let (mut page, readiness) = self.list_object_changes_page_after_linearizable(
            after_sequence,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        stamp_object_change_page_proof(
            after_sequence,
            limit,
            "ListObjectChanges",
            &mut page,
            Some(expected_object_change_page_proof_checksum.as_ref()),
        )?;
        Ok((page, readiness))
    }

    pub fn list_object_changes_after_linearizable_committed_at(
        &self,
        after_sequence: u64,
        limit: usize,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(Vec<ObjectChangeRecord>, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "list object changes after linearizable committed at",
            now_ms,
            min_commit_index,
            |options| {
                self.store.list_object_changes_after_with_metadata_read(
                    after_sequence,
                    limit,
                    options,
                )
            },
        )
    }

    pub fn list_object_changes_page_after_linearizable_committed_at(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectChangePage, MetadataRaftReadiness), ObjectError> {
        let (mut page, readiness) = self.read_metadata_at_raft_position(
            "list object changes page after linearizable committed at",
            now_ms,
            min_commit_index,
            |options| {
                self.store
                    .list_object_changes_page_after_with_metadata_read(
                        after_sequence,
                        limit,
                        max_payload_bytes,
                        options,
                    )
            },
        )?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_change_page_proof(
            after_sequence,
            limit,
            "ListObjectChanges",
            &mut page,
            None,
        )?;
        Ok((page, readiness))
    }

    pub fn list_object_changes_page_after_linearizable_change_page_proof_fenced_committed_at(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_object_change_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectChangePage, MetadataRaftReadiness), ObjectError> {
        let (mut page, readiness) = self.list_object_changes_page_after_linearizable_committed_at(
            after_sequence,
            limit,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )?;
        stamp_object_change_page_proof(
            after_sequence,
            limit,
            "ListObjectChanges",
            &mut page,
            Some(expected_object_change_page_proof_checksum.as_ref()),
        )?;
        Ok((page, readiness))
    }

    pub fn list_object_changes_after_with_read_proof_linearizable(
        &self,
        after_sequence: u64,
        limit: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<ObjectChangeRecord>>, ObjectError> {
        let (records, readiness) =
            self.list_object_changes_after_linearizable(after_sequence, limit, now_ms)?;
        Ok(proven_metadata_read(records, &readiness))
    }

    pub fn list_object_changes_page_after_with_read_proof_linearizable(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectChangePage>, ObjectError> {
        let (page, readiness) = self.list_object_changes_page_after_linearizable(
            after_sequence,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(proven_metadata_read(page, &readiness))
    }

    pub fn list_object_changes_after_with_read_proof_linearizable_committed_at(
        &self,
        after_sequence: u64,
        limit: usize,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<Vec<ObjectChangeRecord>>, ObjectError> {
        let (records, readiness) = self.list_object_changes_after_linearizable_committed_at(
            after_sequence,
            limit,
            now_ms,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(records, &readiness))
    }

    pub fn list_object_changes_page_after_with_read_proof_linearizable_committed_at(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectChangePage>, ObjectError> {
        let (page, readiness) = self.list_object_changes_page_after_linearizable_committed_at(
            after_sequence,
            limit,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(page, &readiness))
    }

    pub fn object_change_cursor_linearizable(
        &self,
        consumer_id: &str,
        now_ms: u64,
    ) -> Result<(ObjectChangeCursor, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "object change cursor linearizable",
            now_ms,
            0,
            |options| {
                self.store
                    .object_change_cursor_with_metadata_read(consumer_id, options)
            },
        )
    }

    pub fn object_change_cursor_linearizable_committed_at(
        &self,
        consumer_id: &str,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectChangeCursor, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "object change cursor linearizable committed at",
            now_ms,
            min_commit_index,
            |options| {
                self.store
                    .object_change_cursor_with_metadata_read(consumer_id, options)
            },
        )
    }

    pub fn object_change_cursor_with_read_proof_linearizable(
        &self,
        consumer_id: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectChangeCursor>, ObjectError> {
        let (cursor, readiness) = self.object_change_cursor_linearizable(consumer_id, now_ms)?;
        Ok(proven_metadata_read(cursor, &readiness))
    }

    pub fn object_change_cursor_with_read_proof_linearizable_committed_at(
        &self,
        consumer_id: &str,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectChangeCursor>, ObjectError> {
        let (cursor, readiness) = self.object_change_cursor_linearizable_committed_at(
            consumer_id,
            now_ms,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(cursor, &readiness))
    }
}
