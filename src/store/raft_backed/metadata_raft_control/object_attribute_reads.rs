// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn get_object_tags_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<(BTreeMap<String, String>, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position("get object tags linearizable", now_ms, 0, |options| {
            self.store
                .get_object_tags_with_metadata_read(bucket, key, options)
        })
    }

    pub fn get_object_tags_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<BTreeMap<String, String>>, ObjectError> {
        let (tags, readiness) = self.get_object_tags_linearizable(bucket, key, now_ms)?;
        Ok(proven_metadata_read(tags, &readiness))
    }

    pub fn get_object_legal_hold_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<(bool, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object legal hold linearizable",
            now_ms,
            0,
            |options| {
                self.store
                    .get_object_legal_hold_with_metadata_read(bucket, key, options)
            },
        )
    }

    pub fn get_object_legal_hold_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<bool>, ObjectError> {
        let (enabled, readiness) = self.get_object_legal_hold_linearizable(bucket, key, now_ms)?;
        Ok(proven_metadata_read(enabled, &readiness))
    }

    pub fn get_object_retention_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<(u64, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object retention linearizable",
            now_ms,
            0,
            |options| {
                self.store
                    .get_object_retention_with_metadata_read(bucket, key, options)
            },
        )
    }

    pub fn get_object_retention_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<u64>, ObjectError> {
        let (retention, readiness) = self.get_object_retention_linearizable(bucket, key, now_ms)?;
        Ok(proven_metadata_read(retention, &readiness))
    }
}
