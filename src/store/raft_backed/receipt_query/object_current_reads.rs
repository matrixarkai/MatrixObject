// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn get_object_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position("get object linearizable", now_ms, 0, |options| {
            self.store
                .get_object_with_metadata_read(bucket, key, options)
        })
    }

    pub fn get_object_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object linearizable committed at",
            now_ms,
            min_commit_index,
            |options| {
                self.store
                    .get_object_with_metadata_read(bucket, key, options)
            },
        )
    }

    pub fn get_object_with_read_admission_linearizable(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object with read admission linearizable",
            now_ms,
            0,
            |options| {
                self.store.get_object_with_read_admission_and_metadata_read(
                    bucket,
                    key,
                    max_payload_bytes,
                    options,
                )
            },
        )
    }

    pub fn get_object_with_read_admission_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object with read admission linearizable committed at",
            now_ms,
            min_commit_index,
            |options| {
                self.store.get_object_with_read_admission_and_metadata_read(
                    bucket,
                    key,
                    max_payload_bytes,
                    options,
                )
            },
        )
    }

    pub fn get_object_conditional_linearizable(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<(ConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object conditional linearizable",
            now_ms,
            0,
            |options| {
                self.store.get_object_conditional_with_metadata_read(
                    bucket,
                    key,
                    preconditions,
                    options,
                )
            },
        )
    }

    pub fn get_object_conditional_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(ConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object conditional linearizable committed at",
            now_ms,
            min_commit_index,
            |options| {
                self.store.get_object_conditional_with_metadata_read(
                    bucket,
                    key,
                    preconditions,
                    options,
                )
            },
        )
    }

    pub fn get_object_conditional_with_read_admission_linearizable(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object conditional with read admission linearizable",
            now_ms,
            0,
            |options| {
                self.store
                    .get_object_conditional_with_read_admission_and_metadata_read(
                        bucket,
                        key,
                        max_payload_bytes,
                        preconditions,
                        options,
                    )
            },
        )
    }

    pub fn get_object_conditional_with_read_admission_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.read_metadata_at_raft_position(
            "get object conditional with read admission linearizable committed at",
            now_ms,
            min_commit_index,
            |options| {
                self.store
                    .get_object_conditional_with_read_admission_and_metadata_read(
                        bucket,
                        key,
                        max_payload_bytes,
                        preconditions,
                        options,
                    )
            },
        )
    }
}
