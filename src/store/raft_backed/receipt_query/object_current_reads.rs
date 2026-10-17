impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn get_object_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn get_object_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        let options =
            self.metadata_read_options_committed_at("object read", now_ms, min_commit_index)?;
        self.store
            .get_object_with_metadata_read(bucket, key, options)
    }

    pub fn get_object_with_read_admission_linearizable(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_with_read_admission_and_metadata_read(
            bucket,
            key,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
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
        let options = self.metadata_read_options_committed_at(
            "admitted object read",
            now_ms,
            min_commit_index,
        )?;
        self.store.get_object_with_read_admission_and_metadata_read(
            bucket,
            key,
            max_payload_bytes,
            options,
        )
    }

    pub fn get_object_conditional_linearizable(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<(ConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_conditional_with_metadata_read(
            bucket,
            key,
            preconditions,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
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
        let options = self.metadata_read_options_committed_at(
            "conditional object read",
            now_ms,
            min_commit_index,
        )?;
        self.store
            .get_object_conditional_with_metadata_read(bucket, key, preconditions, options)
    }

    pub fn get_object_conditional_with_read_admission_linearizable(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.store
            .get_object_conditional_with_read_admission_and_metadata_read(
                bucket,
                key,
                max_payload_bytes,
                preconditions,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
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
        let options = self.metadata_read_options_committed_at(
            "conditional admitted object read",
            now_ms,
            min_commit_index,
        )?;
        self.store
            .get_object_conditional_with_read_admission_and_metadata_read(
                bucket,
                key,
                max_payload_bytes,
                preconditions,
                options,
            )
    }
}
