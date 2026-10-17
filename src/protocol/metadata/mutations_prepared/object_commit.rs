#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedObjectMetadataCommit {
    pub current_key: String,
    pub object_id: ObjectId,
    pub current_value: String,
    pub object_value: String,
    pub delete_object: bool,
    pub expected_current_version: u64,
    pub expected_object_version: u64,
    pub batch: PreparedMetadataBatch,
    pub object_request_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedObjectMetadataCommit {
    pub fn computed_raft_entry_checksum(&self) -> String {
        prepared_object_metadata_commit_checksum(&PreparedObjectMetadataCommit {
            raft_entry_checksum: String::new(),
            ..self.clone()
        })
    }

    pub fn computed_object_request_checksum(&self) -> String {
        metadata_batch_request_checksum(
            &self.batch.mutations,
            false,
            "",
            0,
            self.batch.lease_owner.as_deref(),
            self.batch.lease_epoch,
            self.batch.now_ms,
        )
    }

    pub fn verify_raft_entry_checksum(&self) -> Result<(), ObjectError> {
        let expected_checksum = self.computed_raft_entry_checksum();
        if self.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(format!(
                "prepared object metadata commit raft entry checksum mismatch: expected {} got {}",
                expected_checksum, self.raft_entry_checksum
            )));
        }
        Ok(())
    }

    pub fn verify_batch_raft_entry_checksum(&self) -> Result<(), ObjectError> {
        let expected_checksum = prepared_metadata_batch_checksum(&PreparedMetadataBatch {
            raft_entry_checksum: String::new(),
            ..self.batch.clone()
        });
        if self.batch.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(format!(
                "prepared object metadata commit batch checksum mismatch: expected {} got {}",
                expected_checksum, self.batch.raft_entry_checksum
            )));
        }
        Ok(())
    }

    pub fn verify_object_request_checksum(&self) -> Result<(), ObjectError> {
        let expected_checksum = self.computed_object_request_checksum();
        if self.object_request_checksum != expected_checksum
            || self.batch.request_checksum != expected_checksum
        {
            return Err(ObjectError::Corruption(format!(
                "prepared object metadata commit request checksum mismatch: expected {} got object={} batch={}",
                expected_checksum, self.object_request_checksum, self.batch.request_checksum
            )));
        }
        Ok(())
    }

    pub fn verify_expected_raft_entry_checksum(
        &self,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Ok(());
        }
        if self.raft_entry_checksum != expected_raft_entry_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared object metadata commit raft entry checksum mismatch: expected {} got {}",
                expected_raft_entry_checksum, self.raft_entry_checksum
            )));
        }
        Ok(())
    }

    pub fn verify_expected_object_request_checksum(
        &self,
        expected_object_request_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_object_request_checksum = expected_object_request_checksum.as_ref();
        if expected_object_request_checksum.is_empty() {
            return Ok(());
        }
        if self.object_request_checksum != expected_object_request_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared object metadata commit request checksum mismatch: expected {} got {}",
                expected_object_request_checksum, self.object_request_checksum
            )));
        }
        Ok(())
    }

    pub fn verify_expected_metadata_control_proof_checksum(
        &self,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Ok(());
        }
        if self.batch.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared object metadata commit control proof checksum mismatch: expected {} got {}",
                expected_metadata_control_proof_checksum, self.batch.metadata_control_proof_checksum
            )));
        }
        Ok(())
    }

    pub fn verify_prepared_object_metadata_commit(&self) -> Result<(), ObjectError> {
        self.verify_batch_raft_entry_checksum()?;
        self.verify_object_request_checksum()?;
        self.verify_raft_entry_checksum()
    }

    pub fn verify_expected_fences(
        &self,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_object_commit_checksum: impl AsRef<str>,
        expected_batch_raft_entry_checksum: impl AsRef<str>,
        expected_object_request_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.verify_prepared_object_metadata_commit()?;
        self.verify_expected_metadata_control_proof_checksum(
            expected_metadata_control_proof_checksum,
        )?;
        self.verify_expected_raft_entry_checksum(expected_object_commit_checksum)?;
        self.batch
            .verify_expected_raft_entry_checksum(expected_batch_raft_entry_checksum)?;
        self.verify_expected_object_request_checksum(expected_object_request_checksum)
    }

    pub fn require_expected_fences(
        self,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_object_commit_checksum: impl AsRef<str>,
        expected_batch_raft_entry_checksum: impl AsRef<str>,
        expected_object_request_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_fences(
            expected_metadata_control_proof_checksum,
            expected_object_commit_checksum,
            expected_batch_raft_entry_checksum,
            expected_object_request_checksum,
        )?;
        Ok(self)
    }
}

impl ProvenMetadataRead<PreparedObjectMetadataCommit> {
    pub fn verify_prepared_object_metadata_commit(&self) -> Result<(), ObjectError> {
        self.value.verify_prepared_object_metadata_commit()
    }

    pub fn verify_expected_prepared_object_metadata_commit_fences(
        &self,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_object_commit_checksum: impl AsRef<str>,
        expected_batch_raft_entry_checksum: impl AsRef<str>,
        expected_object_request_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value.verify_expected_fences(
            expected_metadata_control_proof_checksum,
            expected_object_commit_checksum,
            expected_batch_raft_entry_checksum,
            expected_object_request_checksum,
        )
    }

    pub fn require_expected_prepared_object_metadata_commit_fences(
        self,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_object_commit_checksum: impl AsRef<str>,
        expected_batch_raft_entry_checksum: impl AsRef<str>,
        expected_object_request_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_prepared_object_metadata_commit_fences(
            expected_metadata_control_proof_checksum,
            expected_object_commit_checksum,
            expected_batch_raft_entry_checksum,
            expected_object_request_checksum,
        )?;
        Ok(self)
    }
}

