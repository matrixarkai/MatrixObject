#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkHeadBatchRead {
    pub heads: Vec<ChunkHead>,
    pub stats: ChunkHeadBatchStats,
    pub min_read_index: u64,
    pub min_commit_index: u64,
    pub proof_format: String,
    pub proof_checksum: String,
}

impl ChunkHeadBatchRead {
    pub fn verify_storage_fence(
        &self,
        expected_min_read_index: u64,
        expected_min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        if self.min_read_index == expected_min_read_index
            && self.min_commit_index == expected_min_commit_index
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "chunk head batch proof fence mismatch: expected min_read_index={} min_commit_index={} got min_read_index={} min_commit_index={}",
            expected_min_read_index,
            expected_min_commit_index,
            self.min_read_index,
            self.min_commit_index
        )))
    }

    pub fn verify_proof_format(&self) -> Result<(), ObjectError> {
        if self.proof_format == CHUNK_HEAD_BATCH_PROOF_FORMAT {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "chunk head batch proof format mismatch: expected {} got {}",
            CHUNK_HEAD_BATCH_PROOF_FORMAT, self.proof_format
        )))
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_head_batch_proof_checksum: &str,
    ) -> Result<(), ObjectError> {
        self.verify_proof_format()?;
        if expected_chunk_head_batch_proof_checksum.is_empty()
            || self.proof_checksum == expected_chunk_head_batch_proof_checksum
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "chunk head batch proof checksum mismatch: expected {} got {}",
            expected_chunk_head_batch_proof_checksum, self.proof_checksum
        )))
    }
}

impl ProvenStorageRead<ChunkHeadBatchRead> {
    pub fn require_chunk_head_batch_storage_fence(
        self,
        expected_min_read_index: u64,
        expected_min_commit_index: u64,
    ) -> Result<Self, ObjectError> {
        self.value.verify_proof_format()?;
        self.value
            .verify_storage_fence(expected_min_read_index, expected_min_commit_index)?;
        Ok(self)
    }

    pub fn require_chunk_head_batch_proof_checksum(
        self,
        expected_chunk_head_batch_proof_checksum: &str,
    ) -> Result<Self, ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_chunk_head_batch_proof_checksum)?;
        Ok(self)
    }
}

