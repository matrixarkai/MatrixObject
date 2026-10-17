#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkReadResult {
    pub data: Vec<u8>,
    pub write_sequence: u64,
    pub checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkHead {
    pub logical_length: u64,
    pub write_sequence: u64,
    pub checksum: String,
    pub state_checksum: String,
    pub discarded_ranges: Vec<DataRange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRangeReadResult {
    pub range: DataRange,
    pub data: Vec<u8>,
    pub cache_hit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkReadBatchRead {
    pub ranges: Vec<ChunkRangeReadResult>,
    pub stats: ChunkReadBatchStats,
    pub min_read_index: u64,
    pub min_commit_index: u64,
    pub proof_format: String,
    pub proof_checksum: String,
}

impl ChunkReadBatchRead {
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
            "chunk read batch proof fence mismatch: expected min_read_index={} min_commit_index={} got min_read_index={} min_commit_index={}",
            expected_min_read_index,
            expected_min_commit_index,
            self.min_read_index,
            self.min_commit_index
        )))
    }

    pub fn verify_proof_format(&self) -> Result<(), ObjectError> {
        if self.proof_format == CHUNK_READ_BATCH_PROOF_FORMAT {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "chunk read batch proof format mismatch: expected {} got {}",
            CHUNK_READ_BATCH_PROOF_FORMAT, self.proof_format
        )))
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_read_batch_proof_checksum: &str,
    ) -> Result<(), ObjectError> {
        self.verify_proof_format()?;
        if expected_chunk_read_batch_proof_checksum.is_empty()
            || self.proof_checksum == expected_chunk_read_batch_proof_checksum
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "chunk read batch proof checksum mismatch: expected {} got {}",
            expected_chunk_read_batch_proof_checksum, self.proof_checksum
        )))
    }
}

impl ProvenStorageRead<ChunkReadBatchRead> {
    pub fn require_chunk_read_batch_storage_fence(
        self,
        expected_min_read_index: u64,
        expected_min_commit_index: u64,
    ) -> Result<Self, ObjectError> {
        self.value.verify_proof_format()?;
        self.value
            .verify_storage_fence(expected_min_read_index, expected_min_commit_index)?;
        Ok(self)
    }

    pub fn require_chunk_read_batch_proof_checksum(
        self,
        expected_chunk_read_batch_proof_checksum: &str,
    ) -> Result<Self, ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_chunk_read_batch_proof_checksum)?;
        Ok(self)
    }
}

