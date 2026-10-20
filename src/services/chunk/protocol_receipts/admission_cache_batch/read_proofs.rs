// SPDX-License-Identifier: Apache-2.0
/// Whether a read's bytes are worth keeping in the read cache.
///
/// # WHY A READ HAS TO SAY WHICH IT IS
///
/// The cache exists to make a client's next read cheap, and it is bounded, so every byte
/// in it displaces another. A read the store performs for its OWN bookkeeping consumes its
/// bytes immediately and never asks for them again -- and filling the cache with those is
/// not a warm cache, it is a cache with a client's bytes evicted out of it.
///
/// The striped write path is the case that forced the distinction. Recomputing a stripe
/// group's redundancy unit means reading the group's other data units and XORing them, and
/// at 64 KiB units that read a cacheable 64 KiB per write. One put an object therefore
/// filled the cache to its bound out of the WRITE path, with nothing having been read: the
/// footprint audit counted 212 MB of cached payload against 8 MB of logical data, so a
/// store that held one and a half copies of its payload held twenty-seven times its data
/// in total.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadCacheIntent {
    /// A read on a client's behalf: look in the cache, and leave the bytes there.
    Fill,
    /// A read the store does for itself: do not look, and do not leave anything behind.
    ///
    /// The lookup is skipped as well as the fill, deliberately. An entry one of these reads
    /// could hit is an entry one of them put there, so looking costs a probe and a lock to
    /// find what bypassing never stored.
    Bypass,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaReadResult {
    pub data: Vec<u8>,
    pub write_sequence: u64,
    pub checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaHead {
    pub logical_length: u64,
    pub write_sequence: u64,
    pub checksum: String,
    pub state_checksum: String,
    pub discarded_ranges: Vec<DataRange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRangeReadResult {
    pub range: DataRange,
    pub data: Vec<u8>,
    pub cache_hit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaReadBatchRead {
    pub ranges: Vec<ReplicaRangeReadResult>,
    pub stats: ReplicaReadBatchStats,
    pub min_read_index: u64,
    pub min_commit_index: u64,
    pub proof_format: String,
    pub proof_checksum: String,
}

impl ReplicaReadBatchRead {
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

impl ProvenStorageRead<ReplicaReadBatchRead> {
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

