// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Metadata command application and checkpoint transfer for MatrixRaft.

use matrixraft::{
    ApplyRequest, ApplyResponse, LogId, RaftError, SnapshotChunk, SnapshotMetadata, StateMachine,
};

const METADATA_RAFT_PAYLOAD_LIMIT: usize = 8 * 1024 * 1024;
const METADATA_RAFT_SNAPSHOT_LIMIT: usize = 64 * 1024 * 1024;
const METADATA_RAFT_SNAPSHOT_MAGIC: &[u8; 8] = b"MOMRAFT1";

/// Metadata commands driven by a single MatrixRaft group.
///
/// A response starts with zero for success or one for a committed application
/// refusal, followed by its UTF-8 message. Refusals advance the log checkpoint:
/// a lease conflict must not stall every later committed command. Malformed
/// commands and invalid delivery order are Raft errors and do not advance it.
/// Only commands supported by `ObjectCommand::to_payload` can be proposed.
pub struct MetadataStateMachine {
    store: MatrixObjectStore,
    group_id: u64,
    last_log_id: LogId,
    last_payload: Vec<u8>,
    last_response: Vec<u8>,
    installing: Vec<u8>,
    installing_meta: Option<SnapshotMetadata>,
    min_commit_index: u64,
    failed: bool,
}

impl MetadataStateMachine {
    /// Construct the default metadata group (group 1).
    pub fn new(store: MatrixObjectStore) -> Self {
        Self::for_group(1, store).expect("the default metadata group is valid")
    }

    pub fn for_group(group_id: u64, store: MatrixObjectStore) -> Result<Self, RaftError> {
        if group_id == 0 {
            return Err(RaftError::InvalidRequest(
                "metadata group must be positive".into(),
            ));
        }
        Ok(Self {
            store,
            group_id,
            last_log_id: LogId { term: 0, index: 0 },
            last_payload: Vec::new(),
            last_response: Vec::new(),
            installing: Vec::new(),
            installing_meta: None,
            min_commit_index: 0,
            failed: false,
        })
    }

    /// Fence on the metadata service commit index, separate from the Raft log index.
    pub fn set_min_commit_index(&mut self, min_commit_index: u64) {
        self.min_commit_index = min_commit_index;
    }
    pub fn last_log_id(&self) -> LogId {
        self.last_log_id.clone()
    }
    pub fn store(&self) -> &MatrixObjectStore {
        &self.store
    }
    /// Bootstrap access. Mutating a live replicated service through this bypasses Raft.
    pub fn store_mut(&mut self) -> &mut MatrixObjectStore {
        &mut self.store
    }
    pub fn into_store(self) -> MatrixObjectStore {
        self.store
    }

    fn checkpoint_bytes(&self) -> Result<Vec<u8>, RaftError> {
        if self.failed {
            return Err(RaftError::Storage(
                "metadata apply failed; install a checkpoint before serving".into(),
            ));
        }
        let metadata = self
            .store
            .meta_service
            .export_snapshot()
            .to_bytes()
            .map_err(|e| RaftError::Storage(e.to_string()))?;
        let size = 64usize
            .saturating_add(self.last_payload.len())
            .saturating_add(self.last_response.len())
            .saturating_add(metadata.len());
        if size > METADATA_RAFT_SNAPSHOT_LIMIT {
            return Err(RaftError::Storage(
                "metadata checkpoint exceeds size limit".into(),
            ));
        }
        let mut out = Vec::with_capacity(size);
        out.extend_from_slice(METADATA_RAFT_SNAPSHOT_MAGIC);
        for value in [
            self.group_id,
            self.last_log_id.term,
            self.last_log_id.index,
            self.last_payload.len() as u64,
            self.last_response.len() as u64,
            metadata.len() as u64,
        ] {
            out.extend_from_slice(&value.to_be_bytes());
        }
        out.extend_from_slice(&self.last_payload);
        out.extend_from_slice(&self.last_response);
        out.extend_from_slice(&metadata);
        out.extend_from_slice(&fnv1a64(&out).to_be_bytes());
        Ok(out)
    }

    fn install_checkpoint(
        &mut self,
        bytes: &[u8],
        meta: &SnapshotMetadata,
    ) -> Result<(), RaftError> {
        let invalid = |message: &str| RaftError::InvalidRequest(message.into());
        if bytes.len() < 64 || &bytes[..8] != METADATA_RAFT_SNAPSHOT_MAGIC {
            return Err(invalid("invalid metadata Raft checkpoint header"));
        }
        let number =
            |offset: usize| u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        if number(bytes.len() - 8) != fnv1a64(&bytes[..bytes.len() - 8]) {
            return Err(invalid("metadata checkpoint checksum mismatch"));
        }
        let group = number(8);
        let log = LogId {
            term: number(16),
            index: number(24),
        };
        if group != self.group_id
            || log != meta.last_log_id
            || log.index < self.last_log_id.index
            || log.term < self.last_log_id.term
            || (log.index == 0) != (log.term == 0)
        {
            return Err(invalid(
                "metadata checkpoint group or log identity is invalid",
            ));
        }
        let mut lengths = [0usize; 3];
        for (i, length) in lengths.iter_mut().enumerate() {
            *length = usize::try_from(number(32 + i * 8))
                .map_err(|_| invalid("metadata checkpoint length overflow"))?;
        }
        let [payload_len, response_len, metadata_len] = lengths;
        if payload_len > METADATA_RAFT_PAYLOAD_LIMIT
            || response_len > METADATA_RAFT_PAYLOAD_LIMIT + 1
            || 64usize
                .checked_add(payload_len)
                .and_then(|n| n.checked_add(response_len))
                .and_then(|n| n.checked_add(metadata_len))
                != Some(bytes.len())
        {
            return Err(invalid("metadata checkpoint lengths are invalid"));
        }
        let payload = &bytes[56..56 + payload_len];
        let response = &bytes[56 + payload_len..56 + payload_len + response_len];
        if log.index == 0 {
            if !payload.is_empty() || !response.is_empty() {
                return Err(invalid("empty log has an apply receipt"));
            }
        } else {
            if payload.starts_with(crate::placement_outcome::PLACEMENT_OUTCOME_MAGIC) {
                crate::placement_outcome::PlacementOutcome::from_payload(payload).map_err(|e| invalid(&e.to_string()))?;
            } else {
                ObjectCommand::from_payload(payload).map_err(|e| invalid(&e.to_string()))?;
            }
            if !matches!(response.first(), Some(0 | 1))
                || std::str::from_utf8(&response[1..]).is_err()
            {
                return Err(invalid("invalid metadata apply response"));
            }
        }
        if log.index == self.last_log_id.index
            && log.index != 0
            && (log != self.last_log_id
                || payload != self.last_payload
                || response != self.last_response)
        {
            return Err(invalid("checkpoint conflicts with the applied entry"));
        }
        let metadata = &bytes[56 + payload_len + response_len..bytes.len() - 8];
        let snapshot =
            MetaSnapshot::from_bytes(metadata).map_err(|e| RaftError::Storage(e.to_string()))?;
        let mut candidate = self.store.meta_service.clone();
        candidate
            .install_placement_roster(&snapshot)
            .map_err(|e| RaftError::Storage(e.to_string()))?;
        candidate
            .install_raft_snapshot_bytes(metadata, self.min_commit_index)
            .map_err(|e| RaftError::Storage(e.to_string()))?;
        self.store.meta_service = candidate;
        self.last_log_id = log;
        self.last_payload = payload.to_vec();
        self.last_response = response.to_vec();
        self.failed = false;
        Ok(())
    }
}

impl StateMachine for MetadataStateMachine {
    fn apply(&mut self, request: ApplyRequest) -> Result<ApplyResponse, RaftError> {
        if self.failed {
            return Err(RaftError::Storage(
                "metadata apply failed; install a checkpoint before serving".into(),
            ));
        }
        if request.group_id != self.group_id
            || request.log_id.term == 0
            || request.log_id.index == 0
        {
            return Err(RaftError::InvalidRequest(
                "invalid metadata log identity".into(),
            ));
        }
        if request.payload.len() > METADATA_RAFT_PAYLOAD_LIMIT {
            return Err(RaftError::InvalidRequest(
                "metadata command exceeds size limit".into(),
            ));
        }
        if request.log_id == self.last_log_id && request.payload == self.last_payload {
            return Ok(ApplyResponse {
                applied_index: self.last_log_id.index,
                response: self.last_response.clone(),
            });
        }
        if self.last_log_id.index.checked_add(1) != Some(request.log_id.index)
            || request.log_id.term < self.last_log_id.term
        {
            return Err(RaftError::InvalidRequest(
                "metadata entry is stale, conflicting, or out of order".into(),
            ));
        }
        let applied = if request.payload.starts_with(crate::placement_outcome::PLACEMENT_OUTCOME_MAGIC) {
            let outcome = crate::placement_outcome::PlacementOutcome::from_payload(&request.payload)
                .map_err(|e| RaftError::InvalidRequest(e.to_string()))?;
            outcome.apply(&mut self.store.meta_service)
        } else {
            let command = ObjectCommand::from_payload(&request.payload)
                .map_err(|e| RaftError::InvalidRequest(e.to_string()))?;
            self.store.apply_command(command).map(|_| ())
        };
        let response = match applied {
            Ok(_) => vec![0],
            Err(ObjectError::Corruption(message)) => {
                self.failed = true;
                return Err(RaftError::Storage(message));
            }
            Err(error) => {
                let message = error.to_string();
                let mut end = message.len().min(METADATA_RAFT_PAYLOAD_LIMIT);
                while !message.is_char_boundary(end) {
                    end -= 1;
                }
                let mut out = vec![1];
                out.extend_from_slice(message[..end].as_bytes());
                out
            }
        };
        self.last_log_id = request.log_id;
        self.last_payload = request.payload;
        self.last_response = response.clone();
        Ok(ApplyResponse {
            applied_index: self.last_log_id.index,
            response,
        })
    }

    fn snapshot(&self) -> Result<SnapshotChunk, RaftError> {
        Ok(SnapshotChunk {
            meta: SnapshotMetadata {
                snapshot_id: format!(
                    "metadata-{}-{}-{}",
                    self.group_id, self.last_log_id.term, self.last_log_id.index
                ),
                last_log_id: self.last_log_id.clone(),
                membership: Vec::new(),
                members: Vec::new(),
            },
            offset: 0,
            data: self.checkpoint_bytes()?,
            done: true,
        })
    }

    fn install_snapshot(&mut self, chunk: SnapshotChunk) -> Result<(), RaftError> {
        if chunk.offset == 0 {
            self.installing.clear();
            self.installing_meta = Some(chunk.meta.clone());
        }
        if self.installing_meta.as_ref() != Some(&chunk.meta)
            || chunk.offset != self.installing.len() as u64
            || chunk.data.len() > METADATA_RAFT_SNAPSHOT_LIMIT.saturating_sub(self.installing.len())
        {
            self.installing.clear();
            self.installing_meta = None;
            return Err(RaftError::InvalidRequest(
                "invalid metadata checkpoint stream".into(),
            ));
        }
        self.installing.extend_from_slice(&chunk.data);
        if !chunk.done {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.installing);
        self.installing_meta = None;
        self.install_checkpoint(&bytes, &chunk.meta)
    }
}
