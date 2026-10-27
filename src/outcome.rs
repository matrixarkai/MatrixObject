// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Decided page changes shared by local recovery and replicated logs.
use crate::{
    chunk_local_state_checksum, ChunkId, DataRange, MatrixReplicaService, ObjectError, PageId,
    PageState,
};
use prost::Message;
use std::sync::Arc;

pub const MAX_OUTCOME_BYTES: usize = 8 * 1024 * 1024;
pub const OUTCOME_MAGIC: &[u8; 4] = b"MOPO";

#[derive(Clone, PartialEq, Message)]
pub struct BytePatch {
    #[prost(uint64, tag = "1")]
    pub offset: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
pub struct DiscardRange {
    #[prost(uint64, tag = "1")]
    pub offset: u64,
    #[prost(uint64, tag = "2")]
    pub length: u64,
}
/// A state transition, with no operation to re-execute during recovery.
#[derive(Clone, PartialEq, Message)]
pub struct PageOutcome {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub volume_id: u64,
    #[prost(uint64, tag = "3")]
    pub chunk_id: u64,
    #[prost(uint64, tag = "4")]
    pub page_index: u64,
    /// Empty only when the page did not exist.
    #[prost(string, tag = "5")]
    pub before_checksum: String,
    #[prost(uint64, tag = "6")]
    pub logical_length: u64,
    #[prost(uint64, tag = "7")]
    pub write_sequence: u64,
    #[prost(fixed64, tag = "8")]
    pub data_checksum: u64,
    #[prost(message, repeated, tag = "9")]
    pub patches: Vec<BytePatch>,
    #[prost(message, repeated, tag = "10")]
    pub discarded_ranges: Vec<DiscardRange>,
    #[prost(string, tag = "11")]
    pub after_checksum: String,
    #[prost(uint64, tag = "12")]
    pub data_length: u64,
    #[prost(uint64, repeated, packed = "true", tag = "13")]
    pub physical_deltas: Vec<u64>,
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.to_string())
}
impl PageOutcome {
    pub fn between(
        page: &PageId,
        before: Option<&PageState>,
        after: &PageState,
    ) -> Result<Self, ObjectError> {
        if after.data.len() > 256 * 1024 * 1024 {
            return Err(invalid("page outcome exceeds page size limit"));
        }
        crate::validate_chunk_state(after)?;
        if let Some(state) = before {
            crate::validate_chunk_state(state)?;
        }
        let old = before.map(|s| s.data.as_slice()).unwrap_or(&[]);
        let mut patches = Vec::new();
        let mut at = 0;
        while at < after.data.len() {
            if old.get(at).copied().unwrap_or(0) == after.data[at] {
                at += 1;
                continue;
            }
            let start = at;
            // Include short unchanged runs to bound protobuf overhead for alternating bytes.
            let mut end = at + 1;
            at += 1;
            while at < after.data.len() && at - end < 16 {
                if old.get(at).copied().unwrap_or(0) != after.data[at] {
                    end = at + 1;
                }
                at += 1;
            }
            patches.push(BytePatch {
                offset: start as u64,
                data: after.data[start..end].to_vec(),
            });
        }
        let result = Self {
            version: 1,
            volume_id: page.chunk_id.volume_id(),
            chunk_id: page.chunk_id.id(),
            page_index: page.page_index,
            before_checksum: before.map(chunk_local_state_checksum).unwrap_or_default(),
            logical_length: after.logical_length,
            write_sequence: after.write_sequence,
            data_checksum: after.checksum_fnv1a64,
            patches,
            discarded_ranges: after
                .discarded_ranges
                .iter()
                .map(|r| DiscardRange {
                    offset: r.offset,
                    length: r.length,
                })
                .collect(),
            after_checksum: chunk_local_state_checksum(after),
            data_length: after.data.len() as u64,
            physical_deltas: vec![0; 4],
        };
        result.to_payload()?;
        Ok(result)
    }
    pub fn counters(service: &MatrixReplicaService) -> [u64; 4] {
        [
            service.physical_write_count(),
            service.physical_write_bytes(),
            service.physical_discard_ranges(),
            service.physical_discard_bytes(),
        ]
    }
    pub fn with_counters(mut self, before: [u64; 4], after: [u64; 4]) -> Self {
        self.physical_deltas = after
            .iter()
            .zip(before)
            .map(|(a, b)| a.saturating_sub(b))
            .collect();
        self
    }
    pub fn to_payload(&self) -> Result<Vec<u8>, ObjectError> {
        if self.encoded_len() > MAX_OUTCOME_BYTES - 4 {
            return Err(invalid("page outcome exceeds size limit"));
        }
        let mut bytes = OUTCOME_MAGIC.to_vec();
        self.encode(&mut bytes)
            .map_err(|_| invalid("page outcome encoding failed"))?;
        Ok(bytes)
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_OUTCOME_BYTES || !bytes.starts_with(OUTCOME_MAGIC) {
            return Err(invalid("invalid page outcome envelope"));
        }
        let outcome =
            Self::decode(&bytes[4..]).map_err(|_| invalid("invalid page outcome protobuf"))?;
        if outcome.version != 1 || outcome.to_payload()? != bytes {
            return Err(invalid("unsupported or noncanonical page outcome"));
        }
        Ok(outcome)
    }
    pub fn page_id(&self) -> Result<PageId, ObjectError> {
        Ok(PageId {
            chunk_id: ChunkId::new(self.volume_id, self.chunk_id)?,
            page_index: self.page_index,
        })
    }
    pub fn apply(
        &self,
        service: &mut MatrixReplicaService,
        max_pages: usize,
    ) -> Result<(), ObjectError> {
        if self.version != 1 {
            return Err(invalid("unsupported page outcome version"));
        }
        let page = self.page_id()?;
        let before = match service.read_chunk_state(&page) {
            Ok(state) => Some(state),
            Err(ObjectError::NotFound(_)) => None,
            Err(err) => return Err(err),
        };
        if before
            .as_ref()
            .map(chunk_local_state_checksum)
            .unwrap_or_default()
            != self.before_checksum
        {
            return Err(invalid("page outcome prior state mismatch"));
        }
        if before.is_none() && service.page_count() >= max_pages {
            return Err(invalid("page outcome exceeds page count limit"));
        }
        // Bound allocation before materializing zero-filled extensions.
        service.ensure_capacity_for_chunk_len(&page, self.data_length)?;
        let length = usize::try_from(self.data_length)
            .map_err(|_| invalid("page outcome length overflow"))?;
        if length > 256 * 1024 * 1024 {
            return Err(invalid("page outcome exceeds page size limit"));
        }
        let mut end = 0;
        for patch in &self.patches {
            let next = patch
                .offset
                .checked_add(patch.data.len() as u64)
                .ok_or_else(|| invalid("page outcome patch overflow"))?;
            if patch.data.is_empty() || patch.offset < end || next > self.data_length {
                return Err(invalid("invalid page outcome patch range"));
            }
            end = next;
        }
        let mut data = before
            .as_ref()
            .map(|s| s.data.as_ref().clone())
            .unwrap_or_default();
        data.resize(length, 0);
        for patch in &self.patches {
            data[patch.offset as usize..patch.offset as usize + patch.data.len()]
                .copy_from_slice(&patch.data);
        }
        let state = PageState {
            logical_length: self.logical_length,
            write_sequence: self.write_sequence,
            checksum_fnv1a64: self.data_checksum,
            data: Arc::new(data),
            ec_parity_fragment: None,
            ec_fragment_digests: None,
            discarded_ranges: self
                .discarded_ranges
                .iter()
                .map(|r| DataRange {
                    offset: r.offset,
                    length: r.length,
                })
                .collect(),
        };
        if chunk_local_state_checksum(&state) != self.after_checksum {
            return Err(invalid("page outcome resulting state mismatch"));
        }
        if self.physical_deltas.len() != 4 {
            return Err(invalid("invalid page outcome counters"));
        }
        service.install_chunk_state(page, state)?;
        service.mark_metadata_mutation();
        service.note_physical_write(self.physical_deltas[0], self.physical_deltas[1]);
        service.note_physical_discard(self.physical_deltas[2], self.physical_deltas[3]);
        Ok(())
    }
}
