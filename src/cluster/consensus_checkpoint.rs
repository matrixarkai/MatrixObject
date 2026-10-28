// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Application checkpoint envelopes and opt-in WAL reclamation.
use crate::ObjectError;
use matrixraft::{
    peer_runtime::{DurablePeer, MAX_CHECKPOINT_BYTES},
    LogId, RaftError,
};
use prost::Message;
use std::path::PathBuf;

#[derive(Clone, PartialEq, Message)]
pub(super) struct Image {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint32, tag = "2")]
    kind: u32,
    #[prost(uint64, tag = "3")]
    index: u64,
    #[prost(uint64, tag = "4")]
    term: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub state: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    pub catalog: Vec<u8>,
}
pub(super) fn storage(error: impl ToString) -> RaftError {
    RaftError::Storage(error.to_string())
}
impl Image {
    pub(super) fn new(kind: u32, id: LogId, state: Vec<u8>, catalog: Vec<u8>) -> Self {
        Self {
            version: 1,
            kind,
            index: id.index,
            term: id.term,
            state,
            catalog,
        }
    }
    pub(super) fn decode_at(bytes: &[u8], kind: u32, id: LogId) -> Result<Self, RaftError> {
        if bytes.len() > MAX_CHECKPOINT_BYTES {
            return Err(storage("application checkpoint exceeds size bound"));
        }
        let image = Self::decode(bytes).map_err(storage)?;
        if image.version != 1
            || image.kind != kind
            || image.index != id.index
            || image.term != id.term
            || image.index == 0
            || image.term == 0
            || image.state.is_empty()
            || image.encode_to_vec() != bytes
        {
            return Err(storage(
                "application checkpoint identity or encoding mismatch",
            ));
        }
        Ok(image)
    }
}
pub(super) struct Policy {
    kind: u32,
    wal: PathBuf,
    interval: u64,
    baseline: u64,
    observed: u64,
    checkpoint: u64,
}
impl Policy {
    pub(super) fn new(kind: u32, wal: PathBuf, interval: u64) -> Result<Self, ObjectError> {
        if interval != 0 && !(1024..=128 * 1024 * 1024).contains(&interval) {
            return Err(ObjectError::InvalidArgument(
                "checkpoint byte interval must be zero or between 1 KiB and 128 MiB".into(),
            ));
        }
        Ok(Self {
            kind,
            wal,
            interval,
            baseline: 0,
            observed: 0,
            checkpoint: 0,
        })
    }
    pub(super) fn restored(&mut self, index: u64) {
        self.checkpoint = index;
        self.observed = index;
        // A reopened WAL may already contain a large suffix. Reclaim it on the
        // next applied advance rather than treating its total size as new space.
        self.baseline = 0;
    }
    pub(super) fn maybe_checkpoint(
        &mut self,
        peer: &mut DurablePeer,
        term: u64,
        capture: impl FnOnce() -> Result<(Vec<u8>, Vec<u8>), ObjectError>,
    ) -> Result<(), RaftError> {
        let index = peer.applied_index();
        if self.interval == 0 || index <= self.observed || index <= self.checkpoint {
            return Ok(());
        }
        self.observed = index;
        let size = std::fs::metadata(&self.wal).map_err(storage)?.len();
        if size.saturating_sub(self.baseline) < self.interval {
            return Ok(());
        }
        let (state, catalog) = capture().map_err(storage)?;
        let image = Image::new(self.kind, LogId { index, term }, state, catalog);
        if image.encoded_len() > MAX_CHECKPOINT_BYTES {
            // Do not discard any application state to manufacture a checkpoint.
            // Retry only after another interval of history, keeping capture work
            // bounded even while the application needs a larger snapshot API.
            self.baseline = size;
            eprintln!("application checkpoint exceeds supported size; retaining WAL history (kind={}, bytes={}, limit={})",
                self.kind,image.encoded_len(),MAX_CHECKPOINT_BYTES);
            return Ok(());
        }
        peer.checkpoint(image.encode_to_vec())?;
        self.checkpoint = index;
        self.baseline = std::fs::metadata(&self.wal).map_err(storage)?.len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_envelope_binds_kind_position_and_canonical_bytes() {
        let image = Image {
            version: 1,
            kind: 1,
            index: 9,
            term: 3,
            state: vec![1, 2, 3],
            catalog: vec![4],
        };
        let bytes = image.encode_to_vec();
        Image::decode_at(&bytes, 1, LogId { index: 9, term: 3 }).unwrap();
        assert!(Image::decode_at(&bytes, 2, LogId { index: 9, term: 3 }).is_err());
        assert!(Image::decode_at(&bytes, 1, LogId { index: 10, term: 3 }).is_err());
        assert!(Image::decode_at(&bytes, 1, LogId { index: 9, term: 4 }).is_err());
        let mut unknown = bytes.clone();
        unknown.extend([0x78, 1]);
        assert!(Image::decode_at(&unknown, 1, LogId { index: 9, term: 3 }).is_err());
        for cut in 0..bytes.len() {
            if Image::decode_at(&bytes[..cut], 1, LogId { index: 9, term: 3 }).is_ok() {
                // A catalog is optional in the envelope; its service-specific
                // equality check is performed before installing metadata state.
                let parsed = Image::decode(&bytes[..cut]).unwrap();
                assert!(parsed.catalog.is_empty());
            }
        }
    }
    #[test]
    fn checkpoint_policy_bounds_the_configured_interval() {
        for value in [0, 1024, 128 * 1024 * 1024] {
            Policy::new(1, PathBuf::new(), value).unwrap();
        }
        for value in [1, 1023, 128 * 1024 * 1024 + 1, u64::MAX] {
            assert!(Policy::new(1, PathBuf::new(), value).is_err());
        }
    }
}
