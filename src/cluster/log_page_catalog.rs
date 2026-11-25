// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Node-local log authority and receipts reconstructed from decided page batches.
use crate::log_page_outcome::{
    LogPageAppendIdentity, LogPageOutcome, LogPageState, LOG_PAGE_APPEND, LOG_PAGE_FENCE,
    LOG_PAGE_INITIALIZE,
};
use crate::ChunkId;
use prost::Message;
use std::io;
fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
#[derive(Clone, PartialEq, Eq, Message)]
pub(super) struct LogPageReceipt {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    #[prost(message, optional, tag = "2")]
    pub append: Option<LogPageAppendIdentity>,
    #[prost(uint64, tag = "3")]
    pub published_length: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LogPageEntry {
    pub state: LogPageState,
    pub receipts: imbl::OrdMap<u64, LogPageReceipt>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct LogPageCatalog {
    pub chunks: imbl::OrdMap<ChunkId, LogPageEntry>,
}
impl LogPageEntry {
    pub fn validate(&self) -> io::Result<()> {
        self.state.validate().map_err(invalid)?;
        let mut next = 0;
        let mut epoch = 0;
        let mut published_length = 0;
        for (offset, receipt) in &self.receipts {
            let append = receipt
                .append
                .as_ref()
                .ok_or_else(|| invalid("log receipt omits identity"))?;
            if *offset != next
                || append.offset != *offset
                || receipt.epoch == 0
                || receipt.epoch > self.state.writer_epoch
                || receipt.epoch < epoch
                || append.attempt.len() != 16
                || append.length == 0
                || append.length as usize > crate::log_chunk::MAX_RECORD_DATA
            {
                return Err(invalid("invalid log checkpoint receipt"));
            }
            next = next
                .checked_add(u64::from(append.length))
                .ok_or_else(|| invalid("log receipt length overflow"))?;
            if ![published_length, next].contains(&receipt.published_length) {
                return Err(invalid("log receipt has an invalid publication boundary"));
            }
            epoch = receipt.epoch;
            published_length = receipt.published_length;
        }
        if next != self.state.length || published_length != self.state.published_length {
            return Err(invalid("log receipts do not cover the committed state"));
        }
        Ok(())
    }
}
impl LogPageCatalog {
    /// Mutates only one persistent map path and the affected chunk's receipt path.
    /// The caller fences/discards its working catalog on any later batch error.
    pub fn fold(&mut self, outcome: &LogPageOutcome) -> io::Result<()> {
        let id = ChunkId::new(outcome.volume_id, outcome.chunk_id).map_err(invalid)?;
        let before = self.chunks.get(&id);
        outcome
            .check_before(before.map(|entry| &entry.state))
            .map_err(invalid)?;
        let mut entry = if outcome.kind == LOG_PAGE_INITIALIZE {
            LogPageEntry {
                state: outcome.after.as_ref().unwrap().clone(),
                receipts: imbl::OrdMap::new(),
            }
        } else {
            before.unwrap().clone()
        };
        if outcome.kind == LOG_PAGE_APPEND {
            let append = outcome.append.as_ref().unwrap();
            if entry.receipts.contains_key(&append.offset) {
                return Err(invalid("log append repeats an existing offset"));
            }
            entry.receipts.insert(
                append.offset,
                LogPageReceipt {
                    epoch: outcome.after.as_ref().unwrap().writer_epoch,
                    append: Some(append.clone()),
                    published_length: outcome.after.as_ref().unwrap().published_length,
                },
            );
        } else if outcome.kind == LOG_PAGE_FENCE {
            let length = outcome.after.as_ref().unwrap().length;
            let (prefix, _) = entry.receipts.split(&length);
            if prefix.iter().next_back().is_some_and(|(_, receipt)| {
                let append = receipt.append.as_ref().unwrap();
                append.offset + u64::from(append.length) != length
            }) || (length != 0 && prefix.is_empty())
            {
                return Err(invalid("log fence cuts through an append identity"));
            }
            entry.receipts = prefix;
        }
        entry.state = outcome.after.as_ref().unwrap().clone();
        self.chunks.insert(id, entry);
        Ok(())
    }
}

#[cfg(all(test, unix))]
#[path = "log_page_catalog_tests.rs"]
mod tests;
