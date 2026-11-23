// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded pending attempts and exact durable record confirmation for native writers.
use super::*;
use crate::ObjectError;

/// Stable context supplied by the object writer. Epoch is deliberately excluded: a new
/// lease holder must be able to recognize an earlier request after fencing its predecessor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendIdentity {
    pub object_incarnation: [u8; 16],
    pub attempt_id: [u8; 16],
    pub request_offset: u64,
    pub request_length: u64,
}
const TAG_DOMAIN: &[u8] = b"MOSRS-LOG-ATTEMPT1";
impl AppendIdentity {
    fn check_range(&self, logical_offset: u64, length: u64) -> Result<(), ObjectError> {
        let end = self.request_offset.checked_add(self.request_length);
        if self.request_length == 0
            || length == 0
            || logical_offset < self.request_offset
            || logical_offset
                .checked_add(length)
                .zip(end)
                .is_none_or(|(last, end)| last > end)
        {
            return Err(ObjectError::InvalidArgument(
                "identified fragment is outside its original request range".into(),
            ));
        }
        Ok(())
    }
    /// SHA-256 truncated to sixteen bytes over the fixed domain, incarnation, client ID,
    /// original range, fragment logical offset, volume, chunk, generation, physical offset
    /// and record length. Integer fields are little endian. This is part of durable identity.
    pub fn record_tag(
        &self,
        volume: u64,
        chunk: u64,
        generation: u32,
        physical_offset: u64,
        logical_offset: u64,
        length: u32,
    ) -> Result<[u8; 16], ObjectError> {
        self.check_range(logical_offset, u64::from(length))?;
        physical_offset
            .checked_add(u64::from(length))
            .ok_or_else(|| {
                ObjectError::InvalidArgument("identified physical range overflow".into())
            })?;
        let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
        hash.update(TAG_DOMAIN);
        hash.update(&self.object_incarnation);
        hash.update(&self.attempt_id);
        for value in [
            self.request_offset,
            self.request_length,
            logical_offset,
            volume,
            chunk,
        ] {
            hash.update(&value.to_le_bytes());
        }
        hash.update(&generation.to_le_bytes());
        hash.update(&physical_offset.to_le_bytes());
        hash.update(&length.to_le_bytes());
        Ok(hash.finish().as_ref()[..16].try_into().unwrap())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PendingAttempt {
    expected_offset: u64,
    logical_offset: u64,
    identity: AppendIdentity,
}
pub(super) struct PendingRecord {
    pub offset: u64,
    pub length: u32,
    pub tag: [u8; 16],
}
impl PendingRecord {
    fn end(&self) -> u64 {
        self.offset + u64::from(self.length)
    }
}
impl ChunkLogWriter {
    /// Open a writer requiring identified records on all three endpoints. Read-only version 2
    /// probes run before opening or fencing chunks; an unsupported endpoint is never downgraded.
    pub fn open_identified(
        endpoints: &[String],
        volume: u64,
        chunk: u64,
        generation: u32,
        epoch: u64,
        options: ReplicaClientOptions,
    ) -> Result<Self, ChunkLogError> {
        if endpoints.len() != LOG_REPLICAS
            || epoch == 0
            || endpoints
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != LOG_REPLICAS
        {
            return Err(ChunkLogError::Refused(ObjectError::InvalidArgument(
                "identified writes require three distinct replicas and a nonzero epoch".into(),
            )));
        }
        for endpoint in endpoints {
            let mut client = RemoteReplicaClient::connect(endpoint, &options).map_err(|_| {
                ChunkLogError::NoQuorum {
                    wanted: 1,
                    durable: 0,
                }
            })?;
            client
                .log_identity(volume, chunk, generation, 0)
                .map_err(ChunkLogError::from)?;
        }
        let mut writer = Self::open(endpoints, volume, chunk, generation, epoch, options)?;
        writer.identified = true;
        for member in &mut writer.members {
            member.confirmed = member.durable.min(writer.next);
        }
        Ok(writer)
    }

    /// Admit one identified piece of an original request and wait for all three replicas.
    /// A retry of an unsettled piece must retain its complete context and bytes. A different
    /// piece is refused until `settle` finishes the pending one, bounding retained descriptors
    /// by the existing chunk and record limits. Any error after admission leaves the outcome
    /// unconfirmed; a length alone must not be interpreted as success by the caller.
    pub fn append_identified(
        &mut self,
        expected_offset: u64,
        logical_offset: u64,
        data: &[u8],
        identity: AppendIdentity,
    ) -> Result<u64, ChunkLogError> {
        if let Some(error) = &self.fatal {
            return Err(error.clone());
        }
        if !self.identified {
            return Err(ChunkLogError::Refused(ObjectError::InvalidArgument(
                "open an identified writer before supplying append identities".into(),
            )));
        }
        identity
            .check_range(logical_offset, data.len() as u64)
            .map_err(ChunkLogError::Refused)?;
        let attempt = PendingAttempt {
            expected_offset,
            logical_offset,
            identity,
        };
        if let Some(pending) = self.pending_attempt {
            if pending == attempt && self.window.as_slice() == data {
                return self.drive();
            }
            return Err(ChunkLogError::Refused(ObjectError::PreconditionFailed(
                "settle the pending identified append before admitting another request".into(),
            )));
        }
        if expected_offset != self.next {
            return Err(ChunkLogError::Refused(ObjectError::PreconditionFailed(
                "identified append offset differs from the current chunk end".into(),
            )));
        }
        let end = self
            .next
            .checked_add(data.len() as u64)
            .filter(|end| *end <= LOG_CHUNK_LIMIT)
            .ok_or_else(|| {
                ChunkLogError::Refused(ObjectError::InvalidArgument(
                    "the log chunk is full: switch to the next chunk".into(),
                ))
            })?;
        // Build descriptors before mutating the window; range failures cannot admit a partial attempt.
        let mut records = std::mem::take(&mut self.pending_records);
        let mut offset = expected_offset;
        for piece in data.chunks(LOG_RECORD_BYTES.min(MAX_OP_DATA)) {
            let logical = logical_offset + (offset - expected_offset);
            let tag = identity
                .record_tag(
                    self.volume,
                    self.chunk,
                    self.generation,
                    offset,
                    logical,
                    piece.len() as u32,
                )
                .map_err(ChunkLogError::Refused)?;
            records.push_back(PendingRecord {
                offset,
                length: piece.len() as u32,
                tag,
            });
            offset += piece.len() as u64;
        }
        self.pending_records = records;
        self.pending_attempt = Some(attempt);
        self.window.extend_from_slice(data);
        self.next = end;
        self.drive()
    }

    pub(super) fn confirm_pending(&mut self, index: usize) -> Result<(), ChunkLogError> {
        let member = &mut self.members[index];
        if member.busy || member.given_up || member.client.is_none() || !member.known {
            return Ok(());
        }
        if member.durable >= self.window_start {
            member.confirmed = member.confirmed.max(self.window_start);
        }
        for record in &self.pending_records {
            let end = record.end();
            if record.offset < member.applied && member.applied < end {
                let error = ChunkLogError::AttemptMismatch {
                    offset: record.offset,
                };
                self.fatal = Some(error.clone());
                return Err(error);
            }
            if end <= member.confirmed {
                continue;
            }
            if end > member.durable {
                break;
            }
            if member.confirmed != record.offset {
                let error = ChunkLogError::AttemptMismatch {
                    offset: record.offset,
                };
                self.fatal = Some(error.clone());
                return Err(error);
            }
            let reply = match member.client.as_mut().unwrap().log_identity(
                self.volume,
                self.chunk,
                self.generation,
                record.offset,
            ) {
                Ok(reply) => reply,
                Err(LogCallError::Transport(_)) => {
                    member.client = None;
                    member.known = false;
                    member.applied = 0;
                    member.durable = 0;
                    member.confirmed = 0;
                    return Ok(());
                }
                Err(error) => {
                    let error = ChunkLogError::from(error);
                    self.fatal = Some(error.clone());
                    return Err(error);
                }
            };
            if reply.epoch > self.epoch {
                let error = ChunkLogError::StaleEpoch {
                    current: reply.epoch,
                };
                self.fatal = Some(error.clone());
                return Err(error);
            }
            let expected = crate::log_chunk::AppendRecordIdentity {
                offset: record.offset,
                length: record.length,
                attempt_tag: Some(record.tag),
            };
            if reply.epoch != self.epoch || reply.record_identity() != Some(expected) {
                let error = ChunkLogError::AttemptMismatch {
                    offset: record.offset,
                };
                self.fatal = Some(error.clone());
                return Err(error);
            }
            // The nonce identifies an attempt, but a caller may incorrectly reuse it with
            // different bytes. After an uncertain reply, enforce the same payload equality
            // that the storage append checks for an exact retry. Equality never replaces the tag.
            let read = match member.client.as_mut().unwrap().log_read(
                self.volume,
                self.chunk,
                self.generation,
                record.offset,
                record.length,
            ) {
                Ok(reply) => reply,
                Err(LogCallError::Transport(_)) => {
                    member.client = None;
                    member.known = false;
                    member.applied = 0;
                    member.durable = 0;
                    member.confirmed = 0;
                    return Ok(());
                }
                Err(error) => {
                    let error = ChunkLogError::from(error);
                    self.fatal = Some(error.clone());
                    return Err(error);
                }
            };
            if read.epoch > self.epoch {
                let error = ChunkLogError::StaleEpoch {
                    current: read.epoch,
                };
                self.fatal = Some(error.clone());
                return Err(error);
            }
            let begin = (record.offset - self.window_start) as usize;
            if read.epoch != self.epoch
                || read.durable_length < end
                || read.data != self.window[begin..begin + record.length as usize]
            {
                let error = ChunkLogError::AttemptMismatch {
                    offset: record.offset,
                };
                self.fatal = Some(error.clone());
                return Err(error);
            }
            member.confirmed = end;
        }
        Ok(())
    }
}
