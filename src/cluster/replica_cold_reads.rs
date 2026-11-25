// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Capture immutable cold ranges under serialization, then read outside it.
use super::*;
use crate::cluster::replica_page_backing::PageReadPlan;
use crate::{PageId, ReplicaReadResult};
fn corrupt(error: impl ToString) -> ObjectError {
    ObjectError::Corruption(error.to_string())
}
pub(super) enum ReadPart {
    Ready(ReplicaReadResult),
    Stored(PageReadPlan, String),
}
impl ReadPart {
    fn finish(self) -> Result<ReplicaReadResult, ObjectError> {
        match self {
            Self::Ready(read) => Ok(read),
            Self::Stored(plan, checksum) => Ok(ReplicaReadResult {
                data: plan.read().map_err(corrupt)?,
                write_sequence: plan.metadata().write_sequence,
                checksum,
            }),
        }
    }
}
pub(super) enum Captured {
    Ready(ReplicaReply),
    Read(ReadPart),
    Reads(Vec<ReadPart>),
    Digest(Vec<ReadPart>, bool),
    State(PageReadPlan, u64, String),
}
impl Captured {
    pub fn finish(self) -> Result<ReplicaReply, ObjectError> {
        Ok(match self {
            Self::Ready(reply) => reply,
            Self::Read(part) => ReplicaReply::Read(part.finish()?),
            Self::Reads(parts) => ReplicaReply::Reads(
                parts
                    .into_iter()
                    .map(ReadPart::finish)
                    .collect::<Result<_, _>>()?,
            ),
            Self::Digest(parts, batch) => {
                let mut result = Vec::with_capacity(parts.len());
                for part in parts {
                    let read = part.finish()?;
                    let mut sha256 = [0; 32];
                    sha256.copy_from_slice(
                        ring::digest::digest(&ring::digest::SHA256, &read.data).as_ref(),
                    );
                    result.push((read.write_sequence, read.data.len() as u64, sha256));
                }
                if batch {
                    ReplicaReply::Digests(result)
                } else {
                    let (write_sequence, length, sha256) = result.pop().unwrap();
                    ReplicaReply::Digest {
                        write_sequence,
                        length,
                        sha256,
                    }
                }
            }
            Self::State(plan, offset, state_checksum) => {
                ReplicaReply::StateRange(super::super::state_transfer::ChunkStateRange {
                    version: 1,
                    offset,
                    data_length: plan.metadata().data_length,
                    data: plan.read().map_err(corrupt)?,
                    state_checksum,
                })
            }
        })
    }
}
fn part(
    node: &mut NodeReplica,
    id: &PageId,
    offset: u64,
    length: u64,
) -> Result<ReadPart, ObjectError> {
    if node.service.page_write_sequence(id).is_some() {
        let read = node.service.read_chunk(id, offset, length)?;
        node.resident.as_mut().unwrap().touch(&node.service, id)?;
        return Ok(ReadPart::Ready(read));
    }
    let end = crate::validate_pipeline_range("chunk read", offset, length)?;
    let cache = node.resident.as_ref().unwrap();
    let metadata = cache
        .backing
        .metadata(id)
        .map_err(corrupt)?
        .ok_or_else(|| ObjectError::NotFound("chunk is missing".into()))?;
    if end > metadata.logical_length {
        return Err(ObjectError::InvalidArgument(
            "chunk read exceeds logical length".into(),
        ));
    }
    if metadata
        .discarded_ranges
        .iter()
        .take_while(|r| r.offset < end)
        .any(|r| {
            length != 0
                && r.length != 0
                && r.offset < end
                && r.offset.saturating_add(r.length) > offset
        })
    {
        return Err(corrupt("chunk read intersects discarded range"));
    }
    if end > metadata.data_length {
        return Err(corrupt("chunk payload is shorter than logical length"));
    }
    let plan = cache
        .backing
        .capture_range(id, offset, length)
        .map_err(corrupt)?
        .ok_or_else(|| corrupt("cold read lost its page"))?;
    let checksum = metadata.head(id).checksum;
    Ok(ReadPart::Stored(plan, checksum))
}
pub(super) fn capture(
    node: &mut NodeReplica,
    request: &ReplicaRequest,
    max_payload: usize,
) -> Result<Option<Captured>, ObjectError> {
    if !matches!(
        request,
        ReplicaRequest::ReadChunk { .. }
            | ReplicaRequest::ReadChunkBatch { .. }
            | ReplicaRequest::DigestChunk { .. }
            | ReplicaRequest::DigestChunkBatch { .. }
            | ReplicaRequest::ReadChunkStateRange { .. }
    ) {
        return Ok(None);
    }
    if node.health.failed() {
        return Err(corrupt(
            "replica journal failed; restart and recover before serving requests",
        ));
    }
    if let Some(journal) = &mut node.journal {
        if let Err(error) = journal.check_health() {
            node.health.fail();
            return Err(corrupt(error));
        }
    }
    check_page_api_request(node, request)?;
    use crate::cluster::message::check_reply_size;
    Ok(Some(match request {
        ReplicaRequest::ReadChunk {
            page_id,
            offset,
            length,
        } => {
            check_reply_size(
                usize::try_from(*length)
                    .unwrap_or(usize::MAX)
                    .saturating_add(40),
                max_payload,
            )?;
            Captured::Read(part(node, page_id, *offset, *length)?)
        }
        ReplicaRequest::ReadChunkBatch { reads } => {
            let size = reads
                .iter()
                .try_fold(0usize, |sum, (_, _, len)| {
                    usize::try_from(*len)
                        .ok()
                        .and_then(|n| sum.checked_add(n.saturating_add(64)))
                })
                .unwrap_or(usize::MAX);
            check_reply_size(size, max_payload)?;
            Captured::Reads(
                reads
                    .iter()
                    .map(|(id, off, len)| part(node, id, *off, *len))
                    .collect::<Result<_, _>>()?,
            )
        }
        ReplicaRequest::DigestChunk {
            page_id,
            offset,
            length,
        } => {
            if *length > 1024 * 1024 {
                return Err(ObjectError::InvalidArgument(
                    "page digest range exceeds one MiB".into(),
                ));
            }
            Captured::Digest(vec![part(node, page_id, *offset, *length)?], false)
        }
        ReplicaRequest::DigestChunkBatch { reads } => {
            if reads.len() > 8 || reads.iter().any(|(_, _, len)| *len > 1024 * 1024) {
                return Err(ObjectError::InvalidArgument(
                    "digest batch exceeds range bounds".into(),
                ));
            }
            Captured::Digest(
                reads
                    .iter()
                    .map(|(id, off, len)| part(node, id, *off, *len))
                    .collect::<Result<_, _>>()?,
                true,
            )
        }
        ReplicaRequest::ReadChunkStateRange {
            page_id,
            expected_state_checksum,
            offset,
            length,
        } => {
            let transfer = &node.service;
            super::super::state_transfer::validate_request(
                expected_state_checksum,
                *offset,
                *length,
            )?;
            check_reply_size(
                usize::try_from(*length)
                    .unwrap_or(usize::MAX)
                    .saturating_add(super::super::state_transfer::RANGE_OVERHEAD),
                max_payload,
            )?;
            if transfer.page_write_sequence(page_id).is_some() {
                Captured::Ready(ReplicaReply::StateRange(
                    super::super::state_transfer::read_range(
                        transfer,
                        page_id,
                        expected_state_checksum,
                        *offset,
                        *length,
                    )?,
                ))
            } else {
                let backing = &node.resident.as_ref().unwrap().backing;
                let meta = backing
                    .metadata(page_id)
                    .map_err(corrupt)?
                    .ok_or_else(|| ObjectError::NotFound("chunk is missing".into()))?;
                if meta.head(page_id).state_checksum != *expected_state_checksum {
                    return Err(ObjectError::PreconditionFailed(
                        "page state changed during transfer".into(),
                    ));
                }
                if *offset > meta.data_length {
                    return Err(ObjectError::InvalidArgument(
                        "page state range exceeds payload".into(),
                    ));
                }
                let len = (*length).min(meta.data_length - *offset);
                let plan = backing
                    .capture_range(page_id, *offset, len)
                    .map_err(corrupt)?
                    .ok_or_else(|| corrupt("cold transfer lost its page"))?;
                Captured::State(plan, *offset, expected_state_checksum.clone())
            }
        }
        _ => unreachable!(),
    }))
}
