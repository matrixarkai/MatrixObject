// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded raw page ranges fenced by the complete advertised page state.
use crate::ObjectError;
use prost::Message;

pub const MAX_STATE_RANGE_BYTES: usize = 1024 * 1024;
pub const MAX_STATE_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const RANGE_OVERHEAD: usize = 256;

#[derive(Clone, PartialEq, Message)]
pub(crate) struct StateRangeRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub volume: u64,
    #[prost(uint64, tag = "3")]
    pub chunk: u64,
    #[prost(uint64, tag = "4")]
    pub page: u64,
    #[prost(string, tag = "5")]
    pub state_checksum: String,
    #[prost(uint64, tag = "6")]
    pub offset: u64,
    #[prost(uint64, tag = "7")]
    pub length: u64,
}

/// Raw physical page bytes, including bytes covered by logical discarded ranges.
/// The caller must retain the matching head and its discard metadata.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct ChunkStateRange {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub offset: u64,
    #[prost(uint64, tag = "3")]
    pub data_length: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub data: Vec<u8>,
    #[prost(string, tag = "5")]
    pub state_checksum: String,
}

pub(crate) fn validate_request(
    checksum: &str,
    offset: u64,
    length: u64,
) -> Result<(), ObjectError> {
    if checksum.is_empty()
        || checksum.len() > 128
        || offset > MAX_STATE_BYTES
        || !(1..=MAX_STATE_RANGE_BYTES as u64).contains(&length)
    {
        return Err(ObjectError::InvalidArgument(
            "invalid page state range bounds".into(),
        ));
    }
    Ok(())
}

impl ChunkStateRange {
    pub(crate) fn decode_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_STATE_RANGE_BYTES + RANGE_OVERHEAD {
            return Err(ObjectError::Corruption(
                "page state range reply exceeds bound".into(),
            ));
        }
        let value: Self = super::conditional_requests::decode(bytes)?;
        if value.version != 1
            || value.data_length > MAX_STATE_BYTES
            || value.offset > value.data_length
            || value.data.len() > MAX_STATE_RANGE_BYTES
            || value.data.len() as u64 > value.data_length - value.offset
            || value.state_checksum.is_empty()
            || value.state_checksum.len() > 128
        {
            return Err(ObjectError::Corruption(
                "invalid page state range reply".into(),
            ));
        }
        Ok(value)
    }
}

#[cfg(feature = "replica-server")]
pub(super) fn read_range(
    service: &crate::MatrixReplicaService,
    page: &crate::PageId,
    expected: &str,
    offset: u64,
    length: u64,
) -> Result<ChunkStateRange, ObjectError> {
    validate_request(expected, offset, length)?;
    let state = service.read_chunk_state(page)?;
    let checksum = crate::chunk_state_handoff_checksum(page, &state, &state.checksum());
    if checksum != expected {
        return Err(ObjectError::PreconditionFailed(
            "page state changed during transfer".into(),
        ));
    }
    let total = state.data.len() as u64;
    if total > MAX_STATE_BYTES || offset > total {
        return Err(ObjectError::InvalidArgument(
            "page state range exceeds payload".into(),
        ));
    }
    let end = total.min(offset + length);
    Ok(ChunkStateRange {
        version: 1,
        offset,
        data_length: total,
        data: state.data[offset as usize..end as usize].to_vec(),
        state_checksum: checksum,
    })
}

#[cfg(all(test, feature = "replica-server"))]
mod tests {
    use super::*;
    use crate::cluster::{ReplicaReply, ReplicaRequest, RequestCode};
    use crate::{ChunkId, MatrixReplicaService, PageId};

    #[test]
    fn page_state_ranges_are_canonical_bounded_and_refuse_changed_state() {
        let page = PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 3,
        };
        let mut service = MatrixReplicaService::new();
        service.write_chunk(page.clone(), 0, b"abcdefgh").unwrap();
        service.discard_chunk(&page, 2, 2).unwrap();
        let head = service.head_chunk(&page).unwrap();
        let request = ReplicaRequest::ReadChunkStateRange {
            page_id: page.clone(),
            expected_state_checksum: head.state_checksum.clone(),
            offset: 0,
            length: 8,
        };
        let payload = request.encode_payload();
        assert_eq!(
            ReplicaRequest::decode(RequestCode::ReadChunkStateRange, &payload).unwrap(),
            request
        );
        let mut unknown = payload.clone();
        unknown.extend_from_slice(&[64, 1]);
        assert!(ReplicaRequest::decode(request.code(), &unknown).is_err());
        let mut duplicate = payload.clone();
        duplicate.extend_from_slice(&[8, 1]);
        assert!(ReplicaRequest::decode(request.code(), &duplicate).is_err());
        assert!(ReplicaRequest::decode(request.code(), &vec![0; 513]).is_err());
        assert!(validate_request("", 0, 1).is_err());
        assert!(validate_request("x", MAX_STATE_BYTES + 1, 1).is_err());
        assert!(validate_request("x", 0, 0).is_err());
        assert!(validate_request("x", 0, MAX_STATE_RANGE_BYTES as u64 + 1).is_err());
        let value = read_range(&service, &page, &head.state_checksum, 0, 8).unwrap();
        assert_eq!(
            value.data,
            service.read_chunk_state(&page).unwrap().data.as_slice()
        );
        assert!(service.read_chunk(&page, 2, 2).is_err());
        let reply = ReplicaReply::StateRange(value.clone());
        assert_eq!(
            ReplicaReply::decode(request.code(), &reply.encode_payload()).unwrap(),
            reply
        );
        assert!(read_range(&service, &page, &head.state_checksum, 9, 1).is_err());
        assert!(read_range(&service, &page, &head.state_checksum, 8, 1)
            .unwrap()
            .data
            .is_empty());
        for field in 0..5 {
            let mut bad = value.clone();
            match field {
                0 => bad.version = 2,
                1 => bad.offset = 9,
                2 => bad.data_length = MAX_STATE_BYTES + 1,
                3 => bad.state_checksum.clear(),
                _ => bad.data_length = 7,
            }
            assert!(ChunkStateRange::decode_payload(&bad.encode_to_vec()).is_err());
        }
        let mut duplicate = value.encode_to_vec();
        duplicate.extend_from_slice(&[8, 1]);
        assert!(ChunkStateRange::decode_payload(&duplicate).is_err());
        service.write_chunk(page.clone(), 7, b"X").unwrap();
        assert!(matches!(
            read_range(&service, &page, &head.state_checksum, 0, 8),
            Err(ObjectError::PreconditionFailed(_))
        ));
    }
}
