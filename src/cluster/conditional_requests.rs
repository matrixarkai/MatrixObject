// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Canonical protobuf requests for conditional node mutations.
use crate::ObjectError;
use prost::Message;

pub(super) fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, ObjectError> {
    let value = T::decode(bytes).map_err(|e| ObjectError::Corruption(e.to_string()))?;
    if value.encode_to_vec() != bytes {
        return Err(ObjectError::Corruption(
            "noncanonical conditional request".into(),
        ));
    }
    Ok(value)
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkWrite {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub volume: u64,
    #[prost(uint64, tag = "3")]
    pub chunk: u64,
    #[prost(uint64, tag = "4")]
    pub page: u64,
    #[prost(uint64, tag = "5")]
    pub offset: u64,
    #[prost(uint64, tag = "6")]
    pub expected_sequence: u64,
    #[prost(bytes = "vec", tag = "7")]
    pub data: Vec<u8>,
}
#[cfg(feature = "meta-server")]
#[derive(Clone, PartialEq, Message)]
pub(super) struct Placement {
    #[prost(string, tag = "1")]
    pub bucket: String,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(uint64, tag = "3")]
    pub version: u64,
    #[prost(uint64, tag = "4")]
    pub length: u64,
    #[prost(string, repeated, tag = "5")]
    pub nodes: Vec<String>,
    #[prost(uint64, tag = "6")]
    pub updated: u64,
}
#[cfg(feature = "meta-server")]
impl From<&super::meta_message::CommittedPlacement> for Placement {
    fn from(p: &super::meta_message::CommittedPlacement) -> Self {
        Self {
            bucket: p.bucket.clone(),
            key: p.key.clone(),
            version: p.version,
            length: p.length,
            nodes: p.node_ids.clone(),
            updated: p.update_time_ms,
        }
    }
}
#[cfg(feature = "meta-server")]
impl From<Placement> for super::meta_message::CommittedPlacement {
    fn from(p: Placement) -> Self {
        Self {
            bucket: p.bucket,
            key: p.key,
            version: p.version,
            length: p.length,
            node_ids: p.nodes,
            update_time_ms: p.updated,
        }
    }
}
#[cfg(feature = "meta-server")]
#[derive(Clone, PartialEq, Message)]
pub(super) struct PlacementCommit {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, optional, tag = "2")]
    pub placement: Option<Placement>,
    #[prost(message, optional, tag = "3")]
    pub expected: Option<Placement>,
    #[prost(string, tag = "4")]
    pub plan_checksum: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ReplicaRequest, RequestCode};
    use crate::{ChunkId, PageId};
    #[test]
    fn conditional_node_requests_are_canonical_binary_protobuf() {
        let request = ReplicaRequest::WriteChunkIfSequence {
            page_id: PageId {
                chunk_id: ChunkId::new(1, 2).unwrap(),
                page_index: 3,
            },
            offset: 4,
            expected_sequence: 5,
            data: vec![6, 7, 8],
        };
        let bytes = request.encode_payload();
        assert_eq!(
            ReplicaRequest::decode(RequestCode::WriteChunkIfSequence, &bytes).unwrap(),
            request
        );
        let mut extra = bytes.clone();
        extra.extend([0x78, 1]);
        assert!(ReplicaRequest::decode(RequestCode::WriteChunkIfSequence, &extra).is_err());
        let mut duplicate = bytes.clone();
        duplicate.extend([0x08, 1]);
        assert!(ReplicaRequest::decode(RequestCode::WriteChunkIfSequence, &duplicate).is_err());
        let mut wrong: ChunkWrite = decode(&bytes).unwrap();
        wrong.version = 2;
        assert!(
            ReplicaRequest::decode(RequestCode::WriteChunkIfSequence, &wrong.encode_to_vec())
                .is_err()
        );
        for end in 0..bytes.len() {
            if let Ok(decoded) =
                ReplicaRequest::decode(RequestCode::WriteChunkIfSequence, &bytes[..end])
            {
                assert_ne!(decoded, request);
            }
        }
        #[cfg(feature = "meta-server")]
        {
            use crate::cluster::{CommittedPlacement, MetaRequest, MetaRequestCode};
            let placement = CommittedPlacement {
                bucket: "bucket".into(),
                key: "key".into(),
                version: 2,
                length: 4,
                node_ids: vec!["node".into()],
                update_time_ms: 9,
            };
            let mut previous = placement.clone();
            previous.version = 1;
            for expected in [None, Some(previous)] {
                let request = MetaRequest::CommitObjectPlacementIfCurrent {
                    placement: placement.clone(),
                    expected,
                    expected_plan_checksum: "checksum".into(),
                };
                let bytes = request.encode_payload();
                assert_eq!(
                    MetaRequest::decode(MetaRequestCode::CommitObjectPlacementIfCurrent, &bytes)
                        .unwrap(),
                    request
                );
                let mut extra = bytes.clone();
                extra.extend([0x78, 1]);
                assert!(MetaRequest::decode(
                    MetaRequestCode::CommitObjectPlacementIfCurrent,
                    &extra
                )
                .is_err());
                let mut wrong: PlacementCommit = decode(&bytes).unwrap();
                wrong.version = 2;
                assert!(MetaRequest::decode(
                    MetaRequestCode::CommitObjectPlacementIfCurrent,
                    &wrong.encode_to_vec()
                )
                .is_err());
                wrong.version = 1;
                wrong.placement = None;
                assert!(MetaRequest::decode(
                    MetaRequestCode::CommitObjectPlacementIfCurrent,
                    &wrong.encode_to_vec()
                )
                .is_err());
            }
        }
    }
}
