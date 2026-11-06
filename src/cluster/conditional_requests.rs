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
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkWriteBatch {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, repeated, tag = "2")]
    pub writes: Vec<ChunkWrite>,
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkDigest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub write_sequence: u64,
    #[prost(uint64, tag = "3")]
    pub length: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub sha256: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkDigestBatch {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, repeated, tag = "2")]
    pub digests: Vec<ChunkDigest>,
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkRead {
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
    pub length: u64,
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkReadBatch {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, repeated, tag = "2")]
    pub reads: Vec<ChunkRead>,
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkReadResult {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub write_sequence: u64,
    #[prost(string, tag = "3")]
    pub checksum: String,
    #[prost(bytes = "vec", tag = "4")]
    pub data: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkReadBatchResult {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, repeated, tag = "2")]
    pub reads: Vec<ChunkReadResult>,
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkDiscard {
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
    #[prost(uint64, tag = "7")]
    pub length: u64,
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
    use crate::cluster::{ReplicaReply, ReplicaRequest, RequestCode};
    use crate::{ChunkId, PageId};
    #[test]
    fn conditional_write_batches_require_canonical_bounded_protobuf() {
        let write = crate::cluster::message::ConditionalChunkWrite {
            page_id: PageId { chunk_id: ChunkId::new(1, 2).unwrap(), page_index: 3 },
            offset: 0,
            data: b"abc".to_vec(),
            expected_sequence: 0,
        };
        let request = ReplicaRequest::WriteChunkBatchIfSequence {
            writes: vec![write.clone(); 2],
        };
        let bytes = request.encode_payload();
        assert_eq!(RequestCode::from_u8(14), Some(RequestCode::WriteChunkBatchIfSequence));
        assert_eq!(ReplicaRequest::decode(request.code(), &bytes).unwrap(), request);
        let mut maximum: ChunkWriteBatch = decode(&bytes).unwrap();
        maximum.writes.resize(8, maximum.writes[0].clone());
        for write in &mut maximum.writes {
            write.data.resize(512 * 1024, 7);
        }
        let maximum_bytes = maximum.encode_to_vec();
        assert!(maximum_bytes.len() <= 4 * 1024 * 1024 + 512);
        assert!(ReplicaRequest::decode(request.code(), &maximum_bytes).is_ok());
        maximum.writes[0].data.push(7);
        assert!(ReplicaRequest::decode(request.code(), &maximum.encode_to_vec()).is_err());
        let digest_request = ReplicaRequest::WriteChunkBatchIfSequenceDigest {
            writes: vec![write; 2],
        };
        assert_eq!(
            RequestCode::from_u8(16),
            Some(RequestCode::WriteChunkBatchIfSequenceDigest)
        );
        assert_eq!(
            ReplicaRequest::decode(digest_request.code(), &digest_request.encode_payload())
                .unwrap(),
            digest_request
        );
        let mut value: ChunkWriteBatch = decode(&bytes).unwrap();
        value.version = 2;
        assert!(ReplicaRequest::decode(request.code(), &value.encode_to_vec()).is_err());
        value.version = 1;
        value.writes[0].version = 2;
        assert!(ReplicaRequest::decode(request.code(), &value.encode_to_vec()).is_err());
        value.writes[0].version = 1;
        value.writes.resize(9, value.writes[0].clone());
        assert!(ReplicaRequest::decode(request.code(), &value.encode_to_vec()).is_err());
        let mut extra = bytes.clone();
        extra.extend([0x08, 0x01]);
        assert!(ReplicaRequest::decode(request.code(), &extra).is_err());
        let mut truncated = bytes;
        truncated.pop();
        assert!(ReplicaRequest::decode(request.code(), &truncated).is_err());
    }

    #[test]
    fn conditional_write_digest_batches_require_canonical_bounded_protobuf() {
        let digest = [7; 32];
        let reply = ReplicaReply::Digests(vec![(1, 3, digest), (2, 4, digest)]);
        let bytes = reply.encode_payload();
        assert_eq!(
            ReplicaReply::decode(RequestCode::WriteChunkBatchIfSequenceDigest, &bytes).unwrap(),
            reply
        );
        let mut value: ChunkDigestBatch = decode(&bytes).unwrap();
        value.version = 2;
        assert!(
            ReplicaReply::decode(
                RequestCode::WriteChunkBatchIfSequenceDigest,
                &value.encode_to_vec()
            )
            .is_err()
        );
        value.version = 1;
        value.digests[0].version = 2;
        assert!(
            ReplicaReply::decode(
                RequestCode::WriteChunkBatchIfSequenceDigest,
                &value.encode_to_vec()
            )
            .is_err()
        );
        value.digests[0].version = 1;
        value.digests[0].sha256.pop();
        assert!(
            ReplicaReply::decode(
                RequestCode::WriteChunkBatchIfSequenceDigest,
                &value.encode_to_vec()
            )
            .is_err()
        );
        value.digests[0].sha256.push(7);
        value.digests.resize(9, value.digests[0].clone());
        assert!(
            ReplicaReply::decode(
                RequestCode::WriteChunkBatchIfSequenceDigest,
                &value.encode_to_vec()
            )
            .is_err()
        );
        let mut extra = bytes.clone();
        extra.extend([0x18, 1]);
        assert!(
            ReplicaReply::decode(RequestCode::WriteChunkBatchIfSequenceDigest, &extra).is_err()
        );
        let mut truncated = bytes;
        truncated.pop();
        assert!(
            ReplicaReply::decode(RequestCode::WriteChunkBatchIfSequenceDigest, &truncated)
                .is_err()
        );
    }
    #[test]
    fn read_batches_use_canonical_bounded_protobuf() {
        let page = PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 3,
        };
        let request = ReplicaRequest::ReadChunkBatch {
            reads: vec![(page.clone(), 4, 5), (page, 9, 6)],
        };
        let bytes = request.encode_payload();
        assert_eq!(RequestCode::from_u8(17), Some(RequestCode::ReadChunkBatch));
        assert_eq!(ReplicaRequest::decode(request.code(), &bytes).unwrap(), request);

        let reply = ReplicaReply::Reads(vec![
            crate::ReplicaReadResult { data: vec![1; 5], write_sequence: 7, checksum: "a".into() },
            crate::ReplicaReadResult { data: vec![2; 6], write_sequence: 8, checksum: "b".into() },
        ]);
        let encoded = reply.encode_payload();
        assert_eq!(reply.payload_len(), encoded.len());
        assert_eq!(ReplicaReply::decode(RequestCode::ReadChunkBatch, &encoded).unwrap(), reply);

        let mut invalid: ChunkReadBatch = decode(&bytes).unwrap();
        invalid.reads[0].length = 64 * 1024 + 1;
        assert!(ReplicaRequest::decode(request.code(), &invalid.encode_to_vec()).is_err());
        invalid.reads[0].length = 5;
        invalid.reads.resize(9, invalid.reads[0].clone());
        assert!(ReplicaRequest::decode(request.code(), &invalid.encode_to_vec()).is_err());
        let mut extra = encoded;
        extra.extend([0x18, 1]);
        assert!(ReplicaReply::decode(RequestCode::ReadChunkBatch, &extra).is_err());
    }

    #[test]
    fn conditional_discard_requests_require_canonical_bounded_protobuf() {
        let request = ReplicaRequest::DiscardChunkIfSequence {
            page_id: PageId {
                chunk_id: ChunkId::new(1, 2).unwrap(),
                page_index: 3,
            },
            offset: 4,
            length: 6,
            expected_sequence: 5,
        };
        let bytes = request.encode_payload();
        assert_eq!(
            RequestCode::from_u8(9),
            Some(RequestCode::DiscardChunkIfSequence)
        );
        assert_eq!(
            ReplicaRequest::decode(request.code(), &bytes).unwrap(),
            request
        );
        for suffix in [[0x78, 1], [0x08, 1]] {
            let mut invalid = bytes.clone();
            invalid.extend(suffix);
            assert!(ReplicaRequest::decode(request.code(), &invalid).is_err());
        }
        for (version, sequence) in [(0, 5), (2, 5), (1, 0)] {
            let mut value: ChunkDiscard = decode(&bytes).unwrap();
            value.version = version;
            value.expected_sequence = sequence;
            assert!(ReplicaRequest::decode(request.code(), &value.encode_to_vec()).is_err());
        }
        assert!(ReplicaRequest::decode(request.code(), &[0; 97]).is_err());
        let mut truncated = bytes.clone();
        truncated.pop();
        assert!(ReplicaRequest::decode(request.code(), &truncated).is_err());
    }

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

#[cfg(feature = "meta-server")]
#[derive(Clone, PartialEq, Message)]
pub(super) struct ManifestRead {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub key: String,
}
#[cfg(feature = "meta-server")]
#[derive(Clone, PartialEq, Message)]
pub(super) struct ManifestReads {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, repeated, tag = "2")]
    pub reads: Vec<ManifestRead>,
}
#[cfg(feature = "meta-server")]
#[derive(Clone, PartialEq, Message)]
pub(super) struct ManifestPublish {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, optional, tag = "2")]
    pub outcome: Option<crate::object_manifest::ManifestOutcome>,
    #[prost(uint64, tag = "3")]
    pub update_time_ms: u64,
}

#[cfg(all(test, feature = "meta-server"))]
mod manifest_tests {
    use super::*;
    use crate::cluster::meta_wire::MetaRequestCode;
    use crate::cluster::{MetaReply, MetaRequest};
    #[test]
    fn manifest_requests_and_replies_require_canonical_bounded_protobuf() {
        let (_, create, _) = crate::tests::manifest_publication_fixture();
        let outcome = create.manifest.unwrap();
        let value = outcome.after.clone().unwrap();
        let requests = [
            MetaRequest::ReadManifest {
                bucket: value.bucket.clone(),
                key: value.key.clone(),
            },
            MetaRequest::PublishManifest {
                outcome,
                update_time_ms: 42,
            },
        ];
        for request in requests {
            let bytes = request.encode_payload();
            assert_eq!(
                MetaRequest::decode(request.code(), &bytes).unwrap(),
                request
            );
            let mut unknown = bytes.clone();
            unknown.extend([0xa0, 0x06, 1]);
            assert!(MetaRequest::decode(request.code(), &unknown).is_err());
            assert!(MetaRequest::decode(request.code(), &bytes[..bytes.len() - 1]).is_err());
        }
        let reply = MetaReply::Manifest(value);
        let bytes = reply.encode_payload();
        assert_eq!(reply.payload_len(), bytes.len());
        for code in [
            MetaRequestCode::ReadManifest,
            MetaRequestCode::PublishManifest,
        ] {
            assert_eq!(MetaReply::decode(code, &bytes).unwrap(), reply);
            let mut unknown = bytes.clone();
            unknown.extend([0xa0, 0x06, 1]);
            assert!(MetaReply::decode(code, &unknown).is_err());
            assert!(MetaReply::decode(code, &vec![0; 1024 * 1024 + 1]).is_err());
        }
        assert!(MetaRequest::decode(MetaRequestCode::ReadManifest, &vec![0; 8193]).is_err());
        assert!(MetaRequest::decode(
            MetaRequestCode::PublishManifest,
            &ManifestPublish {
                version: 1,
                outcome: None,
                update_time_ms: 1
            }
            .encode_to_vec()
        )
        .is_err());
    }

    #[test]
    fn manifest_route_batches_are_canonical_bounded_and_ordered() {
        let (_, create, _) = crate::tests::manifest_publication_fixture();
        let first = create.manifest.unwrap().after.unwrap();
        let mut second = first.clone();
        second.key.push_str("-second");
        second.validate().unwrap();
        let request = MetaRequest::ReadManifestRoutesBatch {
            objects: vec![
                (first.bucket.clone(), first.key.clone()),
                (second.bucket.clone(), second.key.clone()),
            ],
        };
        let code = MetaRequestCode::ReadManifestRoutesBatch;
        let bytes = request.encode_payload();
        assert_eq!(MetaRequest::decode(code, &bytes).unwrap(), request);
        let mut unknown = bytes.clone();
        unknown.extend([0xa0, 0x06, 1]);
        assert!(MetaRequest::decode(code, &unknown).is_err());
        assert!(MetaRequest::decode(code, &bytes[..bytes.len() - 1]).is_err());
        assert!(MetaRequest::decode(code, &MetaRequest::ReadManifestRoutesBatch {
            objects: vec![],
        }.encode_payload()).is_err());
        assert!(MetaRequest::decode(code, &MetaRequest::ReadManifestRoutesBatch {
            objects: vec![(first.bucket.clone(), first.key.clone()); 33],
        }.encode_payload()).is_err());

        let reply = MetaReply::ManifestRoutesBatch {
            objects: vec![
                crate::cluster::ManifestRouteEntry {
                    manifest: first,
                    routes: vec![],
                },
                crate::cluster::ManifestRouteEntry {
                    manifest: second,
                    routes: vec![],
                },
            ],
        };
        let bytes = reply.encode_payload();
        assert_eq!(reply.payload_len(), bytes.len());
        assert_eq!(MetaReply::decode(code, &bytes).unwrap(), reply);
        let mut unknown = bytes.clone();
        unknown.extend([0xa0, 0x06, 1]);
        assert!(MetaReply::decode(code, &unknown).is_err());
        assert!(MetaReply::decode(code, &bytes[..bytes.len() - 1]).is_err());
        assert!(MetaReply::decode(code, &MetaReply::ManifestRoutesBatch {
            objects: vec![],
        }.encode_payload()).is_err());
    }
}

#[cfg(feature = "meta-server")]
#[derive(Clone, PartialEq, Message)]
pub(super) struct AllocateObjectPages {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub page_count: u32,
}

#[cfg(all(test, feature = "meta-server"))]
mod allocation_tests {
    use super::*;
    use crate::cluster::meta_wire::MetaRequestCode;
    use crate::cluster::{MetaReply, MetaRequest};
    use crate::object_manifest::ObjectPageAllocation;
    #[test]
    fn object_allocations_require_canonical_bounded_protobuf_and_page_indexes() {
        let code = MetaRequestCode::AllocateObjectPages;
        for count in [1, 4096] {
            let request = MetaRequest::AllocateObjectPages { page_count: count };
            let bytes = request.encode_payload();
            assert_eq!(MetaRequest::decode(code, &bytes).unwrap(), request);
            let mut duplicate = bytes.clone();
            duplicate.extend([8, 1]);
            assert!(MetaRequest::decode(code, &duplicate).is_err());
            for cut in 0..bytes.len() {
                assert!(MetaRequest::decode(code, &bytes[..cut]).is_err());
            }
            for id in [1, u32::MAX as u64, (u32::MAX as u64) + 1, u64::MAX] {
                let allocation = ObjectPageAllocation {
                    format_version: 1,
                    allocation_id: id,
                    page_count: count,
                };
                let page = allocation.page(count - 1).unwrap();
                assert_eq!((page.chunk_id.volume_id() << 32) | page.chunk_id.id(), id);
                assert_eq!(page.page_index, u64::from(count - 1));
                assert!(allocation.page(count).is_err());
                let reply = MetaReply::ObjectPages(allocation);
                let bytes = reply.encode_payload();
                assert_eq!(bytes.len(), reply.payload_len());
                assert_eq!(MetaReply::decode(code, &bytes).unwrap(), reply);
                for cut in 0..bytes.len() {
                    assert!(MetaReply::decode(code, &bytes[..cut]).is_err());
                }
                let mut unknown = bytes;
                unknown.extend([0xa0, 6, 1]);
                assert!(MetaReply::decode(code, &unknown).is_err());
            }
        }
        for count in [0, 4097, u32::MAX] {
            assert!(MetaRequest::decode(
                code,
                &MetaRequest::AllocateObjectPages { page_count: count }.encode_payload()
            )
            .is_err());
        }
        assert!(MetaRequest::decode(code, &[0; 33]).is_err());
        assert!(MetaReply::decode(code, &[0; 33]).is_err());
        for (version, id) in [(0, 1), (2, 1), (1, 0)] {
            assert!(MetaReply::decode(
                code,
                &ObjectPageAllocation {
                    format_version: version,
                    allocation_id: id,
                    page_count: 1
                }
                .encode_to_vec()
            )
            .is_err());
        }
    }
}
