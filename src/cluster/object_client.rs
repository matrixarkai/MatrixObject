// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded object writes across metadata and data quorums.
use super::{
    ChunkGroupRoute, RemoteMetaClient, RemoteReplicaClient, RemoteReplicaSet,
    ReplicaClientOptions,
};
use crate::object_manifest::{ManifestOutcome, ObjectManifest, ObjectPageAllocation};
use crate::ObjectError;

// Match the retained write window so a client can reuse every verified group
// in a wide object workload without reconnecting between objects.
const MAX_CACHED_GROUP_CONNECTIONS: usize = 32;
const DEFAULT_OBJECT_PAGE_LEASE_PAGES: u32 = 64;

#[derive(Debug)]
pub(super) struct ObjectPageLease {
    allocation: ObjectPageAllocation,
    next_index: u32,
    configured_size: u32,
}

#[derive(Debug)]
pub(super) struct CachedGroupConnection {
    group: String,
    route: ChunkGroupRoute,
    client: RemoteGroupClient,
}

#[derive(Debug)]
pub(super) enum RemoteGroupClient {
    Raft(RemoteReplicaClient),
    Star(RemoteReplicaSet),
}

enum GroupWriteReceipt {
    Applied,
    Digests(Vec<(u64, u64, [u8; 32])>),
}

impl RemoteGroupClient {
    pub(super) fn connect(
        route: &ChunkGroupRoute,
        options: &ReplicaClientOptions,
    ) -> Result<Self, ObjectError> {
        match route.standalone_star_route()? {
            Some((endpoints, quorum)) => RemoteReplicaSet::connect_routed_quorum(
                &endpoints,
                quorum,
                1,
                options,
            )
            .map(Self::Star),
            None => RemoteReplicaClient::connect_routed_raft_catalog(route, options)
                .map(Self::Raft),
        }
    }

    fn prepare_for_reuse(&mut self, options: &ReplicaClientOptions) -> bool {
        match self {
            Self::Raft(client) => client.prepare_for_reuse(options),
            Self::Star(set) => set.prepare_for_reuse(options),
        }
    }

    fn write_chunks_with_receipt(
        &mut self,
        writes: &[(crate::PageId, u64, &[u8], u64)],
    ) -> Result<GroupWriteReceipt, ObjectError> {
        match self {
            Self::Raft(client) => client
                .write_chunks_if_sequence_with_digests(writes)
                .map(GroupWriteReceipt::Digests),
            Self::Star(set) => {
                if writes
                    .iter()
                    .any(|(_, offset, _, expected)| *offset != 0 || *expected != 0)
                {
                    return Err(ObjectError::InvalidArgument(
                        "star object writes require fresh whole pages".into(),
                    ));
                }
                set.write_chunks_if_sequence(writes)?;
                // Star acknowledgements come from a durable write quorum. Hashing the
                // caller's input again here only rechecks local memory and does not inspect
                // a replica, so it adds a full-object pass without strengthening the receipt.
                Ok(GroupWriteReceipt::Applied)
            }
        }
    }

    fn write_batch_limit(&self) -> usize {
        match self {
            Self::Raft(_) => 8,
            Self::Star(_) => 32,
        }
    }

    pub(super) fn read_batch_limit(&self) -> usize {
        match self {
            Self::Raft(_) => 8,
            Self::Star(_) => 32,
        }
    }

}

#[derive(Clone, Debug)]
pub struct ObjectWriteOptions {
    pub max_object_bytes: usize,
    /// Each page is at most 1 MiB, leaving room in the replicated outcome envelope.
    pub page_bytes: usize,
    /// Maximum number of configured data groups selected by placement.
    pub target_groups: u32,
    /// Maximum data groups written concurrently, from 1 through 4.
    pub max_parallel_groups: usize,
    /// Durable page IDs reserved per metadata call for repeated writes on one client.
    /// Set to 1 to reserve exactly the pages needed for each object.
    pub page_allocation_lease_pages: u32,
    pub update_time_ms: u64,
    /// The replica deadline also bounds metadata work for this object operation.
    pub replica: ReplicaClientOptions,
}
impl Default for ObjectWriteOptions {
    fn default() -> Self {
        Self {
            max_object_bytes: 64 * 1024 * 1024,
            page_bytes: 1024 * 1024,
            target_groups: 1,
            max_parallel_groups: 4,
            page_allocation_lease_pages: DEFAULT_OBJECT_PAGE_LEASE_PAGES,
            update_time_ms: 0,
            replica: ReplicaClientOptions::default(),
        }
    }
}

#[derive(Debug)]
pub enum ObjectWriteFailure {
    /// Publication was not admitted. Some candidate pages may already exist.
    BeforePublication {
        error: ObjectError,
        staged: Option<Box<ObjectManifest>>,
    },
    /// Publication was attempted but its exact result is not confirmed visible.
    /// A different current generation does not prove this attempt never committed.
    PublicationUnconfirmed {
        error: ObjectError,
        outcome: Box<ManifestOutcome>,
    },
}
impl std::fmt::Display for ObjectWriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BeforePublication { error, .. } => write!(f, "object was not published: {error}"),
            Self::PublicationUnconfirmed { error, .. } => {
                write!(f, "object publication is unconfirmed: {error}")
            }
        }
    }
}
impl std::error::Error for ObjectWriteFailure {}

fn connect_group(
    group: &ChunkGroupRoute,
    options: &ReplicaClientOptions,
) -> Result<RemoteGroupClient, ObjectError> {
    RemoteGroupClient::connect(group, options)
}

impl RemoteMetaClient {
    fn take_object_page_range(
        &mut self,
        pages: u32,
        configured_size: u32,
    ) -> Result<(ObjectPageAllocation, u32), ObjectError> {
        if let Some(lease) = &mut self.object_page_lease {
            if lease.configured_size == configured_size
                && lease.next_index + pages <= lease.allocation.page_count
            {
                let first = lease.next_index;
                lease.next_index += pages;
                return Ok((lease.allocation.clone(), first));
            }
        }
        // A discarded tail has never been handed to a writer and is never reused.
        self.object_page_lease = None;
        let allocation = self.allocate_object_pages(pages.max(configured_size))?;
        allocation.validate()?;
        self.object_page_lease = Some(ObjectPageLease {
            allocation: allocation.clone(),
            next_index: pages,
            configured_size,
        });
        Ok((allocation, 0))
    }

    pub(super) fn take_group_connection(
        &mut self,
        group: &str,
        route: &ChunkGroupRoute,
        options: &ReplicaClientOptions,
    ) -> Option<RemoteGroupClient> {
        let index = self
            .object_connections
            .iter()
            .position(|cached| cached.group == group)?;
        let mut cached = self.object_connections.remove(index);
        (cached.route == *route && cached.client.prepare_for_reuse(options))
            .then_some(cached.client)
    }

    pub(super) fn cache_group_connection(
        &mut self,
        group: String,
        route: ChunkGroupRoute,
        client: RemoteGroupClient,
    ) {
        if let Some(index) = self
            .object_connections
            .iter()
            .position(|cached| cached.group == group)
        {
            self.object_connections.remove(index);
        }
        if self.object_connections.len() == MAX_CACHED_GROUP_CONNECTIONS {
            self.object_connections.remove(0);
        }
        self.object_connections.push(CachedGroupConnection {
            group,
            route,
            client,
        });
    }

}

include!("chunk_object_client.rs");
include!("chunk_reclaim_client.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{MetaClientOptions, MetaReply, MetaRequest, MetaRequestCode, StatusCode};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;
    include!("refused_publication_tests.rs");
    fn receive(stream: &mut TcpStream) -> MetaRequest {
        let mut header = [0; 6];
        stream.read_exact(&mut header).unwrap();
        assert_eq!(header[0], crate::cluster::PROTOCOL_STAMP);
        let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
        assert!(size <= 4096);
        let mut payload = vec![0; size];
        stream.read_exact(&mut payload).unwrap();
        MetaRequest::decode(MetaRequestCode::from_u8(header[1]).unwrap(), &payload).unwrap()
    }
    fn receive_replica(stream: &mut TcpStream) -> crate::cluster::ReplicaRequest {
        let mut header = [0; 6];
        stream.read_exact(&mut header).unwrap();
        assert_eq!(header[0], crate::cluster::PROTOCOL_STAMP);
        let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
        assert!(size <= 4096);
        let mut payload = vec![0; size];
        stream.read_exact(&mut payload).unwrap();
        crate::cluster::ReplicaRequest::decode(
            crate::cluster::RequestCode::from_u8(header[1]).unwrap(),
            &payload,
        )
        .unwrap()
    }
    #[test]
    fn page_lease_renews_without_reusing_candidate_pages() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            for (allocation_id, page_count) in [(7, 64), (8, 64), (9, 1)] {
                assert!(matches!(
                    receive(&mut stream),
                    MetaRequest::AllocateObjectPages { page_count: requested }
                        if requested == page_count
                ));
                stream
                    .write_all(&crate::cluster::meta_message::encode_meta_reply(
                        &MetaReply::ObjectPages(ObjectPageAllocation {
                            format_version: 1,
                            allocation_id,
                            page_count,
                        }),
                    ))
                    .unwrap();
            }
            assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        });
        let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        for index in 0..65 {
            let (allocation, first) = client.take_object_page_range(1, 64).unwrap();
            let page = allocation.page(first).unwrap();
            assert_eq!(page.chunk_id.id(), if index < 64 { 7 } else { 8 });
            assert_eq!(page.page_index, if index < 64 { index } else { 0 });
        }
        let (allocation, first) = client.take_object_page_range(1, 1).unwrap();
        assert_eq!(allocation.page(first).unwrap().chunk_id.id(), 9);
        drop(client);
        worker.join().unwrap();
    }
    #[test]
    fn older_replica_refusal_uses_separate_write_and_digest() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            assert!(matches!(
                receive_replica(&mut stream),
                crate::cluster::ReplicaRequest::WriteChunkIfSequenceDigest { .. }
            ));
            stream
                .write_all(&crate::cluster::message::encode_failure(
                    StatusCode::UnknownRequest,
                    "unsupported request",
                ))
                .unwrap();
            assert!(matches!(
                receive_replica(&mut stream),
                crate::cluster::ReplicaRequest::WriteChunkIfSequence { .. }
            ));
            stream
                .write_all(&crate::cluster::message::encode_reply(
                    &crate::cluster::ReplicaReply::Applied,
                ))
                .unwrap();
            assert!(matches!(
                receive_replica(&mut stream),
                crate::cluster::ReplicaRequest::DigestChunk { length: 4, .. }
            ));
            let mut sha256 = [0u8; 32];
            sha256.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, b"abcd").as_ref());
            stream
                .write_all(&crate::cluster::message::encode_reply(
                    &crate::cluster::ReplicaReply::Digest {
                        write_sequence: 1,
                        length: 4,
                        sha256,
                    },
                ))
                .unwrap();
        });
        let mut client =
            RemoteReplicaClient::connect(&endpoint, &ReplicaClientOptions::default()).unwrap();
        let page = crate::PageId {
            chunk_id: crate::ChunkId::new(1, 1).unwrap(),
            page_index: 0,
        };
        assert_eq!(
            client
                .write_chunk_if_sequence_with_digest(page, 0, b"abcd", 0)
                .unwrap()
                .0,
            1
        );
        worker.join().unwrap();
    }

}
