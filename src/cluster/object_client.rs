// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded object writes across metadata and data quorums.
use super::{
    ChunkGroupRoute, RemoteMetaClient, RemoteReplicaClient, RemoteReplicaSet,
    ReplicaClientOptions,
};
use crate::object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest, ObjectPageAllocation};
use crate::ObjectError;

// Match the retained write window so a client can reuse every verified group
// in a wide object workload without reconnecting between objects.
const MAX_CACHED_GROUP_CONNECTIONS: usize = 32;
const MAX_RETAINED_WRITE_CONNECTIONS: usize = 32;
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

    fn probe_chunk_digests(
        &mut self,
        pages: &[(crate::PageId, u64, u64)],
    ) -> Result<Vec<Option<(u64, u64, [u8; 32])>>, ObjectError> {
        match self {
            Self::Raft(client) => client.probe_chunk_digests_pipelined(pages),
            Self::Star(set) => set.probe_written_chunks(pages),
        }
    }

    pub(super) fn read_chunks(
        &mut self,
        pages: &[(crate::PageId, u64, u64)],
        extents: &[&ObjectExtent],
    ) -> Result<Vec<crate::ReplicaReadResult>, ObjectError> {
        if pages.len() != extents.len() {
            return Err(ObjectError::Corruption(
                "object read page and extent counts differ".into(),
            ));
        }
        match self {
            Self::Raft(client) => {
                let reads = client.read_chunks_pipelined(pages)?;
                if reads.len() != extents.len() {
                    return Err(ObjectError::Corruption(
                        "object read reply count differs from extent count".into(),
                    ));
                }
                for (read, extent) in reads.iter().zip(extents) {
                    if !extent.data_sha256.is_empty()
                        && (read.write_sequence != extent.write_sequence
                            || read.data.len() as u64 != extent.length
                            || ring::digest::digest(&ring::digest::SHA256, &read.data).as_ref()
                                != extent.data_sha256.as_slice())
                    {
                        return Err(ObjectError::Corruption(
                            "object extent does not match published digest".into(),
                        ));
                    }
                }
                Ok(reads)
            }
            Self::Star(set) if extents.iter().all(|extent| extent.data_sha256.len() == 32) => {
                let verified: Vec<_> = pages
                    .iter()
                    .zip(extents)
                    .map(|((page, offset, length), extent)| {
                        (
                            page.clone(),
                            *offset,
                            *length,
                            extent.write_sequence,
                            extent.data_sha256.as_slice().try_into().unwrap(),
                        )
                    })
                    .collect();
                set.read_chunks_verified(&verified)
            }
            Self::Star(set) => set.read_chunks(pages),
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
fn before(error: ObjectError) -> ObjectWriteFailure {
    ObjectWriteFailure::BeforePublication {
        error,
        staged: None,
    }
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::InvalidArgument(message.into())
}

fn preflight(
    bucket: &str,
    key: &str,
    expected: Option<&ObjectManifest>,
    length: usize,
    options: &ObjectWriteOptions,
) -> Result<(u32, u64), ObjectError> {
    crate::validate_bucket(bucket)?;
    crate::validate_key(key)?;
    if length > options.max_object_bytes
        || !(1..=1024 * 1024).contains(&options.page_bytes)
        || !(1..=4096).contains(&options.target_groups)
        || !(1..=4).contains(&options.max_parallel_groups)
        || !(1..=4096).contains(&options.page_allocation_lease_pages)
        || options.replica.max_payload_bytes < options.page_bytes + 256
    {
        return Err(invalid("invalid object write size or options"));
    }
    let pages = length.div_ceil(options.page_bytes);
    if pages > 4096 {
        return Err(invalid("object write exceeds 4096 pages"));
    }
    let generation = if let Some(previous) = expected {
        previous.validate()?;
        if previous.bucket != bucket || previous.key != key {
            return Err(invalid("expected manifest names another object"));
        }
        previous
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("object generation exhausted"))?
    } else {
        1
    };
    Ok((pages as u32, generation))
}

fn connect_group(
    group: &ChunkGroupRoute,
    options: &ReplicaClientOptions,
) -> Result<RemoteGroupClient, ObjectError> {
    RemoteGroupClient::connect(group, options)
}

fn write_group_extents(
    indexes: &[usize],
    configured: &ChunkGroupRoute,
    client: Option<RemoteGroupClient>,
    manifest: &ObjectManifest,
    data: &[u8],
    options: &ObjectWriteOptions,
) -> Result<RemoteGroupClient, ObjectError> {
    let mut client = match client {
        Some(client) => client,
        None => connect_group(configured, &options.replica)?,
    };
    let write_batch_limit = client.write_batch_limit();
    for batch in indexes.chunks(write_batch_limit) {
        let mut writes = Vec::with_capacity(batch.len());
        for &index in batch {
            let extent = &manifest.extents[index];
            let start = extent.object_offset as usize;
            let bytes = &data[start..start + extent.length as usize];
            let page = crate::PageId {
                chunk_id: crate::ChunkId::new(extent.volume_id, extent.chunk_id)?,
                page_index: extent.page_index,
            };
            writes.push((page.clone(), 0, bytes, 0));
        }
        match client.write_chunks_with_receipt(&writes)? {
            GroupWriteReceipt::Applied => {}
            GroupWriteReceipt::Digests(digests) => {
                if digests.len() != batch.len() {
                    return Err(ObjectError::Corruption(
                        "written page verification count changed".into(),
                    ));
                }
                for (&index, (sequence, actual_length, sha256)) in batch.iter().zip(digests) {
                    let extent = &manifest.extents[index];
                    let start = extent.object_offset as usize;
                    let bytes = &data[start..start + extent.length as usize];
                    let digest_matches = if extent.data_sha256.is_empty() {
                        // Older staged manifests have no recorded SHA digest.
                        sha256.as_ref()
                            == ring::digest::digest(&ring::digest::SHA256, bytes).as_ref()
                    } else {
                        sha256.as_ref() == extent.data_sha256.as_slice()
                    };
                    if sequence != 1
                        || actual_length != extent.length
                        || !digest_matches
                    {
                        return Err(ObjectError::Corruption(
                            "written page verification failed".into(),
                        ));
                    }
                }
            }
        }
    }
    Ok(client)
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

    fn connect_group_batch(
        &mut self,
        batch: &[(String, ChunkGroupRoute)],
        options: &ReplicaClientOptions,
    ) -> Result<Vec<RemoteGroupClient>, ObjectError> {
        let mut clients: Vec<Option<RemoteGroupClient>> = batch
            .iter()
            .map(|(group, route)| self.take_group_connection(group, route, options))
            .collect();
        let missing: Vec<_> = clients
            .iter()
            .enumerate()
            .filter_map(|(index, client)| client.is_none().then_some(index))
            .collect();
        if missing.len() == 1 {
            let index = missing[0];
            clients[index] = Some(connect_group(&batch[index].1, options)?);
        } else if !missing.is_empty() {
            std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for index in missing {
                    let configured = &batch[index].1;
                    match std::thread::Builder::new()
                        .name("object-group-verify".into())
                        .spawn_scoped(scope, move || connect_group(configured, options))
                    {
                        Ok(handle) => handles.push((index, handle)),
                        Err(error) => {
                            return Err(ObjectError::PreconditionFailed(format!(
                                "cannot start object group verifier: {error}"
                            )))
                        }
                    }
                }
                for (index, handle) in handles {
                    clients[index] = Some(handle.join().map_err(|_| {
                        ObjectError::Corruption("object group verifier panicked".into())
                    })??);
                }
                Ok(())
            })?;
        }
        Ok(clients.into_iter().map(Option::unwrap).collect())
    }

    /// Continue a caller-retained unpublished write without allocating new pages.
    /// Each invocation makes at most one conditional write per missing page.
    /// An uncertain data response returns the same candidate for a later probe.
    pub fn resume_unpublished_object(
        &mut self,
        expected: Option<ObjectManifest>,
        staged: ObjectManifest,
        data: &[u8],
        options: &ObjectWriteOptions,
    ) -> Result<ObjectManifest, ObjectWriteFailure> {
        self.with_operation_deadline(options.replica.deadline, |client| {
            let mut bounded = options.clone();
            bounded.replica.deadline = client.operation_deadline();
            client.resume_unpublished_object_inner(expected, staged, data, &bounded, false, None)
        })
    }

    /// Resume only the fresh suffix pages of a caller-retained append candidate.
    pub fn resume_unpublished_append(
        &mut self,
        expected: ObjectManifest,
        staged: ObjectManifest,
        suffix: &[u8],
        options: &ObjectWriteOptions,
    ) -> Result<ObjectManifest, ObjectWriteFailure> {
        self.with_operation_deadline(options.replica.deadline, |client| {
            let mut bounded = options.clone();
            bounded.replica.deadline = client.operation_deadline();
            client.resume_unpublished_object_inner(Some(expected), staged, suffix, &bounded, true, None)
        })
    }

    fn resume_unpublished_object_inner(
        &mut self,
        expected: Option<ObjectManifest>,
        staged: ObjectManifest,
        data: &[u8],
        options: &ObjectWriteOptions,
        append: bool,
        owned: Option<&crate::staged_allocation::StagedAllocation>,
    ) -> Result<ObjectManifest, ObjectWriteFailure> {
        let staged_error = |error| ObjectWriteFailure::BeforePublication {
            error,
            staged: Some(Box::new(staged.clone())),
        };
        self.check_operation_deadline().map_err(&staged_error)?;
        staged.validate().map_err(&staged_error)?;
        let (pages, generation) = preflight(
            &staged.bucket,
            &staged.key,
            expected.as_ref(),
            data.len(),
            options,
        )
        .map_err(&staged_error)?;
        let mut candidate_data = staged.clone();
        if append {
            let previous = expected.as_ref().unwrap();
            if previous.deleted || previous.format_version != 2 || staged.format_version != 2
                || previous.length.checked_add(data.len() as u64) != Some(staged.length)
                || staged.length > options.max_object_bytes as u64
                || staged.extents.get(..previous.extents.len()) != Some(previous.extents.as_slice())
            {
                return Err(staged_error(invalid("staged append does not retain expected extents")));
            }
            candidate_data.extents = candidate_data.extents.split_off(previous.extents.len());
            candidate_data.length = data.len() as u64;
            for extent in &mut candidate_data.extents {
                extent.object_offset = extent.object_offset.checked_sub(previous.length)
                    .ok_or_else(|| staged_error(invalid("staged append offset precedes suffix")))?;
            }
        }
        if staged.deleted
            || staged.generation != generation
            || candidate_data.length != data.len() as u64
            || candidate_data.extents.len() != pages as usize
            || candidate_data
                .extents
                .iter()
                .map(|e| &e.group)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                > options.target_groups as usize
            || candidate_data
                .extents
                .iter()
                .zip(data.chunks(options.page_bytes))
                .enumerate()
                .any(|(index, (extent, bytes))| {
                    extent.object_offset != (index * options.page_bytes) as u64
                        || extent.page_offset != 0
                        || extent.length != bytes.len() as u64
                        || extent.write_sequence != 1
                        || extent.data_checksum != crate::fnv1a64(bytes)
                        || (!extent.data_sha256.is_empty()
                            && extent.data_sha256
                                != ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
                })
        {
            return Err(staged_error(invalid(
                "staged object does not match the write",
            )));
        }
        let current = match self.read_manifest(&staged.bucket, &staged.key) {
            Ok(value) => Some(value),
            Err(ObjectError::NotFound(_)) => None,
            Err(error) => return Err(staged_error(error)),
        };
        if current.as_ref() == Some(&staged) {
            return Ok(staged);
        }
        if current != expected {
            return Err(staged_error(ObjectError::PreconditionFailed(
                "object manifest changed before resume".into(),
            )));
        }
        if let Some(allocation) = owned {
            if self.read_staged_allocation(allocation.allocation_id).map_err(&staged_error)? != *allocation {
                return Err(staged_error(invalid("staged allocation ownership changed")));
            }
        }
        let mut clients = std::collections::BTreeMap::new();
        let mut routes = std::collections::BTreeMap::new();
        let mut grouped: std::collections::BTreeMap<String, Vec<usize>> =
            std::collections::BTreeMap::new();
        for (index, extent) in candidate_data.extents.iter().enumerate() {
            grouped.entry(extent.group.clone()).or_default().push(index);
        }
        let group_ids: Vec<String> = grouped.keys().cloned().collect();
        for ids in group_ids.chunks(8) {
            self.check_operation_deadline().map_err(&staged_error)?;
            for (id, route) in ids.iter().zip(self.read_chunk_routes(ids).map_err(&staged_error)?) {
                routes.insert(id.clone(), route);
            }
        }
        let mut missing: std::collections::BTreeMap<String, Vec<usize>> =
            std::collections::BTreeMap::new();
        for (group, indexes) in &grouped {
            let route = routes.get(group).ok_or_else(|| {
                staged_error(ObjectError::Corruption("staged page route missing".into()))
            })?;
            let mut client = self
                .take_group_connection(group, route, &options.replica)
                .map(Ok)
                .unwrap_or_else(|| connect_group(route, &options.replica))
                .map_err(&staged_error)?;
            for batch in indexes.chunks(8) {
                self.check_operation_deadline().map_err(&staged_error)?;
                let pages = batch.iter().map(|&index| {
                    let extent = &candidate_data.extents[index];
                    Ok((crate::PageId {
                        chunk_id: crate::ChunkId::new(extent.volume_id, extent.chunk_id)?,
                        page_index: extent.page_index,
                    }, 0, extent.length))
                }).collect::<Result<Vec<_>, ObjectError>>().map_err(&staged_error)?;
                let probes = client.probe_chunk_digests(&pages).map_err(&staged_error)?;
                for (&index, probe) in batch.iter().zip(probes) {
                    let extent = &candidate_data.extents[index];
                    match probe {
                        Some((sequence, length, digest)) => {
                            let start = extent.object_offset as usize;
                            let bytes = &data[start..start + extent.length as usize];
                            if sequence != 1
                                || length != extent.length
                                || digest.as_ref()
                                    != ring::digest::digest(&ring::digest::SHA256, bytes).as_ref()
                            {
                                return Err(staged_error(ObjectError::Corruption(
                                    "staged page verification failed".into(),
                                )));
                            }
                        }
                        None => missing.entry(group.clone()).or_default().push(index),
                    }
                }
            }
            clients.insert(group.clone(), client);
        }
        for (group, indexes) in missing {
            self.check_operation_deadline().map_err(&staged_error)?;
            let route = routes.get(&group).ok_or_else(|| {
                staged_error(ObjectError::Corruption("staged page route missing".into()))
            })?;
            let client = clients.remove(&group).ok_or_else(|| {
                staged_error(ObjectError::Corruption("staged page connection missing".into()))
            })?;
            let client = write_group_extents(
                &indexes,
                route,
                Some(client),
                &candidate_data,
                data,
                options,
            )
            .map_err(&staged_error)?;
            self.cache_group_connection(group, route.clone(), client);
        }
        for (group, client) in clients {
            let route = routes.get(&group).ok_or_else(|| {
                staged_error(ObjectError::Corruption("staged page route missing".into()))
            })?;
            self.cache_group_connection(group, route.clone(), client);
        }
        self.check_operation_deadline().map_err(&staged_error)?;
        let outcome = ManifestOutcome::decide(expected, staged.clone()).map_err(&staged_error)?;
        let (publication, unadmitted, outcome) = self.publish_preserving_outcome(
            outcome, owned.map(|allocation| allocation.allocation_id), options.update_time_ms);
        match publication {
            Ok(value) => Ok(value),
            Err(error) if unadmitted => Err(staged_error(error)),
            Err(error) => {
                if self
                    .read_manifest(&staged.bucket, &staged.key)
                    .ok()
                    .as_ref()
                    == Some(&staged)
                {
                    Ok(staged)
                } else {
                    Err(ObjectWriteFailure::PublicationUnconfirmed {
                        error,
                        outcome: Box::new(outcome),
                    })
                }
            }
        }
    }

    /// Write fresh pages, verify them and conditionally publish one generation.
    /// Data errors never trigger automatic writes or publication. Pages are not
    /// reclaimed on failure because they may still be in flight or held by readers.
    pub fn write_object(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
        options: &ObjectWriteOptions,
    ) -> Result<ObjectManifest, ObjectWriteFailure> {
        self.with_operation_deadline(options.replica.deadline, |client| {
            let mut bounded = options.clone();
            bounded.replica.deadline = client.operation_deadline();
            client.write_object_inner(bucket, key, expected, data, &bounded, false, None, false)
        })
    }

    /// Copy a committed source snapshot by publishing its immutable extents.
    /// No data pages are allocated, fetched or rewritten. Shared pages must remain
    /// retained while either manifest or an older reader can still use them.
    pub fn copy_object(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        options: &ObjectWriteOptions,
    ) -> Result<ObjectManifest, ObjectWriteFailure> {
        self.with_operation_deadline(options.replica.deadline, |client| {
            let (_, generation) = preflight(bucket, key, expected.as_ref(), 0, options)
                .map_err(before)?;
            let mut copied = client.read_manifest(source_bucket, source_key).map_err(before)?;
            copied.validate().map_err(before)?;
            if copied.deleted {
                return Err(before(ObjectError::NotFound("copy source is deleted".into())));
            }
            if copied.length > options.max_object_bytes as u64 {
                return Err(before(invalid("copy source exceeds object byte limit")));
            }
            copied.bucket = bucket.into();
            copied.key = key.into();
            copied.generation = generation;
            let outcome = ManifestOutcome::decide(expected, copied.clone()).map_err(before)?;
            let (publication, unadmitted, outcome) = client.publish_preserving_outcome(
                outcome, None, options.update_time_ms,
            );
            match publication {
                Ok(value) => Ok(value),
                Err(error) if unadmitted => Err(ObjectWriteFailure::BeforePublication {
                    error, staged: Some(Box::new(copied)),
                }),
                Err(error) => Err(ObjectWriteFailure::PublicationUnconfirmed {
                    error, outcome: Box::new(outcome),
                }),
            }
        })
    }

    /// Append fresh immutable pages and conditionally publish the next generation.
    /// Existing pages are retained without reading or rewriting their payloads.
    pub fn append_object(
        &mut self,
        expected: ObjectManifest,
        data: &[u8],
        options: &ObjectWriteOptions,
    ) -> Result<ObjectManifest, ObjectWriteFailure> {
        self.with_operation_deadline(options.replica.deadline, |client| {
            let mut bounded = options.clone();
            bounded.replica.deadline = client.operation_deadline();
            let bucket = expected.bucket.clone();
            let key = expected.key.clone();
            client.write_object_inner(&bucket, &key, Some(expected), data, &bounded, true, None, false)
        })
    }

    /// Write or append using an allocation durably reserved before any page I/O.
    /// The caller retains the allocation record for abort or failure recovery.
    pub fn write_staged_object(&mut self, allocation: &crate::staged_allocation::StagedAllocation,
        expected: Option<ObjectManifest>, data: &[u8], options: &ObjectWriteOptions)
        -> Result<ObjectManifest, ObjectWriteFailure> {
        self.with_operation_deadline(options.replica.deadline, |client| {
            let mut bounded = options.clone();
            bounded.replica.deadline = client.operation_deadline();
            client.write_object_inner(&allocation.bucket, &allocation.key, expected, data, &bounded,
                allocation.append, Some(allocation), false)
        })
    }

    /// Append using bounded publication metadata. All metadata binaries must support version 11 outcomes.
    pub fn append_staged_object_compact(&mut self, allocation: &crate::staged_allocation::StagedAllocation,
        expected: ObjectManifest, data: &[u8], options: &ObjectWriteOptions)
        -> Result<ObjectManifest, ObjectWriteFailure> {
        if !allocation.append || data.is_empty() {
            return Err(before(ObjectError::InvalidArgument("compact publication requires a nonempty staged append".into())));
        }
        self.with_operation_deadline(options.replica.deadline, |client| {
            let mut bounded = options.clone();
            bounded.replica.deadline = client.operation_deadline();
            client.write_object_inner(&allocation.bucket, &allocation.key, Some(expected), data,
                &bounded, true, Some(allocation), true)
        })
    }

    /// Probe and complete the same owned candidate without allocating or replaying publication.
    pub fn resume_staged_object(&mut self, allocation: &crate::staged_allocation::StagedAllocation,
        expected: Option<ObjectManifest>, staged: ObjectManifest, data: &[u8], options: &ObjectWriteOptions)
        -> Result<ObjectManifest, ObjectWriteFailure> {
        allocation.check_publication(&ManifestOutcome::decide(expected.clone(), staged.clone()).map_err(before)?)
            .map_err(before)?;
        self.with_operation_deadline(options.replica.deadline, |client| {
            let mut bounded = options.clone();
            bounded.replica.deadline = client.operation_deadline();
            client.resume_unpublished_object_inner(expected, staged, data, &bounded, allocation.append, Some(allocation))
        })
    }

    fn write_object_inner(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
        options: &ObjectWriteOptions,
        append: bool,
        owned: Option<&crate::staged_allocation::StagedAllocation>,
        compact: bool,
    ) -> Result<ObjectManifest, ObjectWriteFailure> {
        self.check_operation_deadline().map_err(before)?;
        let (pages, generation) =
            preflight(bucket, key, expected.as_ref(), data.len(), options).map_err(before)?;
        if let Some(allocation) = owned {
            allocation.validate().map_err(before)?;
            if allocation.bucket != bucket || allocation.key != key || allocation.append != append
                || allocation.page_count < pages
                || allocation.expected_generation != expected.as_ref().map_or(0, |manifest| manifest.generation)
                || self.read_staged_allocation(allocation.allocation_id).map_err(before)? != *allocation {
                return Err(before(invalid("staged allocation does not match object write")));
            }
        }
        if append {
            let previous = expected.as_ref().unwrap();
            if previous.deleted || previous.format_version != 2
                || previous.length.checked_add(data.len() as u64)
                    .is_none_or(|length| length > options.max_object_bytes as u64)
                || previous.extents.len() + pages as usize > 4096
            {
                return Err(before(invalid("invalid object append size or generation")));
            }
        }
        let count = options.target_groups.min(pages);
        let max_group_bytes = if count > 0 {
            let mut planned_group_bytes = vec![0u64; count as usize];
            for (index, page) in data.chunks(options.page_bytes).enumerate() {
                planned_group_bytes[index % count as usize] += page.len() as u64;
            }
            planned_group_bytes.into_iter().max().unwrap_or(0)
        } else { 0 };
        let (current, preplanned) = if (1..=32).contains(&count) {
            let (current, plan, routes) = self.plan_object_write_routes(
                bucket, key, max_group_bytes, u64::from(count),
            ).map_err(before)?;
            (current, Some((plan, routes)))
        } else {
            let current = match self.read_manifest(bucket, key) {
                Ok(value) => Some(value),
                Err(ObjectError::NotFound(_)) => None,
                Err(error) => return Err(before(error)),
            };
            (current, None)
        };
        if current != expected {
            return Err(before(ObjectError::PreconditionFailed(
                "object manifest changed before write".into(),
            )));
        }
        let mut manifest = ObjectManifest {
            format_version: 2,
            bucket: bucket.into(),
            key: key.into(),
            generation,
            length: data.len() as u64,
            extents: Vec::new(),
            deleted: false,
        };
        let mut groups: std::collections::BTreeMap<String, ChunkGroupRoute> =
            std::collections::BTreeMap::new();
        let mut connections: Vec<(String, RemoteGroupClient)> = Vec::new();
        if pages > 0 {
            let (plan, planned_routes) = if let Some((plan, routes)) = preplanned {
                (plan, Some(routes))
            } else {
                (self.plan_object_placement(bucket, key, max_group_bytes, u64::from(count))
                    .map_err(before)?, None)
            };
            if plan.node_ids.len() != count as usize
                || plan
                    .node_ids
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != count as usize
            {
                return Err(before(ObjectError::Corruption(
                    "placement returned invalid data groups".into(),
                )));
            }
            // Verify every selected group before allocation or data writes.
            // Bound concurrent verification by the write width and retain one
            // wide-plan window so verification connections can carry the data.
            let mut route_map = planned_routes.map(|routes| {
                plan.node_ids.iter().cloned().zip(routes).collect::<std::collections::BTreeMap<_, _>>()
            });
            let retained_connection_limit =
                plan.node_ids.len().min(MAX_RETAINED_WRITE_CONNECTIONS);
            for window in plan.node_ids.rchunks(8) {
                let ids: Vec<String> = window.iter().rev().cloned().collect();
                let routes = if let Some(routes) = &mut route_map {
                    ids.iter().map(|id| routes.remove(id).ok_or_else(|| before(ObjectError::Corruption(
                        "selected group route missing".into(),
                    )))).collect::<Result<Vec<_>, _>>()?
                } else {
                    self.read_chunk_routes(&ids).map_err(before)?
                };
                let targets: Vec<_> = ids.into_iter().zip(routes).collect();
                for batch in targets.chunks(options.max_parallel_groups) {
                    let clients = self
                        .connect_group_batch(batch, &options.replica)
                        .map_err(before)?;
                    for ((group, configured), client) in batch.iter().zip(clients) {
                        if connections.len() == retained_connection_limit {
                            let (old_group, old_client) = connections.remove(0);
                            let old_route = groups.get(&old_group).ok_or_else(|| before(
                                ObjectError::Corruption("verified group route missing".into()),
                            ))?;
                            self.cache_group_connection(
                                old_group,
                                old_route.clone(),
                                old_client,
                            );
                        }
                        groups.insert(group.clone(), configured.clone());
                        connections.push((group.clone(), client));
                    }
                }
            }
            let (allocation, first_page) = if let Some(allocation) = owned {
                (allocation.allocation().map_err(before)?, 0)
            } else {
                self.take_object_page_range(pages, options.page_allocation_lease_pages).map_err(before)?
            };
            for (index, bytes) in data.chunks(options.page_bytes).enumerate() {
                let page = allocation.page(first_page + index as u32).map_err(before)?;
                manifest.extents.push(ObjectExtent {
                    group: plan.node_ids[index % plan.node_ids.len()].clone(),
                    volume_id: page.chunk_id.volume_id(),
                    chunk_id: page.chunk_id.id(),
                    page_index: page.page_index,
                    object_offset: (index * options.page_bytes) as u64,
                    page_offset: 0,
                    length: bytes.len() as u64,
                    write_sequence: 1,
                    data_checksum: crate::fnv1a64(bytes),
                    data_sha256: ring::digest::digest(&ring::digest::SHA256, bytes)
                        .as_ref()
                        .to_vec(),
                });
            }
        }
        let published = if append {
            let previous = expected.as_ref().unwrap();
            let mut combined = manifest.clone();
            combined.length += previous.length;
            for extent in &mut combined.extents {
                extent.object_offset += previous.length;
            }
            let mut extents = previous.extents.clone();
            extents.extend(combined.extents);
            combined.extents = extents;
            combined
        } else { manifest.clone() };
        let outcome = ManifestOutcome::decide(expected, published.clone()).map_err(before)?;
        let staged_error = |error| ObjectWriteFailure::BeforePublication {
            error,
            staged: Some(Box::new(published.clone())),
        };
        // Group I/O may overlap, but publication waits for every started group.
        // Visit the first target first and keep the manifest in object order.
        let first_group = connections.last().map(|(group, _)| group.clone());
        let mut order: Vec<_> = (0..manifest.extents.len()).collect();
        order.sort_by_key(|index| {
            let group = manifest.extents[*index].group.as_str();
            (Some(group) != first_group.as_deref(), group)
        });
        let mut grouped: Vec<Vec<usize>> = Vec::new();
        for index in order {
            if grouped.last().is_some_and(|indexes| manifest.extents[indexes[0]].group == manifest.extents[index].group) {
                grouped.last_mut().unwrap().push(index);
            } else {
                grouped.push(vec![index]);
            }
        }
        for batch in grouped.chunks(options.max_parallel_groups) {
            let mut jobs = Vec::new();
            for indexes in batch {
                let group = &manifest.extents[indexes[0]].group;
                let configured = groups.get(group).ok_or_else(|| staged_error(
                    ObjectError::Corruption("object extent group was not verified".into())))?;
                let client = connections.iter().position(|(name, _)| name == group)
                    .map(|index| connections.remove(index).1)
                    .or_else(|| self.take_group_connection(group, configured, &options.replica));
                jobs.push((group.clone(), indexes, configured.clone(), client));
            }
            if jobs.len() == 1 {
                let (group, indexes, configured, client) = jobs.pop().unwrap();
                let client = write_group_extents(
                    indexes,
                    &configured,
                    client,
                    &manifest,
                    data,
                    options,
                )
                .map_err(&staged_error)?;
                self.cache_group_connection(group, configured, client);
            } else {
                let completed = std::thread::scope(|scope| {
                    let mut handles = Vec::new();
                    let mut first_error = None;
                    for (group, indexes, configured, client) in jobs {
                        let manifest = &manifest;
                        match std::thread::Builder::new().name("object-group-write".into())
                            .spawn_scoped(scope, move || {
                                write_group_extents(
                                    indexes,
                                    &configured,
                                    client,
                                    manifest,
                                    data,
                                    options,
                                )
                                .map(|client| (group, configured, client))
                            }) {
                            Ok(handle) => handles.push(handle),
                            Err(error) => {
                                first_error = Some(ObjectError::PreconditionFailed(format!("cannot start object group writer: {error}")));
                                break;
                            }
                        }
                    }
                    let mut completed = Vec::new();
                    for handle in handles {
                        match handle.join().unwrap_or_else(|_| Err(ObjectError::Corruption("object group writer panicked".into()))) {
                            Ok(connection) => completed.push(connection),
                            Err(error) => {
                                if first_error.is_none() { first_error = Some(error); }
                            }
                        }
                    }
                    first_error.map_or(Ok(completed), Err)
                }).map_err(&staged_error)?;
                for (group, configured, client) in completed {
                    self.cache_group_connection(group, configured, client);
                }
            }
        }
        self.check_operation_deadline().map_err(&staged_error)?;
        let (publication, unadmitted, outcome) = if compact {
            self.publish_compact_append_preserving_outcome(outcome,
                owned.expect("compact append requires owned staging").allocation_id, options.update_time_ms)
        } else {
            self.publish_preserving_outcome(outcome, owned.map(|allocation| allocation.allocation_id), options.update_time_ms)
        };
        match publication {
            Ok(value) => Ok(value),
            Err(error) if unadmitted => Err(staged_error(error)),
            Err(error) => {
                // Never replay publication. Only the exact resulting value proves
                // current visibility; a failed read or another value stays uncertain.
                if self.read_manifest(bucket, key).ok().as_ref() == Some(&published) {
                    Ok(published)
                } else {
                    Err(ObjectWriteFailure::PublicationUnconfirmed {
                        error,
                        outcome: Box::new(outcome),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{MetaClientOptions, MetaReply, MetaRequest, MetaRequestCode, StatusCode};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};
    include!("compact_append_client_tests.rs");
    fn accept(listener: &TcpListener) -> TcpStream {
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    return stream;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < end =>
                {
                    std::thread::sleep(Duration::from_millis(5))
                }
                other => panic!("missing client connection: {other:?}"),
            }
        }
    }
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
    fn mismatched_page_digest_refuses_staged_write() {
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
                .write_all(&crate::cluster::message::encode_reply(
                    &crate::cluster::ReplicaReply::Digest {
                        write_sequence: 1,
                        length: 4,
                        sha256: [0; 32],
                    },
                ))
                .unwrap();
            assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        });
        let client =
            RemoteReplicaClient::connect(&endpoint, &ReplicaClientOptions::default()).unwrap();
        let manifest = ObjectManifest {
            format_version: 1,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 1,
            length: 4,
            extents: vec![ObjectExtent {
                group: "group".into(),
                volume_id: 1,
                chunk_id: 1,
                page_index: 0,
                object_offset: 0,
                page_offset: 0,
                length: 4,
                write_sequence: 1,
                data_checksum: 0,
                data_sha256: Vec::new(),
            }],
            deleted: false,
        };
        let route = ChunkGroupRoute {
            version: 1,
            group: None,
            fence: vec![],
        };
        assert!(matches!(
            write_group_extents(
                &[0],
                &route,
                Some(RemoteGroupClient::Raft(client)),
                &manifest,
                b"abcd",
                &ObjectWriteOptions::default()
            ),
            Err(ObjectError::Corruption(_))
        ));
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
    #[test]
    fn expired_object_deadline_sends_nothing_and_restores_client_scope() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let mut client =
            RemoteMetaClient::connect_catalog(&[endpoint], &MetaClientOptions::default()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let options = ObjectWriteOptions {
            replica: ReplicaClientOptions {
                deadline: Some(Instant::now()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(matches!(
            client.write_object("bucket", "key", None, b"", &options),
            Err(ObjectWriteFailure::BeforePublication { staged: None, .. })
        ));
        assert!(client
            .read_object("bucket", "key", 1, &options.replica)
            .is_err());
        assert!(client.operation_deadline().is_none());
        let mut byte = [0];
        assert!(
            matches!(server.read(&mut byte), Err(error) if matches!(error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut))
        );
    }

    #[test]
    fn object_deadline_bounds_publication_without_replay() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let mut stream = accept(&listener);
            assert!(matches!(receive(&mut stream), MetaRequest::ReadManifest { .. }));
            stream.write_all(&crate::cluster::message::encode_failure(StatusCode::NotFound, "object missing")).unwrap();
            assert!(matches!(receive(&mut stream), MetaRequest::PublishManifest { .. }));
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0, "expired publication must close without replay or readback");
            assert!(matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));
        });
        let mut client = RemoteMetaClient::connect_catalog(&[endpoint], &MetaClientOptions::default()).unwrap();
        let started = Instant::now();
        let options = ObjectWriteOptions {
            replica: ReplicaClientOptions { deadline: Some(started + Duration::from_millis(500)), ..Default::default() },
            ..Default::default()
        };
        assert!(matches!(client.write_object("bucket", "key", None, b"", &options),
            Err(ObjectWriteFailure::PublicationUnconfirmed { .. })));
        assert!(started.elapsed() < Duration::from_secs(3), "must not wait for the default 30 second response timeout");
        assert!(client.operation_deadline().is_none());
        worker.join().unwrap();
    }

    #[test]
    fn object_write_preflight_bounds_work_and_checks_expected_generation() {
        let mut options = ObjectWriteOptions::default();
        assert_eq!(
            preflight("bucket", "key", None, 0, &options).unwrap(),
            (0, 1)
        );
        assert_eq!(
            preflight("bucket", "key", None, options.page_bytes + 1, &options).unwrap(),
            (2, 1)
        );
        assert!(preflight(
            "bucket",
            "key",
            None,
            options.max_object_bytes + 1,
            &options
        )
        .is_err());
        options.page_bytes = 1;
        assert!(preflight("bucket", "key", None, 4097, &options).is_err());
        options.page_bytes = 0;
        assert!(preflight("bucket", "key", None, 0, &options).is_err());
        options = ObjectWriteOptions::default();
        options.target_groups = 0;
        assert!(preflight("bucket", "key", None, 0, &options).is_err());
        options = ObjectWriteOptions::default();
        for limit in [0, 5] {
            options.max_parallel_groups = limit;
            assert!(preflight("bucket", "key", None, 1, &options).is_err());
        }
        options = ObjectWriteOptions::default();
        options.replica.max_payload_bytes = options.page_bytes;
        assert!(preflight("bucket", "key", None, 1, &options).is_err());
        options = ObjectWriteOptions::default();
        let mut previous = ObjectManifest {
            format_version: 1,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 4,
            length: 0,
            extents: vec![],
            deleted: true,
        };
        assert_eq!(
            preflight("bucket", "key", Some(&previous), 0, &options).unwrap(),
            (0, 5)
        );
        assert!(preflight("bucket", "other", Some(&previous), 0, &options).is_err());
        previous.generation = u64::MAX;
        assert!(preflight("bucket", "key", Some(&previous), 0, &options).is_err());
    }
    #[test]
    fn typed_publication_refusals_do_not_become_uncertain_or_replay() {
        for (deadline, catalog) in [(false, false), (false, true), (true, false), (true, true)] {
            for typed in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                listener.set_nonblocking(true).unwrap();
                let endpoint = listener.local_addr().unwrap().to_string();
                let worker = std::thread::spawn(move || {
                    let mut stream = accept(&listener);
                    assert!(matches!(
                        receive(&mut stream),
                        MetaRequest::ReadManifest { .. }
                    ));
                    stream
                        .write_all(&crate::cluster::message::encode_failure(
                            StatusCode::NotFound,
                            "object missing",
                        ))
                        .unwrap();
                    let MetaRequest::PublishManifest { outcome, .. } = receive(&mut stream) else {
                        panic!("publication missing");
                    };
                    if typed {
                        stream
                            .write_all(&crate::cluster::meta_message::encode_meta_reply(
                                &MetaReply::NotLeader,
                            ))
                            .unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_millis(200)))
                            .unwrap();
                        let mut byte = [0];
                        match stream.read(&mut byte) {
                            Ok(0) => {},
                            Err(error) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {},
                            other => panic!("unadmitted publication must not be replayed or read back: {other:?}"),
                        }
                    } else {
                        // Identical text in an ordinary failure is not admission evidence.
                        stream
                            .write_all(&crate::cluster::message::encode_failure(
                                StatusCode::PreconditionFailed,
                                "metadata request was not admitted by a leader",
                            ))
                            .unwrap();
                        assert!(matches!(
                            receive(&mut stream),
                            MetaRequest::ReadManifest { .. }
                        ));
                        stream
                            .write_all(&crate::cluster::message::encode_failure(
                                StatusCode::NotFound,
                                "object missing",
                            ))
                            .unwrap();
                    }
                    outcome
                });
                let options = MetaClientOptions {
                    read_timeout: Duration::from_secs(2),
                    ..Default::default()
                };
                let mut client = if catalog {
                    RemoteMetaClient::connect_catalog(&[endpoint], &options).unwrap()
                } else {
                    RemoteMetaClient::connect(&endpoint, &options).unwrap()
                };
                let mut write_options = ObjectWriteOptions::default();
                if deadline {
                    write_options.replica.deadline = Some(Instant::now() + Duration::from_secs(2));
                }
                let result = client.write_object("bucket", "key", None, b"", &write_options);
                let attempted = worker.join().unwrap();
                if typed {
                    let ObjectWriteFailure::BeforePublication {
                        staged: Some(staged),
                        ..
                    } = result.unwrap_err()
                    else {
                        panic!("typed refusal must preserve the unadmitted candidate");
                    };
                    assert_eq!(*staged, attempted.after.unwrap());
                } else {
                    let ObjectWriteFailure::PublicationUnconfirmed { outcome, .. } =
                        result.unwrap_err()
                    else {
                        panic!("ordinary error text cannot prove non-admission");
                    };
                    assert_eq!(*outcome, attempted);
                }
            }
        }
    }

    #[test]
    fn lost_publication_replies_are_confirmed_by_exact_readback_without_replay() {
        for mode in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let worker = std::thread::spawn(move || {
                let mut stream = accept(&listener);
                assert!(matches!(
                    receive(&mut stream),
                    MetaRequest::ReadManifest { .. }
                ));
                stream
                    .write_all(&crate::cluster::message::encode_failure(
                        StatusCode::NotFound,
                        "object missing",
                    ))
                    .unwrap();
                let MetaRequest::PublishManifest { outcome, .. } = receive(&mut stream) else {
                    panic!("publication missing");
                };
                drop(stream); // The publication could have committed, but the reply is lost.
                let mut stream = accept(&listener);
                assert!(matches!(
                    receive(&mut stream),
                    MetaRequest::ReadManifest { .. }
                ));
                if mode != 2 {
                    let mut value = outcome.after.clone().unwrap();
                    if mode == 1 {
                        value.generation += 1;
                    }
                    stream
                        .write_all(&crate::cluster::meta_message::encode_meta_reply(
                            &MetaReply::Manifest(value),
                        ))
                        .unwrap();
                }
                outcome
            });
            let mut client = RemoteMetaClient::connect_catalog(
                &[endpoint],
                &MetaClientOptions {
                    read_timeout: Duration::from_secs(2),
                    ..Default::default()
                },
            )
            .unwrap();
            let result =
                client.write_object("bucket", "key", None, b"", &ObjectWriteOptions::default());
            let attempted = worker.join().unwrap();
            if mode == 0 {
                assert_eq!(result.unwrap(), attempted.after.unwrap());
            } else {
                let ObjectWriteFailure::PublicationUnconfirmed { outcome, .. } =
                    result.unwrap_err()
                else {
                    panic!("publication must stay unconfirmed");
                };
                assert_eq!(*outcome, attempted);
            }
        }
    }
}
