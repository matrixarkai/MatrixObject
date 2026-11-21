// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Included into `object_client.rs`, so it reaches the group connections and the page lease that
// the page-granular writer already uses. It moves a chunk object's pages over the network; where
// each page goes, what a read fetches and how it is verified is `crate::chunk_object`, which is
// pure and tested without a cluster.

/// Why a chunk object write did not complete.
#[derive(Debug)]
pub enum ChunkWriteFailure {
    /// Nothing was published. Pages already written are orphans for the reclaim sweep.
    BeforePublication(ObjectError),
    /// The answer to the publication was lost and the catalog does not show the new manifest: it
    /// may or may not have applied. Never replayed blindly: the caller decides from the catalog.
    PublicationUnconfirmed {
        error: ObjectError,
        outcome: Box<crate::chunk_catalog::ChunkOutcome>,
    },
}

impl ChunkWriteFailure {
    pub fn into_error(self) -> ObjectError {
        match self {
            Self::BeforePublication(error) | Self::PublicationUnconfirmed { error, .. } => error,
        }
    }
}

impl RemoteGroupClient {
    /// Read whole or partial pages with no digest expected: the caller verifies what comes back.
    pub(super) fn read_pages(
        &mut self,
        pages: &[(crate::PageId, u64, u64)],
    ) -> Result<Vec<crate::ReplicaReadResult>, ObjectError> {
        let reads = match self {
            Self::Raft(client) => client.read_chunks_pipelined(pages)?,
            Self::Star(set) => set.read_chunks(pages)?,
        };
        if reads.len() != pages.len() {
            return Err(ObjectError::Corruption("a page read returned a different number of pages".into()));
        }
        Ok(reads)
    }
}

/// Write `writes` (each one whole page) to one group and check what the group says it stored.
fn write_group_pages(
    configured: &ChunkGroupRoute,
    client: Option<RemoteGroupClient>,
    writes: &[(crate::PageId, &[u8])],
    options: &ObjectWriteOptions,
) -> Result<RemoteGroupClient, ObjectError> {
    let mut client = match client {
        Some(client) => client,
        None => connect_group(configured, &options.replica)?,
    };
    let limit = client.write_batch_limit();
    for batch in writes.chunks(limit) {
        let requests: Vec<(crate::PageId, u64, &[u8], u64)> =
            batch.iter().map(|(page, bytes)| (page.clone(), 0u64, *bytes, 0u64)).collect();
        match client.write_chunks_with_receipt(&requests)? {
            GroupWriteReceipt::Applied => {}
            GroupWriteReceipt::Digests(digests) => {
                if digests.len() != batch.len() {
                    return Err(ObjectError::Corruption("written page verification count changed".into()));
                }
                for ((_, bytes), (sequence, length, sha256)) in batch.iter().zip(digests) {
                    if sequence != 1
                        || length != bytes.len() as u64
                        || sha256[..] != crate::chunk_integrity::sha256(bytes)[..]
                    {
                        return Err(ObjectError::Corruption("written page verification failed".into()));
                    }
                }
            }
        }
    }
    Ok(client)
}

impl RemoteMetaClient {
    /// Write `data` as stripes of fresh pages, digest pages included, and return the extents that name
    /// them, the first starting at `base_offset` of the object. Nothing is published.
    fn write_chunk_stripes(
        &mut self,
        bucket: &str,
        key: &str,
        data: &[u8],
        options: &ObjectWriteOptions,
        stripe_bytes: u64,
        base_offset: u64,
    ) -> Result<Vec<crate::chunk_manifest::ChunkExtent>, ObjectError> {
        let page_bytes = options.page_bytes as u64;
        let stripes = crate::chunk_object::plan_stripes(data.len() as u64, page_bytes, stripe_bytes)?;
        let mut extents = Vec::with_capacity(stripes.len());
        if !stripes.is_empty() {
            let count = options.target_groups.min(stripes.len() as u32).max(1) as usize;
            let mut group_bytes = vec![0u64; count];
            for (index, stripe) in stripes.iter().enumerate() {
                group_bytes[index % count] += stripe.length;
            }
            let plan = self
                .plan_object_placement(bucket, key, group_bytes.iter().copied().max().unwrap_or(0), count as u64)
                ?;
            if plan.node_ids.len() != count
                || plan.node_ids.iter().collect::<std::collections::BTreeSet<_>>().len() != count
            {
                return Err(ObjectError::Corruption("placement returned invalid data groups".into()));
            }
            let routes = self.read_chunk_routes(&plan.node_ids)?;
            if routes.len() != count {
                return Err(ObjectError::Corruption("selected group route missing".into()));
            }
            // Digests first, so no page is allocated for an extent that cannot be stored.
            let digests: Vec<crate::chunk_integrity::ExtentDigests> = stripes
                .iter()
                .map(|stripe| {
                    crate::chunk_integrity::digest_extent(&data[stripe.range()], options.page_bytes)
                        .map_err(|error| error.into_object_error())
                })
                .collect::<Result<_, _>>()?;
            let mut per_group: Vec<Vec<(crate::PageId, &[u8])>> = (0..count).map(|_| Vec::new()).collect();
            for (index, stripe) in stripes.iter().enumerate() {
                let stored = crate::chunk_integrity::stored_pages(stripe.length, page_bytes) as u32;
                let (allocation, first) = self
                    .take_object_page_range(stored, options.page_allocation_lease_pages)
                    ?;
                let first_page = allocation.page(first)?;
                let group = index % count;
                let placement = crate::chunk_object::Placement {
                    group: plan.node_ids[group].clone(),
                    volume_id: first_page.chunk_id.volume_id(),
                    chunk_id: first_page.chunk_id.id(),
                    first_page: u64::from(first),
                };
                for (offset, bytes) in
                    crate::chunk_object::pages_of(&data[stripe.range()], options.page_bytes, &digests[index])
                {
                    let page = allocation.page(first + offset as u32)?;
                    per_group[group].push((page, bytes));
                }
                let placed = crate::chunk_object::Stripe { object_offset: base_offset + stripe.object_offset, length: stripe.length };
                extents.push(crate::chunk_object::extent_for(&placement, &placed, &digests[index], page_bytes));
            }
            let mut jobs: Vec<_> = (0..count)
                .map(|group| {
                    let name = plan.node_ids[group].clone();
                    let client = self.take_group_connection(&name, &routes[group], &options.replica);
                    (name, routes[group].clone(), client, std::mem::take(&mut per_group[group]))
                })
                .collect();
            let width = options.max_parallel_groups.max(1);
            while !jobs.is_empty() {
                let batch: Vec<_> = jobs.drain(..jobs.len().min(width)).collect();
                let completed: Vec<(String, ChunkGroupRoute, RemoteGroupClient)> = if batch.len() == 1 {
                    let (name, route, client, writes) = batch.into_iter().next().unwrap();
                    let client = write_group_pages(&route, client, &writes, options)?;
                    vec![(name, route, client)]
                } else {
                    std::thread::scope(|scope| {
                        let handles: Vec<_> = batch
                            .into_iter()
                            .map(|(name, route, client, writes)| {
                                scope.spawn(move || {
                                    write_group_pages(&route, client, &writes, options).map(|client| (name, route, client))
                                })
                            })
                            .collect();
                        let mut done = Vec::new();
                        let mut first_error = None;
                        for handle in handles {
                            match handle.join().unwrap_or_else(|_| {
                                Err(ObjectError::Corruption("object group writer panicked".into()))
                            }) {
                                Ok(connection) => done.push(connection),
                                Err(error) => {
                                    first_error.get_or_insert(error);
                                }
                            }
                        }
                        first_error.map_or(Ok(done), Err)
                    })
                    ?
                };
                for (name, route, client) in completed {
                    self.cache_group_connection(name, route, client);
                }
            }
        }
        Ok(extents)
    }
}

impl RemoteMetaClient {
    /// Write `data` as a chunk object and publish it, one generation after `expected`.
    ///
    /// The order is the crash-safety rule: every data page, then the digest page of each multi-page
    /// extent, then the publication. The publication is the commit. A writer that dies before it
    /// leaves pages no manifest names and the object keeps its previous generation.
    pub fn write_chunk_object(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<&crate::chunk_manifest::ChunkManifest>,
        data: &[u8],
        options: &ObjectWriteOptions,
        stripe_bytes: u64,
    ) -> Result<crate::chunk_manifest::ChunkManifest, ChunkWriteFailure> {
        let before = ChunkWriteFailure::BeforePublication;
        self.check_operation_deadline().map_err(before)?;
        crate::validate_bucket(bucket).map_err(before)?;
        crate::validate_key(key).map_err(before)?;
        if data.len() > options.max_object_bytes || options.page_bytes == 0 {
            return Err(before(ObjectError::InvalidArgument("invalid object write size or options".into())));
        }
        let generation = match expected {
            Some(previous) => {
                previous.validate().map_err(before)?;
                if previous.bucket != bucket || previous.key != key {
                    return Err(before(ObjectError::InvalidArgument("expected manifest names another object".into())));
                }
                previous
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| before(ObjectError::InvalidArgument("object generation exhausted".into())))?
            }
            None => 1,
        };
        let page_bytes = options.page_bytes as u64;
        let extents = self.write_chunk_stripes(bucket, key, data, options, stripe_bytes, 0).map_err(before)?;
        self.check_operation_deadline().map_err(before)?;
        let manifest = crate::chunk_object::build_manifest(bucket, key, generation, page_bytes, extents).map_err(before)?;
        let outcome = crate::chunk_catalog::ChunkOutcome::decide(expected.cloned(), manifest).map_err(before)?;
        self.publish_chunk_outcome(bucket, key, outcome)
    }

    /// Append `data` to the live paged object `expected` and publish it one generation later.
    ///
    /// The appended bytes are new extents after the last one, written like a whole object's (every
    /// data page, then the digest page of a multi-page extent) and published with the generation
    /// check, so an append that read a stale manifest is refused and nothing is changed. A writer that
    /// dies before the publication leaves pages no manifest names. An append that would take the
    /// object past the extents a manifest may name is refused whole; the object is then rewritten.
    pub fn append_chunk_object(
        &mut self,
        expected: &crate::chunk_manifest::ChunkManifest,
        data: &[u8],
        options: &ObjectWriteOptions,
        stripe_bytes: u64,
    ) -> Result<crate::chunk_manifest::ChunkManifest, ChunkWriteFailure> {
        let before = ChunkWriteFailure::BeforePublication;
        self.check_operation_deadline().map_err(before)?;
        expected.validate().map_err(before)?;
        if expected.deleted || expected.kind != crate::chunk_manifest::CHUNK_KIND_PAGED {
            return Err(before(ObjectError::InvalidArgument("only a live paged chunk object is appended to".into())));
        }
        if data.is_empty() {
            return Err(before(ObjectError::InvalidArgument("an append needs at least one byte".into())));
        }
        if options.page_bytes as u64 != expected.page_bytes {
            return Err(before(ObjectError::InvalidArgument(
                "an append uses the page size the object was written with".into(),
            )));
        }
        let new_length = expected
            .length
            .checked_add(data.len() as u64)
            .filter(|length| *length <= options.max_object_bytes as u64)
            .ok_or_else(|| before(ObjectError::InvalidArgument("the appended object would be larger than the size limit".into())))?;
        let added = crate::chunk_object::plan_stripes(data.len() as u64, expected.page_bytes, stripe_bytes).map_err(before)?.len();
        if expected.extents.len() + added > crate::chunk_manifest::MAX_CHUNK_EXTENTS {
            return Err(before(ObjectError::InvalidArgument(format!(
                "append refused: the object already holds {} extents and this append adds {added}; a manifest names at most {}",
                expected.extents.len(),
                crate::chunk_manifest::MAX_CHUNK_EXTENTS
            ))));
        }
        let new_extents = self.write_chunk_stripes(&expected.bucket, &expected.key, data, options, stripe_bytes, expected.length).map_err(before)?;
        self.check_operation_deadline().map_err(before)?;
        let mut extents = expected.extents.clone();
        extents.extend(new_extents);
        let generation = expected
            .generation
            .checked_add(1)
            .ok_or_else(|| before(ObjectError::InvalidArgument("object generation exhausted".into())))?;
        let manifest = crate::chunk_object::build_manifest(&expected.bucket, &expected.key, generation, expected.page_bytes, extents).map_err(before)?;
        debug_assert_eq!(manifest.length, new_length);
        let outcome = crate::chunk_catalog::ChunkOutcome::decide(Some(expected.clone()), manifest).map_err(before)?;
        self.publish_chunk_outcome(&expected.bucket, &expected.key, outcome)
    }

    /// Publish `expected` again one generation later with exactly its extents: the fence an append
    /// resolve uses. No publication that built on an older generation can succeed afterwards.
    pub fn republish_chunk_manifest(
        &mut self,
        expected: &crate::chunk_manifest::ChunkManifest,
    ) -> Result<crate::chunk_manifest::ChunkManifest, ChunkWriteFailure> {
        let before = ChunkWriteFailure::BeforePublication;
        expected.validate().map_err(before)?;
        let mut again = expected.clone();
        again.generation = expected
            .generation
            .checked_add(1)
            .ok_or_else(|| before(ObjectError::InvalidArgument("object generation exhausted".into())))?;
        let outcome = crate::chunk_catalog::ChunkOutcome::decide(Some(expected.clone()), again).map_err(before)?;
        self.publish_chunk_outcome(&expected.bucket, &expected.key, outcome)
    }

    /// Publish a decided outcome. A refusal the leader decided (the generation moved, the catalog is
    /// full) means nothing was published; any other failure is confirmed against the catalog before
    /// it is called anything, and never replayed.
    fn publish_chunk_outcome(
        &mut self,
        bucket: &str,
        key: &str,
        outcome: crate::chunk_catalog::ChunkOutcome,
    ) -> Result<crate::chunk_manifest::ChunkManifest, ChunkWriteFailure> {
        match self.publish_chunk_manifest(outcome.clone()) {
            Ok(manifest) => Ok(manifest),
            Err(error @ (ObjectError::PreconditionFailed(_) | ObjectError::InvalidArgument(_))) => {
                Err(ChunkWriteFailure::BeforePublication(error))
            }
            Err(error) => {
                let after = outcome.after().ok().cloned();
                if after.is_some() && self.read_chunk_manifest(bucket, key).ok() == after {
                    Ok(after.unwrap())
                } else {
                    Err(ChunkWriteFailure::PublicationUnconfirmed { error, outcome: Box::new(outcome) })
                }
            }
        }
    }

    /// Delete a chunk object: publish a tombstone one generation after the manifest read. Its pages
    /// are not freed here; the reclaim sweep frees pages no manifest names.
    pub fn delete_chunk_object(&mut self, bucket: &str, key: &str) -> Result<crate::chunk_manifest::ChunkManifest, ChunkWriteFailure> {
        let before = ChunkWriteFailure::BeforePublication;
        let current = self.read_chunk_manifest(bucket, key).map_err(before)?;
        if current.deleted {
            return Err(before(ObjectError::NotFound(format!("{bucket}/{key} is already deleted"))));
        }
        let tombstone = crate::chunk_manifest::ChunkManifest {
            format_version: crate::chunk_manifest::CHUNK_MANIFEST_FORMAT,
            bucket: bucket.to_string(),
            key: key.to_string(),
            generation: current.generation.checked_add(1).ok_or_else(|| before(ObjectError::InvalidArgument("object generation exhausted".into())))?,
            deleted: true,
            ..Default::default()
        };
        let outcome = crate::chunk_catalog::ChunkOutcome::decide(Some(current), tombstone).map_err(before)?;
        self.publish_chunk_outcome(bucket, key, outcome)
    }

    /// Read `offset..offset + length` of the object `manifest` describes. Every page that comes back
    /// is checked against the manifest's digests; nothing is returned unverified.
    pub fn read_chunk_object_range(
        &mut self,
        manifest: &crate::chunk_manifest::ChunkManifest,
        offset: u64,
        length: u64,
        options: &ObjectWriteOptions,
    ) -> Result<Vec<u8>, ObjectError> {
        self.check_operation_deadline()?;
        let page_bytes = manifest.page_bytes;
        let reads = crate::chunk_object::plan_read(manifest, offset, length)?;
        let mut out = Vec::with_capacity(length as usize);
        for read in reads {
            let extent = &manifest.extents[read.extent];
            let route = self
                .read_chunk_routes(std::slice::from_ref(&extent.group))?
                .pop()
                .ok_or_else(|| ObjectError::Corruption("group route missing".into()))?;
            let mut client = match self.take_group_connection(&extent.group, &route, &options.replica) {
                Some(client) => client,
                None => connect_group(&route, &options.replica)?,
            };
            let data_pages = crate::chunk_integrity::data_pages(extent.length, page_bytes);
            let wanted = crate::chunk_object::pages_to_fetch(extent, &read, page_bytes);
            let chunk_id = crate::ChunkId::new(extent.volume_id, extent.chunk_id)?;
            let base = extent.chunk_offset / page_bytes;
            let requests: Vec<(crate::PageId, u64, u64)> = wanted
                .iter()
                .map(|&page| {
                    let local = page - base;
                    // the last data page is as long as the extent leaves it; the digest page is
                    // thirty-two bytes per data page
                    let length = if local == data_pages {
                        data_pages * crate::chunk_integrity::DIGEST_BYTES as u64
                    } else if local + 1 == data_pages {
                        extent.length - local * page_bytes
                    } else {
                        page_bytes
                    };
                    (crate::PageId { chunk_id: chunk_id.clone(), page_index: page }, 0, length)
                })
                .collect();
            let mut fetched: Vec<Vec<u8>> = Vec::with_capacity(requests.len());
            for batch in requests.chunks(client.read_batch_limit()) {
                for result in client.read_pages(batch)? {
                    fetched.push(result.data);
                }
            }
            self.cache_group_connection(extent.group.clone(), route, client);
            let digest_page = if read.digest_page { fetched.pop() } else { None };
            let slices: Vec<&[u8]> = fetched.iter().map(|page| page.as_slice()).collect();
            out.extend(crate::chunk_object::verify_and_cut(extent, page_bytes, &read, digest_page.as_deref(), &slices)?);
        }
        Ok(out)
    }

    /// Read a whole chunk object: its manifest and its verified bytes. A deleted or never-published
    /// object is `NotFound`.
    pub fn read_chunk_object(
        &mut self,
        bucket: &str,
        key: &str,
        options: &ObjectWriteOptions,
    ) -> Result<(crate::chunk_manifest::ChunkManifest, Vec<u8>), ObjectError> {
        let manifest = self.read_chunk_manifest(bucket, key)?;
        if manifest.deleted {
            return Err(ObjectError::NotFound(format!("{bucket}/{key} is deleted")));
        }
        let bytes = self.read_chunk_object_range(&manifest, 0, manifest.length, options)?;
        Ok((manifest, bytes))
    }
}
