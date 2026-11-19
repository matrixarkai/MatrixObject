// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

/// A read lease to open: the generation of an object and the digest of its whole manifest.
#[cfg(feature = "replica-server")]
struct ReadLease {
    bucket: String,
    key: String,
    generation: u64,
    digest: Vec<u8>,
    /// Whether every extent of the generation carries a published SHA-256 (manifest format 2 and
    /// later). Only such a read can go without a lease: it is what makes a page reused under a
    /// slow reader fail instead of returning other data.
    digest_checked: bool,
}

/// How long a read lease the client opens lasts, in milliseconds.
const READ_LEASE_TTL_MS: u64 = 60_000;
/// The reclaim grace window `T`: what reclaims pages frees a superseded generation only this long
/// after it was superseded (docs/read-session-leases.md).
pub const READ_GRACE: Duration = Duration::from_secs(60);
/// A fenced read that finishes within this (`T / 2`, from when it learned the manifest) takes no
/// read lease once the cluster has raised the read grace feature.
pub const SHORT_READ: Duration = Duration::from_secs(30);
/// How long an answer of "not raised" is believed before the feature set is read again.
const READ_GRACE_RECHECK: Duration = Duration::from_secs(30);
/// A held lease with less than this left is not used again: a read could outlive it.
const READ_LEASE_MIN_REMAINING: Duration = Duration::from_secs(5);

type LeaseKey = (String, String, u64, Vec<u8>);

#[derive(Debug)]
struct Held {
    handle: u64,
    deadline: Instant,
    used: u64,
    /// Reads running now that rely on this lease; it is never released under one.
    users: u32,
}

#[derive(Debug, Default)]
struct LeaseTable {
    held: std::collections::HashMap<LeaseKey, Held>,
    tick: u64,
}

enum Placed {
    /// Kept for later reads; these handles were dropped to make room and are to be released.
    Cached(Vec<u64>),
    /// No room (or an entry for the key is in use): the read releases it when it is done. These
    /// handles were dropped on the way and are to be released too.
    Alone(Vec<u64>),
}

/// Read leases kept by a gateway, shared by every connection it has.
///
/// In a catalog with publication fencing every ordinary read must hold a lease on the exact
/// manifest generation it reads, which was one durable open and one durable release per read.
/// A lease lasts 60 s, so reads of the same generation within that time can share one. The
/// table that holds leases on the metadata side is small (128 handles for the whole cluster), so
/// this keeps few: a hot set, least recently used out, and a lease is only dropped when no read
/// is using it. A lease that is not used again expires by itself.
#[derive(Debug)]
pub struct ReadLeaseCache {
    capacity: usize,
    /// How long a lease opened now is counted as lasting, a little under its 60 s.
    window: Duration,
    table: std::sync::Mutex<LeaseTable>,
    hits: std::sync::atomic::AtomicU64,
    opens: std::sync::atomic::AtomicU64,
}

impl Default for ReadLeaseCache {
    fn default() -> Self {
        Self::new(16)
    }
}

impl ReadLeaseCache {
    /// A cache of at most `capacity` leases; 0 keeps none (every read opens and releases).
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            window: Duration::from_millis(READ_LEASE_TTL_MS - 100),
            table: Default::default(),
            hits: Default::default(),
            opens: Default::default(),
        }
    }

    /// Reads that used a lease already held.
    pub fn hits(&self) -> u64 {
        self.hits.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Leases opened because none was held.
    pub fn opens(&self) -> u64 {
        self.opens.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Leases held now.
    pub fn held(&self) -> usize {
        self.table().held.len()
    }

    fn table(&self) -> std::sync::MutexGuard<'_, LeaseTable> {
        // Only whole values are assigned, so a panic elsewhere cannot leave this half-written.
        self.table.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(feature = "replica-server")]
    fn key(lease: &ReadLease) -> LeaseKey {
        (lease.bucket.clone(), lease.key.clone(), lease.generation, lease.digest.clone())
    }

    /// The deadline of the lease held for `key`, taken for one read, when it has time left.
    fn lookup(&self, key: &LeaseKey, now: Instant) -> Option<Instant> {
        let mut table = self.table();
        table.tick += 1;
        let tick = table.tick;
        let held = table.held.get_mut(key)?;
        if held.deadline < now + READ_LEASE_MIN_REMAINING {
            return None;
        }
        held.used = tick;
        held.users += 1;
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(held.deadline)
    }

    /// Keep the lease just opened for `key`, taken for one read.
    fn insert(&self, key: LeaseKey, handle: u64, deadline: Instant, now: Instant) -> Placed {
        self.opens.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut table = self.table();
        table.tick += 1;
        let tick = table.tick;
        // What expired is gone from the metadata side too.
        table.held.retain(|_, held| held.deadline > now || held.users > 0);
        if self.capacity == 0 || table.held.contains_key(&key) {
            return Placed::Alone(Vec::new());
        }
        let mut dropped = Vec::new();
        while table.held.len() >= self.capacity {
            let oldest = table
                .held
                .iter()
                .filter(|(_, held)| held.users == 0)
                .min_by_key(|(_, held)| held.used)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else {
                // Every lease is in use: nothing a reader holds is given up for this one.
                return Placed::Alone(dropped);
            };
            dropped.push(table.held.remove(&oldest).unwrap().handle);
        }
        table.held.insert(key, Held { handle, deadline, used: tick, users: 1 });
        Placed::Cached(dropped)
    }

    /// A read that used these leases has finished with them.
    fn finish(&self, keys: &[LeaseKey]) {
        let mut table = self.table();
        for key in keys {
            if let Some(held) = table.held.get_mut(key) {
                held.users = held.users.saturating_sub(1);
            }
        }
    }
}

impl RemoteMetaClient {
    /// Whether the cluster has raised the read grace feature. Raised bits stay raised, so a yes is
    /// kept; a no (or a failed read of the feature set) is asked again after a while.
    #[cfg(feature = "replica-server")]
    fn read_grace_raised(&mut self) -> bool {
        if let Some((true, _)) = self.read_grace {
            return true;
        }
        let now = Instant::now();
        if let Some((false, asked)) = self.read_grace {
            if now.duration_since(asked) < READ_GRACE_RECHECK {
                return false;
            }
        }
        let raised = self
            .read_cluster_features()
            .map(|features| features.has(crate::cluster_features::FEATURE_READ_GRACE))
            .unwrap_or(false);
        self.read_grace = Some((raised, now));
        raised
    }

    /// Run a lease-free read again once, after reading the manifest again, when it failed with
    /// corruption: a generation superseded and reclaimed under a slow reader looks exactly like
    /// that (the extent no longer matches the manifest), and the second attempt names the new
    /// generation. Corruption that is real fails the second attempt too.
    #[cfg(feature = "replica-server")]
    fn retry_stale_read<T>(
        &mut self,
        mut attempt: impl FnMut(&mut Self) -> Result<T, ObjectError>,
    ) -> Result<T, ObjectError> {
        match attempt(self) {
            Err(ObjectError::Corruption(_)) if matches!(self.read_grace, Some((true, _))) => attempt(self),
            other => other,
        }
    }

    /// Use `cache` for the read leases of every read this client makes.
    pub fn share_read_leases(&mut self, cache: std::sync::Arc<ReadLeaseCache>) {
        self.read_lease_cache = Some(cache);
    }

    fn observe_read_protection(&mut self, fence: u64) -> Result<(), ObjectError> {
        if self.read_protection_fence != 0 && fence != self.read_protection_fence {
            return Err(ObjectError::PreconditionFailed(
                "metadata read protection changed or disappeared".into(),
            ));
        }
        self.read_protection_fence = fence;
        Ok(())
    }

    /// Open a read lease on the generation and digest named, which the metaserver checks
    /// against its current manifest.
    #[cfg(feature = "replica-server")]
    fn open_leased_read_session(&mut self, lease: &ReadLease, ttl_ms: u64) -> Result<u64, ObjectError> {
        let digest = lease.digest.clone();
        let reply = self.retention_call(crate::manifest_retention::ManifestRetentionRequest {
            version: 3,
            action: 8,
            bucket: lease.bucket.clone(),
            key: lease.key.clone(),
            generation: lease.generation,
            ttl_ms,
            manifest_sha256: digest.clone(),
            ..Default::default()
        })?;
        if reply.version != 4 || reply.lease_ms != ttl_ms || reply.manifest_sha256 != digest {
            return Err(ObjectError::Corruption(
                "selected manifest grant changed identity or protection window".into(),
            ));
        }
        Ok(reply.retention_id)
    }

    #[cfg(feature = "replica-server")]
    fn with_selected_read_sessions<T>(
        &mut self,
        entries: Vec<(
            crate::object_manifest::ObjectManifest,
            Option<Vec<super::ChunkGroupRoute>>,
        )>,
        options: &super::ReplicaClientOptions,
        read: impl FnOnce(
            &mut Self,
            Vec<(
                crate::object_manifest::ObjectManifest,
                Option<Vec<super::ChunkGroupRoute>>,
            )>,
            &super::ReplicaClientOptions,
        ) -> Result<T, ObjectError>,
    ) -> Result<T, ObjectError> {
        if self.read_protection_fence == 0 {
            return read(self, entries, options);
        }
        let mut selected = std::collections::BTreeMap::new();
        let mut leases = Vec::new();
        for (manifest, _) in &entries {
            let identity = (&manifest.bucket, &manifest.key);
            if let Some(previous) = selected.insert(identity, manifest) {
                if previous != manifest {
                    return Err(ObjectError::Corruption(
                        "batch selected different generations of one object".into(),
                    ));
                }
                continue;
            }
            // The manifest was validated in full before this (`read_object_from_manifest_routes`),
            // so it is not validated again; the digest is the remembered one when this client
            // last digested this very manifest.
            leases.push(ReadLease {
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                generation: manifest.generation,
                digest: self.manifest_digest(manifest)?,
                digest_checked: manifest.format_version >= 2,
            });
        }
        self.with_read_leases(&leases, options, |client, bounded| read(client, entries, bounded))
    }

    /// Hold a read lease on each of `leases` while `read` runs, and release them after. A
    /// client with no read protection has nothing to hold and just reads.
    #[cfg(feature = "replica-server")]
    fn with_read_leases<T>(
        &mut self,
        leases: &[ReadLease],
        options: &super::ReplicaClientOptions,
        read: impl FnOnce(&mut Self, &super::ReplicaClientOptions) -> Result<T, ObjectError>,
    ) -> Result<T, ObjectError> {
        if self.read_protection_fence == 0 || leases.is_empty() {
            return read(self, options);
        }
        // Once the cluster has raised the read grace feature, a read that will be over within half
        // the grace window needs no lease: a superseded generation's pages outlive it. A read that
        // may run longer (no deadline, or a later one) takes a lease as before.
        if self.read_grace_raised() {
            let start = Instant::now();
            let short = self.short_read;
            if leases.iter().all(|lease| lease.digest_checked)
                && options.deadline.is_some_and(|deadline| deadline <= start + short)
            {
                let value = read(self, options)?;
                if start.elapsed() > short {
                    return Err(ObjectError::PreconditionFailed(
                        "a read ran past half the reclaim grace window; retry it".into(),
                    ));
                }
                return Ok(value);
            }
        }
        if let Some(cache) = self.read_lease_cache.clone() {
            if leases.len() <= cache.capacity {
                return self.with_cached_read_leases(&cache, leases, options, read);
            }
        }
        // Later grants cannot extend the first protected generation's window.
        const TTL_MS: u64 = READ_LEASE_TTL_MS;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(TTL_MS - 100))
            .ok_or_else(|| ObjectError::InvalidArgument("read session deadline overflow".into()))?;
        self.with_operation_deadline(Some(deadline), |client| {
            let mut bounded = options.clone();
            bounded.deadline = client.operation_deadline();
            let mut handles = Vec::new();
            let result = (|| {
                for lease in leases {
                    handles.push(client.open_leased_read_session(lease, TTL_MS)?);
                }
                client.check_operation_deadline()?;
                read(client, &bounded)
            })();
            // Payload I/O has ended. Uncertain cleanup leaves bounded leases.
            for handle in handles {
                let _ = client.release_manifest(handle);
            }
            result.and_then(|value| {
                client.check_operation_deadline()?;
                Ok(value)
            })
        })
    }

    /// `with_read_leases` for a gateway that keeps leases: a lease already held for exactly this
    /// generation and digest, with time left, is used again instead of opened, and a lease this
    /// read opens is kept for the next one instead of released. A kept lease is released when
    /// the cache evicts it, or left to expire. Leases are only ever released here when no read
    /// holds them.
    #[cfg(feature = "replica-server")]
    fn with_cached_read_leases<T>(
        &mut self,
        cache: &ReadLeaseCache,
        leases: &[ReadLease],
        options: &super::ReplicaClientOptions,
        read: impl FnOnce(&mut Self, &super::ReplicaClientOptions) -> Result<T, ObjectError>,
    ) -> Result<T, ObjectError> {
        let start = Instant::now();
        let opened_deadline = start
            .checked_add(cache.window)
            .ok_or_else(|| ObjectError::InvalidArgument("read session deadline overflow".into()))?;
        // `held`: leases this read holds in the cache. `own`: leases opened for this read alone,
        // because the cache had no room. `evicted`: handles the cache dropped to make room.
        let mut held: Vec<LeaseKey> = Vec::new();
        let mut own: Vec<u64> = Vec::new();
        let mut evicted: Vec<u64> = Vec::new();
        let mut deadline = opened_deadline;
        let mut misses = Vec::new();
        for lease in leases {
            let key = ReadLeaseCache::key(lease);
            match cache.lookup(&key, start) {
                Some(until) => {
                    deadline = deadline.min(until);
                    held.push(key);
                }
                None => misses.push((lease, key)),
            }
        }
        let result = self.with_operation_deadline(Some(deadline), |client| {
            let mut bounded = options.clone();
            bounded.deadline = client.operation_deadline();
            let mut opened = Vec::new();
            let mut failure = None;
            for (lease, key) in &misses {
                match client.open_leased_read_session(lease, READ_LEASE_TTL_MS) {
                    Ok(handle) => opened.push((key.clone(), handle)),
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
            if let Some(error) = failure {
                // Nothing is cached from a read that did not get all its leases.
                for (_, handle) in opened {
                    let _ = client.release_manifest(handle);
                }
                return Err(error);
            }
            for (key, handle) in opened {
                match cache.insert(key.clone(), handle, opened_deadline, start) {
                    Placed::Cached(dropped) => {
                        held.push(key);
                        evicted.extend(dropped);
                    }
                    Placed::Alone(dropped) => {
                        own.push(handle);
                        evicted.extend(dropped);
                    }
                }
            }
            let result = (|| {
                client.check_operation_deadline()?;
                read(client, &bounded)
            })();
            for handle in own.drain(..) {
                let _ = client.release_manifest(handle);
            }
            result.and_then(|value| {
                client.check_operation_deadline()?;
                Ok(value)
            })
        });
        cache.finish(&held);
        for handle in evicted {
            let _ = self.release_manifest(handle);
        }
        result
    }

    /// Read the bytes of `range` from the extents a ranged manifest reply carried, under a read
    /// lease on the digest the metaserver named for the whole manifest.
    #[cfg(feature = "replica-server")]
    fn read_selected_range(
        &mut self,
        entry: super::ManifestRangeEntry,
        max_bytes: usize,
        options: &super::ReplicaClientOptions,
        range: (u64, u64),
    ) -> Result<(crate::object_manifest::ObjectManifest, Vec<u8>), ObjectError> {
        self.check_operation_deadline()?;
        entry.manifest.validate_range_slice(range.0, range.1)?;
        let super::ManifestRangeEntry { manifest, manifest_sha256, routes, .. } = entry;
        // With nothing selected there is nothing to read, so nothing to protect.
        let leases = if manifest.extents.is_empty() {
            Vec::new()
        } else {
            vec![ReadLease {
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                generation: manifest.generation,
                digest: manifest_sha256,
                digest_checked: manifest.format_version >= 2,
            }]
        };
        self.with_read_leases(&leases, options, |client, bounded| {
            client.read_validated_manifest_routes(manifest, Some(routes), max_bytes, bounded, Some(range))
        })
    }

    #[cfg(feature = "replica-server")]
    fn read_selected_object(
        &mut self,
        manifest: crate::object_manifest::ObjectManifest,
        routes: Option<Vec<super::ChunkGroupRoute>>,
        max_bytes: usize,
        options: &super::ReplicaClientOptions,
        range: Option<(u64, u64)>,
    ) -> Result<(crate::object_manifest::ObjectManifest, Vec<u8>), ObjectError> {
        self.with_selected_read_sessions(
            vec![(manifest, routes)],
            options,
            |client, mut entries, bounded| {
                let (manifest, routes) = entries.pop().unwrap();
                client.read_object_from_manifest_routes(manifest, routes, max_bytes, bounded, range)
            },
        )
    }

    #[cfg(feature = "replica-server")]
    fn read_selected_batch(
        &mut self,
        entries: Vec<super::ManifestRouteEntry>,
        objects: &[(&str, &str, usize)],
        options: &super::ReplicaClientOptions,
    ) -> Result<Vec<(crate::object_manifest::ObjectManifest, Vec<u8>)>, ObjectError> {
        let entries = entries
            .into_iter()
            .map(|entry| (entry.manifest, Some(entry.routes)))
            .collect();
        self.with_selected_read_sessions(entries, options, |client, entries, bounded| {
            entries
                .into_iter()
                .zip(objects)
                .map(|((manifest, routes), (_, _, limit))| {
                    client.read_object_from_manifest_routes(manifest, routes, *limit, bounded, None)
                })
                .collect()
        })
    }
}

#[cfg(all(test, feature = "replica-server"))]
mod compact_selected_grants {
    use super::*;

    #[test]
    fn client_refuses_changed_digest_window_or_reply_kind() {
        for mode in 0..5 {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut header = [0; HEADER_BYTES];
                socket.read_exact(&mut header).unwrap();
                let mut payload =
                    vec![0; u32::from_be_bytes(header[2..].try_into().unwrap()) as usize];
                socket.read_exact(&mut payload).unwrap();
                let MetaRequest::ManifestRetention(request) =
                    MetaRequest::decode(MetaRequestCode::from_u8(header[1]).unwrap(), &payload)
                        .unwrap()
                else {
                    panic!()
                };
                assert_eq!(
                    (request.version, request.action, request.generation),
                    (3, 8, 1)
                );
                let mut reply = crate::manifest_retention::ManifestRetentionReply {
                    version: 4,
                    retention_id: 42,
                    expires_at_ms: 2000,
                    lease_ms: 1000,
                    manifest_sha256: request.manifest_sha256,
                    ..Default::default()
                };
                match mode {
                    1 => reply.manifest_sha256[0] ^= 1,
                    2 => reply.lease_ms = 2000,
                    3 => {
                        reply.version = 2;
                        reply.lease_ms = 0;
                        reply.manifest_sha256.clear();
                    }
                    4 => reply.retention_id = 0,
                    _ => {}
                }
                socket
                    .write_all(&super::super::meta_message::encode_meta_reply(
                        &MetaReply::ManifestRetention(reply),
                    ))
                    .unwrap();
            });
            let mut client =
                RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
            let manifest = crate::object_manifest::ObjectManifest {
                format_version: 2,
                bucket: "bucket".into(),
                key: "object".into(),
                generation: 1,
                ..Default::default()
            };
            let digest = client.manifest_digest(&manifest).unwrap();
            let lease = ReadLease {
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                generation: manifest.generation,
                digest,
                digest_checked: true,
            };
            let result = client.open_leased_read_session(&lease, 1000);
            if mode == 0 {
                assert_eq!(result.unwrap(), 42);
            } else {
                assert!(result.is_err());
            }
            worker.join().unwrap();
        }
    }
}

#[cfg(test)]
mod ordinary_read_protection {
    use super::*;
    #[test]
    fn policy_cannot_regress_across_connection_replacement() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let mut client =
            RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        client.observe_read_protection(0).unwrap();
        client.observe_read_protection(7).unwrap();
        let next = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        client.adopt_connection(next);
        assert!(client.observe_read_protection(0).is_err());
        assert!(client.observe_read_protection(8).is_err());
        client.observe_read_protection(7).unwrap();
        assert_eq!(client.read_protection_fence, 7);
    }
}

#[cfg(all(test, feature = "replica-server"))]
mod shared_read_leases {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// What a stand-in metaserver saw: (action, retention id or generation).
    type Seen = Arc<Mutex<Vec<(u32, u64)>>>;

    /// The action recorded for a read of the cluster feature set.
    const FEATURES: u32 = 100;

    struct Harness {
        client: RemoteMetaClient,
        seen: Seen,
        worker: std::thread::JoinHandle<()>,
    }

    impl Harness {
        fn new(cache: Option<Arc<ReadLeaseCache>>, fence: u64) -> Self {
            Self::with_features(cache, fence, 0)
        }

        /// A stand-in whose cluster has raised `raised` of the feature bits.
        fn with_features(cache: Option<Arc<ReadLeaseCache>>, fence: u64, raised: u64) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let seen: Seen = Arc::default();
            let log = Arc::clone(&seen);
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                let mut next_handle = 0u64;
                loop {
                    let mut header = [0; HEADER_BYTES];
                    if socket.read_exact(&mut header).is_err() {
                        return;
                    }
                    let mut payload =
                        vec![0; u32::from_be_bytes(header[2..].try_into().unwrap()) as usize];
                    socket.read_exact(&mut payload).unwrap();
                    let request =
                        MetaRequest::decode(MetaRequestCode::from_u8(header[1]).unwrap(), &payload)
                            .unwrap();
                    let request = match request {
                        MetaRequest::Features(_) => {
                            log.lock().unwrap().push((FEATURES, 0));
                            let features = crate::cluster_features::ClusterFeatures {
                                raised,
                                declared: Default::default(),
                            };
                            socket
                                .write_all(&super::super::meta_message::encode_meta_reply(
                                    &super::super::features_commands::reply(&features),
                                ))
                                .unwrap();
                            continue;
                        }
                        MetaRequest::ManifestRetention(request) => request,
                        other => panic!("unexpected request {other:?}"),
                    };
                    let reply = if request.action == 8 {
                        next_handle += 1;
                        log.lock().unwrap().push((8, request.generation));
                        crate::manifest_retention::ManifestRetentionReply {
                            version: 4,
                            retention_id: next_handle,
                            expires_at_ms: 60_000,
                            lease_ms: request.ttl_ms,
                            manifest_sha256: request.manifest_sha256,
                            ..Default::default()
                        }
                    } else {
                        assert_eq!(request.action, 2);
                        log.lock().unwrap().push((2, request.retention_id));
                        crate::manifest_retention::ManifestRetentionReply {
                            version: 1,
                            ..Default::default()
                        }
                    };
                    socket
                        .write_all(&super::super::meta_message::encode_meta_reply(
                            &MetaReply::ManifestRetention(reply),
                        ))
                        .unwrap();
                }
            });
            let mut client =
                RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
            client.read_protection_fence = fence;
            if let Some(cache) = cache {
                client.share_read_leases(cache);
            }
            Self { client, seen, worker }
        }

        fn read(&mut self, generation: u64, digest: u8) {
            let leases = [lease(generation, digest)];
            self.client
                .with_read_leases(&leases, &Default::default(), |_, _| Ok(()))
                .unwrap();
        }

        /// A read that is to be over by `deadline`.
        fn read_by(&mut self, generation: u64, digest: u8, deadline: Option<Instant>) {
            let leases = [lease(generation, digest)];
            let options = super::super::ReplicaClientOptions { deadline, ..Default::default() };
            self.client.with_read_leases(&leases, &options, |_, _| Ok(())).unwrap();
        }

        fn finish(self) -> Vec<(u32, u64)> {
            drop(self.client);
            self.worker.join().unwrap();
            let seen = self.seen.lock().unwrap().clone();
            seen
        }
    }

    fn lease(generation: u64, digest: u8) -> ReadLease {
        ReadLease {
            bucket: "bucket".into(),
            key: "object".into(),
            generation,
            digest: vec![digest; 32],
            digest_checked: true,
        }
    }

    fn feature_reads(seen: &[(u32, u64)]) -> usize {
        seen.iter().filter(|(action, _)| *action == FEATURES).count()
    }

    fn opens(seen: &[(u32, u64)]) -> usize {
        seen.iter().filter(|(action, _)| *action == 8).count()
    }

    fn releases(seen: &[(u32, u64)]) -> Vec<u64> {
        seen.iter().filter(|(action, _)| *action == 2).map(|(_, id)| *id).collect()
    }

    /// Reads of one generation share one lease: one open, no release, the rest are hits.
    #[test]
    fn reads_of_one_generation_share_one_lease() {
        let cache = Arc::new(ReadLeaseCache::new(4));
        let mut harness = Harness::new(Some(Arc::clone(&cache)), 1);
        for _ in 0..5 {
            harness.read(1, 7);
        }
        let seen = harness.finish();
        assert_eq!((opens(&seen), releases(&seen).len()), (1, 0), "{seen:?}");
        assert_eq!((cache.opens(), cache.hits(), cache.held()), (1, 4, 1));
    }

    /// Without a cache every read opens and releases, as before.
    #[test]
    fn without_a_cache_every_read_opens_and_releases() {
        let mut harness = Harness::new(None, 1);
        for _ in 0..3 {
            harness.read(1, 7);
        }
        let seen = harness.finish();
        assert_eq!((opens(&seen), releases(&seen).len()), (3, 3), "{seen:?}");
    }

    /// A lease is for exactly one generation and one digest.
    #[test]
    fn another_generation_or_digest_opens_another_lease() {
        let cache = Arc::new(ReadLeaseCache::new(8));
        let mut harness = Harness::new(Some(Arc::clone(&cache)), 1);
        harness.read(1, 7);
        harness.read(2, 7);
        harness.read(1, 8);
        harness.read(1, 7);
        let seen = harness.finish();
        assert_eq!(opens(&seen), 3, "{seen:?}");
        assert_eq!(cache.hits(), 1);
    }

    /// Past its capacity the cache drops the least recently used lease, and releases it.
    #[test]
    fn the_least_recently_used_lease_is_released_when_the_cache_is_full() {
        let cache = Arc::new(ReadLeaseCache::new(2));
        let mut harness = Harness::new(Some(Arc::clone(&cache)), 1);
        harness.read(1, 1); // handle 1
        harness.read(2, 1); // handle 2
        harness.read(3, 1); // handle 3: drops generation 1 (handle 1)
        harness.read(2, 1); // a hit that makes generation 3 the older
        harness.read(1, 1); // handle 4: drops generation 3 (handle 3)
        let seen = harness.finish();
        assert_eq!(opens(&seen), 4, "{seen:?}");
        assert_eq!(releases(&seen), vec![1, 3], "{seen:?}");
        assert_eq!((cache.hits(), cache.held()), (1, 2));
    }

    /// A lease a read is using is never released for another, and the other is released as soon
    /// as its own read is done.
    #[test]
    fn a_lease_in_use_is_not_dropped_for_another_read() {
        let cache = Arc::new(ReadLeaseCache::new(1));
        let mut harness = Harness::new(Some(Arc::clone(&cache)), 1);
        let first = [lease(1, 1)];
        let second = [lease(2, 1)];
        harness
            .client
            .with_read_leases(&first, &Default::default(), |client, options| {
                client.with_read_leases(&second, options, |_, _| Ok(()))
            })
            .unwrap();
        let seen = harness.finish();
        assert_eq!(opens(&seen), 2, "{seen:?}");
        // Handle 1 is the outer read's and stays; handle 2 was opened for the inner read alone.
        assert_eq!(releases(&seen), vec![2], "{seen:?}");
        assert_eq!(cache.held(), 1);
    }

    /// A read that needs more leases than the cache can hold does not use it.
    #[test]
    fn a_read_of_more_leases_than_the_cache_holds_opens_and_releases_them_all() {
        let cache = Arc::new(ReadLeaseCache::new(1));
        let mut harness = Harness::new(Some(Arc::clone(&cache)), 1);
        let both = [lease(1, 1), lease(2, 1)];
        harness
            .client
            .with_read_leases(&both, &Default::default(), |_, _| Ok(()))
            .unwrap();
        let seen = harness.finish();
        assert_eq!((opens(&seen), releases(&seen).len()), (2, 2), "{seen:?}");
        assert_eq!((cache.held(), cache.opens()), (0, 0));
    }

    /// A lease with too little time left is not used: the read opens another.
    #[test]
    fn a_lease_about_to_expire_is_opened_again() {
        let mut cache = ReadLeaseCache::new(4);
        cache.window = READ_LEASE_MIN_REMAINING + Duration::from_millis(300);
        let cache = Arc::new(cache);
        let mut harness = Harness::new(Some(Arc::clone(&cache)), 1);
        harness.read(1, 1);
        harness.read(1, 1);
        std::thread::sleep(Duration::from_millis(500));
        harness.read(1, 1);
        let seen = harness.finish();
        assert_eq!(opens(&seen), 2, "{seen:?}");
        assert_eq!(cache.hits(), 1);
    }

    /// In a catalog without publication fencing a read takes no lease at all.
    #[test]
    fn an_unfenced_catalog_takes_no_lease() {
        let cache = Arc::new(ReadLeaseCache::new(4));
        let mut harness = Harness::new(Some(Arc::clone(&cache)), 0);
        harness.read(1, 1);
        let seen = harness.finish();
        assert!(seen.is_empty(), "{seen:?}");
        assert_eq!(cache.held(), 0);
    }

    /// A connection that is replaced keeps the gateway's cache.
    #[test]
    fn a_replaced_connection_keeps_the_lease_cache() {
        let cache = Arc::new(ReadLeaseCache::new(4));
        let mut first = Harness::new(Some(Arc::clone(&cache)), 1);
        let second = Harness::new(None, 1);
        first.client.adopt_connection(second.client);
        assert!(first.client.read_lease_cache.is_some());
        first.read(1, 1);
        assert_eq!(cache.opens(), 1);
        drop(first.client);
    }

    /// With the cluster's read grace feature raised, reads that will be over within half the grace
    /// window take no lease at all: no open, no release, however many generations they read.
    #[test]
    fn a_short_read_takes_no_lease_once_the_grace_feature_is_raised() {
        let cache = Arc::new(ReadLeaseCache::new(4));
        let mut harness = Harness::with_features(
            Some(Arc::clone(&cache)),
            1,
            crate::cluster_features::FEATURE_READ_GRACE,
        );
        for generation in 1..=6 {
            harness.read_by(generation, 1, Some(Instant::now() + Duration::from_secs(5)));
        }
        let seen = harness.finish();
        assert_eq!((opens(&seen), releases(&seen).len()), (0, 0), "{seen:?}");
        assert_eq!(feature_reads(&seen), 1, "the feature set is read once, not per read: {seen:?}");
        assert_eq!(cache.opens(), 0);
    }

    /// Without the feature a short read takes a lease exactly as before (and shares it).
    #[test]
    fn without_the_grace_feature_a_short_read_takes_a_lease_as_before() {
        let cache = Arc::new(ReadLeaseCache::new(4));
        let mut harness = Harness::with_features(Some(Arc::clone(&cache)), 1, 0);
        for generation in [1, 1, 2, 2] {
            harness.read_by(generation, 1, Some(Instant::now() + Duration::from_secs(5)));
        }
        let seen = harness.finish();
        assert_eq!((opens(&seen), releases(&seen).len()), (2, 0), "{seen:?}");
        assert_eq!(feature_reads(&seen), 1, "a no is believed for a while: {seen:?}");
    }

    /// A read that may run past half the window (no deadline, or a later one) takes a lease even
    /// with the feature raised.
    #[test]
    fn a_read_that_may_run_long_takes_a_lease_even_with_the_grace_feature() {
        let mut harness = Harness::with_features(None, 1, crate::cluster_features::FEATURE_READ_GRACE);
        harness.read_by(1, 1, None);
        harness.read_by(2, 1, Some(Instant::now() + SHORT_READ + Duration::from_secs(15)));
        harness.read_by(3, 1, Some(Instant::now() + Duration::from_secs(5)));
        let seen = harness.finish();
        assert_eq!((opens(&seen), releases(&seen).len()), (2, 2), "{seen:?}");
    }

    /// A generation whose extents carry no published SHA-256 (manifest format 1) keeps its lease: the
    /// digest check is what makes a reused page fail instead of returning other data.
    #[test]
    fn a_generation_without_published_digests_keeps_its_lease_with_the_feature() {
        let mut harness = Harness::with_features(None, 1, crate::cluster_features::FEATURE_READ_GRACE);
        let legacy = [ReadLease { digest_checked: false, ..lease(1, 1) }];
        let options = super::super::ReplicaClientOptions {
            deadline: Some(Instant::now() + Duration::from_secs(5)),
            ..Default::default()
        };
        harness.client.with_read_leases(&legacy, &options, |_, _| Ok(())).unwrap();
        harness.read_by(2, 1, Some(Instant::now() + Duration::from_secs(5)));
        let seen = harness.finish();
        assert_eq!((opens(&seen), releases(&seen).len()), (1, 1), "{seen:?}");
    }

    /// A lease-free read that runs past the window fails with a retryable error and returns no bytes.
    #[test]
    fn a_read_that_overruns_half_the_grace_window_fails_retryably() {
        let mut harness = Harness::with_features(None, 1, crate::cluster_features::FEATURE_READ_GRACE);
        harness.client.short_read = Duration::from_millis(150);
        let leases = [lease(1, 1)];
        let options = super::super::ReplicaClientOptions {
            deadline: Some(Instant::now() + Duration::from_millis(100)),
            ..Default::default()
        };
        let result = harness.client.with_read_leases(&leases, &options, |_, _| {
            std::thread::sleep(Duration::from_millis(250));
            Ok(b"bytes".to_vec())
        });
        match result {
            Err(ObjectError::PreconditionFailed(message)) => assert!(message.contains("reclaim grace"), "{message}"),
            other => panic!("an overrunning read must fail retryably, got {other:?}"),
        }
        let seen = harness.finish();
        assert_eq!(opens(&seen), 0, "{seen:?}");
    }

    /// Corruption on a lease-free read is retried once (a generation reclaimed under a slow reader
    /// looks like it); real corruption fails the second attempt too. Without the feature there is
    /// no retry: a read under a lease cannot see a reclaimed generation.
    #[test]
    fn corruption_on_a_lease_free_read_is_retried_once() {
        let attempts = std::cell::Cell::new(0);
        let corrupt = |client: &mut RemoteMetaClient, then_ok: bool| {
            attempts.set(0);
            client.retry_stale_read(|_| {
                attempts.set(attempts.get() + 1);
                if then_ok && attempts.get() == 2 {
                    Ok(7)
                } else {
                    Err(ObjectError::Corruption("object extent does not match manifest".into()))
                }
            })
        };
        let mut raised = Harness::with_features(None, 1, crate::cluster_features::FEATURE_READ_GRACE);
        raised.client.read_grace = Some((true, Instant::now()));
        assert_eq!(corrupt(&mut raised.client, true).unwrap(), 7);
        assert_eq!(attempts.get(), 2);
        assert!(corrupt(&mut raised.client, false).is_err());
        assert_eq!(attempts.get(), 2, "one retry, not a loop");
        let mut plain = Harness::with_features(None, 1, 0);
        assert!(corrupt(&mut plain.client, true).is_err());
        assert_eq!(attempts.get(), 1, "no retry without the feature");
        let other = raised.client.retry_stale_read(|_| -> Result<u8, _> {
            attempts.set(attempts.get() + 10);
            Err(ObjectError::PreconditionFailed("not corruption".into()))
        });
        assert!(other.is_err());
        assert_eq!(attempts.get(), 11, "only corruption is retried");
    }
}
