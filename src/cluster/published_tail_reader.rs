// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Initialized active-tail reads. These operations require a pinned, authoritative route.
use super::*;

fn refused(message: &str) -> ChunkLogError {
    ChunkLogError::Refused(ObjectError::PreconditionFailed(message.to_owned()))
}

impl ChunkLogReader {
    /// Stabilize the largest complete prefix before exposing it to a reader.
    ///
    /// The caller must hold a metadata read pin for this generation and the supplied
    /// writer epoch throughout the call. Each initialization belongs to the ready storage
    /// incarnation at the corresponding endpoint. Replacement members must remain outside
    /// that ready set until caught up. `check_route` checks that pin before mutations and
    /// before returning; it does not acquire a pin or authorize a new allocation.
    ///
    /// This never initializes, fences or truncates a member. A lone complete prefix is
    /// copied, with its original record identities, to a second qualified member. Writes
    /// still require all three acknowledgments. This is not the legacy median-length read.
    pub fn stabilize_publication(
        &mut self,
        epoch: u64,
        initializations: &[[u8; 16]; LOG_REPLICAS],
        mut check_route: impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<u64, ChunkLogError> {
        let (_, length, _) =
            self.stabilize_publication_inner(epoch, initializations, None, None, &mut check_route)?;
        check_route()?;
        Ok(length)
    }

    /// Read one payload copy only after its complete prefix is durable on two qualified
    /// members. The same metadata pin required by `stabilize_publication` must cover the
    /// data read and final route check. No bytes above the captured complete prefix escape.
    pub fn read_published(
        &mut self,
        epoch: u64,
        initializations: &[[u8; 16]; LOG_REPLICAS],
        offset: u64,
        length: usize,
        check_route: impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<Vec<u8>, ChunkLogError> {
        self.read_publication_bound(
            epoch,
            initializations,
            None,
            None,
            offset,
            length,
            check_route,
        )
    }
    /// Read through an actual metadata read pin's physical binding. The caller maps the
    /// binding's configured member order to endpoints and checks pin validity throughout.
    /// Probes, identity checks, repair appends and payload reads use protocol5 exclusively.
    pub fn read_physical_pinned(
        &mut self,
        binding: &crate::initialized_log_manifest::PhysicalMemberBinding,
        offset: u64,
        length: usize,
        check_route: impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<Vec<u8>, ChunkLogError> {
        if self.volume != binding.volume_id
            || self.chunk != binding.chunk_id
            || self.generation != binding.generation
        {
            return Err(refused("physical pin does not bind this chunk"));
        }
        self.read_publication_bound(
            binding.writer_epoch,
            &binding.initializations,
            Some(&binding.storage_incarnations),
            None,
            offset,
            length,
            check_route,
        )
    }
    pub fn physical_published_length(
        &mut self,
        binding: &crate::initialized_log_manifest::PhysicalMemberBinding,
        mut check_route: impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<u64, ChunkLogError> {
        self.check_physical_coordinates(binding)?;
        let (_, length, _) = self.stabilize_publication_inner(
            binding.writer_epoch,
            &binding.initializations,
            Some(&binding.storage_incarnations),
            None,
            &mut check_route,
        )?;
        check_route()?;
        Ok(length)
    }
    pub fn read_physical_sealed(
        &mut self,
        binding: &crate::initialized_log_manifest::PhysicalMemberBinding,
        footer: &SealedFooter,
        offset: u64,
        length: usize,
        check_route: impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<Vec<u8>, ChunkLogError> {
        self.check_physical_coordinates(binding)?;
        self.read_publication_bound(
            binding.writer_epoch,
            &binding.initializations,
            Some(&binding.storage_incarnations),
            Some(footer),
            offset,
            length,
            check_route,
        )
    }
    fn check_physical_coordinates(
        &self,
        binding: &crate::initialized_log_manifest::PhysicalMemberBinding,
    ) -> Result<(), ChunkLogError> {
        if self.volume != binding.volume_id
            || self.chunk != binding.chunk_id
            || self.generation != binding.generation
        {
            return Err(refused("physical pin does not bind this chunk"));
        }
        Ok(())
    }
    fn read_publication_bound(
        &mut self,
        epoch: u64,
        initializations: &[[u8; 16]; LOG_REPLICAS],
        storage: Option<&[[u8; 16]; LOG_REPLICAS]>,
        footer: Option<&SealedFooter>,
        offset: u64,
        length: usize,
        mut check_route: impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<Vec<u8>, ChunkLogError> {
        let (source, published, capacity) = self.stabilize_publication_inner(
            epoch,
            initializations,
            storage,
            footer,
            &mut check_route,
        )?;
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| refused("read range overflow"))?;
        if end > published {
            return Err(refused("read exceeds complete prefix"));
        }
        let mut bytes = Vec::with_capacity(length);
        while bytes.len() < length {
            let count = (length - bytes.len()).min(capacity) as u32;
            let request = LogOpRequest::read_initialized_publication(
                self.volume,
                self.chunk,
                self.generation,
                offset + bytes.len() as u64,
                count,
                initializations[source],
            );
            let request = bind_storage(request, source, initializations, storage);
            let reply = self.read_call(source, |c| c.log_call(request.clone()))?;
            self.check_publication_reply(
                &reply,
                epoch,
                initializations[source],
                storage.map(|r| r[source]),
            )?;
            if footer.is_some_and(|footer| !matches_footer(&reply, footer)) {
                return Err(refused("sealed physical read footer changed"));
            }
            if reply.data.len() != count as usize {
                return Err(refused("short complete-prefix read"));
            }
            bytes.extend_from_slice(&reply.data);
        }
        check_route()?;
        Ok(bytes)
    }

    fn check_publication_reply(
        &self,
        reply: &LogOpReply,
        epoch: u64,
        initialization: [u8; 16],
        storage: Option<[u8; 16]>,
    ) -> Result<(), ChunkLogError> {
        if reply.epoch != epoch {
            return Err(ChunkLogError::StaleEpoch {
                current: reply.epoch,
            });
        }
        if (storage.is_some() && reply.storage_incarnation() != storage)
            || (storage.is_none() && reply.publication_protocol != 4)
            || reply.publication_initialization() != Some(initialization)
            || !reply.publication_mode
        {
            return Err(refused("unqualified publication member"));
        }
        Ok(())
    }

    fn publication_identity_at(
        &mut self,
        member: usize,
        offset: u64,
        epoch: u64,
        initialization: [u8; 16],
        storage: Option<[u8; 16]>,
    ) -> Result<LogOpReply, ChunkLogError> {
        let request = LogOpRequest::identity_initialized_publication(
            self.volume,
            self.chunk,
            self.generation,
            offset,
            initialization,
        );
        let request = storage.map_or_else(
            || request.clone(),
            |root| request.clone().for_storage(root, initialization),
        );
        let reply = self.read_call(member, |c| c.log_call(request.clone()))?;
        self.check_publication_reply(&reply, epoch, initialization, storage)?;
        Ok(reply)
    }

    // Start every qualified member's read-only probe before waiting for any reply.
    // Still collect all replies: a later member may report a newer writer epoch.
    fn publication_probes(
        &mut self,
        initializations: &[[u8; 16]; LOG_REPLICAS],
        storage: Option<&[[u8; 16]; LOG_REPLICAS]>,
    ) -> [Result<LogOpReply, LogCallError>; LOG_REPLICAS] {
        let request = LogOpRequest::publication_probe(self.volume, self.chunk, self.generation);
        let requests: [LogOpRequest; LOG_REPLICAS] = std::array::from_fn(|member| {
            bind_storage(request.clone(), member, initializations, storage)
        });
        let encode = |request: &LogOpRequest| {
            std::sync::Arc::new(crate::cluster::message::encode_request(
                &crate::cluster::message::ReplicaRequest::LogOp(request.clone()),
            ))
        };
        let first = encode(&requests[0]);
        let frames: [std::sync::Arc<Vec<u8>>; LOG_REPLICAS] = std::array::from_fn(|member| {
            if storage.is_none() || member == 0 {
                first.clone()
            } else {
                encode(&requests[member])
            }
        });
        let reused: [bool; LOG_REPLICAS] = std::array::from_fn(|i| self.clients[i].is_some());
        let mut failures: [Option<LogCallError>; LOG_REPLICAS] = std::array::from_fn(|_| None);
        for (member, failure) in failures.iter_mut().enumerate() {
            if let Err(error) = self.client(member) {
                *failure = Some(error);
            }
        }
        let mut replies: [Option<Result<crate::cluster::message::ReplicaReply, ObjectError>>;
            LOG_REPLICAS] = std::array::from_fn(|_| None);
        {
            let mut pending: [Option<crate::cluster::client::PendingReplicaReply<'_>>;
                LOG_REPLICAS] = std::array::from_fn(|_| None);
            for (member, client) in self.clients.iter_mut().enumerate() {
                if failures[member].is_some() {
                    continue;
                }
                match client.as_mut().unwrap().begin_encoded_before(
                    crate::cluster::wire::RequestCode::LogOp,
                    frames[member].clone(),
                    self.options.deadline,
                ) {
                    Ok(reply) => pending[member] = Some(reply),
                    Err(error) => replies[member] = Some(Err(error)),
                }
            }
            while pending.iter().any(Option::is_some) {
                let mut progress = false;
                for (member, slot) in pending.iter_mut().enumerate() {
                    let result = slot.as_mut().and_then(|reply| {
                        let before = reply.transferred_bytes();
                        let result = reply.poll();
                        progress |= result.is_some() || reply.transferred_bytes() != before;
                        result
                    });
                    if let Some(result) = result {
                        replies[member] = Some(result);
                        *slot = None;
                    }
                }
                if !progress {
                    crate::cluster::replica_set::wait_for_pending(&pending);
                }
            }
        }
        std::array::from_fn(|member| {
            if let Some(error) = failures[member].take() {
                return Err(error);
            }
            let result = match replies[member].take().unwrap() {
                Ok(crate::cluster::message::ReplicaReply::LogOp(reply)) => Ok(reply),
                Ok(_) => Err(LogCallError::Refused(ObjectError::Corruption(
                    "unexpected publication probe reply".into(),
                ))),
                Err(error) => Err(LogCallError::from_object(
                    error,
                    self.clients[member]
                        .as_ref()
                        .unwrap()
                        .needs_standalone_reconnect(),
                )),
            };
            if matches!(result, Err(LogCallError::Transport(_))) {
                self.clients[member] = None;
                if reused[member] {
                    // Match read_call: retry an idle connection once, only for this read.
                    return self
                        .client(member)
                        .and_then(|client| client.log_call(requests[member].clone()));
                }
            }
            result
        })
    }

    fn stabilize_publication_inner(
        &mut self,
        epoch: u64,
        initializations: &[[u8; 16]; LOG_REPLICAS],
        storage: Option<&[[u8; 16]; LOG_REPLICAS]>,
        footer: Option<&SealedFooter>,
        check_route: &mut impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<(usize, u64, usize), ChunkLogError> {
        check_route()?;
        if epoch == 0 || self.endpoints.len() != LOG_REPLICAS {
            return Err(refused(
                "publication read requires three members and a writer epoch",
            ));
        }
        if let Some(roots) = storage {
            let mut ids = std::collections::HashSet::new();
            let mut tags = std::collections::HashSet::new();
            if roots.iter().any(|r| *r == [0; 16] || !ids.insert(*r))
                || initializations
                    .iter()
                    .any(|r| *r == [0; 16] || !tags.insert(*r))
            {
                return Err(refused(
                    "physical read needs distinct nonzero member identities",
                ));
            }
        }
        let mut addresses = std::collections::HashSet::new();
        for endpoint in &self.endpoints {
            let address: std::net::SocketAddr = endpoint
                .parse()
                .map_err(|_| refused("publication member must have a socket address"))?;
            if address.port() == 0
                || address.ip().is_unspecified()
                || address.ip().is_multicast()
                || matches!(address.ip(), std::net::IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some())
                || !addresses.insert(address)
            {
                return Err(refused(
                    "publication members must have distinct usable addresses",
                ));
            }
        }
        let mut answers = Vec::with_capacity(LOG_REPLICAS);
        for (member, result) in self
            .publication_probes(initializations, storage)
            .into_iter()
            .enumerate()
        {
            let initialization = initializations[member];
            let answer = match result {
                Ok(reply) => {
                    if reply.epoch > epoch {
                        return Err(ChunkLogError::StaleEpoch {
                            current: reply.epoch,
                        });
                    }
                    (reply.epoch == epoch
                        && footer.is_none_or(|footer| matches_footer(&reply, footer))
                        && match storage {
                            Some(roots) => reply.storage_incarnation() == Some(roots[member]),
                            None => reply.publication_protocol == 4,
                        }
                        && reply
                            .initialized_publication_prefix(initialization)
                            .is_some())
                    .then_some(reply)
                }
                Err(LogCallError::Transport(_)) => {
                    self.clients[member] = None;
                    None
                }
                Err(_) => None,
            };
            answers.push(answer);
        }
        if answers.iter().flatten().count() < LOG_QUORUM {
            return Err(ChunkLogError::NoQuorum {
                wanted: 1,
                durable: 0,
            });
        }
        let source = answers
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.as_ref().map(|r| (i, r.published_length)))
            .max_by_key(|(_, length)| *length)
            .unwrap()
            .0;
        let maximum = answers[source].as_ref().unwrap();
        let published = maximum.published_length;
        let capacity = usize::try_from(maximum.publication_payload_limit)
            .ok()
            .and_then(|n| n.checked_sub(256))
            .filter(|n| *n > 0)
            .ok_or_else(|| refused("invalid publication read capacity"))?
            .min(MAX_OP_DATA);
        let tip = maximum.record_identity();
        let mut copies = 0;
        for answer in &answers {
            let Some(answer) = answer else { continue };
            if answer.published_length == published {
                if answer.record_identity() != tip {
                    return Err(ChunkLogError::AttemptMismatch { offset: published });
                }
                copies += 1;
            } else if let Some(short_tip) = answer.record_identity() {
                let observed = self.publication_identity_at(
                    source,
                    short_tip.offset,
                    epoch,
                    initializations[source],
                    storage.map(|r| r[source]),
                )?;
                if observed.record_identity() != Some(short_tip)
                    || !observed.record_publication
                    || observed.record_published_length != answer.published_length
                {
                    return Err(ChunkLogError::AttemptMismatch {
                        offset: short_tip.offset,
                    });
                }
            }
        }
        if copies >= LOG_QUORUM {
            return Ok((source, published, capacity));
        }
        for target in 0..LOG_REPLICAS {
            let Some(answer) = answers[target].as_ref() else {
                continue;
            };
            let writable = match storage {
                Some(roots) => answer.has_storage_write_protocol(roots[target]),
                None => answer.has_publication_write_protocol(),
            };
            if target == source || !writable {
                continue;
            }
            let mut route_error = None;
            let result = self.copy_publication_prefix(
                source,
                target,
                epoch,
                initializations,
                storage,
                published,
                &mut || {
                    let result = check_route();
                    if let Err(error) = &result {
                        route_error = Some(error.clone());
                    }
                    result
                },
            );
            if let Some(error) = route_error {
                return Err(error);
            }
            match result {
                Ok(()) => return Ok((source, published, capacity)),
                Err(ChunkLogError::StaleEpoch { current }) => {
                    return Err(ChunkLogError::StaleEpoch { current })
                }
                Err(ChunkLogError::AttemptMismatch { offset }) => {
                    return Err(ChunkLogError::AttemptMismatch { offset })
                }
                Err(_) => {
                    check_route()?;
                }
            }
        }
        Err(ChunkLogError::NoQuorum {
            wanted: published,
            durable: 0,
        })
    }

    fn copy_publication_prefix(
        &mut self,
        source: usize,
        target: usize,
        epoch: u64,
        initializations: &[[u8; 16]; LOG_REPLICAS],
        storage: Option<&[[u8; 16]; LOG_REPLICAS]>,
        published: u64,
        check_route: &mut impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<(), ChunkLogError> {
        // Replay from the beginning, including matching partial records. The destination
        // compares both identity and bytes on retries; no payload equality creates ownership.
        let mut offset = 0;
        while offset < published {
            let record = self.publication_identity_at(
                source,
                offset,
                epoch,
                initializations[source],
                storage.map(|r| r[source]),
            )?;
            let identity = record
                .record_identity()
                .ok_or(ChunkLogError::AttemptMismatch { offset })?;
            let end = offset
                .checked_add(identity.length as u64)
                .ok_or_else(|| refused("record range overflow"))?;
            if identity.offset != offset
                || identity.length == 0
                || end > published
                || !record.record_publication
                || record.record_published_length > published
            {
                return Err(ChunkLogError::AttemptMismatch { offset });
            }
            let tag = identity
                .attempt_tag
                .ok_or(ChunkLogError::AttemptMismatch { offset })?;
            let request = LogOpRequest::read_initialized_publication(
                self.volume,
                self.chunk,
                self.generation,
                offset,
                identity.length,
                initializations[source],
            );
            let request = bind_storage(request, source, initializations, storage);
            let data = self.read_call(source, |c| c.log_call(request.clone()))?;
            self.check_publication_reply(
                &data,
                epoch,
                initializations[source],
                storage.map(|r| r[source]),
            )?;
            if data.data.len() != identity.length as usize {
                return Err(refused("short repair record"));
            }
            check_route()?;
            let request = LogOpRequest::append_published(
                self.volume,
                self.chunk,
                self.generation,
                epoch,
                offset,
                data.data,
                tag,
                record.record_published_length,
                initializations[target],
            );
            let request = bind_storage(request, target, initializations, storage);
            // A transport error is unknown. Only a later fresh probe may certify durability.
            let reply = self.client(target)?.log_call(request)?;
            self.check_publication_reply(
                &reply,
                epoch,
                initializations[target],
                storage.map(|r| r[target]),
            )?;
            offset = end;
        }
        let request = LogOpRequest::publication_probe(self.volume, self.chunk, self.generation);
        let request = bind_storage(request, target, initializations, storage);
        let reply = self.read_call(target, |c| c.log_call(request.clone()))?;
        self.check_publication_reply(
            &reply,
            epoch,
            initializations[target],
            storage.map(|r| r[target]),
        )?;
        if reply.published_length < published {
            return Err(refused("repair is not durable"));
        }
        check_route()?;
        Ok(())
    }
}

fn bind_storage(
    request: LogOpRequest,
    member: usize,
    initializations: &[[u8; 16]; LOG_REPLICAS],
    storage: Option<&[[u8; 16]; LOG_REPLICAS]>,
) -> LogOpRequest {
    match storage {
        Some(roots) => request.for_storage(roots[member], initializations[member]),
        None => request,
    }
}

fn matches_footer(reply: &LogOpReply, footer: &SealedFooter) -> bool {
    reply.sealed
        && reply.length == footer.length
        && reply.published_length == footer.length
        && reply.crc == footer.crc
        && reply.sha256 == footer.sha256
}
