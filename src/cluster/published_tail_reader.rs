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
            self.stabilize_publication_inner(epoch, initializations, &mut check_route)?;
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
        mut check_route: impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<Vec<u8>, ChunkLogError> {
        let (source, published, capacity) =
            self.stabilize_publication_inner(epoch, initializations, &mut check_route)?;
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
            let reply = self.read_call(source, |c| c.log_call(request.clone()))?;
            self.check_publication_reply(&reply, epoch, initializations[source])?;
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
    ) -> Result<(), ChunkLogError> {
        if reply.epoch != epoch {
            return Err(ChunkLogError::StaleEpoch {
                current: reply.epoch,
            });
        }
        if reply.publication_initialization() != Some(initialization) || !reply.publication_mode {
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
    ) -> Result<LogOpReply, ChunkLogError> {
        let request = LogOpRequest::identity_initialized_publication(
            self.volume,
            self.chunk,
            self.generation,
            offset,
            initialization,
        );
        let reply = self.read_call(member, |c| c.log_call(request.clone()))?;
        self.check_publication_reply(&reply, epoch, initialization)?;
        Ok(reply)
    }

    fn stabilize_publication_inner(
        &mut self,
        epoch: u64,
        initializations: &[[u8; 16]; LOG_REPLICAS],
        check_route: &mut impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<(usize, u64, usize), ChunkLogError> {
        check_route()?;
        if epoch == 0 || self.endpoints.len() != LOG_REPLICAS {
            return Err(refused(
                "publication read requires three members and a writer epoch",
            ));
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
        for (member, initialization) in initializations.iter().enumerate() {
            let request = LogOpRequest::publication_probe(self.volume, self.chunk, self.generation);
            let answer = match self.read_call(member, |c| c.log_call(request.clone())) {
                Ok(reply) => {
                    if reply.epoch > epoch {
                        return Err(ChunkLogError::StaleEpoch {
                            current: reply.epoch,
                        });
                    }
                    (reply.epoch == epoch
                        && reply
                            .initialized_publication_prefix(*initialization)
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
            if target == source || !answer.has_publication_write_protocol() {
                continue;
            }
            let mut route_error = None;
            let result = self.copy_publication_prefix(
                source,
                target,
                epoch,
                initializations,
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
        published: u64,
        check_route: &mut impl FnMut() -> Result<(), ChunkLogError>,
    ) -> Result<(), ChunkLogError> {
        // Replay from the beginning, including matching partial records. The destination
        // compares both identity and bytes on retries; no payload equality creates ownership.
        let mut offset = 0;
        while offset < published {
            let record =
                self.publication_identity_at(source, offset, epoch, initializations[source])?;
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
            let data = self.read_call(source, |c| c.log_call(request.clone()))?;
            self.check_publication_reply(&data, epoch, initializations[source])?;
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
            // A transport error is unknown. Only a later fresh probe may certify durability.
            let reply = self.client(target)?.log_call(request)?;
            self.check_publication_reply(&reply, epoch, initializations[target])?;
            offset = end;
        }
        let request = LogOpRequest::publication_probe(self.volume, self.chunk, self.generation);
        let reply = self.read_call(target, |c| c.log_call(request.clone()))?;
        self.check_publication_reply(&reply, epoch, initializations[target])?;
        if reply.published_length < published {
            return Err(refused("repair is not durable"));
        }
        check_route()?;
        Ok(())
    }
}
