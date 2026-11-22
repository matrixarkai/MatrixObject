// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A chunk object write or append that keeps working when one data node stops answering.
//!
//! A routed Star group needs every member to acknowledge a write, so one stopped member makes every
//! attempt against that group fail. [`RemoteMetaClient::write_chunk_object_with_failover`] and
//! [`RemoteMetaClient::append_chunk_object_with_failover`] wrap the plain calls: when an attempt is
//! refused before publication they find the members of the groups the next attempt would use, report
//! the ones that do not answer as offline, wait, and try again. Placement then chooses another group.
//!
//! # WHAT MAKES A RETRY SAFE
//!
//! Publication is conditional on the exact generation the caller passed in. A failed attempt has not
//! published, so the object is unchanged, and the next attempt takes fresh pages from the metadata
//! plane. Only one of two attempts can ever publish, so an acknowledged write is never duplicated. The
//! pages of a refused attempt belong to no manifest; the reclaim sweep frees them once they have been
//! unnamed for its grace. An attempt whose publication result is unknown is never retried here: the
//! caller must read the object first. A refusal the caller caused (an invalid request, a generation
//! that moved) is returned as it is, without waiting.
//!
//! # THE SWITCH SCHEDULE
//!
//! The wait before the first switch is `initial_wait` and doubles for every further switch within one
//! call, up to `max_wait`, so a cluster-wide fault does not produce a burst of simultaneous re-plans.
//! The defaults are 1 s doubling to 64 s; tests shorten them.
use super::{ChunkGroupRoute, ChunkWriteFailure, ObjectWriteOptions, RemoteMetaClient, RemoteReplicaClient};
use crate::chunk_manifest::ChunkManifest;
use crate::ObjectError;
use std::time::{Duration, Instant};

/// How long to wait before switching, and how often to switch, for one call.
#[derive(Clone, Debug)]
pub struct FailoverPolicy {
    /// Wait before the first re-plan. Doubles with every further switch.
    pub initial_wait: Duration,
    /// Ceiling for the doubling wait.
    pub max_wait: Duration,
    /// Re-plans allowed after the first attempt. Zero disables switching.
    pub max_switches: u32,
    /// Report members that do not answer a probe as offline, so placement stops choosing their group
    /// before heartbeat expiry would. Their own heartbeat restores them.
    pub report_unreachable: bool,
}

impl Default for FailoverPolicy {
    fn default() -> Self {
        Self {
            initial_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(64),
            max_switches: 6,
            report_unreachable: true,
        }
    }
}

impl FailoverPolicy {
    /// The wait before switch number `switch` (zero based).
    pub fn wait_before(&self, switch: u32) -> Duration {
        self.initial_wait
            .saturating_mul(1u32.checked_shl(switch.min(30)).unwrap_or(u32::MAX))
            .min(self.max_wait)
    }
}

/// A write or append that was acknowledged.
#[derive(Debug)]
pub struct ChunkFailover {
    pub manifest: ChunkManifest,
    /// Re-plans needed before it was acknowledged.
    pub switches: u32,
}

/// A write or append that was not acknowledged.
#[derive(Debug)]
pub struct ChunkFailoverFailure {
    /// The error of the last attempt.
    pub failure: ChunkWriteFailure,
    pub switches: u32,
}

impl std::fmt::Display for ChunkFailoverFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} after {} switch(es)", self.failure, self.switches)
    }
}
impl std::error::Error for ChunkFailoverFailure {}

struct Probed {
    member_id: String,
    endpoint: String,
}

/// A refusal that is about the request or the object, not about a data node: retrying cannot help.
fn is_the_callers(error: &ObjectError) -> bool {
    matches!(
        error,
        ObjectError::InvalidArgument(_) | ObjectError::PreconditionFailed(_) | ObjectError::NotFound(_)
    )
}

impl RemoteMetaClient {
    /// Write a chunk object like `write_chunk_object`, switching to another group when a member stops answering.
    pub fn write_chunk_object_with_failover(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<&ChunkManifest>,
        data: &[u8],
        options: &ObjectWriteOptions,
        stripe_bytes: u64,
        policy: &FailoverPolicy,
    ) -> Result<ChunkFailover, ChunkFailoverFailure> {
        self.with_failover(bucket, key, expected.map(|manifest| manifest.generation), data.len(), options, policy, |client| {
            client.write_chunk_object(bucket, key, expected, data, options, stripe_bytes)
        })
    }

    /// Append to a chunk object like `append_chunk_object`, switching to another group when a member stops answering.
    pub fn append_chunk_object_with_failover(
        &mut self,
        expected: &ChunkManifest,
        data: &[u8],
        options: &ObjectWriteOptions,
        stripe_bytes: u64,
        policy: &FailoverPolicy,
    ) -> Result<ChunkFailover, ChunkFailoverFailure> {
        self.with_failover(&expected.bucket, &expected.key, Some(expected.generation), data.len(), options, policy, |client| {
            client.append_chunk_object(expected, data, options, stripe_bytes)
        })
    }

    fn with_failover<F>(
        &mut self,
        bucket: &str,
        key: &str,
        generation: Option<u64>,
        length: usize,
        options: &ObjectWriteOptions,
        policy: &FailoverPolicy,
        mut attempt: F,
    ) -> Result<ChunkFailover, ChunkFailoverFailure>
    where
        F: FnMut(&mut Self) -> Result<ChunkManifest, ChunkWriteFailure>,
    {
        let mut switches = 0u32;
        loop {
            let failure = match attempt(self) {
                Ok(manifest) => return Ok(ChunkFailover { manifest, switches }),
                Err(failure) => failure,
            };
            let error = match &failure {
                ChunkWriteFailure::PublicationUnconfirmed { .. } => return Err(ChunkFailoverFailure { failure, switches }),
                ChunkWriteFailure::BeforePublication(error) if is_the_callers(error) => {
                    return Err(ChunkFailoverFailure { failure, switches })
                }
                ChunkWriteFailure::BeforePublication(error) => error.to_string(),
            };
            let _ = error;
            // A conflict is the caller's to resolve; only a failure of the data path is retried.
            match self.read_chunk_manifest(bucket, key) {
                Ok(current) if Some(current.generation) == generation => {}
                Err(ObjectError::NotFound(_)) if generation.is_none() => {}
                _ => return Err(ChunkFailoverFailure { failure, switches }),
            }
            if switches >= policy.max_switches {
                return Err(ChunkFailoverFailure { failure, switches });
            }
            if policy.report_unreachable {
                for member in self.unreachable_next_members(bucket, key, length, options) {
                    let _ = self.report_member_offline(&member);
                }
            }
            let wait = policy.wait_before(switches);
            if options.replica.deadline.is_some_and(|deadline| Instant::now() + wait >= deadline) {
                return Err(ChunkFailoverFailure { failure, switches });
            }
            std::thread::sleep(wait);
            switches += 1;
        }
    }

    /// Members that do not answer, among the groups a new attempt would plan.
    fn unreachable_next_members(&mut self, bucket: &str, key: &str, length: usize, options: &ObjectWriteOptions) -> Vec<Probed> {
        let count = u64::from(options.target_groups.clamp(1, 32));
        let group_bytes = (length as u64).div_ceil(count).max(1);
        let mut routes: Vec<ChunkGroupRoute> = Vec::new();
        if let Ok(plan) = self.plan_object_placement(bucket, key, group_bytes, count) {
            if let Ok(planned) = self.read_chunk_routes(&plan.node_ids) {
                routes.extend(planned);
            }
        }
        let mut probe = options.replica.clone();
        probe.deadline = None;
        probe.connect_timeout = probe.connect_timeout.min(Duration::from_secs(1));
        probe.read_timeout = probe.read_timeout.min(Duration::from_secs(1));
        let mut seen = std::collections::BTreeSet::new();
        let mut unreachable = Vec::new();
        for route in routes {
            let Some(group) = route.group.as_ref() else { continue };
            for (member_id, endpoint) in group.member_ids.iter().zip(&group.api_endpoints) {
                if !seen.insert(member_id.clone()) {
                    continue;
                }
                let alive = RemoteReplicaClient::connect(endpoint, &probe)
                    .and_then(|mut client| client.ping().map(|_| ()))
                    .is_ok();
                if !alive {
                    unreachable.push(Probed { member_id: member_id.clone(), endpoint: endpoint.clone() });
                }
            }
        }
        unreachable
    }

    fn report_member_offline(&mut self, member: &Probed) -> Result<(), ObjectError> {
        let nodes = self.list_placement_nodes()?;
        let node = nodes
            .iter()
            .find(|node| node.node_id == member.member_id)
            .ok_or_else(|| ObjectError::NotFound("group member is not in the roster".into()))?;
        self.report_chunk_node(&member.member_id, &member.endpoint, node.used_bytes, node.capacity_bytes, false)
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_switch_wait_doubles_from_one_second_to_sixty_four_and_stops() {
        let policy = FailoverPolicy::default();
        let waits: Vec<u64> = (0..9).map(|n| policy.wait_before(n).as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 64, 64, 64]);
        assert_eq!(policy.wait_before(u32::MAX), Duration::from_secs(64));
    }

    #[test]
    fn a_refusal_the_caller_caused_is_not_retried() {
        assert!(is_the_callers(&ObjectError::InvalidArgument("x".into())));
        assert!(is_the_callers(&ObjectError::PreconditionFailed("x".into())));
        assert!(is_the_callers(&ObjectError::NotFound("x".into())));
        assert!(!is_the_callers(&ObjectError::Corruption("x".into())));
    }
}
