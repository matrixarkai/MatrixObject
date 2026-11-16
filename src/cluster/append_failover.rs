// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! An append that keeps working when one data node stops answering.
//!
//! A routed Star group needs every member to acknowledge a write, so one stopped member makes
//! every attempt against that group fail. [`RemoteMetaClient::append_object_with_failover`]
//! wraps `append_object`: when an attempt is refused before publication it finds the members
//! of the groups the next attempt would use, reports the ones that do not answer as offline,
//! waits, and plans again. Placement then chooses another group. The attempt is rewritten in
//! full there and is acknowledged only when that group's full write quorum acknowledged it
//! and the manifest was conditionally published.
//!
//! # WHAT MAKES A RETRY SAFE
//!
//! Publication is conditional on the exact manifest the caller passed in. A failed attempt
//! has not published, so the manifest is unchanged, and a second attempt takes fresh page
//! identities from metadata. Only one of two attempts can ever publish, so an acknowledged
//! append is never duplicated. Pages of a refused attempt stay where they were written; they
//! belong to no manifest. They are returned to the caller in [`AbandonedAttempt`] for later
//! reclamation and are never deleted here.
//!
//! An attempt whose publication result is unknown (`PublicationUnconfirmed`) is never
//! retried here: the caller must inspect the manifest first.
//!
//! # THE SWITCH SCHEDULE
//!
//! The wait before the first switch is `initial_wait` and doubles for every further switch
//! within one call, up to `max_wait`, so a cluster-wide fault does not produce a burst of
//! simultaneous re-plans. The defaults are 1 s doubling to 64 s; tests shorten them.
use super::{
    ChunkGroupRoute, ObjectWriteFailure, ObjectWriteOptions, RemoteMetaClient, RemoteReplicaClient,
};
use crate::object_manifest::{ObjectExtent, ObjectManifest};
use crate::ObjectError;
use prost::Message;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long to wait before switching, and how often to switch, for one append.
#[derive(Clone, Debug)]
pub struct AppendFailoverPolicy {
    /// Wait before the first re-plan. Doubles with every further switch.
    pub initial_wait: Duration,
    /// Ceiling for the doubling wait.
    pub max_wait: Duration,
    /// Re-plans allowed after the first attempt. Zero disables switching.
    pub max_switches: u32,
    /// Report members that do not answer a probe as offline, so placement stops choosing
    /// their group before heartbeat expiry would. Their own heartbeat restores them.
    pub report_unreachable: bool,
    /// Append one durable record per refused attempt here before switching, so a later
    /// reclamation can find the pages. The record is written and synced first; if it cannot
    /// be written the append stops and returns the refusal rather than retry unrecorded.
    pub abandoned_log: Option<PathBuf>,
}

impl Default for AppendFailoverPolicy {
    fn default() -> Self {
        Self {
            initial_wait: Duration::from_secs(1),
            max_wait: Duration::from_secs(64),
            max_switches: 6,
            report_unreachable: true,
            abandoned_log: None,
        }
    }
}

impl AppendFailoverPolicy {
    /// The wait before switch number `switch` (zero based).
    pub fn wait_before(&self, switch: u32) -> Duration {
        self.initial_wait
            .saturating_mul(1u32.checked_shl(switch.min(30)).unwrap_or(u32::MAX))
            .min(self.max_wait)
    }
}

/// A refused attempt whose pages belong to no manifest.
#[derive(Clone, Debug)]
pub struct AbandonedAttempt {
    pub error: String,
    /// Pages the attempt wrote or may have written, if the attempt got far enough to name them.
    pub extents: Vec<ObjectExtent>,
    /// Members that did not answer the probe after the failure.
    pub unreachable_members: Vec<String>,
}

#[derive(Debug)]
pub struct FailoverAppend {
    pub manifest: ObjectManifest,
    /// Re-plans needed before the append was acknowledged.
    pub switches: u32,
    pub abandoned: Vec<AbandonedAttempt>,
}

#[derive(Debug)]
pub struct FailoverFailure {
    /// The error of the last attempt, with its own staged candidate or outcome.
    pub failure: ObjectWriteFailure,
    pub switches: u32,
    pub abandoned: Vec<AbandonedAttempt>,
}

impl std::fmt::Display for FailoverFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} after {} switch(es)", self.failure, self.switches)
    }
}
impl std::error::Error for FailoverFailure {}

/// One refused attempt, as stored in the abandoned-pages log. The pages belong to no manifest;
/// nothing deletes them. A reclamation pass must still confirm against live manifests,
/// retained generations and staged ownership before it frees anything.
#[derive(Clone, PartialEq, Message)]
pub struct AbandonedRecord {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub key: String,
    /// Generation of the manifest the attempt was to extend.
    #[prost(uint64, tag = "4")]
    pub expected_generation: u64,
    #[prost(string, tag = "5")]
    pub error: String,
    #[prost(message, repeated, tag = "6")]
    pub extents: Vec<ObjectExtent>,
    #[prost(string, repeated, tag = "7")]
    pub unreachable_members: Vec<String>,
}

const MAX_ABANDONED_RECORD_BYTES: usize = 4 * 1024 * 1024;

/// Frame: 4-byte big-endian length, protobuf body, 8-byte big-endian FNV-1a of the body.
fn append_abandoned_record(path: &Path, record: &AbandonedRecord) -> std::io::Result<()> {
    let body = record.encode_to_vec();
    if body.len() > MAX_ABANDONED_RECORD_BYTES {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "record too large"));
    }
    let mut frame = Vec::with_capacity(body.len() + 12);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&crate::fnv1a64(&body).to_be_bytes());
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&frame)?;
    file.sync_data()
}

/// Read every complete record. A torn final frame (a crash during the append) is ignored; a
/// complete frame whose checksum fails is damage and is reported, never skipped.
pub fn read_abandoned_log(path: &Path) -> Result<Vec<AbandonedRecord>, ObjectError> {
    let mut bytes = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut file) => {
            file.read_to_end(&mut bytes)
                .map_err(|e| ObjectError::Corruption(format!("abandoned log unreadable: {e}")))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(ObjectError::Corruption(format!("abandoned log unreadable: {e}"))),
    }
    let damaged = |why: &str| ObjectError::Corruption(format!("abandoned log damaged: {why}"));
    let mut records = Vec::new();
    let mut at = 0usize;
    while at + 4 <= bytes.len() {
        let length = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        if length > MAX_ABANDONED_RECORD_BYTES {
            return Err(damaged("record length exceeds bound"));
        }
        let end = at + 4 + length + 8;
        if end > bytes.len() {
            break; // torn tail
        }
        let body = &bytes[at + 4..at + 4 + length];
        let stored = u64::from_be_bytes(bytes[at + 4 + length..end].try_into().unwrap());
        if stored != crate::fnv1a64(body) {
            return Err(damaged("checksum mismatch"));
        }
        records.push(AbandonedRecord::decode(body).map_err(|_| damaged("undecodable record"))?);
        at = end;
    }
    Ok(records)
}

struct Probed {
    member_id: String,
    endpoint: String,
}

impl RemoteMetaClient {
    /// Append like `append_object`, switching to another group when a member stops answering.
    pub fn append_object_with_failover(
        &mut self,
        expected: ObjectManifest,
        data: &[u8],
        options: &ObjectWriteOptions,
        policy: &AppendFailoverPolicy,
    ) -> Result<FailoverAppend, FailoverFailure> {
        let mut abandoned = Vec::new();
        let mut switches = 0u32;
        loop {
            let failure = match self.append_object(expected.clone(), data, options) {
                Ok(manifest) => {
                    return Ok(FailoverAppend { manifest, switches, abandoned });
                }
                Err(failure) => failure,
            };
            let (error, staged) = match &failure {
                ObjectWriteFailure::PublicationUnconfirmed { .. } => {
                    return Err(FailoverFailure { failure, switches, abandoned });
                }
                ObjectWriteFailure::BeforePublication { error, staged } => {
                    (error.to_string(), staged.clone())
                }
            };
            // A conflict is the caller's to resolve; only a failure of the data path is retried.
            match self.read_manifest(&expected.bucket, &expected.key) {
                Ok(current) if current == expected => {}
                _ => return Err(FailoverFailure { failure, switches, abandoned }),
            }
            let unreachable = self.unreachable_next_members(&expected, data.len(), options, &staged);
            let attempt = AbandonedAttempt {
                error,
                // The staged candidate carries the object's existing extents first; only the
                // extents after them belong to this attempt.
                extents: staged
                    .map(|m| m.extents.into_iter().skip(expected.extents.len()).collect())
                    .unwrap_or_default(),
                unreachable_members: unreachable.iter().map(|m| m.member_id.clone()).collect(),
            };
            if let Some(path) = &policy.abandoned_log {
                let record = AbandonedRecord {
                    version: 1,
                    bucket: expected.bucket.clone(),
                    key: expected.key.clone(),
                    expected_generation: expected.generation,
                    error: attempt.error.clone(),
                    extents: attempt.extents.clone(),
                    unreachable_members: attempt.unreachable_members.clone(),
                };
                if append_abandoned_record(path, &record).is_err() {
                    abandoned.push(attempt);
                    return Err(FailoverFailure { failure, switches, abandoned });
                }
            }
            abandoned.push(attempt);
            if switches >= policy.max_switches {
                return Err(FailoverFailure { failure, switches, abandoned });
            }
            if policy.report_unreachable {
                for member in &unreachable {
                    let _ = self.report_member_offline(member);
                }
            }
            let wait = policy.wait_before(switches);
            if options
                .replica
                .deadline
                .is_some_and(|deadline| Instant::now() + wait >= deadline)
            {
                return Err(FailoverFailure { failure, switches, abandoned });
            }
            std::thread::sleep(wait);
            switches += 1;
        }
    }

    /// Members that do not answer, among the groups a new attempt would plan plus the groups
    /// the refused attempt named.
    fn unreachable_next_members(
        &mut self,
        expected: &ObjectManifest,
        length: usize,
        options: &ObjectWriteOptions,
        staged: &Option<Box<ObjectManifest>>,
    ) -> Vec<Probed> {
        let mut routes: Vec<ChunkGroupRoute> = Vec::new();
        let count = u64::from(options.target_groups.clamp(1, 32));
        let group_bytes = (length as u64).div_ceil(count).max(1);
        if let Ok((_, _, planned)) =
            self.plan_object_write_routes(&expected.bucket, &expected.key, group_bytes, count)
        {
            routes.extend(planned);
        }
        if let Some(staged) = staged {
            let mut ids: Vec<String> = staged.extents.iter().map(|e| e.group.clone()).collect();
            ids.sort();
            ids.dedup();
            for chunk in ids.chunks(32) {
                if let Ok(named) = self.read_chunk_routes(chunk) {
                    routes.extend(named);
                }
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
        self.report_chunk_node(
            &member.member_id,
            &member.endpoint,
            node.used_bytes,
            node.capacity_bytes,
            false,
        )
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_switch_wait_doubles_from_one_second_to_sixty_four_and_stops() {
        let policy = AppendFailoverPolicy::default();
        let waits: Vec<u64> = (0..9).map(|n| policy.wait_before(n).as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 64, 64, 64]);
        assert_eq!(policy.wait_before(u32::MAX), Duration::from_secs(64));
    }

    #[test]
    fn a_configured_schedule_is_honoured() {
        let policy = AppendFailoverPolicy {
            initial_wait: Duration::from_millis(5),
            max_wait: Duration::from_millis(30),
            ..Default::default()
        };
        let waits: Vec<u128> = (0..5).map(|n| policy.wait_before(n).as_millis()).collect();
        assert_eq!(waits, [5, 10, 20, 30, 30]);
    }
}
