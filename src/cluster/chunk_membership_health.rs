// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded, recoverable health observations for a fenced chunk configuration.
use super::chunk_groups::{project_voter_health, VoterHealth};
use super::{ChunkGroupIdentity, ChunkMembershipFence, MembershipStatus};
use crate::ObjectError;
use prost::Message;
use std::collections::BTreeMap;

#[derive(Clone, PartialEq, Eq, Message)]
struct Report {
    #[prost(uint64, tag = "1")]
    node_id: u64,
    #[prost(uint64, tag = "2")]
    applied_index: u64,
    #[prost(uint64, tag = "3")]
    observed_at_ms: u64,
    #[prost(uint64, tag = "4")]
    used_bytes: u64,
    #[prost(bool, tag = "5")]
    online: bool,
    #[prost(bool, tag = "6")]
    draining: bool,
}
#[derive(Clone, PartialEq, Message)]
struct Image {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    fence: Vec<u8>,
    #[prost(message, repeated, tag = "3")]
    reports: Vec<Report>,
}

/// Health state prepared for inclusion in a committed metadata catalog update.
/// The caller supplies authenticated reports and a metadata-decided observation time.
/// Mutating this value alone does not commit it to metadata consensus.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkMembershipHealth {
    fence: ChunkMembershipFence,
    reports: BTreeMap<u64, Report>,
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
impl ChunkMembershipHealth {
    pub fn new(fence: ChunkMembershipFence) -> Self {
        Self {
            fence,
            reports: BTreeMap::new(),
        }
    }
    pub fn fence(&self) -> &ChunkMembershipFence {
        &self.fence
    }

    /// Install a newer quorum-confirmed membership observation. Reports below its
    /// applied floor, and all reports when the configuration changes, are discarded.
    pub fn advance(
        &mut self,
        identity: &ChunkGroupIdentity,
        status: &MembershipStatus,
    ) -> Result<(), ObjectError> {
        let next = self.fence.advance(identity, status)?;
        if next.voters() != self.fence.voters()
            || next.outgoing_voters() != self.fence.outgoing_voters()
            || next.learners() != self.fence.learners()
        {
            self.reports.clear();
        } else {
            self.reports
                .retain(|_, report| report.applied_index >= next.applied_floor());
        }
        self.fence = next;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        identity: &ChunkGroupIdentity,
        expected_node: u64,
        reported_bootstrap: u64,
        status: &MembershipStatus,
        observed_at_ms: u64,
        used_bytes: u64,
        online: bool,
        draining: bool,
    ) -> Result<(), ObjectError> {
        self.fence
            .check_report(identity, expected_node, reported_bootstrap, status)?;
        let next = Report {
            node_id: expected_node,
            applied_index: status.applied_index,
            observed_at_ms,
            used_bytes,
            online,
            draining,
        };
        self.validate_report(identity, &next)?;
        if let Some(previous) = self.reports.get(&expected_node) {
            if next.applied_index < previous.applied_index
                || next.observed_at_ms < previous.observed_at_ms
                || (next.observed_at_ms == previous.observed_at_ms && next != *previous)
                || (next.applied_index == previous.applied_index
                    && next.used_bytes != previous.used_bytes)
            {
                return Err(invalid("chunk health report would regress or conflict"));
            }
        }
        self.reports.insert(expected_node, next);
        Ok(())
    }
    fn validate_report(
        &self,
        identity: &ChunkGroupIdentity,
        report: &Report,
    ) -> Result<(), ObjectError> {
        if identity.voters.binary_search(&report.node_id).is_err()
            || report.applied_index < self.fence.applied_floor()
            || report.observed_at_ms == 0
            || report.used_bytes > identity.capacity_bytes
        {
            return Err(invalid("chunk health report exceeds configuration bounds"));
        }
        Ok(())
    }

    /// Returns availability, maximum voter usage and the last quorum observation.
    /// Reports at or before the supplied expiration cutoff cannot count as online.
    /// Their usage remains conservative until a newer report or membership update.
    pub fn project(&self, expired_at_or_before_ms: u64) -> (bool, u64, Option<u64>) {
        project_voter_health(self.fence.voters(), self.fence.outgoing_voters(), |id| {
            self.reports.get(&id).map(|report| VoterHealth {
                online: report.online && report.observed_at_ms > expired_at_or_before_ms,
                draining: report.draining,
                used_bytes: report.used_bytes,
                heartbeat_ms: report.observed_at_ms,
            })
        })
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        Image {
            version: 1,
            fence: self.fence.to_bytes(),
            reports: self.reports.values().cloned().collect(),
        }
        .encode_to_vec()
    }
    pub fn from_bytes(bytes: &[u8], identity: &ChunkGroupIdentity) -> Result<Self, ObjectError> {
        if bytes.len() > 16 * 1024 {
            return Err(invalid("chunk health image exceeds size bound"));
        }
        let image = Image::decode(bytes).map_err(|_| invalid("invalid chunk health protobuf"))?;
        if image.version != 1 || image.reports.len() > 64 || image.encode_to_vec() != bytes {
            return Err(invalid("noncanonical or unsupported chunk health image"));
        }
        let mut state = Self::new(ChunkMembershipFence::from_bytes(&image.fence, identity)?);
        let mut previous_id = 0;
        for report in image.reports {
            state.validate_report(identity, &report)?;
            if report.node_id <= previous_id {
                return Err(invalid("chunk health reports are not ordered"));
            }
            previous_id = report.node_id;
            state.reports.insert(report.node_id, report);
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> ChunkGroupIdentity {
        ChunkGroupIdentity {
            version: 2,
            group_id: 4,
            node_id: 1,
            capacity_bytes: 4096,
            max_pages: 128,
            voters: (1..=6).collect(),
            peer_endpoints: (1..=6)
                .map(|id| format!("127.0.0.1:{}", 9000 + id))
                .collect(),
            bootstrap_id: 91,
            bootstrap_voters: vec![1, 2, 3],
        }
    }
    fn status(node_id: u64, index: u64) -> MembershipStatus {
        MembershipStatus {
            version: 1,
            node_id,
            term: 2,
            applied_index: index,
            voters: vec![3, 4, 5],
            outgoing_voters: vec![1, 2, 3],
            learners: vec![6],
        }
    }
    #[test]
    fn fenced_joint_health_survives_restore_and_rejects_stale_reports() {
        let identity = identity();
        let fence = ChunkMembershipFence::from_status(&identity, &status(3, 20)).unwrap();
        for mask in 0u64..64 {
            let mut health = ChunkMembershipHealth::new(fence.clone());
            for node in 1..=6 {
                health
                    .record(
                        &identity,
                        node,
                        91,
                        &status(node, 20),
                        100 + node,
                        node * 10,
                        mask & (1 << (node - 1)) != 0,
                        false,
                    )
                    .unwrap();
            }
            let expected =
                (mask & 0b000111).count_ones() >= 2 && (mask & 0b011100).count_ones() >= 2;
            assert_eq!(health.project(0).0, expected, "mask={mask}");
            assert_eq!(health.project(0).1, 50, "learners do not contribute usage");
            let saved = health.to_bytes();
            let mut restored = ChunkMembershipHealth::from_bytes(&saved, &identity).unwrap();
            assert_eq!(restored, health);
            assert!(
                !restored.project(105).0,
                "expired reports do not make a quorum"
            );
            let before = restored.clone();
            assert!(restored
                .record(&identity, 1, 91, &status(1, 19), 200, 10, true, false)
                .is_err());
            assert!(restored
                .record(&identity, 1, 91, &status(1, 20), 99, 10, true, false)
                .is_err());
            assert!(restored
                .record(&identity, 1, 91, &status(1, 20), 200, 11, true, false)
                .is_err());
            assert_eq!(restored, before, "rejected updates are atomic");
            let mut stable = status(3, 21);
            stable.outgoing_voters.clear();
            restored.advance(&identity, &stable).unwrap();
            assert_eq!(restored.project(0), (false, 0, None));
            assert!(restored
                .record(&identity, 1, 91, &status(1, 22), 200, 10, true, false)
                .is_err());
            for node in [3, 4] {
                stable.node_id = node;
                restored
                    .record(&identity, node, 91, &stable, 200, node * 10, true, false)
                    .unwrap();
            }
            assert_eq!(restored.project(0), (true, 40, Some(200)));
        }
    }
    #[test]
    fn health_image_is_bounded_and_configuration_bound() {
        let identity = identity();
        let mut health = ChunkMembershipHealth::new(
            ChunkMembershipFence::from_status(&identity, &status(3, 20)).unwrap(),
        );
        health
            .record(&identity, 3, 91, &status(3, 21), 100, 30, true, false)
            .unwrap();
        let bytes = health.to_bytes();
        let mut unknown = bytes.clone();
        unknown.extend([0x78, 1]);
        assert!(ChunkMembershipHealth::from_bytes(&unknown, &identity).is_err());
        assert!(ChunkMembershipHealth::from_bytes(&vec![0; 16385], &identity).is_err());
        let mut decoded = Image::decode(bytes.as_slice()).unwrap();
        decoded.reports.push(decoded.reports[0].clone());
        assert!(ChunkMembershipHealth::from_bytes(&decoded.encode_to_vec(), &identity).is_err());
        decoded.reports.pop();
        decoded.reports[0].applied_index = 19;
        assert!(ChunkMembershipHealth::from_bytes(&decoded.encode_to_vec(), &identity).is_err());
        health.advance(&identity, &status(3, 21)).unwrap();
        assert_eq!(health.reports.len(), 1);
        health.advance(&identity, &status(3, 22)).unwrap();
        assert!(health.reports.is_empty());
    }
}
