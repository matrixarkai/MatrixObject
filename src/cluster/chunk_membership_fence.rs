// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Configuration and applied-position fences for chunk membership reports.
use super::{ChunkGroupIdentity, MembershipStatus};
use crate::ObjectError;
use prost::Message;

#[derive(Clone, PartialEq, Eq, Message)]
struct FenceImage {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    bootstrap_id: u64,
    #[prost(uint64, tag = "3")]
    applied_floor: u64,
    #[prost(uint64, repeated, tag = "4")]
    voters: Vec<u64>,
    #[prost(uint64, repeated, tag = "5")]
    outgoing_voters: Vec<u64>,
    #[prost(uint64, repeated, tag = "6")]
    learners: Vec<u64>,
    #[prost(uint64, tag = "7")]
    group_id: u64,
}

/// Retained membership observation used to reject reports from older configurations.
/// Obtain the source status through a trusted quorum-confirmed membership read;
/// serialized bytes alone do not establish how that observation was obtained.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkMembershipFence {
    image: FenceImage,
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
impl ChunkMembershipFence {
    pub fn from_status(
        identity: &ChunkGroupIdentity,
        status: &MembershipStatus,
    ) -> Result<Self, ObjectError> {
        identity.validate()?;
        status.validate_inventory(&identity.voters)?;
        let fence = Self {
            image: FenceImage {
                version: 1,
                group_id: identity.group_id,
                bootstrap_id: identity.bootstrap_id,
                applied_floor: status.applied_index,
                voters: status.voters.clone(),
                outgoing_voters: status.outgoing_voters.clone(),
                learners: status.learners.clone(),
            },
        };
        fence.validate(identity)?;
        Ok(fence)
    }
    fn validate(&self, identity: &ChunkGroupIdentity) -> Result<(), ObjectError> {
        identity.validate()?;
        let image = &self.image;
        if identity.version != 2
            || image.version != 1
            || image.group_id != identity.group_id
            || image.bootstrap_id == 0
            || image.bootstrap_id != identity.bootstrap_id
            || image.applied_floor == 0
            || image.voters.is_empty()
            || image.voters.len() > 7
            || image.outgoing_voters.len() > 7
            || image.learners.len() > 64
        {
            return Err(invalid("invalid chunk membership fence"));
        }
        for set in [&image.voters, &image.outgoing_voters, &image.learners] {
            if set.windows(2).any(|w| w[0] >= w[1])
                || set.iter().any(|id| !identity.voters.contains(id))
            {
                return Err(invalid("membership fence exceeds trusted inventory"));
            }
        }
        if image
            .learners
            .iter()
            .any(|id| image.voters.contains(id) || image.outgoing_voters.contains(id))
        {
            return Err(invalid("membership fence counts a learner as a voter"));
        }
        Ok(())
    }
    pub fn voters(&self) -> &[u64] {
        &self.image.voters
    }
    pub fn outgoing_voters(&self) -> &[u64] {
        &self.image.outgoing_voters
    }
    pub fn learners(&self) -> &[u64] {
        &self.image.learners
    }
    pub fn applied_floor(&self) -> u64 {
        self.image.applied_floor
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        self.image.encode_to_vec()
    }
    pub fn from_bytes(bytes: &[u8], identity: &ChunkGroupIdentity) -> Result<Self, ObjectError> {
        if bytes.len() > 2048 {
            return Err(invalid("membership fence exceeds size bound"));
        }
        let image =
            FenceImage::decode(bytes).map_err(|_| invalid("invalid membership fence protobuf"))?;
        if image.encode_to_vec() != bytes {
            return Err(invalid("noncanonical membership fence"));
        }
        let fence = Self { image };
        fence.validate(identity)?;
        Ok(fence)
    }
    /// Prepare a monotonic replacement; callers must commit it with their catalog update.
    pub fn advance(
        &self,
        identity: &ChunkGroupIdentity,
        status: &MembershipStatus,
    ) -> Result<Self, ObjectError> {
        self.validate(identity)?;
        let next = Self::from_status(identity, status)?;
        if next.image.applied_floor < self.image.applied_floor
            || (next.image.applied_floor == self.image.applied_floor && next != *self)
        {
            return Err(invalid("membership fence would regress or conflict"));
        }
        Ok(next)
    }
    /// Check the report's group, process identity, applied floor and exact configuration.
    /// A valid learner report still must not contribute to a voter quorum.
    pub fn check_report(
        &self,
        identity: &ChunkGroupIdentity,
        expected_node: u64,
        reported_bootstrap: u64,
        status: &MembershipStatus,
    ) -> Result<(), ObjectError> {
        self.validate(identity)?;
        status.validate_inventory(&identity.voters)?;
        if status.node_id != expected_node
            || reported_bootstrap != self.image.bootstrap_id
            || status.applied_index < self.image.applied_floor
            || status.voters != self.image.voters
            || status.outgoing_voters != self.image.outgoing_voters
            || status.learners != self.image.learners
        {
            return Err(invalid(
                "chunk membership report does not satisfy catalog fence",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> ChunkGroupIdentity {
        ChunkGroupIdentity {
            version: 2,
            group_id: 1,
            node_id: 1,
            capacity_bytes: 4096,
            max_pages: 128,
            voters: vec![1, 2, 3, 4],
            peer_endpoints: (1..=4)
                .map(|id| format!("127.0.0.1:{}", 9000 + id))
                .collect(),
            bootstrap_id: 91,
            bootstrap_voters: vec![1, 2, 3],
        }
    }
    fn status(index: u64, voters: Vec<u64>) -> MembershipStatus {
        MembershipStatus {
            version: 1,
            node_id: 1,
            term: 2,
            applied_index: index,
            voters,
            outgoing_voters: vec![],
            learners: vec![],
        }
    }
    #[test]
    fn membership_fence_rejects_stale_reports_after_returning_to_an_earlier_voter_set() {
        let identity = identity();
        let first =
            ChunkMembershipFence::from_status(&identity, &status(10, vec![1, 2, 3])).unwrap();
        let second = first
            .advance(&identity, &status(20, vec![2, 3, 4]))
            .unwrap();
        let restored = second
            .advance(&identity, &status(30, vec![1, 2, 3]))
            .unwrap();
        assert!(restored
            .check_report(&identity, 1, 91, &status(15, vec![1, 2, 3]))
            .is_err());
        assert!(restored
            .check_report(&identity, 1, 91, &status(30, vec![1, 2, 3]))
            .is_ok());
        assert!(restored
            .check_report(&identity, 2, 91, &status(30, vec![1, 2, 3]))
            .is_err());
        assert!(restored
            .check_report(&identity, 1, 92, &status(30, vec![1, 2, 3]))
            .is_err());
        assert!(restored
            .advance(&identity, &status(29, vec![1, 2, 3]))
            .is_err());
        assert!(restored
            .advance(&identity, &status(30, vec![2, 3, 4]))
            .is_err());
        assert_eq!(
            restored
                .advance(&identity, &status(30, vec![1, 2, 3]))
                .unwrap(),
            restored
        );
    }
    #[test]
    fn membership_fence_is_bounded_canonical_and_preserves_joint_configuration() {
        let identity = identity();
        let mut joint = status(20, vec![2, 3]);
        joint.outgoing_voters = vec![1, 2, 3];
        joint.learners = vec![4];
        let fence = ChunkMembershipFence::from_status(&identity, &joint).unwrap();
        let bytes = fence.to_bytes();
        assert_eq!(
            ChunkMembershipFence::from_bytes(&bytes, &identity).unwrap(),
            fence
        );
        let mut unknown = bytes.clone();
        unknown.extend([0x78, 1]);
        assert!(ChunkMembershipFence::from_bytes(&unknown, &identity).is_err());
        assert!(ChunkMembershipFence::from_bytes(&vec![0; 2049], &identity).is_err());
        let mut wrong_group = identity.clone();
        wrong_group.group_id += 1;
        assert!(ChunkMembershipFence::from_bytes(&bytes, &wrong_group).is_err());
        wrong_group = identity.clone();
        wrong_group.bootstrap_id += 1;
        assert!(ChunkMembershipFence::from_bytes(&bytes, &wrong_group).is_err());
        let mut report = joint.clone();
        report.node_id = 4;
        assert!(fence.check_report(&identity, 4, 91, &report).is_ok());
        assert!(!fence.voters().contains(&4) && !fence.outgoing_voters().contains(&4));
        report.outgoing_voters.clear();
        assert!(fence.check_report(&identity, 4, 91, &report).is_err());
        let mut invalid = fence.image.clone();
        invalid.learners = vec![2];
        assert!(ChunkMembershipFence::from_bytes(&invalid.encode_to_vec(), &identity).is_err());
    }
}
