// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded binary membership commands shared by consensus services.
use crate::ObjectError;
use prost::Message;

/// Explicit operator action; a successful mutation reply means locally applied commitment.
/// A retained leader also obtains a fresh quorum read confirmation after applying
/// the change. A leader removed by its own command can report local commitment
/// but cannot confirm a quorum after removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum MembershipOperation {
    Read = 0,
    AddLearner = 1,
    Promote = 2,
    Remove = 3,
    LeaveJoint = 4,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct MembershipRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// Effective group identity obtained from the trusted group description.
    #[prost(uint64, tag = "2")]
    pub bootstrap_id: u64,
    #[prost(enumeration = "MembershipOperation", tag = "3")]
    pub operation: i32,
    #[prost(uint64, tag = "4")]
    pub node_id: u64,
}
impl MembershipRequest {
    pub fn validate(&self) -> Result<(), ObjectError> {
        let op = MembershipOperation::from_i32(self.operation)
            .ok_or_else(|| invalid("unknown membership operation"))?;
        if self.version != 1
            || self.bootstrap_id == 0
            || (matches!(
                op,
                MembershipOperation::Read | MembershipOperation::LeaveJoint
            ) != (self.node_id == 0))
        {
            return Err(invalid("invalid membership request"));
        }
        Ok(())
    }
    pub fn is_read(&self) -> bool {
        self.operation == MembershipOperation::Read as i32
    }
    #[cfg(any(feature = "metadata-consensus", feature = "chunk-consensus"))]
    pub(super) fn change(&self) -> Result<matrixraft::peer_runtime::MembershipChange, ObjectError> {
        use matrixraft::peer_runtime::MembershipChange as C;
        self.validate()?;
        Ok(
            match MembershipOperation::from_i32(self.operation).unwrap() {
                MembershipOperation::AddLearner => C::AddLearner(self.node_id),
                MembershipOperation::Promote => C::Promote(self.node_id),
                MembershipOperation::Remove => C::Remove(self.node_id),
                MembershipOperation::LeaveJoint => C::LeaveJoint,
                MembershipOperation::Read => {
                    return Err(invalid("membership read is not a change"))
                }
            },
        )
    }
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct MembershipStatus {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub node_id: u64,
    #[prost(uint64, tag = "3")]
    pub term: u64,
    #[prost(uint64, tag = "4")]
    pub applied_index: u64,
    #[prost(uint64, repeated, tag = "5")]
    pub voters: Vec<u64>,
    #[prost(uint64, repeated, tag = "6")]
    pub outgoing_voters: Vec<u64>,
    #[prost(uint64, repeated, tag = "7")]
    pub learners: Vec<u64>,
}
fn invalid(s: &str) -> ObjectError {
    ObjectError::InvalidArgument(s.into())
}
impl MembershipStatus {
    pub(super) fn validate_inventory(&self, inventory: &[u64]) -> Result<(), ObjectError> {
        self.validate()?;
        if !inventory.contains(&self.node_id)
            || self.voters.iter().chain(&self.outgoing_voters).chain(&self.learners)
                .any(|id| !inventory.contains(id))
        {
            return Err(ObjectError::Corruption("membership status exceeds trusted inventory".into()));
        }
        Ok(())
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.version != 1
            || self.node_id == 0
            || self.term == 0
            || self.applied_index == 0
            || self.voters.is_empty()
            || self.voters.len() > 7
            || self.outgoing_voters.len() > 7
            || self.learners.len() > 64
        {
            return Err(invalid("invalid membership status"));
        }
        for set in [&self.voters, &self.outgoing_voters, &self.learners] {
            if set.first() == Some(&0) || set.windows(2).any(|w| w[0] >= w[1]) {
                return Err(invalid("unordered membership status"));
            }
        }
        if self
            .learners
            .iter()
            .any(|id| self.voters.contains(id) || self.outgoing_voters.contains(id))
        {
            return Err(invalid("learner is also a voter"));
        }
        Ok(())
    }
    #[cfg(any(feature = "metadata-consensus", feature = "chunk-consensus"))]
    pub(super) fn from_peer(peer: &matrixraft::peer_runtime::DurablePeer) -> Self {
        let state = peer.membership();
        Self {
            version: 1,
            node_id: peer.node_id(),
            term: peer.term(),
            applied_index: peer.applied_index(),
            voters: state.voters,
            outgoing_voters: state.outgoing_voters,
            learners: state.learners,
        }
    }
}

#[cfg(all(test, feature = "meta-server"))]
mod tests {
    use super::*;
    use crate::cluster::{MetaReply, MetaRequest, MetaRequestCode};
    #[test]
    fn membership_protobuf_refuses_unknown_fields_and_invalid_actions() {
        for operation in 0..=4 {
            let value = MembershipRequest {
                version: 1,
                bootstrap_id: 9,
                operation,
                node_id: if operation == 0 || operation == 4 {
                    0
                } else {
                    4
                },
            };
            let request = MetaRequest::Membership(value.clone());
            assert_eq!(
                MetaRequest::decode(MetaRequestCode::Membership, &request.encode_payload())
                    .unwrap(),
                request
            );
            for field in 0..5 {
                let mut bad = value.clone();
                match field {
                    0 => bad.version = 2,
                    1 => bad.bootstrap_id = 0,
                    2 => bad.operation = 99,
                    3 => bad.node_id = if value.node_id == 0 { 4 } else { 0 },
                    _ => {}
                }
                let mut bytes = bad.encode_to_vec();
                if field == 4 {
                    bytes.extend([0x78, 1]);
                }
                assert!(MetaRequest::decode(MetaRequestCode::Membership, &bytes).is_err());
            }
        }
        assert!(MetaRequest::decode(MetaRequestCode::Membership, &[0; 129]).is_err());
        let value = MembershipStatus {
            version: 1,
            node_id: 1,
            term: 2,
            applied_index: 9,
            voters: vec![1, 2, 4],
            outgoing_voters: vec![1, 2, 3],
            learners: vec![5],
        };
        let reply = MetaReply::Membership(value.clone());
        assert_eq!(
            MetaReply::decode(MetaRequestCode::Membership, &reply.encode_payload()).unwrap(),
            reply
        );
        for field in 0..8 {
            let mut bad = value.clone();
            match field {
                0 => bad.version = 2,
                1 => bad.node_id = 0,
                2 => bad.term = 0,
                3 => bad.applied_index = 0,
                4 => bad.voters.clear(),
                5 => bad.voters.swap(0, 1),
                6 => bad.learners.push(3),
                _ => bad.outgoing_voters.push(0),
            }
            assert!(MetaReply::decode(MetaRequestCode::Membership, &bad.encode_to_vec()).is_err());
        }
        let mut bytes = value.encode_to_vec();
        bytes.extend([0x78, 1]);
        assert!(MetaReply::decode(MetaRequestCode::Membership, &bytes).is_err());
        assert!(MetaReply::decode(MetaRequestCode::Membership, &[0; 2049]).is_err());
    }
}

#[cfg(all(test, feature = "replica-server"))]
mod chunk_tests {
    use super::*;
    use crate::cluster::{ChunkGroupIdentity, ReplicaReply, ReplicaRequest, RequestCode};
    #[test]
    fn chunk_membership_frames_and_inventory_identity_are_canonical() {
        for operation in 0..=4 {
            let value = MembershipRequest {
                version: 1,
                bootstrap_id: 9,
                operation,
                node_id: if operation == 0 || operation == 4 {
                    0
                } else {
                    4
                },
            };
            let request = ReplicaRequest::Membership(value.clone());
            let bytes = request.encode_payload();
            assert_eq!(
                ReplicaRequest::decode(RequestCode::Membership, &bytes).unwrap(),
                request
            );
            for suffix in [vec![0x78, 1], vec![8, 1]] {
                let mut bad = bytes.clone();
                bad.extend(suffix);
                assert!(ReplicaRequest::decode(RequestCode::Membership, &bad).is_err());
            }
        }
        assert!(ReplicaRequest::decode(RequestCode::Membership, &[0; 129]).is_err());
        let status = MembershipStatus {
            version: 1,
            node_id: 1,
            term: 2,
            applied_index: 8,
            voters: vec![1, 2, 4],
            outgoing_voters: vec![1, 2, 3],
            learners: vec![5],
        };
        let reply = ReplicaReply::Membership(status);
        assert_eq!(
            ReplicaReply::decode(RequestCode::Membership, &reply.encode_payload()).unwrap(),
            reply
        );
        let mut bad = reply.encode_payload();
        bad.extend([0x78, 1]);
        assert!(ReplicaReply::decode(RequestCode::Membership, &bad).is_err());
        assert!(ReplicaReply::decode(RequestCode::Membership, &[0; 2049]).is_err());
        let identity = ChunkGroupIdentity {
            version: 2,
            group_id: 3,
            node_id: 4,
            capacity_bytes: 4096,
            max_pages: 64,
            voters: vec![1, 2, 3, 4],
            peer_endpoints: (1..=4)
                .map(|id| format!("127.0.0.1:{}", 9100 + id))
                .collect(),
            bootstrap_voters: vec![1, 2, 3],
            bootstrap_id: 9,
        };
        let reply = ReplicaReply::GroupIdentity(identity.clone());
        assert_eq!(
            ReplicaReply::decode(RequestCode::GroupIdentity, &reply.encode_payload()).unwrap(),
            reply
        );
        for field in 0..4 {
            let mut bad = identity.clone();
            match field {
                0 => bad.bootstrap_id = 0,
                1 => bad.bootstrap_voters.clear(),
                2 => bad.bootstrap_voters = vec![5],
                _ => bad.version = 1,
            }
            assert!(
                ReplicaReply::decode(RequestCode::GroupIdentity, &bad.encode_to_vec()).is_err()
            );
        }
    }
}
