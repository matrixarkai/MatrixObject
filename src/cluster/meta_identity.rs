// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded immutable metadata voter configuration, not a health or authentication proof.
use crate::ObjectError;
use prost::Message;
#[derive(Clone, PartialEq, Eq, Message)]
pub struct MetaGroupIdentity {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub group_id: u64,
    #[prost(uint64, tag = "3")]
    pub node_id: u64,
    #[prost(uint64, repeated, tag = "4")]
    pub voters: Vec<u64>,
    #[prost(string, repeated, tag = "5")]
    pub peer_endpoints: Vec<String>,
    /// The effective Raft group ID derived from the persisted bootstrap image.
    #[prost(uint64, tag = "6")]
    pub bootstrap_id: u64,
    /// Version 2: initial voters; voters/peer_endpoints describe the permitted inventory.
    #[prost(uint64, repeated, tag = "7")]
    pub bootstrap_voters: Vec<u64>,
}
impl MetaGroupIdentity {
    pub fn validate(&self) -> Result<(), ObjectError> {
        use std::collections::BTreeSet;
        use std::net::SocketAddr;
        let mut addresses = BTreeSet::new();
        if !matches!(self.version, 1 | 2)
            || self.group_id == 0
            || self.bootstrap_id == 0
            || self.node_id == 0
            || self.voters.is_empty()
            || self.voters.len() > if self.version == 1 { 7 } else { 64 }
            || (self.version == 1 && !self.bootstrap_voters.is_empty())
            || (self.version == 2
                && (self.bootstrap_voters.is_empty()
                    || self.bootstrap_voters.len() > 7
                    || self.bootstrap_voters.windows(2).any(|w| w[0] >= w[1])
                    || self
                        .bootstrap_voters
                        .iter()
                        .any(|id| !self.voters.contains(id))))
            || self.voters.len() != self.peer_endpoints.len()
            || !self.voters.contains(&self.node_id)
            || self.voters[0] == 0
            || self.voters.windows(2).any(|v| v[0] >= v[1])
        {
            return Err(ObjectError::Corruption(
                "invalid metadata group identity".into(),
            ));
        }
        for endpoint in &self.peer_endpoints {
            let address = endpoint
                .parse::<SocketAddr>()
                .map_err(|_| ObjectError::Corruption("invalid metadata peer address".into()))?;
            let canonical = match address {
                SocketAddr::V6(a) => a
                    .ip()
                    .to_ipv4_mapped()
                    .map(|ip| SocketAddr::new(ip.into(), a.port()))
                    .unwrap_or(address),
                _ => address,
            };
            if endpoint.len() > 128
                || address.port() == 0
                || address.ip().is_unspecified()
                || address.ip().is_multicast()
                || !addresses.insert(canonical)
            {
                return Err(ObjectError::Corruption(
                    "invalid metadata peer address".into(),
                ));
            }
        }
        Ok(())
    }
    pub fn same_group(&self, other: &Self) -> bool {
        self.version == other.version
            && self.group_id == other.group_id
            && self.bootstrap_id == other.bootstrap_id
            && self.bootstrap_voters == other.bootstrap_voters
            && self.voters == other.voters
            && self.peer_endpoints == other.peer_endpoints
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{MetaReply, MetaRequest, MetaRequestCode};
    #[test]
    fn metadata_identity_is_bounded_canonical_and_complete() {
        let identity = MetaGroupIdentity {
            bootstrap_voters: Vec::new(),
            version: 1,
            group_id: 4,
            node_id: 1,
            voters: vec![1, 2, 3],
            peer_endpoints: vec![
                "127.0.0.1:12001".into(),
                "127.0.0.1:12002".into(),
                "127.0.0.1:12003".into(),
            ],
            bootstrap_id: 9,
        };
        identity.validate().unwrap();
        let reply = MetaReply::GroupIdentity(identity.clone());
        let bytes = reply.encode_payload();
        assert_eq!(
            MetaReply::decode(MetaRequestCode::GroupIdentity, &bytes).unwrap(),
            reply
        );
        assert_eq!(
            MetaRequest::decode(MetaRequestCode::GroupIdentity, &[]).unwrap(),
            MetaRequest::GroupIdentity
        );
        assert!(MetaRequest::decode(MetaRequestCode::GroupIdentity, &[0]).is_err());
        for suffix in [vec![0x08, 1], vec![0x78, 1]] {
            let mut bad = bytes.clone();
            bad.extend(suffix);
            assert!(MetaReply::decode(MetaRequestCode::GroupIdentity, &bad).is_err());
        }
        assert!(MetaReply::decode(MetaRequestCode::GroupIdentity, &vec![0; 4097]).is_err());
        for field in 0..8 {
            let mut bad = identity.clone();
            match field {
                0 => bad.version = 2,
                1 => bad.group_id = 0,
                2 => bad.node_id = 4,
                3 => bad.bootstrap_id = 0,
                4 => bad.voters.swap(0, 1),
                5 => bad.peer_endpoints.pop().map(|_| ()).unwrap(),
                6 => bad.peer_endpoints[1] = bad.peer_endpoints[0].clone(),
                _ => bad.peer_endpoints[0] = "0.0.0.0:12000".into(),
            }
            assert!(
                MetaReply::decode(MetaRequestCode::GroupIdentity, &bad.encode_to_vec()).is_err()
            );
        }
        let mut other = identity.clone();
        other.node_id = 2;
        assert!(identity.same_group(&other));
        other.bootstrap_id += 1;
        assert!(!identity.same_group(&other));
    }
}
