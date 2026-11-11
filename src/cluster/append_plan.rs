// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Exact-generation append planning without transmitting the existing extent list.
use super::PlacementAssignment;
use super::{ChunkGroupRoute, MetaReply, MetaRequestCode};
use crate::ObjectError;
use prost::Message;

pub(crate) const MAX_REPLY_BYTES: usize = super::MAX_PAYLOAD_BYTES;
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct AppendPlanRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub key: String,
    #[prost(uint64, tag = "4")]
    pub generation: u64,
    #[prost(bytes, tag = "5")]
    pub manifest_sha256: Vec<u8>,
    #[prost(uint64, tag = "6")]
    pub length: u64,
    #[prost(uint64, tag = "7")]
    pub replica_count: u64,
}
impl AppendPlanRequest {
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if self.version != 1
            || self.generation == 0
            || self.manifest_sha256.len() != 32
            || self.length == 0
            || !(1..=4096).contains(&self.replica_count)
            || self.encoded_len() > 8192
        {
            return Err(invalid("invalid compact append plan request"));
        }
        Ok(())
    }
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 8192 {
            return Err(invalid("append plan request exceeds bound"));
        }
        let value: Self = super::conditional_requests::decode(bytes)?;
        value.validate()?;
        Ok(value)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct AppendPlanReply {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(bytes, tag = "2")]
    pub plan: Vec<u8>,
    #[prost(message, repeated, tag = "3")]
    pub routes: Vec<ChunkGroupRoute>,
    #[prost(uint64, tag = "4")]
    pub generation: u64,
    #[prost(bytes, tag = "5")]
    pub manifest_sha256: Vec<u8>,
}
impl AppendPlanReply {
    pub(crate) fn checked_plan(&self) -> Result<PlacementAssignment, ObjectError> {
        if self.version != 1
            || self.generation == 0
            || self.manifest_sha256.len() != 32
            || self.routes.len() > 32
            || self.encoded_len() > MAX_REPLY_BYTES
        {
            return Err(invalid("invalid compact append plan reply"));
        }
        let MetaReply::Plan(plan) =
            MetaReply::decode(MetaRequestCode::PlanObjectPlacement, &self.plan)?
        else {
            unreachable!()
        };
        if plan.node_ids.is_empty()
            || plan.node_ids.len() > 4096
            || self.routes.len()
                != if plan.node_ids.len() <= 32 {
                    plan.node_ids.len()
                } else {
                    0
                }
            || plan
                .node_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != plan.node_ids.len()
            || self.routes.iter().zip(&plan.node_ids).any(|(route, id)| {
                route.encoded_len() > 65536
                    || route
                        .group
                        .as_ref()
                        .is_none_or(|group| &group.placement_id != id)
                    || route.membership_fence().is_err()
            })
        {
            return Err(invalid("compact append routes differ from plan"));
        }
        Ok(plan)
    }
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_REPLY_BYTES {
            return Err(invalid("append plan reply exceeds bound"));
        }
        let value: Self = super::conditional_requests::decode(bytes)?;
        value.checked_plan()?;
        Ok(value)
    }
}
