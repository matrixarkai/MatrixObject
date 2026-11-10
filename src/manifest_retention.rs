// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Explicit durable retention of an immutable manifest generation.
use crate::{object_manifest::ObjectManifest, ObjectError};
use prost::Message;

pub(crate) const MAX_RETAINED_MANIFESTS: usize = 128;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct RetainedManifest {
    #[prost(uint64, tag = "1")]
    pub retention_id: u64,
    #[prost(message, optional, tag = "2")]
    pub manifest: Option<ObjectManifest>,
}

impl RetainedManifest {
    pub fn validate(&self) -> Result<(), ObjectError> {
        let manifest = self
            .manifest
            .as_ref()
            .ok_or_else(|| invalid("retained manifest missing"))?;
        manifest.validate()?;
        if self.retention_id == 0 || manifest.deleted {
            return Err(invalid("invalid retained manifest identity or tombstone"));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestRetentionRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// 1: retain current generation; 2: release; 3: read; 4: list handles.
    #[prost(uint32, tag = "2")]
    pub action: u32,
    #[prost(string, tag = "3")]
    pub bucket: String,
    #[prost(string, tag = "4")]
    pub key: String,
    #[prost(uint64, tag = "5")]
    pub generation: u64,
    /// Handle for read/release; exclusive cursor for listing.
    #[prost(uint64, tag = "6")]
    pub retention_id: u64,
    #[prost(uint32, tag = "7")]
    pub limit: u32,
}

impl ManifestRetentionRequest {
    pub(crate) fn is_read(&self) -> bool {
        matches!(self.action, 3 | 4)
    }
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        if self.version != 1 {
            return Err(invalid("unsupported retention request version"));
        }
        match self.action {
            1 => {
                crate::validate_bucket(&self.bucket)?;
                crate::validate_key(&self.key)?;
                if self.generation == 0 || self.retention_id != 0 || self.limit != 0 {
                    return Err(invalid("invalid retain request fields"));
                }
            }
            2..=4 => {
                if !self.bucket.is_empty()
                    || !self.key.is_empty()
                    || self.generation != 0
                    || (self.action != 4 && (self.retention_id == 0 || self.limit != 0))
                    || (self.action == 4 && !(1..=32).contains(&self.limit))
                {
                    return Err(invalid("invalid retention handle fields"));
                }
            }
            _ => return Err(invalid("invalid retention action")),
        }
        Ok(())
    }
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = decode(bytes, 8192)?;
        value.validate()?;
        Ok(value)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestRetentionReply {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, optional, tag = "2")]
    pub retained: Option<RetainedManifest>,
    #[prost(uint64, repeated, tag = "3")]
    pub retention_ids: Vec<u64>,
    #[prost(bool, tag = "4")]
    pub has_more: bool,
}

impl ManifestRetentionReply {
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = decode(bytes, 1024 * 1024 + 128)?;
        if value.version != 1
            || value.retention_ids.len() > 32
            || value.retention_ids.contains(&0)
            || value
                .retention_ids
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || (value.has_more && value.retention_ids.is_empty())
            || (value.retained.is_some() && (!value.retention_ids.is_empty() || value.has_more))
        {
            return Err(invalid("invalid retention reply"));
        }
        if let Some(retained) = &value.retained {
            retained.validate()?;
        }
        Ok(value)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestRetentionChange {
    #[prost(message, optional, tag = "1")]
    pub before: Option<RetainedManifest>,
    #[prost(message, optional, tag = "2")]
    pub after: Option<RetainedManifest>,
}

pub(crate) fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}

fn decode<T: Message + Default>(bytes: &[u8], limit: usize) -> Result<T, ObjectError> {
    if bytes.len() > limit {
        return Err(invalid("retention message exceeds bound"));
    }
    let value = T::decode(bytes).map_err(|_| invalid("invalid retention protobuf"))?;
    if value.encode_to_vec() != bytes {
        return Err(invalid("noncanonical retention protobuf"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retention_wire_refuses_truncation_unknown_fields_and_conflicting_actions() {
        let request = ManifestRetentionRequest {
            version: 1,
            action: 1,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 17,
            ..Default::default()
        };
        let bytes = request.encode_to_vec();
        assert_eq!(
            ManifestRetentionRequest::from_payload(&bytes).unwrap(),
            request
        );
        for cut in 0..bytes.len() {
            assert!(ManifestRetentionRequest::from_payload(&bytes[..cut]).is_err());
        }
        let mut unknown = bytes;
        unknown.extend_from_slice(&[0x40, 1]);
        assert!(ManifestRetentionRequest::from_payload(&unknown).is_err());
        let mut wrong = request;
        wrong.retention_id = 1;
        assert!(wrong.validate().is_err());
        wrong.action = 3;
        assert!(wrong.validate().is_err());
        let reply = ManifestRetentionReply {
            version: 1,
            retention_ids: vec![4, 9],
            has_more: true,
            ..Default::default()
        };
        assert_eq!(
            ManifestRetentionReply::from_payload(&reply.encode_to_vec()).unwrap(),
            reply
        );
        let mut wrong = reply;
        wrong.retention_ids = vec![9, 4];
        assert!(ManifestRetentionReply::from_payload(&wrong.encode_to_vec()).is_err());
    }
}
