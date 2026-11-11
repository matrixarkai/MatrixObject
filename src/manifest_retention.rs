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
    /// Zero retains permanently; nonzero is a metadata-server lease deadline.
    #[prost(uint64, tag = "3")]
    pub expires_at_ms: u64,
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
    /// 1 retain; 2 release; 3 read; 4 list; 5 open session; 6 renew; 7 expire; 8 grant selected manifest.
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
    #[prost(uint64, tag = "8")]
    pub ttl_ms: u64,
    #[prost(uint64, tag = "9")]
    pub expected_expiry_ms: u64,
    /// Version 3 action 8 binds an already selected manifest without returning it.
    #[prost(bytes, tag = "10")]
    pub manifest_sha256: Vec<u8>,
}

impl ManifestRetentionRequest {
    pub(crate) fn is_read(&self) -> bool {
        matches!(self.action, 3 | 4)
    }
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        if self.version == 3 {
            crate::validate_bucket(&self.bucket)?;
            crate::validate_key(&self.key)?;
            if self.action != 8 || self.generation == 0 || self.retention_id != 0
                || self.limit != 0 || self.expected_expiry_ms != 0
                || !(1000..=3_600_000).contains(&self.ttl_ms)
                || self.manifest_sha256.len() != 32 {
                return Err(invalid("invalid selected manifest grant request"));
            }
            return Ok(());
        }
        if !self.manifest_sha256.is_empty() {
            return Err(invalid("manifest digest requires version 3"));
        }
        if self.version == 2 {
            if !matches!(self.action, 5..=7)
                || (self.action != 7 && !(1000..=3_600_000).contains(&self.ttl_ms))
                || (self.action == 7 && self.ttl_ms != 0) || self.limit != 0 {
                return Err(invalid("invalid read session lease fields"));
            }
            if self.action == 5 {
                crate::validate_bucket(&self.bucket)?;
                crate::validate_key(&self.key)?;
                if self.retention_id != 0 || self.expected_expiry_ms != 0 {
                    return Err(invalid("invalid read session target"));
                }
            } else if !self.bucket.is_empty() || !self.key.is_empty() || self.generation != 0
                || self.retention_id == 0 || self.expected_expiry_ms == 0 {
                return Err(invalid("invalid read session handle"));
            }
            return Ok(());
        }
        if self.ttl_ms != 0 || self.expected_expiry_ms != 0 {
            return Err(invalid("read session fields require version 2"));
        }
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
    #[prost(uint64, tag = "5")]
    pub retention_id: u64,
    #[prost(uint64, tag = "6")]
    pub expires_at_ms: u64,
    /// Relative protection window measured by the metadata server.
    #[prost(uint64, tag = "7")]
    pub lease_ms: u64,
    /// Version 4 acknowledges the exact selected manifest digest.
    #[prost(bytes, tag = "8")]
    pub manifest_sha256: Vec<u8>,
}

impl ManifestRetentionReply {
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = decode(bytes, 1024 * 1024 + 128)?;
        if !((value.version == 1 && value.manifest_sha256.is_empty() && value.retention_id == 0 && value.expires_at_ms == 0 && value.lease_ms == 0
                && value.retained.as_ref().is_none_or(|record| record.expires_at_ms == 0))
            || (value.version == 2 && value.manifest_sha256.is_empty() && value.retention_id != 0 && value.expires_at_ms != 0
                && value.lease_ms == 0 && value.retained.is_none() && value.retention_ids.is_empty() && !value.has_more)
            || (value.version == 3 && value.manifest_sha256.is_empty() && value.retention_id == 0 && value.expires_at_ms == 0
                && (1..=3_600_000).contains(&value.lease_ms)
                && value.retained.as_ref().is_some_and(|record| record.expires_at_ms != 0)
                && value.retention_ids.is_empty() && !value.has_more)
            || (value.version == 4 && value.retention_id != 0 && value.expires_at_ms != 0
                && (1000..=3_600_000).contains(&value.lease_ms)
                && value.manifest_sha256.len() == 32 && value.retained.is_none()
                && value.retention_ids.is_empty() && !value.has_more))
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

#[derive(Clone, PartialEq, Eq, Message)]
pub struct RetentionDeadlineChange {
    #[prost(uint64, tag = "1")]
    pub retention_id: u64,
    #[prost(uint64, tag = "2")]
    pub before: u64,
    #[prost(uint64, tag = "3")]
    pub after: u64,
}
impl RetentionDeadlineChange {
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        if self.retention_id == 0 || self.before == 0 || self.after <= self.before {
            return Err(invalid("invalid read session deadline change"));
        }
        Ok(())
    }
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
    fn read_session_wire_bounds_ttl_and_compact_renewal_identity() {
        let open = ManifestRetentionRequest { version: 2, action: 5, bucket: "bucket".into(),
            key: "object".into(), ttl_ms: 1000, ..Default::default() };
        let bytes = open.encode_to_vec();
        assert_eq!(ManifestRetentionRequest::from_payload(&bytes).unwrap(), open);
        for ttl in [0, 999, 3_600_001, u64::MAX] {
            assert!(ManifestRetentionRequest { ttl_ms: ttl, ..open.clone() }.validate().is_err());
        }
        assert!(ManifestRetentionRequest { version: 1, ..open.clone() }.validate().is_err());
        assert!(ManifestRetentionRequest { retention_id: 1, ..open }.validate().is_err());
        let renew = ManifestRetentionRequest { version: 2, action: 6, retention_id: 2,
            ttl_ms: 1000, expected_expiry_ms: 2000, ..Default::default() };
        assert_eq!(ManifestRetentionRequest::from_payload(&renew.encode_to_vec()).unwrap(), renew);
        assert!(ManifestRetentionRequest { expected_expiry_ms: 0, ..renew.clone() }.validate().is_err());
        assert!(ManifestRetentionRequest { action: 7, ..renew }.validate().is_err());
        let reply = ManifestRetentionReply { version: 2, retention_id: 2, expires_at_ms: 3000, ..Default::default() };
        assert_eq!(ManifestRetentionReply::from_payload(&reply.encode_to_vec()).unwrap(), reply);
        assert!(reply.encoded_len() < 32);
        assert!(ManifestRetentionReply::from_payload(&ManifestRetentionReply { has_more: true, ..reply }.encode_to_vec()).is_err());
        let grant = ManifestRetentionReply { version: 3, lease_ms: 1000,
            retained: Some(RetainedManifest { retention_id: 1, expires_at_ms: 2000,
                manifest: Some(ObjectManifest { format_version: 2, bucket: "bucket".into(),
                    key: "object".into(), generation: 1, ..Default::default() }) }), ..Default::default() };
        assert_eq!(ManifestRetentionReply::from_payload(&grant.encode_to_vec()).unwrap(), grant);
        assert!(ManifestRetentionReply::from_payload(&ManifestRetentionReply {
            lease_ms: 0, ..grant.clone()
        }.encode_to_vec()).is_err());
        assert!(ManifestRetentionReply::from_payload(&ManifestRetentionReply {
            version: 1, lease_ms: 0, ..grant
        }.encode_to_vec()).is_err());
        let expiry = ManifestRetentionRequest { version: 2, action: 7, retention_id: 2,
            expected_expiry_ms: 2000, ..Default::default() };
        assert_eq!(ManifestRetentionRequest::from_payload(&expiry.encode_to_vec()).unwrap(), expiry);
    }
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

#[cfg(test)]
mod compact_selected_grants {
    use super::*;
    #[test]
    fn wire_versions_require_complete_exclusive_bindings() {
        let request = ManifestRetentionRequest { version: 3, action: 8, bucket: "bucket".into(),
            key: "object".into(), generation: 1, ttl_ms: 1000, manifest_sha256: vec![7; 32],
            ..Default::default() };
        assert_eq!(ManifestRetentionRequest::from_payload(&request.encode_to_vec()).unwrap(), request);
        let invalid = [
            ManifestRetentionRequest { version: 2, ..request.clone() },
            ManifestRetentionRequest { action: 5, ..request.clone() },
            ManifestRetentionRequest { generation: 0, ..request.clone() },
            ManifestRetentionRequest { retention_id: 1, ..request.clone() },
            ManifestRetentionRequest { expected_expiry_ms: 1, ..request.clone() },
            ManifestRetentionRequest { ttl_ms: 999, ..request.clone() },
            ManifestRetentionRequest { ttl_ms: 3_600_001, ..request.clone() },
            ManifestRetentionRequest { limit: 1, ..request.clone() },
            ManifestRetentionRequest { manifest_sha256: vec![7; 31], ..request.clone() },
        ];
        for value in invalid { assert!(value.validate().is_err()); }
        let reply = ManifestRetentionReply { version: 4, retention_id: u64::MAX,
            expires_at_ms: u64::MAX, lease_ms: 3_600_000, manifest_sha256: vec![7; 32],
            ..Default::default() };
        assert_eq!(reply.encoded_len(), 63);
        assert_eq!(ManifestRetentionReply::from_payload(&reply.encode_to_vec()).unwrap(), reply);
        let invalid = [
            ManifestRetentionReply { version: 3, ..reply.clone() },
            ManifestRetentionReply { retention_id: 0, ..reply.clone() },
            ManifestRetentionReply { expires_at_ms: 0, ..reply.clone() },
            ManifestRetentionReply { lease_ms: 999, ..reply.clone() },
            ManifestRetentionReply { lease_ms: 3_600_001, ..reply.clone() },
            ManifestRetentionReply { manifest_sha256: vec![7; 31], ..reply.clone() },
            ManifestRetentionReply { has_more: true, ..reply.clone() },
            ManifestRetentionReply { retention_ids: vec![1], ..reply.clone() },
        ];
        for value in invalid {
            assert!(ManifestRetentionReply::from_payload(&value.encode_to_vec()).is_err());
        }
        let mut bytes = reply.encode_to_vec();
        bytes.extend([0xa0, 0x06, 1]);
        assert!(ManifestRetentionReply::from_payload(&bytes).is_err());
    }
}
