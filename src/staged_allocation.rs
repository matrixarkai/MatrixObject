// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable ownership of fresh page namespaces before object publication.
use crate::{
    object_manifest::{ManifestOutcome, ObjectPageAllocation},
    ObjectError,
};
use prost::Message;

pub(crate) const MAX_STAGED_ALLOCATIONS: usize = 128;
pub(crate) const POSITION_CONSUMED: &str = "prepared allocation position is consumed or retired";

/// Admission of this attempt does not establish the result of an earlier attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StagedAllocationFailure {
    PositionConsumed,
    NotAdmitted(ObjectError),
    Unconfirmed(ObjectError),
}
impl StagedAllocationFailure {
    pub fn into_error(self) -> ObjectError {
        match self {
            Self::PositionConsumed => ObjectError::PreconditionFailed(POSITION_CONSUMED.into()),
            Self::NotAdmitted(error) | Self::Unconfirmed(error) => error,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StagedAllocation {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub allocation_id: u64,
    #[prost(uint32, tag = "3")]
    pub page_count: u32,
    #[prost(string, tag = "4")]
    pub bucket: String,
    #[prost(string, tag = "5")]
    pub key: String,
    /// Zero means the object must be absent, including absence of a tombstone.
    #[prost(uint64, tag = "6")]
    pub expected_generation: u64,
    #[prost(bool, tag = "7")]
    pub append: bool,
    /// Absolute metadata-server deadline; zero is the permanent version-1 mode.
    #[prost(uint64, tag = "8")]
    pub expires_at_ms: u64,
    /// Version 3 binds one prepared request to this metadata position.
    #[prost(bytes = "vec", tag = "9")]
    pub request_nonce: Vec<u8>,
    #[prost(uint64, tag = "10")]
    pub request_ttl_ms: u64,
}

impl StagedAllocation {
    pub fn allocation(&self) -> Result<ObjectPageAllocation, ObjectError> {
        self.validate()?;
        Ok(ObjectPageAllocation {
            format_version: 1,
            allocation_id: self.allocation_id,
            page_count: self.page_count,
        })
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        let valid_mode = match self.version {
            1 => self.expires_at_ms == 0 && self.request_nonce.is_empty() && self.request_ttl_ms == 0,
            2 => self.expires_at_ms != 0 && self.request_nonce.is_empty() && self.request_ttl_ms == 0,
            3 => self.request_nonce.len() == 32 && self.request_nonce.iter().any(|byte| *byte != 0)
                && ((self.request_ttl_ms == 0 && self.expires_at_ms == 0)
                    || ((1_000..=3_600_000).contains(&self.request_ttl_ms) && self.expires_at_ms != 0)),
            _ => false,
        };
        if !valid_mode
            || self.allocation_id == 0
            || !(1..=4096).contains(&self.page_count)
            || (self.append && self.expected_generation == 0)
        {
            return Err(invalid("invalid staged allocation"));
        }
        Ok(())
    }
    pub(crate) fn renews(&self, before: &Self) -> bool {
        matches!(self.version, 2 | 3) && self.version == before.version && before.expires_at_ms != 0
            && self.expires_at_ms > before.expires_at_ms
            && self.allocation_id == before.allocation_id && self.page_count == before.page_count
            && self.bucket == before.bucket && self.key == before.key
            && self.expected_generation == before.expected_generation && self.append == before.append
            && self.request_nonce == before.request_nonce && self.request_ttl_ms == before.request_ttl_ms
    }
    pub(crate) fn check_publication(&self, outcome: &ManifestOutcome) -> Result<(), ObjectError> {
        self.validate()?;
        outcome.validate()?;
        let after = outcome.after.as_ref().unwrap();
        if after.deleted
            || after.format_version != 2
            || after.bucket != self.bucket
            || after.key != self.key
            || outcome
                .before
                .as_ref()
                .map_or(0, |before| before.generation)
                != self.expected_generation
        {
            return Err(invalid("publication differs from staged target"));
        }
        let prefix = if self.append {
            let before = outcome
                .before
                .as_ref()
                .ok_or_else(|| invalid("staged append lacks prior manifest"))?;
            if before.deleted
                || before.format_version != 2
                || after.extents.get(..before.extents.len()) != Some(before.extents.as_slice())
            {
                return Err(invalid("staged append does not retain prior extents"));
            }
            before.extents.len()
        } else {
            0
        };
        let allocation = self.allocation()?;
        let fresh = &after.extents[prefix..];
        if fresh.len() > self.page_count as usize {
            return Err(invalid("staged publication exceeds allocation"));
        }
        for (index, extent) in fresh.iter().enumerate() {
            let page = allocation.page(index as u32)?;
            if extent.volume_id != page.chunk_id.volume_id()
                || extent.chunk_id != page.chunk_id.id()
                || extent.page_index != page.page_index
                || extent.page_offset != 0
                || extent.write_sequence != 1
            {
                return Err(invalid("staged publication uses an unowned page"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StagedAllocationChange {
    #[prost(message, optional, tag = "1")]
    pub before: Option<StagedAllocation>,
    #[prost(message, optional, tag = "2")]
    pub after: Option<StagedAllocation>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StagedAllocationRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// 1 allocate, 2 read, 3 abort, 4 publish, 5 list, 6 fence, 7 read fence, 8 renew, 9 expire, 10 cursor, 11 compact append.
    #[prost(uint32, tag = "2")]
    pub action: u32,
    #[prost(uint64, tag = "3")]
    pub allocation_id: u64,
    #[prost(uint32, tag = "4")]
    pub page_count: u32,
    #[prost(string, tag = "5")]
    pub bucket: String,
    #[prost(string, tag = "6")]
    pub key: String,
    #[prost(uint64, tag = "7")]
    pub expected_generation: u64,
    #[prost(bool, tag = "8")]
    pub append: bool,
    #[prost(message, optional, tag = "9")]
    pub outcome: Option<ManifestOutcome>,
    #[prost(uint64, tag = "10")]
    pub update_time_ms: u64,
    #[prost(uint32, tag = "11")]
    pub limit: u32,
    #[prost(uint64, tag = "12")]
    pub ttl_ms: u64,
    #[prost(uint64, tag = "13")]
    pub expected_expiry_ms: u64,
    #[prost(bytes = "vec", tag = "14")]
    pub request_nonce: Vec<u8>,
    #[prost(message, optional, tag = "15")]
    pub manifest_append: Option<crate::manifest_append::ManifestAppend>,
}

impl StagedAllocationRequest {
    pub(crate) fn is_read(&self) -> bool {
        matches!(self.action, 2 | 5 | 7 | 10)
    }
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        if self.version == 4 {
            if self.action != 11 || self.allocation_id == 0 || self.page_count != 0
                || !self.bucket.is_empty() || !self.key.is_empty() || self.expected_generation != 0
                || self.append || self.outcome.is_some() || self.limit != 0 || self.ttl_ms != 0
                || self.expected_expiry_ms != 0 || !self.request_nonce.is_empty() {
                return Err(invalid("invalid compact staged append request"));
            }
            return self.manifest_append.as_ref().ok_or_else(|| invalid("compact append missing"))?.validate();
        }
        if self.manifest_append.is_some() { return Err(invalid("compact append requires version 4")); }
        if self.version == 3 {
            if self.action == 1 {
                StagedAllocation {
                    version: 3, allocation_id: self.allocation_id, page_count: self.page_count,
                    bucket: self.bucket.clone(), key: self.key.clone(),
                    expected_generation: self.expected_generation, append: self.append,
                    expires_at_ms: u64::from(self.ttl_ms != 0),
                    request_nonce: self.request_nonce.clone(), request_ttl_ms: self.ttl_ms,
                }.validate()?;
                if self.expected_expiry_ms != 0 || self.limit != 0 || self.outcome.is_some() || self.update_time_ms != 0 {
                    return Err(invalid("invalid prepared allocation fields"));
                }
                return Ok(());
            }
            if self.action != 10 || self.allocation_id != 0 || self.page_count != 0
                || !self.bucket.is_empty() || !self.key.is_empty() || self.expected_generation != 0
                || self.append || self.outcome.is_some() || self.update_time_ms != 0 || self.limit != 0
                || self.ttl_ms != 0 || self.expected_expiry_ms != 0 || !self.request_nonce.is_empty() {
                return Err(invalid("invalid allocation cursor request"));
            }
            return Ok(());
        }
        if !self.request_nonce.is_empty() {
            return Err(invalid("request identity requires version 3"));
        }
        if self.version == 2 {
            if !matches!(self.action, 1 | 8 | 9)
                || (self.action != 9 && !(1_000..=3_600_000).contains(&self.ttl_ms))
                || (self.action == 9 && self.ttl_ms != 0)
                || (self.action == 1 && self.expected_expiry_ms != 0)
                || (self.action != 1 && self.expected_expiry_ms == 0) {
                return Err(invalid("invalid staged lease fields"));
            }
            if self.action != 1 {
                if self.allocation_id == 0 || self.page_count != 0 || !self.bucket.is_empty()
                    || !self.key.is_empty() || self.expected_generation != 0 || self.append
                    || self.outcome.is_some() || self.update_time_ms != 0 || self.limit != 0 {
                    return Err(invalid("invalid staged lease handle"));
                }
                return Ok(());
            }
        } else if self.version != 1 || self.ttl_ms != 0 || self.expected_expiry_ms != 0 {
            return Err(invalid("unsupported staged request version"));
        }
        if self.action == 1 {
            StagedAllocation {
                version: 1,
                allocation_id: 1,
                page_count: self.page_count,
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                expected_generation: self.expected_generation,
                append: self.append,
                expires_at_ms: 0,
                ..Default::default()
            }
            .validate()?;
            if self.allocation_id != 0
                || self.limit != 0
                || self.outcome.is_some()
                || self.update_time_ms != 0
            {
                return Err(invalid("invalid staged allocation request fields"));
            }
        } else if matches!(self.action, 6 | 7) {
            if self.allocation_id != 0
                || self.page_count != 0
                || !self.bucket.is_empty()
                || !self.key.is_empty()
                || self.expected_generation != 0
                || self.append
                || self.outcome.is_some()
                || self.update_time_ms != 0
                || self.limit != 0
            {
                return Err(invalid("invalid publication fence request fields"));
            }
        } else if (2..=5).contains(&self.action) {
            if !self.bucket.is_empty()
                || !self.key.is_empty()
                || self.page_count != 0
                || self.expected_generation != 0
                || self.append
                || (self.action != 5 && (self.allocation_id == 0 || self.limit != 0))
                || (self.action == 5 && !(1..=32).contains(&self.limit))
                || (self.action != 4 && (self.outcome.is_some() || self.update_time_ms != 0))
            {
                return Err(invalid("invalid staged handle fields"));
            }
            if self.action == 4 {
                self.outcome
                    .as_ref()
                    .ok_or_else(|| invalid("staged publication lacks outcome"))?
                    .validate()?;
            }
        } else {
            return Err(invalid("invalid staged request action"));
        }
        Ok(())
    }
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        let request: Self = decode(bytes, 2 * 1024 * 1024 + 8192)?;
        request.validate()?;
        Ok(request)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StagedAllocationReply {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, optional, tag = "2")]
    pub allocation: Option<StagedAllocation>,
    #[prost(message, optional, tag = "3")]
    pub manifest: Option<crate::object_manifest::ObjectManifest>,
    #[prost(uint64, repeated, tag = "4")]
    pub allocation_ids: Vec<u64>,
    #[prost(bool, tag = "5")]
    pub has_more: bool,
    #[prost(uint64, tag = "6")]
    pub publication_fence: u64,
    #[prost(uint64, tag = "7")]
    pub allocation_cursor: u64,
    #[prost(uint64, tag = "8")]
    pub committed_allocation_id: u64,
    #[prost(bytes, tag = "9")]
    pub manifest_sha256: Vec<u8>,
}
impl StagedAllocationReply {
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = decode(bytes, 1024 * 1024 + 8192)?;
        if value.version == 3 {
            if value.committed_allocation_id == 0 || value.manifest_sha256.len() != 32
                || value.allocation.is_some() || value.manifest.is_some() || !value.allocation_ids.is_empty()
                || value.has_more || value.publication_fence != 0 || value.allocation_cursor != 0 {
                return Err(invalid("invalid compact staged append acknowledgement"));
            }
            return Ok(value);
        }
        if value.committed_allocation_id != 0 || !value.manifest_sha256.is_empty() {
            return Err(invalid("compact acknowledgement requires version 3"));
        }
        if !matches!((value.version, value.allocation_cursor), (1, 0) | (2, 1..))
            || value.allocation_ids.len() > 32
            || value.allocation_ids.contains(&0)
            || value
                .allocation_ids
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || (value.has_more && value.allocation_ids.is_empty())
            || (value.allocation.is_some() && value.manifest.is_some())
            || ((value.allocation.is_some() || value.manifest.is_some())
                && (!value.allocation_ids.is_empty() || value.has_more))
            || (value.publication_fence != 0
                && (value.allocation.is_some()
                    || value.manifest.is_some()
                    || !value.allocation_ids.is_empty()
                    || value.has_more))
            || (value.allocation_cursor != 0
                && (value.allocation.is_some() || value.manifest.is_some()
                    || !value.allocation_ids.is_empty() || value.has_more || value.publication_fence != 0))
        {
            return Err(invalid("invalid staged reply"));
        }
        if let Some(allocation) = &value.allocation {
            allocation.validate()?;
        }
        if let Some(manifest) = &value.manifest {
            manifest.validate()?;
        }
        Ok(value)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct PublicationFenceChange {
    #[prost(uint64, tag = "1")]
    pub before: u64,
    #[prost(uint64, tag = "2")]
    pub after: u64,
}
fn decode<T: Message + Default>(bytes: &[u8], limit: usize) -> Result<T, ObjectError> {
    if bytes.len() > limit {
        return Err(invalid("staged message exceeds bound"));
    }
    let value = T::decode(bytes).map_err(|_| invalid("invalid staged protobuf"))?;
    if value.encode_to_vec() != bytes {
        return Err(invalid("noncanonical staged protobuf"));
    }
    Ok(value)
}
pub(crate) fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}

/// Persist before submission; retry this same handle after an uncertain reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedStagedAllocation {
    pub(crate) request: StagedAllocationRequest,
}
impl PreparedStagedAllocation {
    pub fn allocation_id(&self) -> u64 { self.request.allocation_id }
    pub fn to_bytes(&self) -> Vec<u8> { self.request.encode_to_vec() }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 8192 { return Err(invalid("prepared handle exceeds byte limit")); }
        let request = StagedAllocationRequest::from_payload(bytes)?;
        if request.version != 3 || request.action != 1 {
            return Err(invalid("prepared handle requires an identified allocation"));
        }
        Ok(Self { request })
    }
    pub(crate) fn matches(&self, record: &StagedAllocation) -> bool {
        let request = &self.request;
        record.version == 3 && record.allocation_id == request.allocation_id
            && record.bucket == request.bucket && record.key == request.key
            && record.page_count == request.page_count && record.append == request.append
            && record.expected_generation == request.expected_generation
            && record.request_nonce == request.request_nonce && record.request_ttl_ms == request.ttl_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prepared_staging_codec_binds_position_nonce_and_initial_ttl() {
        let request = StagedAllocationRequest {
            version: 3, action: 1, allocation_id: 7, page_count: 2, bucket: "bucket".into(),
            key: "key".into(), ttl_ms: 1000, request_nonce: vec![9; 32], ..Default::default()
        };
        let bytes = request.encode_to_vec();
        let prepared = PreparedStagedAllocation::from_bytes(&bytes).unwrap();
        assert_eq!(prepared.to_bytes(), bytes);
        assert_eq!(prepared.allocation_id(), 7);
        let record = StagedAllocation {
            version: 3, allocation_id: 7, page_count: 2, bucket: "bucket".into(), key: "key".into(),
            expires_at_ms: 2000, request_nonce: vec![9; 32], request_ttl_ms: 1000, ..Default::default()
        };
        record.validate().unwrap();
        assert!(prepared.matches(&record));
        let renewed = StagedAllocation { expires_at_ms: 3000, ..record.clone() };
        assert!(renewed.renews(&record));
        assert!(prepared.matches(&renewed));
        assert!(!StagedAllocation { request_nonce: vec![8; 32], ..renewed }.renews(&record));
        for command in [
            StagedAllocationRequest { request_nonce: Vec::new(), ..request.clone() },
            StagedAllocationRequest { request_nonce: vec![0; 32], ..request.clone() },
            StagedAllocationRequest { request_nonce: vec![1; 33], ..request.clone() },
            StagedAllocationRequest { allocation_id: 0, ..request.clone() },
            StagedAllocationRequest { version: 2, allocation_id: 0, ..request.clone() },
            StagedAllocationRequest { ttl_ms: 999, ..request.clone() },
            StagedAllocationRequest { action: 10, ..request.clone() },
        ] {
            assert!(PreparedStagedAllocation::from_bytes(&command.encode_to_vec()).is_err());
        }
        let cursor = StagedAllocationRequest { version: 3, action: 10, ..Default::default() };
        assert!(StagedAllocationRequest::from_payload(&cursor.encode_to_vec()).is_ok());
        assert!(PreparedStagedAllocation::from_bytes(&cursor.encode_to_vec()).is_err());
        for cut in 0..bytes.len() {
            assert!(PreparedStagedAllocation::from_bytes(&bytes[..cut]).is_err());
        }
        let reply = StagedAllocationReply { version: 2, allocation_cursor: 8, ..Default::default() };
        assert_eq!(StagedAllocationReply::from_payload(&reply.encode_to_vec()).unwrap(), reply);
        assert!(StagedAllocationReply::from_payload(&StagedAllocationReply {
            version: 1, ..reply.clone()
        }.encode_to_vec()).is_err());
        assert!(StagedAllocationReply::from_payload(&StagedAllocationReply {
            publication_fence: 2, ..reply
        }.encode_to_vec()).is_err());
    }
    #[test]
    fn leased_staging_codec_bounds_ttl_and_preserves_deadline_identity() {
        let request = StagedAllocationRequest { version: 2, action: 1, page_count: 1,
            bucket: "bucket".into(), key: "key".into(), ttl_ms: 1000, ..Default::default() };
        assert_eq!(StagedAllocationRequest::from_payload(&request.encode_to_vec()).unwrap(), request);
        for ttl_ms in [0, 999, 3_600_001, u64::MAX] {
            assert!(StagedAllocationRequest { ttl_ms, ..request.clone() }.validate().is_err());
        }
        assert!(StagedAllocationRequest { version: 1, ..request.clone() }.validate().is_err());
        let renew = StagedAllocationRequest { version: 2, action: 8, allocation_id: 1,
            expected_expiry_ms: 2000, ttl_ms: 1000, ..Default::default() };
        assert_eq!(StagedAllocationRequest::from_payload(&renew.encode_to_vec()).unwrap(), renew);
        assert!(StagedAllocationRequest { expected_expiry_ms: 0, ..renew.clone() }.validate().is_err());
        assert!(StagedAllocationRequest { version: 1, ..renew.clone() }.validate().is_err());
        assert!(StagedAllocationRequest { action: 9, ..renew }.validate().is_err());
        let leased = StagedAllocation { version: 2, allocation_id: 1, page_count: 1,
            bucket: "bucket".into(), key: "key".into(), expires_at_ms: 2000, ..Default::default() };
        leased.validate().unwrap();
        assert!(StagedAllocation { version: 1, ..leased.clone() }.validate().is_err());
        assert!(StagedAllocation { expires_at_ms: 0, ..leased }.validate().is_err());
    }
    #[test]
    fn staged_codec_and_ownership_reject_malformed_requests_and_foreign_pages() {
        let request = StagedAllocationRequest {
            version: 1,
            action: 1,
            page_count: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            ..Default::default()
        };
        let bytes = request.encode_to_vec();
        assert_eq!(
            StagedAllocationRequest::from_payload(&bytes).unwrap(),
            request
        );
        for cut in 0..bytes.len() {
            assert!(StagedAllocationRequest::from_payload(&bytes[..cut]).is_err());
        }
        let mut unknown = bytes;
        unknown.extend_from_slice(&[0x60, 1]);
        assert!(StagedAllocationRequest::from_payload(&unknown).is_err());
        let record = StagedAllocation {
            version: 1,
            allocation_id: 17,
            page_count: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            expected_generation: 0,
            append: false,
            expires_at_ms: 0,
            ..Default::default()
        };
        let mut manifest = crate::object_manifest::ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 1,
            length: 1,
            deleted: false,
            extents: vec![crate::object_manifest::ObjectExtent {
                group: "group-a".into(),
                chunk_id: 17,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![1; 32],
                ..Default::default()
            }],
        };
        record
            .check_publication(&ManifestOutcome::decide(None, manifest.clone()).unwrap())
            .unwrap();
        manifest.extents[0].chunk_id = 18;
        assert!(record
            .check_publication(&ManifestOutcome::decide(None, manifest).unwrap())
            .is_err());
        let mut wrong = request;
        wrong.action = 3;
        assert!(wrong.validate().is_err());
        wrong = StagedAllocationRequest {
            version: 1,
            action: 4,
            allocation_id: 17,
            ..Default::default()
        };
        assert!(wrong.validate().is_err());
        let reply = StagedAllocationReply {
            version: 1,
            allocation_ids: vec![9, 4],
            ..Default::default()
        };
        assert!(StagedAllocationReply::from_payload(&reply.encode_to_vec()).is_err());
    }
}
