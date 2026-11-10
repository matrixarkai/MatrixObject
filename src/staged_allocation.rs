// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable ownership of fresh page namespaces before object publication.
use crate::{
    object_manifest::{ManifestOutcome, ObjectPageAllocation},
    ObjectError,
};
use prost::Message;

pub(crate) const MAX_STAGED_ALLOCATIONS: usize = 128;

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
        if !matches!((self.version, self.expires_at_ms), (1, 0) | (2, 1..))
            || self.allocation_id == 0
            || !(1..=4096).contains(&self.page_count)
            || (self.append && self.expected_generation == 0)
        {
            return Err(invalid("invalid staged allocation"));
        }
        Ok(())
    }
    pub(crate) fn renews(&self, before: &Self) -> bool {
        self.version == 2 && before.version == 2
            && self.expires_at_ms > before.expires_at_ms
            && self.allocation_id == before.allocation_id && self.page_count == before.page_count
            && self.bucket == before.bucket && self.key == before.key
            && self.expected_generation == before.expected_generation && self.append == before.append
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
    /// 1 allocate, 2 read, 3 abort, 4 publish, 5 list, 6 fence, 7 read fence, 8 renew, 9 expire.
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
}

impl StagedAllocationRequest {
    pub(crate) fn is_read(&self) -> bool {
        matches!(self.action, 2 | 5 | 7)
    }
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
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
}
impl StagedAllocationReply {
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = decode(bytes, 1024 * 1024 + 8192)?;
        if value.version != 1
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

#[cfg(test)]
mod tests {
    use super::*;
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
