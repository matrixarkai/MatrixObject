// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Compact decisions bind immutable manifests without embedding their extent lists.
use crate::{object_manifest::ObjectManifest, ObjectError};
use prost::Message;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ReadSessionOpen {
    #[prost(uint64, tag = "1")]
    pub retention_id: u64,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub key: String,
    #[prost(uint64, tag = "4")]
    pub generation: u64,
    #[prost(uint64, tag = "5")]
    pub expires_at_ms: u64,
    #[prost(bytes = "vec", tag = "6")]
    pub manifest_sha256: Vec<u8>,
}
impl ReadSessionOpen {
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if self.retention_id == 0
            || self.generation == 0
            || self.expires_at_ms == 0
            || self.manifest_sha256.len() != 32
        {
            return Err(invalid("invalid compact read session creation"));
        }
        Ok(())
    }
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ReadSessionRemoval {
    #[prost(uint64, tag = "1")]
    pub retention_id: u64,
    #[prost(uint64, tag = "2")]
    pub expected_expiry_ms: u64,
}
impl ReadSessionRemoval {
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        if self.retention_id == 0 || self.expected_expiry_ms == 0 {
            return Err(invalid("invalid compact read session removal"));
        }
        Ok(())
    }
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}

/// The digest that binds a manifest: the hash chain over its extents (`manifest_chain`).
pub(crate) fn manifest_sha256(manifest: &ObjectManifest) -> Result<Vec<u8>, ObjectError> {
    Ok(crate::manifest_chain::manifest_digest(manifest))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_read_session_hash_is_the_pinned_chained_digest_at_wire_boundaries() {
        // The witness is durable; the digest definition must stay stable.
        let first = crate::object_manifest::ObjectExtent {
            group: "g".into(),
            volume_id: 2,
            chunk_id: 3,
            page_index: 4,
            page_offset: 5,
            length: 3,
            write_sequence: 257,
            data_checksum: 0x0102030405060708,
            data_sha256: vec![7; 32],
            ..Default::default()
        };
        let second = crate::object_manifest::ObjectExtent {
            page_index: 5,
            object_offset: 3,
            ..first.clone()
        };
        let pinned = ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "object".into(),
            generation: 129,
            length: 6,
            extents: vec![first, second],
            ..Default::default()
        };
        pinned.validate().unwrap();
        let digest: String = manifest_sha256(&pinned)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(
            digest,
            "20882c514505c6fe174adb56c5a3efbe4f73cf97786c755276bc46c6af4bf219"
        );
        for generation in [1, 127, 128, 16383, 16384, u64::MAX] {
            for count in [0, 1, 127, 128, 4096] {
                let manifest = ObjectManifest {
                    format_version: 2,
                    bucket: "bucket".into(),
                    key: "nested/object-λ".repeat(8),
                    generation,
                    length: count,
                    extents: (0..count)
                        .map(|index| crate::object_manifest::ObjectExtent {
                            group: format!("group-{}", index % 32),
                            chunk_id: 1,
                            page_index: index,
                            object_offset: index,
                            length: 1,
                            write_sequence: generation,
                            data_sha256: vec![index as u8; 32],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                };
                manifest.validate().unwrap();
                assert_eq!(
                    manifest_sha256(&manifest).unwrap(),
                    crate::manifest_chain::manifest_digest(&manifest)
                );
            }
        }
    }
}
