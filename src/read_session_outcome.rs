// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Compact decisions bind immutable manifests without embedding their extent lists.
use crate::{object_manifest::ObjectManifest, ObjectError};
use prost::{
    bytes::{buf::UninitSlice, BufMut},
    Message,
};

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

struct DigestSink {
    context: ring::digest::Context,
    scratch: [u8; 32],
    written: usize,
}
// SAFETY: chunk_mut exposes an initialized buffer. advance_mut checks that
// the entire advanced prefix fits that buffer before hashing it. put_slice
// consumes its input directly; no pointer or uninitialized byte is retained.
unsafe impl BufMut for DigestSink {
    fn remaining_mut(&self) -> usize {
        usize::MAX - self.written
    }
    fn chunk_mut(&mut self) -> &mut UninitSlice {
        let available = self.scratch.len().min(self.remaining_mut());
        UninitSlice::new(&mut self.scratch[..available])
    }
    unsafe fn advance_mut(&mut self, count: usize) {
        assert!(count <= self.scratch.len());
        assert!(count <= self.remaining_mut());
        self.context.update(&self.scratch[..count]);
        self.written = self
            .written
            .checked_add(count)
            .expect("manifest digest length");
    }
    fn put_slice(&mut self, bytes: &[u8]) {
        assert!(bytes.len() <= self.remaining_mut());
        self.context.update(bytes);
        self.written = self
            .written
            .checked_add(bytes.len())
            .expect("manifest digest length");
    }
}
pub(crate) fn manifest_sha256(manifest: &ObjectManifest) -> Result<Vec<u8>, ObjectError> {
    let mut sink = DigestSink {
        context: ring::digest::Context::new(&ring::digest::SHA256),
        scratch: [0; 32],
        written: 0,
    };
    manifest
        .encode(&mut sink)
        .map_err(|_| invalid("manifest digest encoding failed"))?;
    Ok(sink.context.finish().as_ref().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_read_session_hash_matches_protobuf_bytes_at_wire_boundaries() {
        // The witness is durable; its protobuf interpretation must stay stable.
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
            "e63b1fa312d4b400aa116295e19a96fcefb0830dff763672288af68bcd4cacb3"
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
                    ring::digest::digest(&ring::digest::SHA256, &manifest.encode_to_vec()).as_ref()
                );
            }
        }
    }
}
