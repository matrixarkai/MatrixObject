// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Complete, allocation-free manifest keys for an in-process digest cache.
//! This fingerprint is never persisted or sent. Wire digests keep their hash-chain format.
use crate::object_manifest::ObjectManifest;
use std::hash::{Hash, Hasher};

const DOMAIN: &[u8] = b"MatrixObject.ManifestDigestMemo.v2";
const BUFFER_BYTES: usize = 512;
#[derive(Clone)]
struct FingerprintHasher {
    context: ring::digest::Context,
    buffer: [u8; BUFFER_BYTES],
    used: usize,
}
impl FingerprintHasher {
    fn new() -> Self {
        let mut context = ring::digest::Context::new(&ring::digest::SHA256);
        context.update(DOMAIN);
        Self {
            context,
            buffer: [0; BUFFER_BYTES],
            used: 0,
        }
    }
    fn digest(&self) -> [u8; 32] {
        let mut context = self.context.clone();
        context.update(&self.buffer[..self.used]);
        let mut result = [0; 32];
        result.copy_from_slice(context.finish().as_ref());
        result
    }
}
impl Hasher for FingerprintHasher {
    fn write(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let count = bytes.len().min(self.buffer.len() - self.used);
            self.buffer[self.used..self.used + count].copy_from_slice(&bytes[..count]);
            self.used += count;
            bytes = &bytes[count..];
            if self.used == self.buffer.len() {
                self.context.update(&self.buffer);
                self.used = 0;
            }
        }
    }
    fn finish(&self) -> u64 {
        u64::from_le_bytes(self.digest()[..8].try_into().unwrap())
    }
}

/// A full scan bound to an immutable manifest borrow. Only an exact extension can
/// reuse its extent hash state; a caller's mutable manifest always gets a new scan.
pub(super) struct VerifiedManifest<'a> {
    manifest: &'a ObjectManifest,
    extents: FingerprintHasher,
}
impl<'a> VerifiedManifest<'a> {
    pub(super) fn new(manifest: &'a ObjectManifest) -> Self {
        let mut extents = FingerprintHasher::new();
        b"extents".hash(&mut extents);
        for extent in &manifest.extents {
            // Derived Hash includes every extent field, including future additions.
            extent.hash(&mut extents);
        }
        Self { manifest, extents }
    }

    pub(super) fn manifest(&self) -> &'a ObjectManifest {
        self.manifest
    }

    pub(super) fn key(&self) -> [u8; 32] {
        // Exhaustive binding makes a new manifest field a compile-time obligation.
        // The extent count is separate from its stream so a verified prefix can be
        // extended without hashing that prefix again. This key never leaves memory.
        let ObjectManifest {
            format_version, bucket, key, generation, length, extents, deleted,
        } = self.manifest;
        let mut hasher = FingerprintHasher::new();
        b"manifest".hash(&mut hasher);
        format_version.hash(&mut hasher);
        bucket.hash(&mut hasher);
        key.hash(&mut hasher);
        generation.hash(&mut hasher);
        length.hash(&mut hasher);
        deleted.hash(&mut hasher);
        extents.len().hash(&mut hasher);
        self.extents.digest().hash(&mut hasher);
        hasher.digest()
    }

    pub(super) fn extended<'b>(&self, after: &'b ObjectManifest) -> Option<VerifiedManifest<'b>> {
        if !after.extents.starts_with(&self.manifest.extents) {
            return None;
        }
        let mut extents = self.extents.clone();
        for extent in &after.extents[self.manifest.extents.len()..] {
            extent.hash(&mut extents);
        }
        Some(VerifiedManifest { manifest: after, extents })
    }
}

#[cfg(test)]
fn of(manifest: &ObjectManifest) -> [u8; 32] {
    VerifiedManifest::new(manifest).key()
}

#[cfg(test)]
#[path = "manifest_fingerprint_tests.rs"]
mod tests;
