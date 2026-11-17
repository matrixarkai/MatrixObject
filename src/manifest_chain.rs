// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The digest that binds a manifest, as a hash chain over its extents.
//!
//! ```text
//! c_0  = SHA-256("MOC0")
//! c_i  = SHA-256("MOC1" || c_{i-1} || encoding(extent_i))
//! D(m) = SHA-256("MOC2" || encoding(header(m)) || c_n || n as u64 little endian)
//! ```
//!
//! `encoding` is the canonical protobuf encoding of the message; `header(m)` is the manifest
//! with its extents removed (format version, bucket, key, generation, length, deleted). An
//! append of `k` extents to a manifest whose chain is known costs `k` hash steps plus one
//! header hash, whatever the number of extents already present. A checkpoint of the chain is
//! kept for every 64th extent so a later change to a manifest's tail can restart from a
//! nearby point instead of from the first extent.
use crate::object_manifest::{ObjectExtent, ObjectManifest};
use prost::Message;

/// A chain value is kept for every `CHECKPOINT_EVERY` extents.
pub(crate) const CHECKPOINT_EVERY: usize = 64;

#[cfg(test)]
thread_local! {
    static FULL_CHAINS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SOUNDNESS_CHECKS: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}
/// Chains computed over a whole extent list on this thread since the last reset (tests only).
#[cfg(test)]
pub(crate) fn full_chains() -> usize {
    FULL_CHAINS.with(|calls| calls.get())
}
/// Run `work` without its whole-manifest chains being counted (test-only soundness checks).
/// `work` is skipped when a measurement has turned the checks off on this thread.
#[cfg(test)]
pub(crate) fn uncounted<T: Default>(work: impl FnOnce() -> T) -> T {
    if !SOUNDNESS_CHECKS.with(|on| on.get()) {
        return T::default();
    }
    let before = full_chains();
    let result = work();
    FULL_CHAINS.with(|calls| calls.set(before));
    result
}
/// Measurements turn the test-only soundness checks (which recompute what they check) off.
#[cfg(all(test, feature = "metadata-consensus"))]
pub(crate) fn set_soundness_checks(on: bool) {
    SOUNDNESS_CHECKS.with(|flag| flag.set(on));
}
#[cfg(all(test, feature = "metadata-consensus"))]
pub(crate) fn reset_full_chains() {
    FULL_CHAINS.with(|calls| calls.set(0));
}

fn hash(parts: &[&[u8]]) -> [u8; 32] {
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    for part in parts {
        context.update(part);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(context.finish().as_ref());
    out
}

/// The chain over a prefix of a manifest's extents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChainState {
    head: [u8; 32],
    count: usize,
    /// `checkpoints[j]` is the chain value after `(j + 1) * CHECKPOINT_EVERY` extents.
    checkpoints: Vec<[u8; 32]>,
}

impl ChainState {
    pub(crate) fn empty() -> Self {
        Self { head: hash(&[b"MOC0"]), count: 0, checkpoints: Vec::new() }
    }
    pub(crate) fn of(extents: &[ObjectExtent]) -> Self {
        #[cfg(test)]
        FULL_CHAINS.with(|calls| calls.set(calls.get() + 1));
        Self::empty().extended(extents)
    }
    /// `of` for the extents of a stored manifest.
    pub(crate) fn of_stored(extents: &crate::extent_list::ExtentList) -> Self {
        #[cfg(test)]
        FULL_CHAINS.with(|calls| calls.set(calls.get() + 1));
        Self::empty().extended_by(extents.iter())
    }
    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.count
    }
    /// The chain after `suffix` is appended; `self` is unchanged.
    pub(crate) fn extended(&self, suffix: &[ObjectExtent]) -> Self {
        self.extended_by(suffix.iter())
    }
    /// `extended` over any sequence of extents.
    pub(crate) fn extended_by<'a>(&self, suffix: impl Iterator<Item = &'a ObjectExtent>) -> Self {
        let mut next = self.clone();
        let mut scratch = Vec::with_capacity(160);
        for extent in suffix {
            scratch.clear();
            extent.encode(&mut scratch).expect("a Vec grows");
            next.head = hash(&[b"MOC1", &next.head, &scratch]);
            next.count += 1;
            if next.count % CHECKPOINT_EVERY == 0 {
                next.checkpoints.push(next.head);
            }
        }
        next
    }
    /// The chain value after exactly `count` extents, if it is a checkpoint (or the start).
    #[cfg(test)]
    pub(crate) fn checkpoint(&self, count: usize) -> Option<[u8; 32]> {
        if count == 0 {
            Some(hash(&[b"MOC0"]))
        } else if count % CHECKPOINT_EVERY == 0 {
            self.checkpoints.get(count / CHECKPOINT_EVERY - 1).copied()
        } else {
            None
        }
    }
    /// The digest of `manifest`, whose extents this chain should cover exactly. The extent
    /// count is part of the digest, so a chain of a different length gives a digest that no
    /// decision for this manifest carries.
    pub(crate) fn digest_stored(&self, manifest: &crate::extent_list::StoredManifest) -> Vec<u8> {
        let header = ObjectManifest {
            format_version: manifest.format_version,
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            generation: manifest.generation,
            length: manifest.length,
            extents: Vec::new(),
            deleted: manifest.deleted,
        };
        let count = (self.count as u64).to_le_bytes();
        hash(&[b"MOC2", &header.encode_to_vec(), &self.head, &count]).to_vec()
    }
    pub(crate) fn digest(&self, manifest: &ObjectManifest) -> Vec<u8> {
        let header = ObjectManifest {
            format_version: manifest.format_version,
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            generation: manifest.generation,
            length: manifest.length,
            extents: Vec::new(),
            deleted: manifest.deleted,
        };
        let count = (self.count as u64).to_le_bytes();
        hash(&[b"MOC2", &header.encode_to_vec(), &self.head, &count]).to_vec()
    }
}

/// The digest of a whole manifest, from nothing. O(extents); appends use `ChainState`.
pub(crate) fn manifest_digest(manifest: &ObjectManifest) -> Vec<u8> {
    ChainState::of(&manifest.extents).digest(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn synthetic(count: usize, generation: u64) -> ObjectManifest {
        ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "object".into(),
            generation,
            length: count as u64,
            extents: (0..count)
                .map(|index| ObjectExtent {
                    group: format!("group-{}", index % 7),
                    volume_id: 1,
                    chunk_id: index as u64 + 1,
                    page_index: 0,
                    object_offset: index as u64,
                    length: 1,
                    write_sequence: 1,
                    data_checksum: index as u64 * 31,
                    data_sha256: vec![(index % 251) as u8; 32],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The definition, written out independently of `ChainState`: one pass, no checkpoints.
    fn naive_digest(manifest: &ObjectManifest) -> Vec<u8> {
        let sha = |parts: &[&[u8]]| {
            let mut context = ring::digest::Context::new(&ring::digest::SHA256);
            for part in parts {
                context.update(part);
            }
            context.finish().as_ref().to_vec()
        };
        let mut head = sha(&[b"MOC0"]);
        for extent in &manifest.extents {
            head = sha(&[b"MOC1", &head, &extent.encode_to_vec()]);
        }
        let header = ObjectManifest { extents: Vec::new(), ..manifest.clone() };
        sha(&[
            b"MOC2",
            &header.encode_to_vec(),
            &head,
            &(manifest.extents.len() as u64).to_le_bytes(),
        ])
    }

    #[test]
    fn the_chained_digest_matches_its_definition_and_is_pinned_at_the_boundaries() {
        // Golden values: the digest is durable, so its definition must not drift.
        let golden: [(usize, &str); 5] = [
            (0, "3e9d76bbe17fc067c4c961048d6976e06efc964f80954f07105242ed4f783834"),
            (1, "63b0e6a8a53b68179cfcee3c387e5da23b5dbe0af049f084aa3b317691f6ca6d"),
            (64, "175e86d6630d19ef62d8c42aad791a3fab0d4fee0e3636adc98602284deb945c"),
            (65, "7fc9f5529ae5f01f6fe5e53cc2c43aab40da14307a8e53f86bbb363972c90073"),
            (4096, "74a72b0d237a928493bff9cfad28783ef7718bad4e340ff16a17300b709e50a8"),
        ];
        for (count, expected) in golden {
            let manifest = synthetic(count, 129);
            let digest = manifest_digest(&manifest);
            assert_eq!(digest, naive_digest(&manifest), "{count} extents");
            assert_eq!(hex(&digest), expected, "{count} extents");
        }
    }

    #[test]
    fn extending_a_chain_equals_recomputing_it_and_checkpoints_sit_at_multiples_of_64() {
        let whole = synthetic(300, 7);
        let direct = ChainState::of(&whole.extents);
        let mut steps = ChainState::empty();
        for chunk in whole.extents.chunks(5) {
            steps = steps.extended(chunk);
        }
        assert_eq!(direct, steps);
        assert_eq!(direct.checkpoints.len(), 300 / CHECKPOINT_EVERY);
        for count in [0usize, 64, 128, 256] {
            let prefix = ChainState::of(&whole.extents[..count]);
            assert_eq!(direct.checkpoint(count), Some(prefix.head), "checkpoint {count}");
        }
        assert_eq!(direct.checkpoint(65), None);
        assert_eq!(direct.checkpoint(320), None);
        // a digest from the extended chain is the digest of the longer manifest
        let shorter = synthetic(250, 6);
        let mut longer = synthetic(300, 7);
        longer.extents[..250].clone_from_slice(&shorter.extents);
        assert_eq!(
            ChainState::of(&shorter.extents).extended(&longer.extents[250..]).digest(&longer),
            manifest_digest(&longer)
        );
    }

    #[test]
    fn the_digest_depends_on_every_extent_in_order_and_on_the_header() {
        let base = synthetic(70, 3);
        let reference = manifest_digest(&base);
        let mut changed = Vec::new();
        let mut first = base.clone();
        first.extents[0].data_sha256[0] ^= 1;
        changed.push(first);
        let mut middle = base.clone();
        middle.extents[35].length += 1;
        changed.push(middle);
        let mut last = base.clone();
        last.extents[69].chunk_id += 1;
        changed.push(last);
        let mut swapped = base.clone();
        swapped.extents.swap(10, 11);
        changed.push(swapped);
        let mut shorter = base.clone();
        shorter.extents.pop();
        changed.push(shorter);
        for edit in [
            |m: &mut ObjectManifest| m.generation += 1,
            |m: &mut ObjectManifest| m.length += 1,
            |m: &mut ObjectManifest| m.bucket = "other".into(),
            |m: &mut ObjectManifest| m.key = "other".into(),
            |m: &mut ObjectManifest| m.format_version = 1,
            |m: &mut ObjectManifest| m.deleted = true,
        ] {
            let mut header = base.clone();
            edit(&mut header);
            changed.push(header);
        }
        for (index, manifest) in changed.iter().enumerate() {
            assert_ne!(manifest_digest(manifest), reference, "edit {index}");
        }
    }
}
