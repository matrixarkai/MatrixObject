// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! An exact prior manifest and a bounded immutable suffix publication.
use crate::{
    object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest},
    ObjectError,
};
use prost::Message;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestAppend {
    #[prost(string, tag = "1")]
    pub bucket: String,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(uint64, tag = "3")]
    pub before_generation: u64,
    #[prost(uint64, tag = "4")]
    pub before_length: u64,
    #[prost(uint32, tag = "5")]
    pub before_extents: u32,
    #[prost(bytes, tag = "6")]
    pub before_sha256: Vec<u8>,
    #[prost(message, repeated, tag = "7")]
    pub suffix: Vec<ObjectExtent>,
    #[prost(uint64, tag = "8")]
    pub after_length: u64,
    #[prost(bytes, tag = "9")]
    pub after_sha256: Vec<u8>,
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
impl ManifestAppend {
    /// Bind the exact live prefix and immutable suffix for compact staged publication.
    pub fn decide(outcome: &ManifestOutcome) -> Result<Self, ObjectError> {
        outcome.validate()?;
        Self::decide_validated(outcome)
    }
    /// `decide` for an outcome the caller has just validated in full.
    pub(crate) fn decide_validated(outcome: &ManifestOutcome) -> Result<Self, ObjectError> {
        let before_sha256 = crate::read_session_outcome::manifest_sha256(
            outcome.before.as_ref().ok_or_else(|| invalid("append requires a prior manifest"))?,
        )?;
        let after_sha256 = crate::read_session_outcome::manifest_sha256(outcome.after.as_ref().unwrap())?;
        Self::decide_with_digests(outcome, before_sha256, after_sha256)
    }
    /// `decide_validated` where the caller holds the SHA-256 digests of the two manifests'
    /// encodings (it must have computed them from these manifests).
    pub(crate) fn decide_with_digests(
        outcome: &ManifestOutcome,
        before_sha256: Vec<u8>,
        after_sha256: Vec<u8>,
    ) -> Result<Self, ObjectError> {
        let before = outcome
            .before
            .as_ref()
            .ok_or_else(|| invalid("append requires a prior manifest"))?;
        let after = outcome.after.as_ref().unwrap();
        if before.format_version != 2
            || after.format_version != 2
            || before.deleted
            || after.deleted
            || after.extents.get(..before.extents.len()) != Some(before.extents.as_slice())
        {
            return Err(invalid("append changes immutable prefix"));
        }
        let value = Self {
            bucket: before.bucket.clone(),
            key: before.key.clone(),
            before_generation: before.generation,
            before_length: before.length,
            before_extents: before.extents.len() as u32,
            before_sha256,
            suffix: after.extents[before.extents.len()..].to_vec(),
            after_length: after.length,
            after_sha256,
        };
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if self.before_generation == 0
            || self.before_generation == u64::MAX
            || self.before_sha256.len() != 32
            || self.after_sha256.len() != 32
            || self.before_extents > 4095
            || self.suffix.is_empty()
            || self.suffix.len() > 4096 - self.before_extents as usize
            || self.after_length <= self.before_length
            || self.encoded_len() > 1024 * 1024
        {
            return Err(invalid("invalid compact manifest append"));
        }
        let mut end = self.before_length;
        for extent in &self.suffix {
            if extent.object_offset != end
                || extent.length == 0
                || extent.page_offset != 0
                || extent.write_sequence != 1
                || extent.data_sha256.len() != 32
            {
                return Err(invalid("invalid compact append suffix"));
            }
            end = end
                .checked_add(extent.length)
                .ok_or_else(|| invalid("append length overflow"))?;
        }
        if end != self.after_length {
            return Err(invalid("append suffix length mismatch"));
        }
        Ok(())
    }
    /// Expand against a manifest the caller holds (validated here); the server uses
    /// `expand_stored`. Used by tests.
    #[cfg(test)]
    pub(crate) fn expand(&self, before: &ObjectManifest) -> Result<ManifestOutcome, ObjectError> {
        before.validate()?;
        self.expand_stored(before, &crate::object_manifest::StoredDigest::of(before))
    }
    /// `expand` for a manifest read from the catalog, which was validated when it was stored,
    /// together with the chain and digest the catalog keeps for it. Nothing here walks the
    /// extents already present: the new extents are validated against them (an integer
    /// comparison per prior page), and the digest of the result is the stored chain extended
    /// by the suffix.
    pub(crate) fn expand_stored(&self, before: &ObjectManifest, stored: &crate::object_manifest::StoredDigest) -> Result<ManifestOutcome, ObjectError> {
        self.validate()?;
        self.check_prior(before, stored)?;
        let mut after = before.clone();
        after.generation += 1;
        after.length = self.after_length;
        after.extents.extend(self.suffix.iter().cloned());
        // `before` was validated above and `after` is `before` plus the suffix, so only the
        // suffix needs a full check; `decide` would validate both whole manifests again.
        after.validate_extension(before)?;
        let after_digest = stored.chain.extended(&self.suffix).digest(&after);
        let outcome = ManifestOutcome {
            format_version: 1,
            before: Some(before.clone()),
            after: Some(after),
        };
        if after_digest != self.after_sha256 {
            return Err(invalid("compact append resulting digest mismatch"));
        }
        Ok(outcome)
    }

    /// Whether `before`, the manifest the catalog holds, is the one this decision was made
    /// against. A matching stored digest binds the content, so an expansion computed earlier
    /// from a manifest with this digest is the expansion of this one too.
    pub(crate) fn check_prior(&self, before: &ObjectManifest, stored: &crate::object_manifest::StoredDigest) -> Result<(), ObjectError> {
        if before.deleted
            || before.format_version != 2
            || before.bucket != self.bucket
            || before.key != self.key
            || before.generation != self.before_generation
            || before.length != self.before_length
            || before.extents.len() != self.before_extents as usize
            || stored.digest != self.before_sha256
        {
            return Err(ObjectError::PreconditionFailed(
                "compact append prior manifest changed".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chained(count: usize, generation: u64) -> ObjectManifest {
        ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            generation,
            length: count as u64,
            extents: (0..count)
                .map(|index| ObjectExtent {
                    group: "group".into(),
                    chunk_id: index as u64 + 1,
                    object_offset: index as u64,
                    length: 1,
                    write_sequence: 1,
                    data_sha256: vec![(index % 251) as u8; 32],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
    fn longer(before: &ObjectManifest, added: usize) -> ObjectManifest {
        let mut after = before.clone();
        after.generation += 1;
        for step in 0..added {
            after.extents.push(ObjectExtent {
                group: "group".into(),
                chunk_id: 70_000 + step as u64,
                object_offset: after.length,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![200 + step as u8; 32],
                ..Default::default()
            });
            after.length += 1;
        }
        after
    }

    /// A stored chain that is stale (another manifest's) or wrong (its head altered) must not
    /// let an append through, whether or not the digest stored beside it still looks right.
    #[test]
    fn a_stale_or_wrong_stored_chain_is_refused() {
        use crate::object_manifest::StoredDigest;
        let before = chained(130, 4);
        let after = longer(&before, 2);
        let full = ManifestOutcome::decide(Some(before.clone()), after).unwrap();
        let delta = ManifestAppend::decide(&full).unwrap();
        let good = StoredDigest::of(&before);
        assert_eq!(delta.expand_stored(&before, &good).unwrap(), full);
        // stale: the chain and digest of an earlier generation of the same object
        let stale = StoredDigest::of(&chained(129, 3));
        assert!(delta.expand_stored(&before, &stale).is_err());
        // a digest that is not the stored manifest's
        let mut wrong_digest = good.clone();
        wrong_digest.digest[0] ^= 1;
        assert!(delta.expand_stored(&before, &wrong_digest).is_err());
        // the right digest beside a chain whose head or checkpoints were altered: the digest
        // of the result is built from the chain, so it no longer matches the decision
        let mut wrong_chain = good.clone();
        let mut other = chained(130, 4);
        other.extents[5].length = 9;
        wrong_chain.chain = crate::manifest_chain::ChainState::of(&other.extents);
        let mut forged_head = good.clone();
        forged_head.chain = crate::manifest_chain::ChainState::of(&before.extents[..129]);
        for corrupt in [wrong_chain, forged_head] {
            assert_eq!(corrupt.digest, good.digest);
            assert!(delta.expand_stored(&before, &corrupt).is_err());
        }
    }

    /// The page rule for what an append adds is checked against the stored manifest's pages
    /// without walking it again; the same result must come from the full rule.
    #[test]
    fn expansion_refuses_a_page_the_stored_manifest_already_names() {
        use crate::object_manifest::StoredDigest;
        for count in [1usize, 70, 400] {
            let before = chained(count, 2);
            let stored = StoredDigest::of(&before);
            let mut after = longer(&before, 3);
            // the second new extent names the page of the first stored extent
            let repeated = before.extents[0].clone();
            let at = count + 1;
            after.extents[at].chunk_id = repeated.chunk_id;
            after.extents[at].volume_id = repeated.volume_id;
            after.extents[at].page_index = repeated.page_index;
            after.extents[at].group = repeated.group.clone();
            assert!(after.validate().is_err(), "the full rule refuses {count}");
            let chain = stored.chain.extended(&after.extents[count..]);
            let delta = ManifestAppend {
                bucket: before.bucket.clone(),
                key: before.key.clone(),
                before_generation: before.generation,
                before_length: before.length,
                before_extents: count as u32,
                before_sha256: stored.digest.clone(),
                suffix: after.extents[count..].to_vec(),
                after_length: after.length,
                after_sha256: chain.digest(&after),
            };
            delta.validate().unwrap();
            assert!(delta.expand_stored(&before, &stored).is_err(), "{count}");
        }
    }

    /// The outcome an append expands to is byte-for-byte the one built the long way, by
    /// cloning the manifest, extending it and validating both whole manifests.
    #[test]
    fn expansion_encodes_to_the_same_bytes_as_full_validation() {
        for count in [0usize, 1, 128, 3000, 4000] {
            let before = ObjectManifest {
                format_version: 2,
                bucket: "bucket".into(),
                key: "key".into(),
                generation: 5,
                length: count as u64,
                extents: (0..count)
                    .map(|index| ObjectExtent {
                        group: "group".into(),
                        chunk_id: index as u64 + 1,
                        object_offset: index as u64,
                        length: 1,
                        write_sequence: 1,
                        data_sha256: vec![(index % 251) as u8; 32],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            let mut after = before.clone();
            after.generation += 1;
            for step in 0..3u64 {
                after.extents.push(ObjectExtent {
                    group: "group".into(),
                    chunk_id: 90_000 + step,
                    object_offset: after.length,
                    length: 1,
                    write_sequence: 1,
                    data_sha256: vec![9 + step as u8; 32],
                    ..Default::default()
                });
                after.length += 1;
            }
            let long_way = ManifestOutcome::decide(Some(before.clone()), after).unwrap();
            let delta = ManifestAppend::decide(&long_way).unwrap();
            let expanded = delta.expand(&before).unwrap();
            assert_eq!(expanded, long_way);
            assert_eq!(expanded.encode_to_vec(), long_way.encode_to_vec());
            assert_eq!(expanded.to_bytes().unwrap(), long_way.to_bytes().unwrap());
            assert_eq!(delta.encode_to_vec(), ManifestAppend::decide(&expanded).unwrap().encode_to_vec());
        }
    }
    #[test]
    fn compact_append_size_depends_on_suffix_and_checks_both_digests() {
        for count in [0, 1, 128, 4095] {
            let before = ObjectManifest {
                format_version: 2,
                bucket: "bucket".into(),
                key: "key".into(),
                generation: 1,
                length: count as u64,
                extents: (0..count)
                    .map(|index| ObjectExtent {
                        group: "group".into(),
                        chunk_id: index as u64 + 1,
                        object_offset: index as u64,
                        length: 1,
                        write_sequence: 1,
                        data_sha256: vec![1; 32],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            let mut after = before.clone();
            after.generation = 2;
            after.length += 1;
            after.extents.push(ObjectExtent {
                group: "group".into(),
                chunk_id: 9999,
                object_offset: before.length,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![2; 32],
                ..Default::default()
            });
            let full = ManifestOutcome::decide(Some(before.clone()), after).unwrap();
            let delta = ManifestAppend::decide(&full).unwrap();
            assert!(delta.encoded_len() < 180);
            assert_eq!(delta.expand(&before).unwrap(), full);
            let mut changed = before.clone();
            changed.generation += 1;
            assert!(delta.expand(&changed).is_err());
            if !changed.extents.is_empty() {
                changed = before.clone();
                changed.extents[0].data_sha256[0] ^= 1;
                assert!(delta.expand(&changed).is_err());
            }
            let mut wrong = delta.clone();
            wrong.after_sha256[0] ^= 1;
            assert!(wrong.expand(&before).is_err());
            wrong = delta.clone();
            wrong.before_generation = u64::MAX;
            assert!(wrong.validate().is_err());
            wrong = delta.clone();
            wrong.before_length = u64::MAX;
            wrong.suffix[0].object_offset = u64::MAX;
            assert!(wrong.validate().is_err());
            let mut overwritten = full.clone();
            if count != 0 {
                overwritten.after.as_mut().unwrap().extents[0].data_sha256[0] ^= 1;
                assert!(ManifestAppend::decide(&overwritten).is_err());
            }
        }
    }
}
