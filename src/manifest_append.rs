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
            before_sha256: crate::read_session_outcome::manifest_sha256(before)?,
            suffix: after.extents[before.extents.len()..].to_vec(),
            after_length: after.length,
            after_sha256: crate::read_session_outcome::manifest_sha256(after)?,
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
    pub(crate) fn expand(&self, before: &ObjectManifest) -> Result<ManifestOutcome, ObjectError> {
        self.validate()?;
        before.validate()?;
        if before.deleted
            || before.format_version != 2
            || before.bucket != self.bucket
            || before.key != self.key
            || before.generation != self.before_generation
            || before.length != self.before_length
            || before.extents.len() != self.before_extents as usize
            || crate::read_session_outcome::manifest_sha256(before)? != self.before_sha256
        {
            return Err(ObjectError::PreconditionFailed(
                "compact append prior manifest changed".into(),
            ));
        }
        let mut after = before.clone();
        after.generation += 1;
        after.length = self.after_length;
        after.extents.extend(self.suffix.iter().cloned());
        let outcome = ManifestOutcome::decide(Some(before.clone()), after)?;
        if crate::read_session_outcome::manifest_sha256(outcome.after.as_ref().unwrap())?
            != self.after_sha256
        {
            return Err(invalid("compact append resulting digest mismatch"));
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
