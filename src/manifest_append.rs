// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! An exact prior manifest and a bounded immutable suffix publication.
use crate::{
    object_manifest::{ManifestOutcome, ObjectExtent},
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
        Self::decide_with_digests_checked(outcome, before_sha256, after_sha256, false)
    }
    /// `decide_with_digests` where the caller has just compared the two extent lists and found
    /// the prior's extents to be the first extents of the result (`prefix_checked`): the
    /// comparison is not made a second time.
    pub(crate) fn decide_with_digests_checked(
        outcome: &ManifestOutcome,
        before_sha256: Vec<u8>,
        after_sha256: Vec<u8>,
        prefix_checked: bool,
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
            || (!prefix_checked
                && after.extents.get(..before.extents.len()) != Some(before.extents.as_slice()))
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
    /// Whether `before`, the manifest the catalog holds, is the one this decision was made
    /// against; a matching stored digest binds the content.
    pub(crate) fn check_prior(&self, before: &crate::extent_list::StoredManifest, stored: &crate::object_manifest::StoredDigest) -> Result<(), ObjectError> {
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
    /// The manifest this decision produces from the stored `before`, which shares the stored
    /// chunks: the cost is that of the suffix and of the tail chunk, not of the extents already
    /// stored. Checked as `expand_stored` checks: the prior against the decision, the new
    /// extents against the stored ones, and the resulting digest against the decision's.
    pub(crate) fn expand_shared(
        &self,
        before: &crate::extent_list::StoredManifest,
        stored: &crate::object_manifest::StoredDigest,
    ) -> Result<crate::extent_list::StoredManifest, ObjectError> {
        self.validate()?;
        self.check_prior(before, stored)?;
        let after = before.extended(&self.suffix, before.generation + 1, self.after_length);
        after.validate_extension_of(before)?;
        let after_digest = stored.chain.extended(&self.suffix).digest_stored(&after);
        if after_digest != self.after_sha256 {
            return Err(invalid("compact append resulting digest mismatch"));
        }
        Ok(after)
    }
}

#[cfg(test)]
mod tests {
    

}
