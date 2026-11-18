// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The ranged manifest read: the extents of a manifest that overlap one byte range.
//!
//! A ranged GET of a large object needs the extents under its range and nothing else. The
//! whole-manifest reads send every extent (an object of 4000 extents is a quarter of a megabyte
//! encoded, decoded, validated and digested on every GET); this one sends the overlapping extents
//! under a header that carries the object's full `length`, the digest the metaserver already
//! holds for the current value, and the routes of the groups the extents name.
//!
//! The reply's manifest is a slice, not a manifest: it does not pass `ObjectManifest::validate`
//! and must not be stored, digested or compared. [`ObjectManifest::validate_range_slice`] checks
//! what a slice can prove, and the reader still verifies each extent's data against its digest.
use super::manifest_route_names;
use crate::{cluster::ChunkGroupRoute, object_manifest::ObjectManifest, ObjectError};
use prost::Message;

/// The most routes a range reply carries, as a wide manifest reply does.
const MAX_ROUTES: usize = 8;

/// The request body.
#[derive(Clone, PartialEq, Message)]
pub(super) struct ManifestRangeRead {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub key: String,
    #[prost(uint64, tag = "4")]
    pub offset: u64,
    #[prost(uint64, tag = "5")]
    pub length: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ManifestRangePayload {
    /// 1 when no read protection applies, 2 when `publication_fence` is nonzero.
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(message, optional, tag = "2")]
    manifest: Option<ObjectManifest>,
    #[prost(message, repeated, tag = "3")]
    routes: Vec<ChunkGroupRoute>,
    #[prost(uint64, tag = "4")]
    publication_fence: u64,
    #[prost(bytes = "vec", tag = "5")]
    manifest_sha256: Vec<u8>,
}

/// What a range read answers: the slice, the digest of the WHOLE current manifest, and routes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestRangeEntry {
    /// Nonzero requires a leased read of this exact generation before payload I/O.
    pub publication_fence: u64,
    /// The header of the current manifest, with only the extents overlapping the range.
    pub manifest: ObjectManifest,
    /// SHA-256 of the whole current manifest, for the read lease.
    pub manifest_sha256: Vec<u8>,
    pub routes: Vec<ChunkGroupRoute>,
}

impl ManifestRangeEntry {
    fn payload(&self) -> ManifestRangePayload {
        ManifestRangePayload {
            version: if self.publication_fence == 0 { 1 } else { 2 },
            manifest: Some(self.manifest.clone()),
            routes: self.routes.clone(),
            publication_fence: self.publication_fence,
            manifest_sha256: self.manifest_sha256.clone(),
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.payload().encoded_len()
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        self.payload().encode_to_vec()
    }

    /// The most the reply can take: a slice is bounded as a whole manifest is, and routes ride
    /// along as in a wide manifest reply.
    pub(crate) const MAX_BYTES: usize = super::MAX_MANIFEST_ROUTE_WIDE_REPLY_BYTES;

    /// Decode and check what a reply can show without knowing the range it answers: the
    /// envelope, the digest, and that the routes name the groups of the slice.
    pub(super) fn decode(payload: &[u8]) -> Result<Self, ObjectError> {
        if payload.len() > Self::MAX_BYTES {
            return Err(ObjectError::Corruption("manifest range reply exceeds bound".into()));
        }
        let value: ManifestRangePayload = super::super::conditional_requests::decode(payload)?;
        let manifest = value
            .manifest
            .ok_or_else(|| ObjectError::Corruption("manifest range reply missing manifest".into()))?;
        let names = manifest_route_names(&manifest, MAX_ROUTES);
        if !matches!((value.version, value.publication_fence), (1, 0) | (2, 1..))
            || value.manifest_sha256.len() != 32
            || value.routes.len() > MAX_ROUTES
            || value.routes.iter().any(|route| route.membership_fence().is_err())
            || (!value.routes.is_empty() && names.len() != value.routes.len())
            || value.routes.iter().zip(&names).any(|(route, name)| {
                route.group.as_ref().is_none_or(|group| &group.placement_id != name)
            })
        {
            return Err(ObjectError::Corruption("invalid manifest range reply".into()));
        }
        Ok(Self {
            publication_fence: value.publication_fence,
            manifest,
            manifest_sha256: value.manifest_sha256,
            routes: value.routes,
        })
    }
}

impl ManifestRangeRead {
    pub(super) fn new(bucket: &str, key: &str, offset: u64, length: u64) -> Self {
        Self { version: 1, bucket: bucket.into(), key: key.into(), offset, length }
    }

    /// Decode a request body and refuse a malformed one.
    pub(super) fn decode(payload: &[u8]) -> Result<Self, ObjectError> {
        if payload.len() > 8192 {
            return Err(ObjectError::Corruption("manifest range read exceeds bound".into()));
        }
        let value: Self = super::super::conditional_requests::decode(payload)?;
        if value.version != 1 {
            return Err(ObjectError::Corruption("manifest range read version mismatch".into()));
        }
        crate::validate_bucket(&value.bucket)?;
        crate::validate_key(&value.key)?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_manifest::ObjectExtent;

    fn entry() -> ManifestRangeEntry {
        ManifestRangeEntry {
            publication_fence: 0,
            manifest: ObjectManifest {
                format_version: 2,
                bucket: "bucket".into(),
                key: "object".into(),
                generation: 3,
                length: 1000,
                extents: (0..2)
                    .map(|index| ObjectExtent {
                        group: "group-0".into(),
                        chunk_id: index + 1,
                        object_offset: 100 + index * 10,
                        length: 10,
                        write_sequence: 1,
                        data_sha256: vec![1; 32],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            manifest_sha256: vec![7; 32],
            routes: Vec::new(),
        }
    }

    #[test]
    fn a_range_reply_round_trips_and_refuses_each_planted_defect() {
        let good = entry();
        assert_eq!(good.encode().len(), good.encoded_len());
        assert_eq!(ManifestRangeEntry::decode(&good.encode()).unwrap(), good);
        let fenced = ManifestRangeEntry { publication_fence: 5, ..entry() };
        assert_eq!(ManifestRangeEntry::decode(&fenced.encode()).unwrap(), fenced);
        for defect in 0..7 {
            let mut payload = good.payload();
            match defect {
                0 => {
                    payload.manifest_sha256.pop();
                }
                1 => payload.manifest_sha256.clear(),
                2 => payload.manifest = None,
                // A fence is named under the version that says there is none, and the other way.
                3 => payload.publication_fence = 1,
                4 => {
                    payload.version = 2;
                    payload.publication_fence = 0;
                }
                // A route that is no group's.
                5 => payload.routes = vec![ChunkGroupRoute::default()],
                _ => payload.version = 3,
            }
            assert!(
                ManifestRangeEntry::decode(&payload.encode_to_vec()).is_err(),
                "defect {defect} was accepted"
            );
        }
        assert!(ManifestRangeEntry::decode(&vec![0; ManifestRangeEntry::MAX_BYTES + 1]).is_err());
    }

    #[test]
    fn a_range_request_round_trips_and_refuses_each_planted_defect() {
        let good = ManifestRangeRead::new("bucket", "object", 5, 10);
        let decoded = ManifestRangeRead::decode(&good.encode_to_vec()).unwrap();
        assert_eq!((decoded.offset, decoded.length), (5, 10));
        let mut version = good.clone();
        version.version = 2;
        let mut bucket = good.clone();
        bucket.bucket = String::new();
        let mut key = good.clone();
        key.key = "x".repeat(5000);
        for bad in [version, bucket, key] {
            assert!(ManifestRangeRead::decode(&bad.encode_to_vec()).is_err());
        }
        assert!(ManifestRangeRead::decode(&vec![0; 8193]).is_err());
    }
}
