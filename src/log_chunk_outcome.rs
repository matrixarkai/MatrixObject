// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The metadata side of a log chunk: the writer lease an object holds, and the change a
//! placement outcome of version 15 makes to that lease and to the object's manifest.
//!
//! Two changes exist, and both are decided by the leader and checked again by every node that
//! applies them:
//!
//! * grant (action 1): `holder` takes the writer lease of an object. The epoch of the lease is the
//!   raft index of the outcome that grants it, so an epoch names exactly one grant and only goes
//!   up. A live lease held by someone else refuses; the same holder asking again, or anyone after
//!   the lease expired, gets a new epoch and fences the previous writer. When the object has no
//!   chunk being written, the same outcome adds one to its manifest: an empty tail entry.
//! * seal-switch (action 2): the holder of the live lease closes the chunk being written at its
//!   final length and footer and opens the next one, in one manifest change, so a reader never
//!   sees a sealed chunk with no successor or two chunks being written.
//!
//! Every value that a decision depends on is carried by the outcome (the clock reading, the
//! lease before and after), so applying it is a comparison against state and never a read of a
//! clock. An outcome committed to the log that a node cannot apply fails the node; it is never
//! skipped.
//!
//! Nothing emits version 15 yet. A build that reads it can be upgraded first.
use crate::object_manifest::{
    ObjectExtent, ObjectManifest, EXTENT_FLAG_SEALED, EXTENT_FLAG_TAIL, EXTENT_KIND_LOG_CHUNK, MANIFEST_FORMAT_CHUNKED,
};
use crate::ObjectError;
use prost::Message;
use std::collections::BTreeMap;

pub const ACTION_GRANT: u32 = 1;
pub const ACTION_SEAL_SWITCH: u32 = 2;
/// The longest a lease is granted for.
pub const MAX_LEASE_TTL_MS: u64 = 60 * 60 * 1000;
/// Most leases the table holds.
pub const MAX_WRITER_LEASES: usize = 262_144;
const MAX_HOLDER: usize = 128;
const MAX_NAME: usize = 4096;

fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.to_string())
}

/// One writer lease: who holds an object for writing, under which epoch, until when.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct WriterLease {
    #[prost(string, tag = "1")]
    pub holder: String,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(uint64, tag = "3")]
    pub expires_at_ms: u64,
}

impl WriterLease {
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.holder.is_empty()
            || self.holder.len() > MAX_HOLDER
            || !self.holder.bytes().all(|b| b.is_ascii_graphic())
            || self.epoch == 0
            || self.expires_at_ms == 0
        {
            return Err(invalid("invalid writer lease"));
        }
        Ok(())
    }
}

/// The writer leases the metadata state holds, one per object.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriterLeaseTable {
    leases: BTreeMap<(String, String), WriterLeaseValue>,
}
/// A lease as the table keeps it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterLeaseValue {
    pub holder: String,
    pub epoch: u64,
    pub expires_at_ms: u64,
}

impl WriterLeaseTable {
    /// True while no object has ever been granted: the snapshot of such a state has exactly the
    /// encoding it had before leases existed.
    pub fn is_empty(&self) -> bool {
        self.leases.is_empty()
    }
    pub fn len(&self) -> usize {
        self.leases.len()
    }
    pub fn get(&self, bucket: &str, key: &str) -> Option<WriterLease> {
        self.leases.get(&(bucket.to_string(), key.to_string())).map(|lease| WriterLease {
            holder: lease.holder.clone(),
            epoch: lease.epoch,
            expires_at_ms: lease.expires_at_ms,
        })
    }
    /// Put back what `get` returned for this object: the lease, or none. Used to undo a draft.
    pub fn restore(&mut self, bucket: &str, key: &str, lease: Option<&WriterLease>) {
        match lease {
            Some(lease) => self.set(bucket, key, lease),
            None => {
                self.leases.remove(&(bucket.to_string(), key.to_string()));
            }
        }
    }
    pub fn set(&mut self, bucket: &str, key: &str, lease: &WriterLease) {
        self.leases.insert(
            (bucket.to_string(), key.to_string()),
            WriterLeaseValue { holder: lease.holder.clone(), epoch: lease.epoch, expires_at_ms: lease.expires_at_ms },
        );
    }
    /// `(bucket, key, lease)` in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str, &WriterLeaseValue)> {
        self.leases.iter().map(|((bucket, key), lease)| (bucket.as_str(), key.as_str(), lease))
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.leases.len() > MAX_WRITER_LEASES {
            return Err(invalid("writer lease table exceeds its bound"));
        }
        for ((bucket, key), lease) in &self.leases {
            if bucket.is_empty() || bucket.len() > MAX_NAME || key.is_empty() || key.len() > MAX_NAME {
                return Err(invalid("writer lease names no object"));
            }
            WriterLease { holder: lease.holder.clone(), epoch: lease.epoch, expires_at_ms: lease.expires_at_ms }
                .validate()?;
        }
        Ok(())
    }
}

/// The change a version 15 outcome makes to one object's writer lease. The manifest side of the
/// change is the outcome's `manifest`, checked against this by `check_manifest`.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct LogChunkChange {
    #[prost(uint32, tag = "1")]
    pub action: u32,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub key: String,
    /// The lease the object holds when this is decided; absent if it holds none.
    #[prost(message, optional, tag = "4")]
    pub lease_before: Option<WriterLease>,
    /// The lease it holds afterwards.
    #[prost(message, optional, tag = "5")]
    pub lease_after: Option<WriterLease>,
    /// The clock reading the leader decided this from.
    #[prost(uint64, tag = "6")]
    pub now_ms: u64,
}

fn same_chunk(a: &ObjectExtent, b: &ObjectExtent) -> bool {
    a.group == b.group && a.volume_id == b.volume_id && a.chunk_id == b.chunk_id
}
pub(crate) fn is_open_tail(extent: &ObjectExtent) -> bool {
    extent.kind == EXTENT_KIND_LOG_CHUNK && extent.flags & EXTENT_FLAG_TAIL != 0 && extent.flags & EXTENT_FLAG_SEALED == 0
}

impl LogChunkChange {
    /// The shape of the change and of its lease, against the outcome's two indexes.
    pub fn validate(&self, before_index: u64, after_index: u64) -> Result<(), ObjectError> {
        if self.bucket.is_empty()
            || self.bucket.len() > MAX_NAME
            || self.key.is_empty()
            || self.key.len() > MAX_NAME
            || self.now_ms == 0
            || before_index.checked_add(1) != Some(after_index)
        {
            return Err(invalid("invalid log chunk change"));
        }
        let after = self.lease_after.as_ref().ok_or_else(|| invalid("a log chunk change leaves a lease"))?;
        after.validate()?;
        if let Some(before) = &self.lease_before {
            before.validate()?;
            if before.epoch >= after_index {
                return Err(invalid("a lease from the future"));
            }
        }
        match self.action {
            ACTION_GRANT => {
                if after.epoch != after_index
                    || after.expires_at_ms <= self.now_ms
                    || after.expires_at_ms - self.now_ms > MAX_LEASE_TTL_MS
                {
                    return Err(invalid("a grant's epoch is its index and its term is bounded"));
                }
                if let Some(before) = &self.lease_before {
                    // the holder may ask again; anyone else only after the lease ran out
                    if before.holder != after.holder && before.expires_at_ms > self.now_ms {
                        return Err(invalid("the object is leased to another writer"));
                    }
                }
            }
            ACTION_SEAL_SWITCH => {
                // the holder of a live lease switches chunks; the lease itself does not change
                if self.lease_before.as_ref() != Some(after) || after.expires_at_ms <= self.now_ms {
                    return Err(invalid("a chunk switch needs the live lease unchanged"));
                }
            }
            _ => return Err(invalid("unknown log chunk action")),
        }
        Ok(())
    }

    /// The lease side against the state's table: the lease it was decided from is the lease the
    /// table holds. Nothing is changed.
    pub fn check_leases(&self, table: &WriterLeaseTable) -> Result<(), ObjectError> {
        if table.get(&self.bucket, &self.key) != self.lease_before {
            return Err(invalid("log chunk change does not match the lease held"));
        }
        if self.lease_before.is_none() && table.len() >= MAX_WRITER_LEASES {
            return Err(invalid("writer lease table is full"));
        }
        Ok(())
    }

    /// Record the lease the change leaves. `check_leases` has passed.
    pub fn apply_leases(&self, table: &mut WriterLeaseTable) {
        if let Some(after) = &self.lease_after {
            table.set(&self.bucket, &self.key, after);
        }
    }

    /// Whether the outcome carries a manifest change: a grant adds a chunk only to an object that
    /// is not already writing one; a switch always changes the manifest.
    pub fn changes_manifest_for(&self, before: Option<&ObjectManifest>) -> bool {
        match self.action {
            ACTION_GRANT => !before.is_some_and(|manifest| !manifest.deleted && manifest.extents.last().is_some_and(is_open_tail)),
            _ => true,
        }
    }

    /// The manifest change against this one: exactly the grant of an empty tail, or exactly the
    /// seal of the tail and the next one.
    pub fn check_manifest(
        &self,
        after_index: u64,
        before: Option<&ObjectManifest>,
        after: &ObjectManifest,
    ) -> Result<(), ObjectError> {
        if after.bucket != self.bucket
            || after.key != self.key
            || after.deleted
            || after.format_version != MANIFEST_FORMAT_CHUNKED
        {
            return Err(invalid("a log chunk change publishes a chunked manifest of its object"));
        }
        let kept: &[ObjectExtent] = match before {
            Some(prior) if !prior.deleted => &prior.extents,
            _ => &[],
        };
        let prior_length = before.filter(|prior| !prior.deleted).map_or(0, |prior| prior.length);
        if let Some(prior) = before.filter(|prior| !prior.deleted) {
            if !(2..=MANIFEST_FORMAT_CHUNKED).contains(&prior.format_version) {
                return Err(invalid("a chunk cannot follow a manifest that predates content digests"));
            }
        }
        match self.action {
            ACTION_GRANT => {
                if kept.last().is_some_and(is_open_tail) {
                    return Err(invalid("a chunk is already open: a grant then changes no manifest"));
                }
                if after.extents.len() != kept.len() + 1 || !after.extents.starts_with(kept) || after.length != prior_length {
                    return Err(invalid("a grant adds one empty tail and nothing else"));
                }
                check_new_tail(&after.extents[kept.len()], after_index, prior_length, kept)
            }
            _ => {
                let prior = before
                    .filter(|prior| !prior.deleted && prior.format_version == MANIFEST_FORMAT_CHUNKED)
                    .ok_or_else(|| invalid("a chunk switch needs a chunked manifest"))?;
                let open = prior.extents.last().filter(|extent| is_open_tail(extent)).ok_or_else(|| invalid("a chunk switch needs a chunk being written"))?;
                let n = prior.extents.len();
                if after.extents.len() != n + 1 || after.extents[..n - 1] != prior.extents[..n - 1] {
                    return Err(invalid("a chunk switch changes only the last entry and adds one"));
                }
                let sealed = &after.extents[n - 1];
                if sealed.kind != EXTENT_KIND_LOG_CHUNK
                    || sealed.flags & EXTENT_FLAG_SEALED == 0
                    || sealed.flags & EXTENT_FLAG_TAIL != 0
                    || !same_chunk(sealed, open)
                    || sealed.object_offset != open.object_offset
                    || sealed.write_sequence != open.write_sequence
                    || sealed.length < open.length
                {
                    return Err(invalid("a chunk switch seals the chunk that was open, no shorter than recorded"));
                }
                let end = open
                    .object_offset
                    .checked_add(sealed.length)
                    .ok_or_else(|| invalid("object length overflow"))?;
                if after.length != end {
                    return Err(invalid("a chunk switch moves the object length to the end of the sealed chunk"));
                }
                check_new_tail(&after.extents[n], after_index, end, &prior.extents)
            }
        }
    }
}

/// The entry a grant or a switch opens: empty, being written, owned by this outcome.
fn check_new_tail(
    tail: &ObjectExtent,
    write_sequence: u64,
    offset: u64,
    others: &[ObjectExtent],
) -> Result<(), ObjectError> {
    if tail.kind != EXTENT_KIND_LOG_CHUNK
        || tail.flags != EXTENT_FLAG_TAIL
        || tail.length != 0
        || tail.footer_crc != 0
        || !tail.footer_sha256.is_empty()
        || !tail.data_sha256.is_empty()
        || tail.data_checksum != 0
        || tail.page_index != 0
        || tail.page_offset != 0
        || tail.write_sequence != write_sequence
        || tail.object_offset != offset
        || others.iter().any(|other| same_chunk(other, tail))
    {
        return Err(invalid("a new chunk is empty, being written, owned by its outcome and not already named"));
    }
    Ok(())
}
