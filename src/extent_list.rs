// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A manifest's extents held as shared chunks, for the catalog and for the expansion of an append.
//!
//! The wire and durable forms of an object manifest are the protobuf message
//! [`ObjectManifest`](crate::object_manifest::ObjectManifest), whose extents are one flat
//! repeated field. This module is the in-memory form used where a manifest is copied a lot: a
//! list of full chunks of [`CHUNK`] extents behind `Arc`s, each with its encoded size, plus a
//! short tail. Appending to a list shares every full chunk with the list it came from and copies
//! only the tail, so an append costs O(extents / CHUNK + CHUNK) whatever the object holds. The
//! chunk size is the spacing of the hash-chain checkpoints (`manifest_chain`), so the two line up.
//!
//! [`StoredManifest::encode`] produces exactly the bytes `ObjectManifest::encode_to_vec` does,
//! and [`StoredManifest::encoded_len`] exactly its `encoded_len`; the tests below pin both.
use crate::object_manifest::{ObjectExtent, ObjectManifest};
use prost::Message;
use std::sync::Arc;

/// Extents per full chunk: the spacing of the hash-chain checkpoints.
pub(crate) const CHUNK: usize = crate::manifest_chain::CHECKPOINT_EVERY;

/// Protobuf field 6 of `ObjectManifest` (the extents), wire type 2.
const EXTENT_FIELD_KEY: u8 = (6 << 3) | 2;
/// Protobuf field 7 of `ObjectManifest` (`deleted`) set to true: key, then the value 1.
const DELETED_TRUE: [u8; 2] = [(7 << 3), 1];

#[cfg(test)]
thread_local! {
    static STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
/// Extents looked at by a selection on this thread since the last reset (tests only).
#[cfg(test)]
pub(crate) fn selection_steps() -> usize {
    STEPS.with(|steps| steps.get())
}
#[cfg(test)]
pub(crate) fn reset_selection_steps() {
    STEPS.with(|steps| steps.set(0));
}
/// Count one extent looked at by a selection (tests only).
fn step() {
    #[cfg(test)]
    STEPS.with(|steps| steps.set(steps.get() + 1));
}

/// The bytes one extent takes as a repeated message field of the manifest.
fn field_len(extent: &ObjectExtent) -> usize {
    let length = extent.encoded_len();
    1 + prost::encoding::encoded_len_varint(length as u64) + length
}

#[derive(Debug)]
struct Chunk {
    extents: Vec<ObjectExtent>,
    /// The sum of `field_len` over `extents`.
    encoded: usize,
}

/// A list of extents: full chunks shared between copies, and a tail shorter than [`CHUNK`].
#[derive(Clone, Debug, Default)]
pub(crate) struct ExtentList {
    sealed: Arc<Vec<Arc<Chunk>>>,
    tail: Vec<ObjectExtent>,
}

impl ExtentList {
    pub(crate) fn from_vec(extents: Vec<ObjectExtent>) -> Self {
        let mut list = Self::default();
        list.extend(extents);
        list
    }

    pub(crate) fn len(&self) -> usize {
        self.sealed.len() * CHUNK + self.tail.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.sealed.is_empty() && self.tail.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &ObjectExtent> {
        self.sealed
            .iter()
            .flat_map(|chunk| chunk.extents.iter())
            .chain(self.tail.iter())
    }

    /// The extents from `from` on, without visiting the ones before it.
    pub(crate) fn iter_from(&self, from: usize) -> Box<dyn Iterator<Item = &ObjectExtent> + '_> {
        let sealed = self.sealed.len();
        let chunk = from / CHUNK;
        if chunk >= sealed {
            return Box::new(self.tail.iter().skip(from.saturating_sub(sealed * CHUNK)));
        }
        let first = self.sealed[chunk].extents[from % CHUNK..].iter();
        Box::new(
            first
                .chain(self.sealed[chunk + 1..].iter().flat_map(|chunk| chunk.extents.iter()))
                .chain(self.tail.iter()),
        )
    }

    #[cfg(test)]
    pub(crate) fn get(&self, index: usize) -> Option<&ObjectExtent> {
        let (chunk, offset) = (index / CHUNK, index % CHUNK);
        match self.sealed.get(chunk) {
            Some(chunk) => chunk.extents.get(offset),
            None if chunk == self.sealed.len() => self.tail.get(offset),
            None => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn last(&self) -> Option<&ObjectExtent> {
        self.tail.last().or_else(|| self.sealed.last().and_then(|chunk| chunk.extents.last()))
    }

    /// The extents from `from` on, copied (a suffix of an append is short).
    pub(crate) fn tail_from(&self, from: usize) -> Vec<ObjectExtent> {
        self.iter_from(from).cloned().collect()
    }

    pub(crate) fn to_vec(&self) -> Vec<ObjectExtent> {
        self.iter().cloned().collect()
    }

    /// The extents that overlap the object bytes `start..end`, in order, copied.
    ///
    /// The list is contiguous from offset zero (a validated manifest), so the first extent that
    /// ends after `start` is found by binary search: over the full chunks by their last extent,
    /// then inside one chunk. Only the extents returned are visited after that, so the cost is
    /// O(log chunks + log CHUNK + selected) however long the list is.
    pub(crate) fn overlapping(&self, start: u64, end: u64) -> Vec<ObjectExtent> {
        let mut selected = Vec::new();
        if start >= end {
            return selected;
        }
        for extent in self.iter_from(self.first_ending_after(start)) {
            step();
            if extent.object_offset >= end {
                break;
            }
            selected.push(extent.clone());
        }
        selected
    }

    /// The index of the first extent whose end is past `start`, or the length when none is.
    fn first_ending_after(&self, start: u64) -> usize {
        let ends_by = |extent: &ObjectExtent| {
            step();
            extent.object_offset.saturating_add(extent.length) <= start
        };
        let index = self.sealed.partition_point(|chunk| chunk.extents.last().is_some_and(ends_by));
        let (extents, base): (&[ObjectExtent], usize) = match self.sealed.get(index) {
            Some(chunk) => (&chunk.extents, index * CHUNK),
            None => (&self.tail, self.sealed.len() * CHUNK),
        };
        base + extents.partition_point(ends_by)
    }

    pub(crate) fn push(&mut self, extent: ObjectExtent) {
        self.tail.push(extent);
        if self.tail.len() == CHUNK {
            let extents = std::mem::take(&mut self.tail);
            let encoded = extents.iter().map(field_len).sum();
            Arc::make_mut(&mut self.sealed).push(Arc::new(Chunk { extents, encoded }));
        }
    }

    pub(crate) fn extend(&mut self, extents: impl IntoIterator<Item = ObjectExtent>) {
        for extent in extents {
            self.push(extent);
        }
    }

    /// This list followed by `suffix`: the full chunks are shared, the tail is copied.
    pub(crate) fn with_suffix(&self, suffix: &[ObjectExtent]) -> Self {
        let mut next = self.clone();
        next.extend(suffix.iter().cloned());
        next
    }

    /// The bytes the extents take inside the manifest message.
    pub(crate) fn encoded_len(&self) -> usize {
        self.sealed.iter().map(|chunk| chunk.encoded).sum::<usize>() + self.tail.iter().map(field_len).sum::<usize>()
    }

    /// Append the extents to `buf` as field 6 of the manifest message.
    pub(crate) fn encode_into(&self, buf: &mut Vec<u8>) {
        for extent in self.iter() {
            buf.push(EXTENT_FIELD_KEY);
            prost::encoding::encode_varint(extent.encoded_len() as u64, buf);
            extent.encode_raw(buf);
        }
    }

    /// How many leading full chunks this list shares (by identity) with `other`.
    #[cfg(test)]
    pub(crate) fn shared_chunks(&self, other: &Self) -> usize {
        self.sealed
            .iter()
            .zip(other.sealed.iter())
            .take_while(|(left, right)| Arc::ptr_eq(left, right))
            .count()
    }
}

impl<'a> IntoIterator for &'a ExtentList {
    type Item = &'a ObjectExtent;
    type IntoIter = Box<dyn Iterator<Item = &'a ObjectExtent> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl PartialEq for ExtentList {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .sealed
                .iter()
                .zip(other.sealed.iter())
                .all(|(left, right)| Arc::ptr_eq(left, right) || left.extents == right.extents)
            && self.tail == other.tail
    }
}

/// An object manifest whose extents are an [`ExtentList`]. Same fields as the protobuf message.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredManifest {
    pub format_version: u32,
    pub bucket: String,
    pub key: String,
    pub generation: u64,
    pub length: u64,
    pub extents: ExtentList,
    pub deleted: bool,
}

impl StoredManifest {
    pub(crate) fn from_manifest(manifest: ObjectManifest) -> Self {
        Self {
            format_version: manifest.format_version,
            bucket: manifest.bucket,
            key: manifest.key,
            generation: manifest.generation,
            length: manifest.length,
            extents: ExtentList::from_vec(manifest.extents),
            deleted: manifest.deleted,
        }
    }

    pub(crate) fn from_ref(manifest: &ObjectManifest) -> Self {
        Self::from_manifest(manifest.clone())
    }

    pub(crate) fn to_manifest(&self) -> ObjectManifest {
        ObjectManifest {
            format_version: self.format_version,
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            generation: self.generation,
            length: self.length,
            extents: self.extents.to_vec(),
            deleted: self.deleted,
        }
    }

    pub(crate) fn object_id(&self) -> crate::ObjectId {
        crate::ObjectId::new(self.bucket.clone(), self.key.clone())
    }

    /// The encoded size without constructing a temporary manifest header.
    pub(crate) fn encoded_len(&self) -> usize {
        fn scalar(value: u64) -> usize {
            if value == 0 { 0 } else { 1 + prost::encoding::encoded_len_varint(value) }
        }
        fn text(value: &str) -> usize {
            if value.is_empty() { 0 } else {
                1 + prost::encoding::encoded_len_varint(value.len() as u64) + value.len()
            }
        }
        scalar(u64::from(self.format_version)) + text(&self.bucket) + text(&self.key)
            + scalar(self.generation) + scalar(self.length) + self.extents.encoded_len()
            + if self.deleted { 2 } else { 0 }
    }

    /// Append exactly the canonical ObjectManifest protobuf bytes to reusable storage.
    pub(crate) fn encode_into(&self, buf: &mut Vec<u8>) {
        fn scalar(buf: &mut Vec<u8>, tag: u8, value: u64) {
            if value != 0 {
                buf.push(tag << 3);
                prost::encoding::encode_varint(value, buf);
            }
        }
        fn text(buf: &mut Vec<u8>, tag: u8, value: &str) {
            if !value.is_empty() {
                buf.push((tag << 3) | 2);
                prost::encoding::encode_varint(value.len() as u64, buf);
                buf.extend_from_slice(value.as_bytes());
            }
        }
        scalar(buf, 1, u64::from(self.format_version));
        text(buf, 2, &self.bucket);
        text(buf, 3, &self.key);
        scalar(buf, 4, self.generation);
        scalar(buf, 5, self.length);
        self.extents.encode_into(buf);
        if self.deleted { buf.extend_from_slice(&DELETED_TRUE); }
    }

    #[cfg(test)]
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        buf
    }

    /// This manifest with `suffix` appended, at `generation` and `length`; the chunks are shared.
    pub(crate) fn extended(&self, suffix: &[ObjectExtent], generation: u64, length: u64) -> Self {
        Self {
            format_version: self.format_version,
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            generation,
            length,
            extents: self.extents.with_suffix(suffix),
            deleted: self.deleted,
        }
    }

    /// Whether this holds exactly the manifest `other` is.
    pub(crate) fn matches(&self, other: &ObjectManifest) -> bool {
        self.format_version == other.format_version
            && self.bucket == other.bucket
            && self.key == other.key
            && self.generation == other.generation
            && self.length == other.length
            && self.deleted == other.deleted
            && self.extents.len() == other.extents.len()
            && self.extents.iter().zip(other.extents.iter()).all(|(left, right)| left == right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn next(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    }

    fn extent(index: usize, state: &mut u64) -> ObjectExtent {
        // Field values of every size: zero (omitted by protobuf), small, and multi-byte varints.
        let pick = |state: &mut u64| match next(state) % 4 {
            0 => 0,
            1 => next(state) % 128,
            2 => next(state) % 100_000,
            _ => next(state) << 20,
        };
        ObjectExtent {
            group: format!("group-{}", index % 7),
            volume_id: pick(state),
            chunk_id: pick(state),
            page_index: pick(state),
            object_offset: index as u64,
            page_offset: pick(state) % 1000,
            length: 1 + pick(state) % 70_000,
            write_sequence: pick(state),
            data_checksum: next(state),
            data_sha256: vec![(index % 251) as u8; if index % 5 == 0 { 0 } else { 32 }], ..Default::default()
        }
    }

    fn manifest(count: usize, deleted: bool, state: &mut u64) -> ObjectManifest {
        ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: format!("key-{count}"),
            generation: 1 + next(state) % 200_000,
            length: next(state) << 8,
            extents: (0..count).map(|index| extent(index, state)).collect(),
            deleted,
        }
    }

    /// The encoding and the size are those of the protobuf message at every boundary of the
    /// chunking, with and without the `deleted` flag.
    #[test]
    fn the_stored_form_encodes_exactly_as_the_message_does() {
        let mut state = 0x9e3779b97f4a7c15u64;
        for count in [0, 1, 2, 63, 64, 65, 127, 128, 129, 1000, 4096] {
            for deleted in [false, true] {
                let message = manifest(count, deleted && count == 0, &mut state);
                let stored = StoredManifest::from_ref(&message);
                assert_eq!(stored.encoded_len(), message.encoded_len(), "length at {count}");
                assert_eq!(stored.encode(), message.encode_to_vec(), "bytes at {count}");
                assert!(stored.matches(&message), "round trip at {count}");
                assert_eq!(stored.to_manifest(), message, "round trip at {count}");
                assert_eq!(stored.extents.len(), count);
            }
        }
    }

    #[test]
    fn a_deleted_manifest_encodes_its_flag_last() {
        let tombstone = ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "gone".into(),
            generation: 9,
            length: 0,
            extents: Vec::new(),
            deleted: true,
        };
        let stored = StoredManifest::from_ref(&tombstone);
        assert_eq!(stored.encode(), tombstone.encode_to_vec());
        assert!(stored.encode().ends_with(&DELETED_TRUE));
    }

    #[test]
    fn appending_shares_every_full_chunk_and_copies_only_the_tail() {
        let mut state = 7u64;
        let base = StoredManifest::from_ref(&manifest(200, false, &mut state));
        assert_eq!(base.extents.len(), 200);
        let suffix = vec![extent(200, &mut state), extent(201, &mut state)];
        let longer = base.extended(&suffix, base.generation + 1, base.length + 2);
        assert_eq!(longer.extents.len(), 202);
        // 200 = 3 full chunks + 8 in the tail: the three chunks are the same allocations.
        assert_eq!(longer.extents.shared_chunks(&base.extents), 3);
        // The same list built from scratch is equal, and encodes identically.
        let mut flat = base.to_manifest();
        flat.generation += 1;
        flat.length += 2;
        flat.extents.extend(suffix.iter().cloned());
        let rebuilt = StoredManifest::from_ref(&flat);
        assert_eq!(longer, rebuilt);
        assert_eq!(longer.encode(), flat.encode_to_vec());
        assert_eq!(longer.encoded_len(), flat.encoded_len());
        // The list it grew from is untouched.
        assert_eq!(base.extents.len(), 200);
    }

    #[test]
    fn filling_the_tail_seals_a_chunk_and_keeps_the_size_exact() {
        let mut state = 11u64;
        let mut list = ExtentList::default();
        let mut flat = Vec::new();
        for index in 0..(CHUNK * 3 + 5) {
            let next_extent = extent(index, &mut state);
            list.push(next_extent.clone());
            flat.push(next_extent);
            let message = ObjectManifest { extents: flat.clone(), ..Default::default() };
            assert_eq!(list.len(), flat.len());
            assert_eq!(list.encoded_len(), message.encoded_len(), "after {} extents", flat.len());
            assert_eq!(list.get(index), flat.get(index));
            assert_eq!(list.last(), flat.last());
        }
        assert_eq!(list.get(flat.len()), None);
        assert_eq!(list.tail_from(CHUNK * 3).len(), 5);
        assert_eq!(list.to_vec(), flat);
    }

    #[test]
    fn equality_is_by_content_and_by_identity_where_chunks_are_shared() {
        let mut state = 3u64;
        let message = manifest(130, false, &mut state);
        let one = StoredManifest::from_ref(&message);
        let two = StoredManifest::from_ref(&message);
        assert_eq!(one, two);
        assert_eq!(one.extents.shared_chunks(&two.extents), 0, "separately built lists share nothing");
        assert_eq!(one, one.clone());
        let mut changed = message.clone();
        changed.extents[70].length += 1;
        assert_ne!(one, StoredManifest::from_ref(&changed));
        let mut shorter = message;
        shorter.extents.pop();
        assert_ne!(one, StoredManifest::from_ref(&shorter));
    }

    /// Extents laid end to end from offset zero, as a validated manifest has them.
    fn contiguous(count: usize, state: &mut u64) -> Vec<ObjectExtent> {
        let mut offset = 0u64;
        (0..count)
            .map(|index| {
                let mut one = extent(index, state);
                one.length = 1 + next(state) % 1000;
                one.object_offset = offset;
                offset += one.length;
                one
            })
            .collect()
    }

    fn brute(extents: &[ObjectExtent], start: u64, end: u64) -> Vec<ObjectExtent> {
        extents
            .iter()
            .filter(|one| start < end && one.object_offset < end && one.object_offset + one.length > start)
            .cloned()
            .collect()
    }

    /// The binary search names the same extents as a scan, at every chunk boundary and for ranges
    /// that start and end inside, on and past the edges of extents.
    #[test]
    fn the_selection_is_the_scan_at_every_boundary() {
        let mut state = 11u64;
        for count in [0, 1, 2, 63, 64, 65, 127, 128, 129, 500, 4096] {
            let flat = contiguous(count, &mut state);
            let list = ExtentList::from_vec(flat.clone());
            let total = flat.last().map_or(0, |one| one.object_offset + one.length);
            let mut probes = vec![0, 1, total / 2, total.saturating_sub(1), total, total + 1];
            for one in flat.iter().step_by((count / 12).max(1)) {
                probes.extend([
                    one.object_offset,
                    one.object_offset + 1,
                    one.object_offset + one.length - 1,
                    one.object_offset + one.length,
                    one.object_offset + one.length + 1,
                ]);
            }
            for &start in &probes {
                for &end in &probes {
                    assert_eq!(
                        list.overlapping(start, end),
                        brute(&flat, start, end),
                        "count {count} range {start}..{end}"
                    );
                }
            }
        }
    }

    /// The work does not grow with the list: at 4000 extents a one-extent range looks at a
    /// handful, where a scan looks at thousands.
    #[test]
    fn selecting_a_range_looks_at_a_logarithmic_number_of_extents() {
        let mut state = 5u64;
        let flat = contiguous(4000, &mut state);
        let list = ExtentList::from_vec(flat.clone());
        for index in [0, 1, 63, 64, 2000, 3935, 3999] {
            let one = &flat[index];
            reset_selection_steps();
            let selected = list.overlapping(one.object_offset, one.object_offset + one.length);
            assert_eq!(selected, vec![one.clone()]);
            assert!(selection_steps() <= 20, "index {index} looked at {} extents", selection_steps());
        }
    }
}
