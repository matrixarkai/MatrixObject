// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

const PAGE: u64 = 1 << 20;

fn digest(seed: u8) -> Vec<u8> {
    vec![seed; 32]
}

fn paged(group: &str, chunk: u64, first_page: u64, object_offset: u64, length: u64) -> ChunkExtent {
    ChunkExtent {
        group: group.into(),
        volume_id: 1,
        chunk_id: chunk,
        kind: CHUNK_KIND_PAGED,
        object_offset,
        chunk_offset: first_page * PAGE,
        length,
        flags: CHUNK_FLAG_SEALED,
        digest: digest(7),
        ..Default::default()
    }
}

fn log(chunk: u64, object_offset: u64, length: u64, flags: u32) -> ChunkExtent {
    ChunkExtent {
        group: "group-a".into(),
        volume_id: 1,
        chunk_id: chunk,
        kind: CHUNK_KIND_LOG,
        object_offset,
        chunk_offset: 0,
        length,
        flags,
        ..Default::default()
    }
}

fn paged_manifest(extents: Vec<ChunkExtent>) -> ChunkManifest {
    let length = extents.iter().map(|extent| extent.length).sum();
    ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: "key".into(),
        generation: 1,
        length,
        extents,
        deleted: false,
        kind: CHUNK_KIND_PAGED,
        page_bytes: PAGE,
        ..Default::default()
    }
}

fn log_manifest(extents: Vec<ChunkExtent>) -> ChunkManifest {
    let length = extents.iter().map(|extent| extent.length).sum();
    ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: "key".into(),
        generation: 1,
        length,
        extents,
        deleted: false,
        kind: CHUNK_KIND_LOG,
        page_bytes: 0,
        ..Default::default()
    }
}

fn refused(manifest: &ChunkManifest) -> String {
    format!("{:?}", manifest.validate().expect_err("the manifest must be refused"))
}

#[test]
fn a_small_object_is_one_paged_extent_of_one_page() {
    let manifest = paged_manifest(vec![paged("group-a", 9, 3, 0, 4096)]);
    manifest.validate().unwrap();
    let extent = &manifest.extents[0];
    assert_eq!(extent.pages(PAGE).unwrap(), (3, 1), "a few KiB is one page, the fourth of its allocation");
    let bytes = manifest.to_bytes().unwrap();
    assert_eq!(ChunkManifest::from_bytes(&bytes).unwrap(), manifest);
    assert!(bytes.len() < 160, "a small object's manifest stays small: {}", bytes.len());
}

#[test]
fn a_large_object_is_one_extent_per_run_of_an_allocation_not_one_per_page() {
    // 4,095 data pages and the digest page fill the 4,096 pages of one allocation: one entry
    // where page mode needs 4,095
    let whole = paged_manifest(vec![paged("group-a", 9, 0, 0, 4095 * PAGE)]);
    whole.validate().unwrap();
    assert_eq!(whole.extents[0].pages(PAGE).unwrap(), (0, 4095));
    // a short last page is a page
    let short = paged_manifest(vec![paged("group-a", 9, 0, 0, 3 * PAGE + 17)]);
    short.validate().unwrap();
    assert_eq!(short.extents[0].pages(PAGE).unwrap(), (0, 4));
    // two runs in two allocations, one object
    let two = paged_manifest(vec![paged("group-a", 9, 0, 0, 4095 * PAGE), paged("group-b", 10, 0, 4095 * PAGE, 5 * PAGE)]);
    two.validate().unwrap();
}

#[test]
fn a_paged_extent_never_reaches_past_its_allocation() {
    // 4,096 data pages leave no page for the digest page; 4,095 starting at page 1 end at 4,097
    let no_digest_page = paged_manifest(vec![paged("group-a", 9, 0, 0, 4096 * PAGE)]);
    assert!(refused(&no_digest_page).contains("ordering, identity or bounds"));
    let past = paged_manifest(vec![paged("group-a", 9, 1, 0, 4095 * PAGE)]);
    assert!(refused(&past).contains("ordering, identity or bounds"));
    let too_long = paged_manifest(vec![paged("group-a", 9, 0, 0, 4095 * PAGE + 1)]);
    assert!(refused(&too_long).contains("ordering, identity or bounds"));
    // a one-page extent needs no digest page: it may be the last page of the allocation
    paged_manifest(vec![paged("group-a", 9, 4095, 0, PAGE)]).validate().unwrap();
    // two pages and their digest page start at page 4,093 at the latest
    paged_manifest(vec![paged("group-a", 9, 4093, 0, 2 * PAGE)]).validate().unwrap();
    assert!(refused(&paged_manifest(vec![paged("group-a", 9, 4094, 0, 2 * PAGE)])).contains("ordering, identity or bounds"));
}

#[test]
fn a_paged_extent_starts_on_a_page_boundary() {
    let mut extent = paged("group-a", 9, 0, 0, 4096);
    extent.chunk_offset = 17;
    assert!(refused(&paged_manifest(vec![extent.clone()])).contains("ordering, identity or bounds"));
    assert!(extent.pages(PAGE).is_err(), "an unaligned extent names no pages");
}

#[test]
fn extents_are_contiguous_and_cover_the_object_length() {
    let hole = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE), paged("group-a", 9, 1, PAGE + 1, PAGE)]);
    assert!(refused(&hole).contains("ordering, identity or bounds"));
    let mut short = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
    short.length += 1;
    assert!(refused(&short).contains("do not cover the object length"));
    let mut long = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
    long.length -= 1;
    assert!(refused(&long).contains("do not cover the object length"));
}

#[test]
fn a_paged_extent_is_sealed_and_carries_a_digest() {
    let mut open = paged("group-a", 9, 0, 0, PAGE);
    open.flags = CHUNK_FLAG_TAIL;
    assert!(refused(&paged_manifest(vec![open])).contains("ordering, identity or bounds"));
    let mut none = paged("group-a", 9, 0, 0, PAGE);
    none.digest = Vec::new();
    assert!(refused(&paged_manifest(vec![none])).contains("ordering, identity or bounds"));
    let mut short = paged("group-a", 9, 0, 0, PAGE);
    short.digest = vec![1; 31];
    assert!(refused(&paged_manifest(vec![short])).contains("ordering, identity or bounds"));
    let mut footer = paged("group-a", 9, 0, 0, PAGE);
    footer.footer_crc = 5;
    assert!(refused(&paged_manifest(vec![footer])).contains("ordering, identity or bounds"));
    let mut empty = paged("group-a", 9, 0, 0, 0);
    empty.length = 0;
    assert!(refused(&paged_manifest(vec![empty])).contains("ordering, identity or bounds"));
}

#[test]
fn two_extents_of_one_object_never_name_the_same_page() {
    let same = paged_manifest(vec![paged("group-a", 9, 0, 0, 2 * PAGE), paged("group-a", 9, 1, 2 * PAGE, PAGE)]);
    assert!(refused(&same).contains("overlap the same pages"));
    // two pages and their digest page occupy pages 0, 1 and 2: another extent may not take page 2
    let digest_page = paged_manifest(vec![paged("group-a", 9, 0, 0, 2 * PAGE), paged("group-a", 9, 2, 2 * PAGE, PAGE)]);
    assert!(refused(&digest_page).contains("overlap the same pages"));
    // the same pages of another chunk, another volume's chunk, or another group are different pages
    let elsewhere = paged_manifest(vec![
        paged("group-a", 9, 0, 0, 2 * PAGE),
        paged("group-a", 10, 0, 2 * PAGE, PAGE),
        paged("group-b", 9, 1, 3 * PAGE, PAGE),
    ]);
    elsewhere.validate().unwrap();
    // runs of one chunk that touch without overlapping are fine, in either order
    let touching = paged_manifest(vec![paged("group-a", 9, 3, 0, PAGE), paged("group-a", 9, 0, PAGE, 2 * PAGE)]);
    touching.validate().unwrap();
}

#[test]
fn every_extent_has_the_kind_of_its_object() {
    let mut manifest = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
    manifest.extents[0].kind = CHUNK_KIND_LOG;
    assert!(refused(&manifest).contains("ordering, identity or bounds"));
    let mut manifest = log_manifest(vec![log(9, 0, 100, CHUNK_FLAG_SEALED)]);
    manifest.extents[0].kind = CHUNK_KIND_PAGED;
    assert!(refused(&manifest).contains("ordering, identity or bounds"));
    let mut neither = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
    neither.kind = 0;
    assert!(refused(&neither).contains("kind or page size"));
    neither.kind = 3;
    assert!(refused(&neither).contains("kind or page size"));
}

#[test]
fn the_page_size_belongs_to_a_paged_object_and_is_bounded() {
    for bad in [0, MAX_PAGE_BYTES + 1] {
        let mut manifest = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
        manifest.page_bytes = bad;
        assert!(refused(&manifest).contains("kind or page size"), "{bad}");
    }
    let mut manifest = log_manifest(vec![log(9, 0, 100, CHUNK_FLAG_SEALED)]);
    manifest.page_bytes = PAGE;
    assert!(refused(&manifest).contains("kind or page size"));
}

#[test]
fn a_log_chunk_starts_at_zero_and_only_the_last_may_be_open() {
    let sealed = |chunk, offset| log(chunk, offset, 64 << 20, CHUNK_FLAG_SEALED);
    log_manifest(vec![sealed(1, 0), sealed(2, 64 << 20), log(3, 128 << 20, 10, CHUNK_FLAG_TAIL)]).validate().unwrap();
    // an open chunk in the middle
    let middle = log_manifest(vec![log(1, 0, 10, CHUNK_FLAG_TAIL), sealed(2, 10)]);
    assert!(refused(&middle).contains("ordering, identity or bounds"));
    // an extent that does not start at the start of its chunk
    let mut offset = sealed(1, 0);
    offset.chunk_offset = 4096;
    assert!(refused(&log_manifest(vec![offset])).contains("ordering, identity or bounds"));
    // sealed and open together
    let both = log(1, 0, 10, CHUNK_FLAG_SEALED | CHUNK_FLAG_TAIL);
    assert!(refused(&log_manifest(vec![both])).contains("ordering, identity or bounds"));
    // the same chunk twice
    let twice = log_manifest(vec![sealed(1, 0), sealed(1, 64 << 20)]);
    assert!(refused(&twice).contains("overlap the same pages"));
    // a log chunk carries no page digest
    let mut digested = sealed(1, 0);
    digested.digest = digest(1);
    assert!(refused(&log_manifest(vec![digested])).contains("ordering, identity or bounds"));
}

#[test]
fn only_an_open_log_chunk_may_be_empty() {
    log_manifest(vec![log(1, 0, 0, CHUNK_FLAG_TAIL)]).validate().unwrap();
    let sealed_empty = log_manifest(vec![log(1, 0, 0, CHUNK_FLAG_SEALED)]);
    assert!(refused(&sealed_empty).contains("ordering, identity or bounds"));
    let flagless_empty = log_manifest(vec![log(1, 0, 0, 0)]);
    assert!(refused(&flagless_empty).contains("ordering, identity or bounds"));
}

#[test]
fn a_log_footer_belongs_to_a_sealed_chunk_and_its_flag_matches_its_digest() {
    let mut sealed = log(1, 0, 100, CHUNK_FLAG_SEALED | CHUNK_FLAG_FOOTER_SHA256);
    sealed.footer_crc = 9;
    sealed.footer_sha256 = digest(4);
    log_manifest(vec![sealed.clone()]).validate().unwrap();
    let mut flag_only = sealed.clone();
    flag_only.footer_sha256 = Vec::new();
    assert!(refused(&log_manifest(vec![flag_only])).contains("ordering, identity or bounds"));
    let mut digest_only = sealed.clone();
    digest_only.flags = CHUNK_FLAG_SEALED;
    assert!(refused(&log_manifest(vec![digest_only])).contains("ordering, identity or bounds"));
    let mut open_with_crc = log(1, 0, 100, CHUNK_FLAG_TAIL);
    open_with_crc.footer_crc = 9;
    assert!(refused(&log_manifest(vec![open_with_crc])).contains("ordering, identity or bounds"));
    let too_big = log(1, 0, 512 * 1024 * 1024 + 1, CHUNK_FLAG_SEALED);
    assert!(refused(&log_manifest(vec![too_big])).contains("ordering, identity or bounds"));
}

#[test]
fn a_deleted_object_holds_nothing() {
    let tombstone = ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: "key".into(),
        generation: 4,
        deleted: true,
        ..Default::default()
    };
    tombstone.validate().unwrap();
    assert_eq!(ChunkManifest::from_bytes(&tombstone.to_bytes().unwrap()).unwrap(), tombstone);
    for broken in [
        ChunkManifest { length: 1, ..tombstone.clone() },
        ChunkManifest { kind: CHUNK_KIND_PAGED, ..tombstone.clone() },
        ChunkManifest { page_bytes: PAGE, ..tombstone.clone() },
        ChunkManifest { extents: vec![paged("group-a", 9, 0, 0, PAGE)], ..tombstone.clone() },
    ] {
        assert!(refused(&broken).contains("deleted chunk manifest contains data"));
    }
}

#[test]
fn an_object_of_no_bytes_is_a_manifest_without_extents() {
    let empty = paged_manifest(Vec::new());
    empty.validate().unwrap();
    assert_eq!(ChunkManifest::from_bytes(&empty.to_bytes().unwrap()).unwrap(), empty);
}

#[test]
fn a_manifest_holds_at_most_four_thousand_and_ninety_six_extents() {
    // 4,096 runs of one page each, every one of another allocation
    let at_limit = paged_manifest((0..4096u64).map(|n| paged("group-a", n + 1, 0, n * PAGE, PAGE)).collect());
    at_limit.validate().unwrap();
    let over = paged_manifest((0..4097u64).map(|n| paged("group-a", n + 1, 0, n * PAGE, PAGE)).collect());
    assert!(refused(&over).contains("version, generation or size"));
}

#[test]
fn the_generation_and_the_names_are_checked() {
    let mut zero = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
    zero.generation = 0;
    assert!(refused(&zero).contains("version, generation or size"));
    let mut version = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
    version.format_version = 2;
    assert!(refused(&version).contains("version, generation or size"));
    let mut bad_group = paged_manifest(vec![paged("group a", 9, 0, 0, PAGE)]);
    assert!(refused(&bad_group).contains("ordering, identity or bounds"));
    bad_group.extents[0].group = String::new();
    assert!(refused(&bad_group).contains("ordering, identity or bounds"));
    // a chunk identity is two 32-bit numbers: one that does not fit names no chunk
    let mut bad_chunk = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
    bad_chunk.extents[0].chunk_id = 1 << 32;
    assert!(bad_chunk.validate().is_err(), "a chunk id past 32 bits names no chunk");
    let mut bad_volume = paged_manifest(vec![paged("group-a", 9, 0, 0, PAGE)]);
    bad_volume.extents[0].volume_id = 1 << 32;
    assert!(bad_volume.validate().is_err(), "a volume id past 32 bits names no volume");
}

#[test]
fn the_framed_bytes_are_checked_for_magic_checksum_and_canonical_form() {
    let manifest = paged_manifest(vec![paged("group-a", 9, 3, 0, 4096)]);
    let bytes = manifest.to_bytes().unwrap();
    assert_eq!(&bytes[..4], b"MOCM");
    // a flipped payload byte
    let mut flipped = bytes.clone();
    flipped[8] ^= 1;
    assert!(ChunkManifest::from_bytes(&flipped).is_err());
    // the object manifest of page mode is not this manifest
    let mut other_magic = bytes.clone();
    other_magic[..4].copy_from_slice(b"MOMF");
    assert!(ChunkManifest::from_bytes(&other_magic).is_err());
    // a truncated frame
    assert!(ChunkManifest::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    assert!(ChunkManifest::from_bytes(&[]).is_err());
    // a manifest that does not validate is not framed at all
    let mut broken = manifest.clone();
    broken.length += 1;
    assert!(broken.to_bytes().is_err());
}

#[test]
fn a_manifest_that_is_not_in_canonical_form_is_refused() {
    use prost::Message;
    let manifest = paged_manifest(vec![paged("group-a", 9, 3, 0, 4096)]);
    // an unknown field (tag 31, varint 1) is skipped by the decoder, so the bytes decode to an
    // equal manifest but are not what this build would have written
    let mut payload = manifest.encode_to_vec();
    payload.extend_from_slice(&[0xf8, 0x01, 0x01]);
    assert_eq!(ChunkManifest::decode(payload.as_slice()).unwrap(), manifest, "the extra field is ignored");
    let mut bytes = b"MOCM".to_vec();
    bytes.extend_from_slice(&payload);
    let mut hash = 0xcbf29ce484222325u64;
    for byte in &bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
    bytes.extend_from_slice(&hash.to_le_bytes());
    let error = format!("{:?}", ChunkManifest::from_bytes(&bytes).expect_err("a non-canonical frame is refused"));
    assert!(error.contains("not canonical"), "{error}");
    // the canonical bytes with the same checksum rule are accepted
    let mut canonical = b"MOCM".to_vec();
    canonical.extend_from_slice(&manifest.encode_to_vec());
    let mut hash = 0xcbf29ce484222325u64;
    for byte in &canonical {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
    canonical.extend_from_slice(&hash.to_le_bytes());
    assert_eq!(ChunkManifest::from_bytes(&canonical).unwrap(), manifest);
}
