// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::collections::BTreeMap;

/// An object stored the way the client would store it, in one chunk, with the pages kept in a map.
struct Stored {
    manifest: ChunkManifest,
    pages: BTreeMap<u64, Vec<u8>>,
}

fn pseudo_random(length: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

fn store(data: &[u8], page_bytes: u64, stripe_bytes: u64) -> Stored {
    let stripes = plan_stripes(data.len() as u64, page_bytes, stripe_bytes).unwrap();
    // runs do not start at the first page, and each one begins where the one before it stopped
    let mut next_page = 3u64;
    let mut pages = BTreeMap::new();
    let mut extents = Vec::new();
    for (index, stripe) in stripes.iter().enumerate() {
        let bytes = &data[stripe.range()];
        let digests = chunk_integrity::digest_extent(bytes, page_bytes as usize).unwrap();
        let placement = Placement { group: format!("g{}", index % 2), volume_id: 1, chunk_id: 7, first_page: next_page };
        for (page, content) in pages_of(bytes, page_bytes as usize, &digests) {
            assert!(pages.insert(next_page + page, content.to_vec()).is_none(), "runs never overlap");
        }
        extents.push(extent_for(&placement, stripe, &digests, page_bytes));
        next_page += chunk_integrity::stored_pages(stripe.length, page_bytes);
    }
    Stored { manifest: build_manifest("bucket", "key", 1, page_bytes, extents).unwrap(), pages }
}

fn read(stored: &Stored, offset: u64, length: u64) -> Result<Vec<u8>, ObjectError> {
    let mut out = Vec::new();
    for read in plan_read(&stored.manifest, offset, length)? {
        let extent = &stored.manifest.extents[read.extent];
        let wanted = pages_to_fetch(extent, &read, stored.manifest.page_bytes);
        let mut fetched: Vec<&Vec<u8>> = wanted.iter().map(|page| stored.pages.get(page).expect("the page is stored")).collect();
        let digest_page = if read.digest_page { Some(fetched.pop().unwrap().as_slice()) } else { None };
        let slices: Vec<&[u8]> = fetched.iter().map(|page| page.as_slice()).collect();
        out.extend(verify_and_cut(extent, stored.manifest.page_bytes, &read, digest_page, &slices)?);
    }
    Ok(out)
}

#[test]
fn stripes_are_whole_pages_never_more_than_an_allocation_and_cover_the_object() {
    // 8-byte pages in 30-byte stripes: three pages, 24 bytes
    assert_eq!(stripe_capacity(8, 30).unwrap(), 24);
    for length in 0..=100u64 {
        let stripes = plan_stripes(length, 8, 30).unwrap();
        assert_eq!(stripes.iter().map(|stripe| stripe.length).sum::<u64>(), length);
        let mut next = 0;
        for stripe in &stripes {
            assert_eq!(stripe.object_offset, next, "stripes are contiguous");
            assert!(stripe.length > 0 && stripe.length <= 24);
            next += stripe.length;
        }
        assert_eq!(stripes.len() as u64, length.div_ceil(24));
    }
    // the default geometry: an object under 64 MiB is one extent, and one byte over is two
    let mib = 1u64 << 20;
    assert_eq!(plan_stripes(64 * mib, mib, DEFAULT_STRIPE_BYTES).unwrap().len(), 1);
    assert_eq!(plan_stripes(64 * mib + 1, mib, DEFAULT_STRIPE_BYTES).unwrap().len(), 2);
    // small pages cap a stripe at 4,095 pages, so a stripe and its digest page fit one allocation
    assert_eq!(stripe_capacity(4096, DEFAULT_STRIPE_BYTES).unwrap(), 4095 * 4096);
    // refusals: no page size, a page past the largest, a stripe smaller than a page, too many extents
    assert!(stripe_capacity(0, 100).is_err());
    assert!(stripe_capacity(MAX_PAGE_BYTES + 1, DEFAULT_STRIPE_BYTES).is_err());
    assert!(stripe_capacity(8, 7).is_err());
    assert!(plan_stripes(8 * 4096, 8, 8).is_ok());
    assert!(plan_stripes(8 * 4097, 8, 8).is_err());
}

#[test]
fn a_one_page_stripe_stores_one_page_and_a_longer_one_stores_its_digest_page_last() {
    let data = pseudo_random(5);
    let digests = chunk_integrity::digest_extent(&data, 8).unwrap();
    let pages = pages_of(&data, 8, &digests);
    assert_eq!(pages.len(), 1, "a small object costs the one page it costs today");
    assert_eq!(pages[0], (0, data.as_slice()));

    let data = pseudo_random(20);
    let digests = chunk_integrity::digest_extent(&data, 8).unwrap();
    let pages = pages_of(&data, 8, &digests);
    assert_eq!(pages.len(), 4, "three data pages and the digest page");
    assert_eq!(pages.iter().map(|(index, _)| *index).collect::<Vec<_>>(), [0, 1, 2, 3]);
    assert_eq!(pages[3].1, digests.digest_page.as_deref().unwrap());
    assert_eq!(pages[3].1.len(), 3 * 32);
    let rebuilt: Vec<u8> = pages[..3].iter().flat_map(|(_, page)| page.iter().copied()).collect();
    assert_eq!(rebuilt, data);
}

#[test]
fn the_manifest_names_every_run_and_a_manifest_with_a_hole_or_a_wrong_length_is_refused() {
    let stored = store(&pseudo_random(100), 8, 30);
    let manifest = &stored.manifest;
    assert_eq!(manifest.length, 100);
    assert_eq!(manifest.extents.len(), 5);
    assert_eq!(manifest.page_bytes, 8);
    for extent in &manifest.extents {
        assert_eq!(extent.chunk_offset % 8, 0);
        assert_eq!(extent.digest.len(), 32);
    }
    // a hole between two extents is not a manifest
    let mut hole = manifest.extents.clone();
    hole[1].object_offset += 1;
    assert!(build_manifest("bucket", "key", 1, 8, hole).is_err());
    // an extent of no bytes is not an extent
    let mut empty = manifest.extents.clone();
    empty[0].length = 0;
    assert!(build_manifest("bucket", "key", 1, 8, empty).is_err());
    // a run that would pass the end of its allocation is refused
    let mut late = manifest.extents.clone();
    late[0].chunk_offset = 4095 * 8;
    assert!(build_manifest("bucket", "key", 1, 8, late).is_err());
}

#[test]
fn every_range_of_an_object_reads_back_exactly_through_the_plan_and_the_verification() {
    let data = pseudo_random(100);
    let stored = store(&data, 8, 30);
    for offset in 0..=100u64 {
        for length in 0..=(100 - offset) {
            let got = read(&stored, offset, length).unwrap();
            assert_eq!(got, &data[offset as usize..(offset + length) as usize], "range {offset}+{length}");
        }
    }
    // a read touches only the pages it needs, plus the digest page of each extent it reaches
    let reads = plan_read(&stored.manifest, 10, 4).unwrap();
    assert_eq!(reads.len(), 1);
    assert_eq!((reads[0].first_page, reads[0].pages, reads[0].digest_page, reads[0].skip, reads[0].take), (1, 1, true, 2, 4));
    let fetched = pages_to_fetch(&stored.manifest.extents[0], &reads[0], 8);
    assert_eq!(fetched, [3 + 1, 3 + 3], "the second data page and the digest page");
    // an object of one small page has no digest page to fetch
    let small = store(&pseudo_random(5), 8, 30);
    let reads = plan_read(&small.manifest, 1, 3).unwrap();
    assert!(!reads[0].digest_page);
    assert_eq!(pages_to_fetch(&small.manifest.extents[0], &reads[0], 8), [3]);
}

#[test]
fn a_read_past_the_end_a_deleted_object_and_a_log_object_are_refused_by_the_plan() {
    let stored = store(&pseudo_random(100), 8, 30);
    assert!(plan_read(&stored.manifest, 0, 101).is_err());
    assert!(plan_read(&stored.manifest, 100, 1).is_err());
    assert!(plan_read(&stored.manifest, u64::MAX, 2).is_err());
    assert!(plan_read(&stored.manifest, 100, 0).unwrap().is_empty());
    let mut deleted = stored.manifest.clone();
    deleted.deleted = true;
    assert!(plan_read(&deleted, 0, 1).is_err());
    let mut log = stored.manifest.clone();
    log.kind = crate::chunk_manifest::CHUNK_KIND_LOG;
    assert!(plan_read(&log, 0, 1).is_err());
}

#[test]
fn a_tampered_page_fails_the_read_that_touches_it_and_not_one_that_avoids_it() {
    let data = pseudo_random(100);
    let mut stored = store(&data, 8, 30);
    // the second data page of the first extent: object bytes 8..16
    let damaged = stored.manifest.extents[0].chunk_offset / 8 + 1;
    stored.pages.get_mut(&damaged).unwrap()[0] ^= 0xFF;
    let error = read(&stored, 8, 8).unwrap_err();
    assert!(matches!(error, ObjectError::Corruption(ref message) if message.contains("integrity")), "{error:?}");
    assert!(read(&stored, 0, 100).is_err(), "a whole read reaches the damaged page");
    // the digest page verified, so a range that stays clear of the damaged page is still served
    assert_eq!(read(&stored, 0, 8).unwrap(), &data[0..8]);
    assert_eq!(read(&stored, 16, 8).unwrap(), &data[16..24]);
    assert_eq!(read(&stored, 24, 76).unwrap(), &data[24..100], "the other extents are untouched");
}

#[test]
fn a_tampered_digest_page_or_a_missing_one_fails_every_read_of_its_extent() {
    let data = pseudo_random(100);
    let mut stored = store(&data, 8, 30);
    let extent = stored.manifest.extents[0].clone();
    let digest_at = extent.chunk_offset / 8 + 3;
    stored.pages.get_mut(&digest_at).unwrap()[40] ^= 1;
    // even a read of an undamaged page of that extent fails: the list it is checked against is wrong
    assert!(read(&stored, 0, 8).is_err());
    assert!(read(&stored, 24, 8).is_ok(), "another extent has its own digest page");
    // a multi-page extent read without its digest page is refused, never read unverified
    let reads = plan_read(&stored.manifest, 0, 8).unwrap();
    let page = vec![0u8; 8];
    let refused = verify_and_cut(&extent, 8, &reads[0], None, &[page.as_slice()]);
    assert!(matches!(refused, Err(ObjectError::Corruption(_))), "{refused:?}");
}

#[test]
fn a_page_cut_short_or_the_wrong_number_of_pages_is_refused() {
    let data = pseudo_random(100);
    let stored = store(&data, 8, 30);
    let extent = &stored.manifest.extents[0];
    let reads = plan_read(&stored.manifest, 0, 24).unwrap();
    let read = &reads[0];
    assert_eq!(read.pages, 3);
    let base = extent.chunk_offset / 8;
    let page = |index: u64| stored.pages[&(base + index)].as_slice();
    let digest_page = Some(stored.pages[&(base + 3)].as_slice());
    // the right pages verify
    assert_eq!(verify_and_cut(extent, 8, read, digest_page, &[page(0), page(1), page(2)]).unwrap(), &data[0..24]);
    // too few pages
    assert!(verify_and_cut(extent, 8, read, digest_page, &[page(0), page(1)]).is_err());
    // a page that came back short
    assert!(verify_and_cut(extent, 8, read, digest_page, &[page(0), &page(1)[..7], page(2)]).is_err());
    // a page in the wrong place
    assert!(verify_and_cut(extent, 8, read, digest_page, &[page(1), page(0), page(2)]).is_err());
}
