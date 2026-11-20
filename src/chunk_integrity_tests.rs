// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

const PAGE: usize = 64;

fn bytes(length: usize) -> Vec<u8> {
    (0..length).map(|n| (n * 7 + 3) as u8).collect()
}

fn pages_of(data: &[u8]) -> Vec<&[u8]> {
    data.chunks(PAGE).collect()
}

/// Three pages, the last short: 64 + 64 + 40 bytes.
fn three_pages() -> (Vec<u8>, ExtentDigests) {
    let data = bytes(2 * PAGE + 40);
    let digests = digest_extent(&data, PAGE).unwrap();
    (data, digests)
}

#[test]
fn a_one_page_extent_has_no_digest_page_and_its_digest_is_the_digest_of_its_bytes() {
    let data = bytes(PAGE - 5);
    let digests = digest_extent(&data, PAGE).unwrap();
    assert_eq!(digests.digest_page, None);
    assert_eq!(digests.digest, sha256(&data));
    verify_whole(&digests.digest, None, PAGE as u64, &data).unwrap();
    // exactly one page is one page
    let full = bytes(PAGE);
    assert_eq!(digest_extent(&full, PAGE).unwrap().digest_page, None);
    assert_eq!(stored_pages(full.len() as u64, PAGE as u64), 1);
}

#[test]
fn a_multi_page_extent_stores_one_digest_page_and_its_digest_is_the_digest_of_that_page() {
    let (data, digests) = three_pages();
    let page = digests.digest_page.as_ref().expect("three pages have a digest page");
    assert_eq!(page.len(), 3 * DIGEST_BYTES);
    assert_eq!(&page[..DIGEST_BYTES], sha256(&data[..PAGE]).as_slice());
    assert_eq!(&page[2 * DIGEST_BYTES..], sha256(&data[2 * PAGE..]).as_slice(), "the short last page is hashed as it is");
    assert_eq!(digests.digest, sha256(page));
    assert_eq!(data_pages(data.len() as u64, PAGE as u64), 3);
    assert_eq!(stored_pages(data.len() as u64, PAGE as u64), 4, "three data pages and the digest page");
    verify_whole(&digests.digest, Some(page), PAGE as u64, &data).unwrap();
}

#[test]
fn an_extent_holds_at_most_4095_data_pages_when_it_has_more_than_one() {
    let page = PAGE as u64;
    assert!(fits_allocation(page, page));
    assert!(fits_allocation(MAX_DATA_PAGES * page, page));
    assert!(!fits_allocation(MAX_DATA_PAGES * page + 1, page), "4,096 data pages leave no slot for the digest page");
    assert!(!fits_allocation(0, page));
    assert!(!fits_allocation(10, 0));
    assert_eq!(stored_pages(MAX_DATA_PAGES * page, page), 4096, "the whole run fits one allocation");
    assert!(matches!(
        digest_extent(&vec![0u8; PAGE * (MAX_DATA_PAGES as usize + 1)], PAGE),
        Err(IntegrityError::Geometry(_))
    ));
}

#[test]
fn a_missing_digest_page_fails_the_read_and_is_never_skipped() {
    let (data, digests) = three_pages();
    let error = verify_whole(&digests.digest, None, PAGE as u64, &data).unwrap_err();
    assert_eq!(error, IntegrityError::DigestPageMissing);
    let error = verify_range(&digests.digest, None, data.len() as u64, PAGE as u64, 1, &[&data[PAGE..2 * PAGE]]).unwrap_err();
    assert_eq!(error, IntegrityError::DigestPageMissing, "a range read needs the digest page too");
}

#[test]
fn a_truncated_or_padded_digest_page_fails_the_read() {
    let (data, digests) = three_pages();
    let page = digests.digest_page.unwrap();
    let truncated = &page[..page.len() - 1];
    assert_eq!(
        verify_whole(&digests.digest, Some(truncated), PAGE as u64, &data).unwrap_err(),
        IntegrityError::DigestPageLength { expected: 3 * DIGEST_BYTES, found: 3 * DIGEST_BYTES - 1 }
    );
    let mut padded = page.clone();
    padded.push(0);
    assert_eq!(
        verify_whole(&digests.digest, Some(&padded), PAGE as u64, &data).unwrap_err(),
        IntegrityError::DigestPageLength { expected: 3 * DIGEST_BYTES, found: 3 * DIGEST_BYTES + 1 }
    );
    assert_eq!(
        verify_whole(&digests.digest, Some(&[]), PAGE as u64, &data).unwrap_err(),
        IntegrityError::DigestPageLength { expected: 3 * DIGEST_BYTES, found: 0 }
    );
}

#[test]
fn a_wrong_or_changed_digest_page_fails_the_read() {
    let (data, digests) = three_pages();
    let page = digests.digest_page.unwrap();
    // one bit of one entry changed
    let mut changed = page.clone();
    changed[40] ^= 1;
    assert_eq!(
        verify_whole(&digests.digest, Some(&changed), PAGE as u64, &data).unwrap_err(),
        IntegrityError::DigestPageMismatch
    );
    // the digest page of a different extent of the same shape
    let other_data = bytes(2 * PAGE + 41);
    let other = digest_extent(&other_data, PAGE).unwrap();
    assert_ne!(other.digest, digests.digest);
    assert_eq!(
        verify_whole(&digests.digest, other.digest_page.as_deref(), PAGE as u64, &data).unwrap_err(),
        IntegrityError::DigestPageMismatch
    );
}

#[test]
fn a_tampered_data_page_fails_a_whole_read_and_names_the_page() {
    let (data, digests) = three_pages();
    for tampered_page in [0usize, 1, 2] {
        let mut damaged = data.clone();
        damaged[tampered_page * PAGE + 3] ^= 0x80;
        assert_eq!(
            verify_whole(&digests.digest, digests.digest_page.as_deref(), PAGE as u64, &damaged).unwrap_err(),
            IntegrityError::PageMismatch { page: tampered_page as u64 }
        );
    }
}

#[test]
fn a_range_read_that_touches_a_tampered_page_fails_and_one_that_does_not_still_succeeds() {
    let (data, digests) = three_pages();
    let digest_page = digests.digest_page.as_deref();
    let mut damaged = data.clone();
    damaged[PAGE + 9] ^= 1; // page 1
    let damaged_pages = pages_of(&damaged);
    // pages 1..3 include the damaged one
    assert_eq!(
        verify_range(&digests.digest, digest_page, data.len() as u64, PAGE as u64, 1, &damaged_pages[1..]).unwrap_err(),
        IntegrityError::PageMismatch { page: 1 }
    );
    // page 0 alone and page 2 alone are undamaged: the digest page itself verified, and each read
    // binds only the pages it touched
    verify_range(&digests.digest, digest_page, data.len() as u64, PAGE as u64, 0, &damaged_pages[..1]).unwrap();
    verify_range(&digests.digest, digest_page, data.len() as u64, PAGE as u64, 2, &damaged_pages[2..]).unwrap();
}

#[test]
fn a_page_of_the_wrong_length_fails_the_read() {
    let (data, digests) = three_pages();
    let digest_page = digests.digest_page.as_deref();
    let long = bytes(PAGE + 1);
    assert_eq!(
        verify_range(&digests.digest, digest_page, data.len() as u64, PAGE as u64, 0, &[&long]).unwrap_err(),
        IntegrityError::PageLength { page: 0, expected: PAGE, found: PAGE + 1 }
    );
    // the last page is shorter than a full page: a full-length one is wrong
    let full_last = bytes(PAGE);
    assert_eq!(
        verify_range(&digests.digest, digest_page, data.len() as u64, PAGE as u64, 2, &[&full_last]).unwrap_err(),
        IntegrityError::PageLength { page: 2, expected: 40, found: PAGE }
    );
}

#[test]
fn a_one_page_extent_is_verified_against_the_manifest_digest_and_refuses_a_digest_page() {
    let data = bytes(PAGE - 1);
    let digests = digest_extent(&data, PAGE).unwrap();
    let mut damaged = data.clone();
    damaged[0] ^= 1;
    assert_eq!(
        verify_whole(&digests.digest, None, PAGE as u64, &damaged).unwrap_err(),
        IntegrityError::PageMismatch { page: 0 }
    );
    assert_eq!(
        verify_whole(&digests.digest, Some(&[0u8; DIGEST_BYTES]), PAGE as u64, &data).unwrap_err(),
        IntegrityError::UnexpectedDigestPage
    );
}

#[test]
fn a_read_that_does_not_fit_the_extent_is_refused() {
    let (data, digests) = three_pages();
    let digest_page = digests.digest_page.as_deref();
    let pages = pages_of(&data);
    let length = data.len() as u64;
    let page = PAGE as u64;
    assert!(matches!(verify_range(&digests.digest, digest_page, length, page, 3, &pages[..1]), Err(IntegrityError::Geometry(_))));
    assert!(matches!(verify_range(&digests.digest, digest_page, length, page, 0, &[]), Err(IntegrityError::Geometry(_))));
    assert!(matches!(verify_range(&digests.digest, digest_page, length, 0, 0, &pages[..1]), Err(IntegrityError::Geometry(_))));
    assert!(matches!(verify_range(&digests.digest[..31], digest_page, length, page, 0, &pages[..1]), Err(IntegrityError::Geometry(_))));
    assert!(matches!(verify_range(&digests.digest, digest_page, 0, page, 0, &pages[..1]), Err(IntegrityError::Geometry(_))));
    assert!(matches!(digest_extent(&[], PAGE), Err(IntegrityError::Geometry(_))));
    assert!(matches!(digest_extent(&bytes(10), 0), Err(IntegrityError::Geometry(_))));
}

#[test]
fn an_integrity_failure_is_a_corruption_error_that_names_which_check_failed() {
    let messages: Vec<String> = [
        IntegrityError::DigestPageMissing,
        IntegrityError::UnexpectedDigestPage,
        IntegrityError::DigestPageLength { expected: 96, found: 95 },
        IntegrityError::DigestPageMismatch,
        IntegrityError::PageMismatch { page: 7 },
        IntegrityError::PageLength { page: 2, expected: 40, found: 64 },
        IntegrityError::Geometry("a page past the end of the extent"),
    ]
    .into_iter()
    .map(|error| match error.into_object_error() {
        ObjectError::Corruption(message) => message,
        other => panic!("an integrity failure must be a corruption error, not {other:?}"),
    })
    .collect();
    for message in &messages {
        assert!(message.starts_with("extent integrity: "), "{message}");
    }
    let mut distinct = messages.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), messages.len(), "every failure reads differently: {messages:?}");
}
