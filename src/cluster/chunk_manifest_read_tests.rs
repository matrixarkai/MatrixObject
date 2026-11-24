// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::object_manifest::ManifestListRequest;

fn chunk_manifest_in(bucket: &str, key: &str, generation: u64, byte: u8) -> ChunkManifest {
    ChunkManifest { bucket: bucket.into(), ..chunk_manifest(key, generation, byte) }
}

fn chunk_tombstone(bucket: &str, key: &str, generation: u64) -> ChunkManifest {
    ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: bucket.into(),
        key: key.into(),
        generation,
        deleted: true,
        ..Default::default()
    }
}

fn publish_to(service: &mut MatrixMetaService, before: Option<&ChunkManifest>, after: ChunkManifest) {
    assert!(super::super::chunk_manifest_command::apply(service, &chunk_outcome(before, after)).unwrap());
}

fn read_chunk(service: &mut MatrixMetaService, bucket: &str, key: &str) -> Result<MetaReply, ObjectError> {
    super::super::meta_server::apply_to_meta(
        service,
        MetaRequest::ReadChunkManifest { bucket: bucket.into(), key: key.into() },
        65536,
    )
}

fn list_chunk(service: &mut MatrixMetaService, request: ManifestListRequest) -> Result<crate::object_manifest::ManifestListPage, ObjectError> {
    match super::super::meta_server::apply_to_meta(service, MetaRequest::ListChunkManifests(request), 65536)? {
        MetaReply::Manifests(page) => Ok(page),
        other => panic!("a manifest page was expected: {other:?}"),
    }
}

fn page_request(bucket: &str, after: &str, limit: u32, revision: Vec<u8>) -> ManifestListRequest {
    ManifestListRequest {
        version: 2,
        bucket: bucket.into(),
        after: after.into(),
        limit,
        expected_revision: revision,
        after_bucket: String::new(),
    }
}

#[test]
fn a_chunk_read_and_a_chunk_listing_round_trip_the_wire_and_a_bad_one_is_refused() {
    assert_eq!(MetaRequestCode::from_u8(99), Some(MetaRequestCode::ReadChunkManifest));
    assert_eq!(MetaRequestCode::from_u8(101), Some(MetaRequestCode::ListChunkManifests));
    assert!(MetaRequestCode::all().contains(&MetaRequestCode::ReadChunkManifest));
    assert!(MetaRequestCode::all().contains(&MetaRequestCode::ListChunkManifests));
    let read = MetaRequest::ReadChunkManifest { bucket: "bucket".into(), key: "a".into() };
    assert_eq!(read.code(), MetaRequestCode::ReadChunkManifest);
    let payload = read.encode_payload();
    assert_eq!(MetaRequest::decode(MetaRequestCode::ReadChunkManifest, &payload).unwrap(), read);
    assert!(MetaRequest::decode(MetaRequestCode::ReadChunkManifest, &vec![0xFF; 9000]).is_err(), "over its bound");
    assert!(MetaRequest::decode(MetaRequestCode::ReadChunkManifest, &payload[..payload.len() - 1]).is_err());
    // an invalid bucket is refused at the door, never looked up
    let bad = MetaRequest::ReadChunkManifest { bucket: String::new(), key: "a".into() };
    assert!(MetaRequest::decode(MetaRequestCode::ReadChunkManifest, &bad.encode_payload()).is_err());

    let list = MetaRequest::ListChunkManifests(page_request("bucket", "", 2, Vec::new()));
    assert_eq!(list.code(), MetaRequestCode::ListChunkManifests);
    assert_eq!(MetaRequest::decode(MetaRequestCode::ListChunkManifests, &list.encode_payload()).unwrap(), list);
    // a limit outside 1..=32 is refused at the door
    let wide = MetaRequest::ListChunkManifests(page_request("bucket", "", 33, Vec::new()));
    assert!(MetaRequest::decode(MetaRequestCode::ListChunkManifests, &wide.encode_payload()).is_err());
}

#[test]
fn a_chunk_read_answers_the_stored_manifest_its_tombstone_and_not_found_for_a_stranger() {
    let (mut service, _groups) = fixture(1, 0);
    let first = chunk_manifest("a", 1, 1);
    publish_to(&mut service, None, first.clone());
    match read_chunk(&mut service, "bucket", "a").unwrap() {
        MetaReply::ChunkManifest(manifest) => assert_eq!(manifest, first),
        other => panic!("a chunk manifest was expected: {other:?}"),
    }
    publish_to(&mut service, Some(&first), chunk_tombstone("bucket", "a", 2));
    match read_chunk(&mut service, "bucket", "a").unwrap() {
        MetaReply::ChunkManifest(manifest) => assert!(manifest.deleted && manifest.generation == 2),
        other => panic!("a tombstone was expected: {other:?}"),
    }
    assert!(matches!(read_chunk(&mut service, "bucket", "never"), Err(ObjectError::NotFound(_))));
    assert!(matches!(read_chunk(&mut service, "other", "a"), Err(ObjectError::NotFound(_))));
}

#[test]
fn a_chunk_listing_pages_in_key_order_stays_in_its_bucket_and_shows_tombstones() {
    let (mut service, _groups) = fixture(1, 0);
    for index in 0..5u8 {
        publish_to(&mut service, None, chunk_manifest(&format!("k{index}"), 1, index + 1));
    }
    publish_to(&mut service, None, chunk_manifest_in("other", "z", 1, 9));
    let first = chunk_manifest("k0", 1, 1);
    publish_to(&mut service, Some(&first), chunk_tombstone("bucket", "k0", 2));

    let page = list_chunk(&mut service, page_request("bucket", "", 2, Vec::new())).unwrap();
    assert_eq!(page.entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(), ["k0", "k1"]);
    assert!(page.entries[0].deleted && page.entries[0].generation == 2, "a tombstone is listed as deleted");
    assert_eq!(page.entries[1].length, 4096);
    assert!(page.has_more);
    assert_eq!(page.revision.len(), 16);
    let second = list_chunk(&mut service, page_request("bucket", "k1", 2, page.revision.clone())).unwrap();
    assert_eq!(second.entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(), ["k2", "k3"]);
    assert!(second.has_more);
    let last = list_chunk(&mut service, page_request("bucket", "k3", 2, second.revision.clone())).unwrap();
    assert_eq!(last.entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(), ["k4"]);
    assert!(!last.has_more, "the other bucket is not part of this listing");
    // the wire carries the page whole
    let reply = MetaReply::Manifests(page);
    assert_eq!(MetaReply::decode(MetaRequestCode::ListChunkManifests, &reply.encode_payload()).unwrap(), reply);
}

#[test]
fn a_chunk_listing_refuses_to_continue_across_a_change_and_the_global_cursor_walks_every_bucket() {
    let (mut service, _groups) = fixture(1, 0);
    publish_to(&mut service, None, chunk_manifest("a", 1, 1));
    publish_to(&mut service, None, chunk_manifest_in("other", "b", 1, 2));
    let page = list_chunk(&mut service, page_request("bucket", "", 1, Vec::new())).unwrap();
    publish_to(&mut service, None, chunk_manifest("c", 1, 3));
    let stale = list_chunk(&mut service, page_request("bucket", "a", 1, page.revision.clone()));
    assert!(matches!(stale, Err(ObjectError::PreconditionFailed(_))), "{stale:?}");

    let global = ManifestListRequest {
        version: 3,
        bucket: String::new(),
        after: String::new(),
        limit: 32,
        expected_revision: Vec::new(),
        after_bucket: String::new(),
    };
    let page = list_chunk(&mut service, global).unwrap();
    let seen: Vec<(String, String)> = page.entries.iter().map(|entry| (entry.bucket.clone(), entry.key.clone())).collect();
    assert_eq!(
        seen,
        [("bucket".to_string(), "a".to_string()), ("bucket".to_string(), "c".to_string()), ("other".to_string(), "b".to_string())]
    );
}
