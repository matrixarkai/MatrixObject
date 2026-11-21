// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn object(extents: usize) -> ObjectManifest {
    ObjectManifest {
        format_version: 2, bucket: "bucket".into(), key: "key".into(),
        generation: 129, length: extents as u64,
        extents: (0..extents).map(|index| ObjectExtent {
            group: "group".into(), chunk_id: index as u64 + 1,
            object_offset: index as u64, length: 1, write_sequence: 1,
            data_sha256: vec![1; 32], ..Default::default()
        }).collect(), ..Default::default()
    }
}
fn original_image(catalog: &ManifestCatalog) -> Vec<u8> {
    frame(b"MOMC", &CatalogImage {
        format_version: if catalog.publication_fence != 0 { 4 }
            else if !catalog.staged.is_empty() { 3 }
            else if catalog.retained.is_empty() { 1 } else { 2 },
        objects: catalog.objects.values().map(|entry| entry.manifest.to_manifest()).collect(),
        retained: catalog.retained.values().map(|record| record.as_ref().clone()).collect(),
        staged: catalog.staged.values().cloned().collect(),
        publication_fence: catalog.publication_fence,
    }, MAX_SNAPSHOT).unwrap()
}
fn catalog(version: u32, extents: usize, fence: u64) -> ManifestCatalog {
    let live = object(extents);
    let mut tombstone = object(0);
    tombstone.key = "tombstone".into(); tombstone.deleted = true;
    let image = CatalogImage {
        format_version: version,
        objects: vec![live.clone(), tombstone],
        retained: if version >= 2 { vec![crate::manifest_retention::RetainedManifest {
            retention_id: 1, manifest: Some(live), expires_at_ms: 16384,
        }] } else { vec![] },
        staged: if version >= 3 { vec![crate::staged_allocation::StagedAllocation {
            version: 1, allocation_id: 2, page_count: 128, bucket: "bucket".into(),
            key: "pending".into(), ..Default::default()
        }] } else { vec![] },
        publication_fence: fence,
    };
    ManifestCatalog::from_bytes(&frame(b"MOMC", &image, MAX_SNAPSHOT).unwrap()).unwrap()
}
fn collect(catalog: &ManifestCatalog) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = Vec::new(); let mut chunks = Vec::new();
    catalog.visit_encoded(&mut |part| {
        chunks.push(part.len()); bytes.extend_from_slice(part);
        Ok::<(), std::convert::Infallible>(())
    }).unwrap();
    (bytes, chunks)
}
#[test]
fn all_catalog_versions_keep_their_original_protobuf_frame_and_checksum() {
    for version in 1..=4 {
        for extents in [0, 1, 127, 128, 512, 4096] {
            for fence in if version == 4 { vec![1, 127, 128, 16384, u64::MAX] } else { vec![0] } {
                let catalog = catalog(version, extents, fence);
                let expected = original_image(&catalog);
                let (streamed, chunks) = collect(&catalog);
                assert_eq!(streamed, expected);
                assert_eq!(catalog.to_bytes().unwrap(), expected);
                assert_eq!(catalog.encoded_size(), expected.len());
                assert_eq!(ManifestCatalog::from_bytes(&streamed).unwrap(), catalog);
                assert_eq!(chunks[0], 4); assert_eq!(*chunks.last().unwrap(), 8);
            }
        }
    }
}
#[test]
fn a_catalog_sink_failure_stops_at_exactly_the_failing_chunk() {
    for version in 1..=4 {
        let catalog = catalog(version, 128, if version == 4 { 16384 } else { 0 });
        let (expected, chunks) = collect(&catalog);
        for failed_chunk in 0..chunks.len() {
            let mut calls = 0; let mut emitted = Vec::new();
            let result = catalog.visit_encoded(&mut |part| {
                let position = calls; calls += 1;
                if position == failed_chunk { return Err(position); }
                emitted.extend_from_slice(part); Ok(())
            });
            assert_eq!(result, Err(failed_chunk));
            assert_eq!(calls, failed_chunk + 1);
            let accepted: usize = chunks[..failed_chunk].iter().sum();
            assert_eq!(emitted, expected[..accepted]);
        }
    }
}
#[test]
fn catalog_output_chunks_are_per_record_as_the_object_count_grows() {
    let mut catalog = ManifestCatalog::default();
    let mut next = 0;
    for size in [1, 512, 8192] {
        while next < size {
            let value = ObjectManifest { format_version: 2, bucket: "b".into(),
                key: format!("object_{next:08}"), generation: 1, ..Default::default() };
            assert!(catalog.apply(&ManifestOutcome::decide(None, value).unwrap()).unwrap()); next += 1;
        }
        let (bytes, chunks) = collect(&catalog);
        assert_eq!(bytes, original_image(&catalog));
        assert_eq!(chunks.len(), size + 3);
        assert!(chunks.iter().all(|length| *length <= 32));
    }
}
#[test]
fn empty_and_fence_only_catalogs_keep_their_original_frame() {
    for fence in [0, 1, 127, 128, u64::MAX] {
        let mut catalog = ManifestCatalog::default();
        catalog.restore_publication_fence(fence);
        let expected = original_image(&catalog);
        assert_eq!(collect(&catalog).0, expected);
        assert_eq!(catalog.to_bytes().unwrap(), expected);
        assert_eq!(ManifestCatalog::from_bytes(&expected).unwrap(), catalog);
    }
}
#[test]
fn chunked_extents_keep_page_authentication_and_sealed_footer_fields() {
    let page = ObjectExtent { group: "g".into(), volume_id: 1, chunk_id: 1,
        length: 4, write_sequence: 1, data_checksum: 7, data_sha256: vec![9; 32],
        ..Default::default() };
    let sealed = ObjectExtent { group: "g".into(), volume_id: 2, chunk_id: 10,
        object_offset: 4, length: 64 * 1024 * 1024, write_sequence: 1,
        kind: EXTENT_KIND_LOG_CHUNK, flags: EXTENT_FLAG_SEALED | EXTENT_FLAG_FOOTER_SHA256,
        footer_crc: 0xdead_beef, footer_sha256: vec![5; 32], ..Default::default() };
    let tail = ObjectExtent { group: "g".into(), volume_id: 2, chunk_id: 11,
        object_offset: 4 + 64 * 1024 * 1024, length: 100, write_sequence: 1,
        kind: EXTENT_KIND_LOG_CHUNK, flags: EXTENT_FLAG_TAIL, ..Default::default() };
    let value = ObjectManifest { format_version: 3, bucket: "b".into(), key: "k".into(),
        generation: 1, length: 104 + 64 * 1024 * 1024, extents: vec![page, sealed, tail],
        deleted: false };
    // This fixture exercises restore and encoding, not a publication entry point.
    let image = CatalogImage { format_version: 1, objects: vec![value], ..Default::default() };
    let catalog = ManifestCatalog::from_bytes(&frame(b"MOMC", &image, MAX_SNAPSHOT).unwrap()).unwrap();
    let expected = original_image(&catalog);
    assert_eq!(collect(&catalog).0, expected);
    assert_eq!(catalog.to_bytes().unwrap(), expected);
    assert_eq!(ManifestCatalog::from_bytes(&expected).unwrap(), catalog);
}
