// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The chunk manifest catalog inside the metadata snapshot: version 23, written only while the
//! catalog is not empty, covered by the checksum only then, and refused when damaged.
use super::*;
use crate::chunk_catalog::ChunkOutcome;
use crate::chunk_manifest::{ChunkExtent, ChunkManifest, CHUNK_FLAG_SEALED, CHUNK_KIND_PAGED, CHUNK_MANIFEST_FORMAT};

fn manifest(key: &str, generation: u64, byte: u8) -> ChunkManifest {
    ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: key.into(),
        generation,
        length: 4096,
        extents: vec![ChunkExtent {
            group: "group-a".into(),
            volume_id: 1,
            chunk_id: 9,
            kind: CHUNK_KIND_PAGED,
            object_offset: 0,
            chunk_offset: 0,
            length: 4096,
            flags: CHUNK_FLAG_SEALED,
            digest: vec![byte; 32],
            ..Default::default()
        }],
        deleted: false,
        kind: CHUNK_KIND_PAGED,
        page_bytes: 1 << 20,
        ..Default::default()
    }
}

fn service_holding(manifests: &[ChunkManifest]) -> MatrixMetaService {
    let mut service = MatrixMetaService::new();
    for manifest in manifests {
        let outcome = ChunkOutcome::decide(None, manifest.clone()).unwrap();
        assert!(service.chunk_catalog.apply(&outcome).unwrap());
    }
    service
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| window == needle)
}

#[test]
fn a_snapshot_without_a_catalog_is_written_as_before() {
    let service = MatrixMetaService::new();
    let bytes = service.export_snapshot().to_bytes().unwrap();
    assert!(!contains(&bytes[..32], b"MOSRSMETA23"));
    assert!(!contains(&bytes, b"MOCC"));
}

#[test]
fn a_catalog_makes_the_snapshot_version_23_and_it_round_trips_into_another_service() {
    let first = manifest("a", 1, 1);
    let second = manifest("b", 1, 2);
    let service = service_holding(&[first.clone(), second.clone()]);
    let snapshot = service.export_snapshot();
    assert_eq!(snapshot.chunk_catalog.len(), 2);
    let bytes = snapshot.to_bytes().unwrap();
    assert!(contains(&bytes[..32], b"MOSRSMETA23"));
    assert!(contains(&bytes, b"MOCC"));

    let decoded = MetaSnapshot::from_bytes(&bytes).unwrap();
    assert_eq!(decoded, snapshot);
    assert_eq!(decoded.chunk_catalog.len(), 2);

    let mut other = MatrixMetaService::new();
    other.install_snapshot(decoded).unwrap();
    assert_eq!(other.chunk_catalog, service.chunk_catalog);
    assert_eq!(**other.chunk_catalog.get(&first.object_id()).unwrap(), first);
    assert_eq!(**other.chunk_catalog.get(&second.object_id()).unwrap(), second);
    // what the first service exports now is what the second exports: a follower that installed it
    // can itself be the source of the next snapshot
    assert_eq!(other.export_snapshot().to_bytes().unwrap(), bytes);
}

#[test]
fn the_checksum_covers_the_catalog_only_when_there_is_one() {
    let empty = MatrixMetaService::new().export_snapshot();
    let one = service_holding(&[manifest("a", 1, 1)]).export_snapshot();
    let other = service_holding(&[manifest("a", 1, 2)]).export_snapshot();
    assert_ne!(one.checksum, empty.checksum, "a catalog must move the checksum");
    assert_ne!(one.checksum, other.checksum, "one changed digest byte must move the checksum");
    // a service that never held one has the checksum it always had
    let mut cleared = one.clone();
    cleared.chunk_catalog = Default::default();
    assert_eq!(cleared.checksum_material_hash(), empty.checksum);
}

#[test]
fn a_damaged_catalog_image_is_refused_and_so_is_a_snapshot_cut_short_inside_it() {
    let service = service_holding(&[manifest("a", 1, 1), manifest("b", 1, 2)]);
    let bytes = service.export_snapshot().to_bytes().unwrap();
    let at = bytes
        .windows(4)
        .position(|window| window == b"MOCC")
        .expect("the image is framed with its own magic");
    // the framing's digest sits behind the magic: a byte changed inside the image is caught there
    let mut damaged = bytes.clone();
    damaged[at + 12] ^= 0xFF;
    assert!(MetaSnapshot::from_bytes(&damaged).is_err());
    // cut inside the image
    assert!(MetaSnapshot::from_bytes(&bytes[..at + 20]).is_err());
}

#[test]
fn a_snapshot_with_a_catalog_and_no_lease_still_carries_the_lease_table_and_the_features() {
    // version 23 states everything version 22 does, so a decoder that reads the lease table at
    // 22 and later finds it where it expects it even when no writer holds a lease
    let service = service_holding(&[manifest("a", 1, 1)]);
    assert!(service.writer_leases.is_empty());
    let snapshot = service.export_snapshot();
    let decoded = MetaSnapshot::from_bytes(&snapshot.to_bytes().unwrap()).unwrap();
    assert!(decoded.writer_leases.is_empty());
    assert!(decoded.cluster_features.is_default());
    assert_eq!(decoded.chunk_catalog.len(), 1);
}

#[test]
fn catalog_snapshot_output_reservation_covers_the_encoded_image() {
    for count in [0, 1, 256, 4096] {
        let values: Vec<_> = (0..count).map(|index| manifest(&format!("key-{index:05}"), 1, 42)).collect();
        let snapshot = service_holding(&values).export_snapshot();
        let bytes = snapshot.to_bytes().unwrap();
        assert!(snapshot.encoded_size_hint() >= bytes.len(), "{count} objects");
        assert_eq!(MetaSnapshot::from_bytes(&bytes).unwrap(), snapshot);
    }
}
