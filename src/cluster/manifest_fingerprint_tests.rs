// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::object_manifest::ObjectExtent;

#[test]
fn buffered_fingerprint_matches_sha256_for_short_and_cross_buffer_writes() {
    let bytes: Vec<_> = (0..2049).map(|n| (n % 251) as u8).collect();
    for length in [0, 1, 511, 512, 513, 1024, 2049] {
        let mut input = DOMAIN.to_vec();
        input.extend_from_slice(&bytes[..length]);
        let expected = ring::digest::digest(&ring::digest::SHA256, &input);
        for step in [1, 7, 511, 512, 513, 4096] {
            let mut hasher = FingerprintHasher::new();
            for chunk in bytes[..length].chunks(step) {
                hasher.write(chunk);
            }
            assert_eq!(hasher.digest().as_slice(), expected.as_ref());
            assert_eq!(
                hasher.finish(),
                u64::from_le_bytes(expected.as_ref()[..8].try_into().unwrap())
            );
            assert_eq!(hasher.digest().as_slice(), expected.as_ref());
        }
    }
}

#[test]
fn manifest_fingerprints_preserve_string_and_vector_boundaries() {
    let first = ObjectManifest {
        bucket: "ab".into(),
        key: "c".into(),
        ..Default::default()
    };
    let second = ObjectManifest {
        bucket: "a".into(),
        key: "bc".into(),
        ..Default::default()
    };
    assert_ne!(of(&first), of(&second));
    assert_eq!(of(&first), of(&first.clone()));
    let first = ObjectManifest {
        extents: vec![ObjectExtent {
            data_sha256: vec![1, 2],
            footer_sha256: vec![3],
            ..Default::default()
        }],
        ..Default::default()
    };
    let second = ObjectManifest {
        extents: vec![ObjectExtent {
            data_sha256: vec![1],
            footer_sha256: vec![2, 3],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_ne!(of(&first), of(&second));
}


#[test]
fn verified_extension_matches_a_full_scan_at_buffer_and_checkpoint_boundaries() {
    let extents: Vec<_> = (0..4096).map(|n| ObjectExtent {
        group: format!("group-{}", n % 3),
        chunk_id: n + 1,
        length: 4096,
        object_offset: n * 4096,
        data_sha256: vec![n as u8; 32],
        ..Default::default()
    }).collect();
    for prefix in [0, 1, 63, 64, 65, 128, 4095] {
        let before = ObjectManifest {
            format_version: 2, bucket: "bucket".into(), key: "key".into(),
            generation: 1, length: prefix as u64 * 4096,
            extents: extents[..prefix].to_vec(), ..Default::default()
        };
        let verified = VerifiedManifest::new(&before);
        for suffix in [0, 1, (4096-prefix).min(65), 4096-prefix] {
            let after = ObjectManifest {
                generation: 2, length: (prefix+suffix) as u64 * 4096,
                extents: extents[..prefix+suffix].to_vec(), ..before.clone()
            };
            let extended = verified.extended(&after).unwrap();
            assert_eq!(extended.key(), of(&after), "prefix={prefix} suffix={suffix}");
            assert_eq!(extended.extended(&after).unwrap().key(), of(&after));
            assert_eq!(verified.key(), of(&before), "extension must not mutate its prefix state");
        }
    }
}

#[test]
fn verified_extension_refuses_changed_or_shortened_prefixes() {
    let before = ObjectManifest {
        extents: (0..5).map(|n| ObjectExtent {
            chunk_id: n, data_sha256: vec![n as u8; 32], ..Default::default()
        }).collect(), ..Default::default()
    };
    let verified = VerifiedManifest::new(&before);
    let mut changed = before.clone();
    changed.extents[2].data_sha256[0] ^= 1;
    assert!(verified.extended(&changed).is_none());
    changed = before.clone();
    changed.extents.swap(1, 2);
    assert!(verified.extended(&changed).is_none());
    changed = before.clone();
    changed.extents.pop();
    assert!(verified.extended(&changed).is_none());
}

#[test]
fn verified_extension_binds_all_result_header_fields() {
    let before = ObjectManifest { bucket: "bucket".into(), key: "key".into(), ..Default::default() };
    let verified = VerifiedManifest::new(&before);
    for field in 0..6 {
        let mut after = before.clone();
        match field {
            0 => after.format_version += 1,
            1 => after.bucket.push('x'),
            2 => after.key.push('x'),
            3 => after.generation += 1,
            4 => after.length += 1,
            _ => after.deleted = true,
        }
        let extended = verified.extended(&after).unwrap();
        assert_eq!(extended.key(), of(&after));
        assert_ne!(extended.key(), verified.key(), "header field {field}");
    }
}
