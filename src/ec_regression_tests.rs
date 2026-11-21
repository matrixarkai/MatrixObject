// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// The erasure-coding sidecar and the digest helpers are the optimised forms of simple loops. These tests
// keep the simple loops beside them and compare the two at every length where the layout changes shape,
// so an optimisation cannot change a byte. They are pinned by name in the workflow.
use super::*;

fn original_ec_sidecar_xor(data: &[u8]) -> Vec<u8> {
    let shards = chunk_ec_fragment_count(data.len());
    let shard_len = chunk_ec_fragment_len(data.len());
    let mut sidecar = vec![0u8; shard_len];
    for index in 0..shards {
        let base = index * shard_len;
        for offset in 0..shard_len {
            if let Some(byte) = data.get(base + offset) {
                sidecar[offset] ^= byte;
            }
        }
    }
    sidecar
}

fn original_ec_fragment_digests(data: &[u8]) -> Vec<u64> {
    let shards = chunk_ec_fragment_count(data.len());
    let shard_len = chunk_ec_fragment_len(data.len());
    (0..shards)
        .map(|index| {
            let start = (index * shard_len).min(data.len());
            let end = ((index + 1) * shard_len).min(data.len());
            let mut shard = data[start..end].to_vec();
            shard.resize(shard_len, 0);
            fnv1a64(&shard)
        })
        .collect()
}

/// Deterministic bytes that are not periodic in the shard length, so a shard-for-shard mixup shows.
fn ec_test_payload(len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ (len as u64).wrapping_mul(0x2545_f491_4f6c_dd1d);
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// Every length where the layout changes shape: all the small ones, each side of every point at
/// which the fragment count steps, past the cap, and a page's worth either side.
fn ec_test_lengths() -> Vec<usize> {
    let mut lengths: Vec<usize> = (0..=300).collect();
    for k in 1..=(CHUNK_EC_MAX_DATA_FRAGMENTS + 2) {
        let edge = CHUNK_EC_MIN_FRAGMENT_BYTES * k;
        lengths.extend([edge - 1, edge, edge + 1]);
    }
    lengths.extend([
        CHUNK_EC_MIN_FRAGMENT_BYTES * CHUNK_EC_MAX_DATA_FRAGMENTS * 2 + 7,
        1_048_575,
        1_048_576,
        1_048_577,
    ]);
    lengths.sort_unstable();
    lengths.dedup();
    lengths
}

#[test]
fn the_ec_sidecar_xor_matches_the_original_byte_loop_at_every_length_that_matters() {
    for len in ec_test_lengths() {
        let data = ec_test_payload(len);
        assert_eq!(
            chunk_ec_parity_from_data(&data),
            original_ec_sidecar_xor(&data),
            "sidecar differs at length {len}"
        );
    }
    // The shapes a random payload cannot reach: all zero, all ones, one set byte at each end.
    for len in [0usize, 1, 2, 7, 4096, 262_144, 300_000] {
        let mut shapes = vec![vec![0u8; len], vec![0xffu8; len]];
        if len > 0 {
            let mut first = vec![0u8; len];
            first[0] = 0x5a;
            let mut last = vec![0u8; len];
            last[len - 1] = 0xa5;
            shapes.extend([first, last]);
        }
        for data in shapes {
            assert_eq!(chunk_ec_parity_from_data(&data), original_ec_sidecar_xor(&data), "shape at {len}");
        }
    }
}

#[test]
fn the_ec_fragment_digests_match_the_original_allocating_form_at_every_length_that_matters() {
    for len in ec_test_lengths() {
        let data = ec_test_payload(len);
        assert_eq!(
            chunk_ec_fragment_digests_from_data(&data),
            original_ec_fragment_digests(&data),
            "digests differ at length {len}"
        );
    }
    for len in [0usize, 1, 2, 7, 4096, 262_144, 300_000] {
        for data in [vec![0u8; len], vec![0xffu8; len]] {
            assert_eq!(
                chunk_ec_fragment_digests_from_data(&data),
                original_ec_fragment_digests(&data),
                "shape at {len}"
            );
        }
    }
}

#[test]
fn a_run_of_zero_bytes_folds_into_fnv1a64_exactly_as_hashing_them_does() {
    for start in [0u64, 1, 0xcbf2_9ce4_8422_2325, u64::MAX, 0x1234_5678_9abc_def0] {
        for count in [0usize, 1, 2, 7, 8, 63, 64, 4095, 4096, 4097, 131_071, 131_072, 1_000_003] {
            assert_eq!(
                fnv1a64_zero_run(start, count),
                fnv1a64_resumed(start, &vec![0u8; count]),
                "start {start:#x}, {count} zero bytes"
            );
        }
    }
}
