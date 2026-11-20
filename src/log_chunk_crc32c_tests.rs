// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::{crc32c, crc32c_update, crc32c_update_portable, crc32c_update_stored};

fn input(length: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9u32;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}

#[test]
fn crc32c_known_vector_and_empty_updates_preserve_the_contract() {
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(crc32c(b""), 0);
    for seed in [0, 1, 0x1234_5678, u32::MAX] {
        assert_eq!(crc32c_update(seed, b""), seed);
    }
}

#[test]
fn crc32c_stored_updates_match_portable_for_offsets_and_incremental_state() {
    let data = input(65536 + 16);
    for offset in 0..16 {
        for length in (0..=160).chain([4095, 4096, 4097, 65535, 65536]) {
            let bytes = &data[offset..offset + length];
            for seed in [0, 0x1234_5678, u32::MAX] {
                assert_eq!(
                    crc32c_update_stored(seed, bytes),
                    crc32c_update_portable(seed, bytes),
                    "offset={offset} length={length} seed={seed}"
                );
            }
        }
    }
    for block in [1, 3, 8, 17, 4096] {
        let mut state = 0;
        for bytes in data.chunks(block) {
            state = crc32c_update_stored(state, bytes);
        }
        assert_eq!(state, crc32c_update_portable(0, &data));
    }
}

#[test]
#[ignore = "explicit local checksum throughput measurement"]
fn measure_crc32c_update_backends() {
    use std::hint::black_box;
    use std::time::Instant;
    #[cfg(target_arch = "x86_64")]
    let accelerated = std::arch::is_x86_feature_detected!("sse4.2");
    #[cfg(not(target_arch = "x86_64"))]
    let accelerated = false;
    let data = input(4 * 1024 * 1024);
    let _ = crc32c(&data);
    for (length, iterations) in [(4096, 16384), (4 * 1024 * 1024, 64)] {
        for round in 0..3 {
            let mut checksums = Vec::new();
            let order = if round % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            };
            for automatic in order {
                let began = Instant::now();
                let mut state = 0;
                for _ in 0..iterations {
                    let bytes = black_box(&data[..length]);
                    state = if automatic {
                        crc32c_update_stored(state, bytes)
                    } else {
                        crc32c_update_portable(state, bytes)
                    };
                }
                let elapsed = began.elapsed();
                black_box(state);
                checksums.push(state);
                println!("LOG_CRC32C_BENCH round={round} backend={} chunk_bytes={length} bytes={} elapsed_ns={} checksum={state} accelerated_available={accelerated}",
                    if automatic { "automatic" } else { "portable" },
                    length * iterations, elapsed.as_nanos());
            }
            assert_eq!(checksums[0], checksums[1]);
        }
    }
}
