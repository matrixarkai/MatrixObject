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

#[test]
fn append_checksums_preserve_existing_journal_bytes_and_sealed_recovery() {
    use super::{Header, LogChunk, KIND_APPEND, KIND_INITIALIZE, KIND_SEAL};
    let root = std::env::temp_dir().join(format!("append-crc-bytes-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let data = input(65536 + 16);
    for mode in 0..3 {
        let path = root.join(format!("mode-{mode}.log"));
        let mut chunk = LogChunk::open(&path, 7, 1 << 20).unwrap();
        let mut expected = Vec::new();
        let mut all = Vec::new();
        let add_header = |out: &mut Vec<u8>, header: Header| {
            out.extend_from_slice(&header.encode()[..header.encoded_len()]);
        };
        if mode == 2 {
            chunk.initialize_publication(11, [9; 16]).unwrap();
            add_header(
                &mut expected,
                Header {
                    kind: KIND_INITIALIZE,
                    flags: 0,
                    generation: 7,
                    epoch: 11,
                    offset: 0,
                    length: 0,
                    data_crc: 0,
                    attempt_tag: Some([9; 16]),
                    published_length: Some(0),
                },
            );
        }
        for (i, length) in [1usize, 7, 8, 9, 4095, 4096, 4097, 65535, 65536]
            .into_iter()
            .enumerate()
        {
            let bytes = &data[i..i + length];
            let offset = all.len() as u64;
            let end = offset + length as u64;
            let tag = [i as u8 + 1; 16];
            match mode {
                0 => {
                    chunk.append(11, offset, bytes).unwrap();
                }
                1 => {
                    chunk.append_identified(11, offset, bytes, tag).unwrap();
                }
                _ => {
                    chunk.append_published(11, offset, bytes, tag, end).unwrap();
                }
            }
            add_header(
                &mut expected,
                Header {
                    kind: KIND_APPEND,
                    flags: 0,
                    generation: 7,
                    epoch: 11,
                    offset,
                    length: length as u32,
                    data_crc: crc32c_update_portable(0, bytes),
                    attempt_tag: (mode != 0).then_some(tag),
                    published_length: (mode == 2).then_some(end),
                },
            );
            expected.extend_from_slice(bytes);
            all.extend_from_slice(bytes);
            assert_eq!(chunk.footer_crc(), crc32c_update_portable(0, &all));
        }
        if mode == 2 {
            chunk.seal_publication(11, false).unwrap();
        } else {
            chunk.seal(11, false).unwrap();
        }
        add_header(
            &mut expected,
            Header {
                kind: KIND_SEAL,
                flags: 0,
                generation: 7,
                epoch: 11,
                offset: all.len() as u64,
                length: 0,
                data_crc: crc32c_update_portable(0, &all),
                attempt_tag: None,
                published_length: None,
            },
        );
        drop(chunk);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            expected,
            "mode{mode} changed persisted bytes"
        );
        let recovered = LogChunk::open(&path, 7, 1 << 20).unwrap();
        assert_eq!(recovered.durable_length(), all.len() as u64);
        assert_eq!(recovered.footer_crc(), crc32c_update_portable(0, &all));
        assert!(recovered.sealed().is_some());
    }
}
