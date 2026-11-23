// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "published-log-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn file(&self) -> PathBuf {
        self.0.join("chunk")
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn frame(offset: u64, body: &[u8], published: Option<u64>, epoch: u64) -> Vec<u8> {
    let h = Header {
        kind: KIND_APPEND,
        flags: 0,
        generation: 7,
        epoch,
        offset,
        length: body.len() as u32,
        data_crc: crc32c(body),
        attempt_tag: Some([offset as u8; 16]),
        published_length: published,
    };
    let mut b = h.encode()[..h.encoded_len()].to_vec();
    b.extend_from_slice(body);
    b
}
fn control(kind: u8, offset: u64, crc: u32, epoch: u64) -> Vec<u8> {
    let h = Header {
        kind,
        flags: 0,
        generation: 7,
        epoch,
        offset,
        length: 0,
        data_crc: crc,
        attempt_tag: None,
        published_length: None,
    };
    h.encode()[..h.encoded_len()].to_vec()
}
fn fixture(frames: &[Vec<u8>]) -> (Dir, PathBuf) {
    let d = Dir::new();
    let p = d.file();
    std::fs::write(&p, frames.concat()).unwrap();
    (d, p)
}
fn open(p: &Path) -> LogChunk {
    LogChunk::open(p, 7, 1024 * 1024).unwrap()
}
fn assert_corrupt_unchanged(frames: &[Vec<u8>]) {
    let (_d, p) = fixture(frames);
    let before = std::fs::read(&p).unwrap();
    assert!(matches!(
        LogChunk::open(&p, 7, 1024 * 1024),
        Err(LogError::Corrupt(_))
    ));
    assert_eq!(std::fs::read(p).unwrap(), before);
}
#[test]
fn extended_header_layout_covers_tag_and_publication_with_crc() {
    let b = frame(0, b"abc", Some(3), 1);
    assert_eq!(PUBLISHED_LOG_HEADER_LEN, 64);
    assert_eq!(&b[..4], b"MOL3");
    assert_eq!(&b[36..52], &[0; 16]);
    assert_eq!(&b[52..60], &3u64.to_le_bytes());
    assert_eq!(
        u32::from_le_bytes(b[60..64].try_into().unwrap()),
        crc32c(&b[..60])
    );
    assert_eq!(b.len(), 67);
}
#[test]
fn older_records_keep_their_visibility_and_encoder() {
    let (_d, p) = fixture(&[frame(0, b"old", None, 1)]);
    let mut c = open(&p);
    assert_eq!(c.published_length(), 3);
    assert_eq!(c.durable_published_length().unwrap(), 3);
    c.append(1, 3, b"new").unwrap();
    assert_eq!(c.published_length(), 6);
    assert_eq!(c.durable_published_length().unwrap(), 3);
    c.commit().unwrap();
    assert_eq!(c.durable_published_length().unwrap(), 6);
    drop(c);
    let b = std::fs::read(&p).unwrap();
    assert_eq!(&b[IDENTIFIED_LOG_HEADER_LEN + 3..][..4], MAGIC);
}
#[test]
fn incomplete_records_replay_as_physical_bytes_without_publishing_them() {
    let (_d, p) = fixture(&[frame(0, b"old", None, 1), frame(3, b"part", Some(3), 1)]);
    for _ in 0..2 {
        let c = open(&p);
        assert_eq!(c.length(), 7);
        assert_eq!(c.published_length(), 3);
        assert_eq!(c.durable_length(), 7);
        assert_eq!(c.durable_published_length().unwrap(), 3);
        assert_eq!(
            c.append_record_identity(3).unwrap().unwrap().attempt_tag,
            Some([3; 16])
        );
        assert_eq!(c.read_vec(0, 7).unwrap(), b"oldpart");
    }
}
#[test]
fn final_record_publishes_all_prior_fragments_at_once() {
    let (_d, p) = fixture(&[
        frame(0, b"old", None, 1),
        frame(3, b"part", Some(3), 1),
        frame(7, b"done", Some(11), 1),
    ]);
    let c = open(&p);
    assert_eq!((c.length(), c.published_length()), (11, 11));
    assert_eq!(c.durable_published_length().unwrap(), 11);
    assert_eq!(c.append_record_identity(7).unwrap().unwrap().length, 4);
}
#[test]
fn every_torn_final_header_or_payload_preserves_the_prior_published_end() {
    let prefix = [frame(0, b"old", None, 1), frame(3, b"part", Some(3), 1)].concat();
    let final_record = frame(7, b"done", Some(11), 1);
    for cut in 0..final_record.len() {
        let mut b = prefix.clone();
        b.extend_from_slice(&final_record[..cut]);
        let (_d, p) = fixture(&[b]);
        let c = open(&p);
        assert_eq!(c.length(), 7, "cut={cut}");
        assert_eq!(c.published_length(), 3, "cut={cut}");
        assert_eq!(std::fs::metadata(&p).unwrap().len(), prefix.len() as u64);
        drop(c);
        assert_eq!(open(&p).durable_published_length().unwrap(), 3);
    }
}
#[test]
fn publication_field_damage_is_corruption_and_never_repaired_as_a_torn_tail() {
    for at in 52..64 {
        let mut b = frame(0, b"abc", Some(3), 1);
        b[at] ^= 1;
        assert_corrupt_unchanged(&[b]);
    }
}
#[test]
fn recomputed_crc_cannot_publish_a_nonboundary_or_move_backwards() {
    for invalid in [0, 2, 4, 5, 7, u64::MAX] {
        assert_corrupt_unchanged(&[
            frame(0, b"old", None, 1),
            frame(3, b"new", Some(invalid), 1),
        ]);
    }
}
#[test]
fn unfinished_group_cannot_be_adopted_by_a_new_epoch_without_truncation() {
    assert_corrupt_unchanged(&[
        frame(0, b"old", None, 1),
        frame(3, b"part", Some(3), 1),
        frame(7, b"done", Some(11), 2),
    ]);
    let (_d, p) = fixture(&[frame(0, b"old", Some(3), 1), frame(3, b"new", Some(6), 2)]);
    assert_eq!(open(&p).published_length(), 6);
}
#[test]
fn legacy_append_cannot_implicitly_publish_an_extended_suffix() {
    for boundary in [0, 3] {
        assert_corrupt_unchanged(&[
            frame(0, b"abc", Some(boundary), 1),
            frame(3, b"legacy", None, 1),
        ]);
    }
}
#[test]
fn current_mutation_apis_refuse_publication_aware_files_without_writing() {
    let (_d, p) = fixture(&[frame(0, b"abc", Some(3), 1)]);
    let before = std::fs::read(&p).unwrap();
    let mut c = open(&p);
    assert!(matches!(c.append(2, 3, b"x"), Err(LogError::Bounds(_))));
    assert!(matches!(
        c.append_identified(2, 3, b"x", [9; 16]),
        Err(LogError::Bounds(_))
    ));
    assert!(matches!(c.truncate(2, 0), Err(LogError::Bounds(_))));
    assert!(matches!(c.seal(2, false), Err(LogError::Bounds(_))));
    assert_eq!(std::fs::read(&p).unwrap(), before);
    assert_eq!((c.length(), c.published_length(), c.epoch()), (3, 3, 1));
}
#[test]
fn seal_cannot_make_an_incomplete_group_visible() {
    assert_corrupt_unchanged(&[
        frame(0, b"part", Some(0), 1),
        control(KIND_SEAL, 4, crc32c(b"part"), 1),
    ]);
}
#[test]
fn truncate_to_a_published_boundary_discards_incomplete_suffix_and_keeps_mode() {
    let (_d, p) = fixture(&[
        frame(0, b"old", Some(3), 1),
        frame(3, b"part", Some(3), 1),
        control(KIND_TRUNCATE, 3, 0, 2),
        frame(3, b"new", Some(6), 2),
    ]);
    let mut c = open(&p);
    assert_eq!(c.read_vec(0, 6).unwrap(), b"oldnew");
    assert_eq!(c.durable_published_length().unwrap(), 6);
    assert!(c.append(2, 6, b"x").is_err());
}
#[test]
fn truncate_cannot_cut_inside_a_complete_object_group() {
    for offset in [1, 2, 3, 4, 5] {
        assert_corrupt_unchanged(&[
            frame(0, b"abc", Some(0), 1),
            frame(3, b"def", Some(6), 1),
            control(KIND_TRUNCATE, offset, 0, 2),
        ]);
    }
}
#[test]
fn durable_visibility_uses_complete_records_and_never_a_physical_fragment_prefix() {
    let (_d, p) = fixture(&[
        frame(0, b"old", None, 1),
        frame(3, b"abc", Some(3), 1),
        frame(6, b"def", Some(9), 1),
        frame(9, b"xx", Some(9), 1),
    ]);
    let mut c = open(&p);
    for (durable, want) in [(0, 0), (3, 3), (6, 3), (9, 9), (11, 9)] {
        c.durable_length = durable;
        assert_eq!(
            c.durable_published_length().unwrap(),
            want,
            "durable={durable}"
        );
    }
}
#[test]
fn indexed_header_damage_fails_durable_publication_and_identity_queries() {
    let (_d, p) = fixture(&[frame(0, b"abc", Some(3), 1), frame(3, b"def", Some(6), 1)]);
    let mut c = open(&p);
    c.durable_length = 3;
    let mut b = std::fs::read(&p).unwrap();
    b[52] ^= 1;
    std::fs::write(&p, b).unwrap();
    assert!(matches!(
        c.durable_published_length(),
        Err(LogError::Corrupt(_))
    ));
    assert!(matches!(
        c.append_record_identity(0),
        Err(LogError::Corrupt(_))
    ));
}
#[test]
fn complete_sealed_groups_read_from_file_with_the_same_publication_boundary() {
    let (_d, p) = fixture(&[
        frame(0, b"abc", Some(0), 1),
        frame(3, b"def", Some(6), 1),
        control(KIND_SEAL, 6, crc32c(b"abcdef"), 1),
    ]);
    let c = open(&p);
    assert!(!c.is_resident());
    assert_eq!(c.resident_bytes(), 0);
    assert_eq!(c.durable_published_length().unwrap(), 6);
    assert_eq!(c.read_vec(0, 6).unwrap(), b"abcdef");
}
#[test]
fn corrupted_version_marker_cannot_erase_a_complete_older_header() {
    let mut identified = frame(0, b"abc", None, 1);
    identified[..4].copy_from_slice(PUBLISHED_MAGIC);
    assert_corrupt_unchanged(&[identified]);
    let h = Header {
        kind: KIND_APPEND,
        flags: 0,
        generation: 7,
        epoch: 1,
        offset: 0,
        length: 3,
        data_crc: crc32c(b"abc"),
        attempt_tag: None,
        published_length: None,
    };
    let mut legacy = h.encode()[..h.encoded_len()].to_vec();
    legacy.extend_from_slice(b"abc");
    legacy[..4].copy_from_slice(PUBLISHED_MAGIC);
    assert_corrupt_unchanged(&[legacy]);
}
#[test]
fn extended_records_refuse_reserved_flags_and_control_kinds_with_valid_crcs() {
    for (at, value) in [(5, 1), (4, KIND_SEAL), (4, KIND_TRUNCATE), (6, 1)] {
        let mut b = frame(0, b"abc", Some(3), 1);
        b[at] = value;
        let crc = crc32c(&b[..60]);
        b[60..64].copy_from_slice(&crc.to_le_bytes());
        assert_corrupt_unchanged(&[b]);
    }
}
#[test]
fn publication_readers_keep_the_existing_extent_memory_bound() {
    assert!(std::mem::size_of::<Extent>() <= 32);
    let (_d, p) = fixture(&[frame(0, b"a", Some(0), 1), frame(1, b"b", Some(2), 1)]);
    let c = open(&p);
    assert_eq!(c.extent_count(), 2);
    assert_eq!(c.published_length(), 2);
}

#[test]
fn recomputed_indexed_publication_must_still_name_a_covered_complete_boundary() {
    for forged in [2u64, 5, 7, 9, u64::MAX] {
        let (_d, p) = fixture(&[
            frame(0, b"abc", Some(3), 1),
            frame(3, b"def", Some(6), 1),
            frame(6, b"ghi", Some(9), 1),
        ]);
        let mut c = open(&p);
        c.durable_length = 6;
        let mut b = std::fs::read(&p).unwrap();
        let at = PUBLISHED_LOG_HEADER_LEN + 3;
        b[at + 52..at + 60].copy_from_slice(&forged.to_le_bytes());
        let checksum = crc32c(&b[at..at + 60]);
        b[at + 60..at + 64].copy_from_slice(&checksum.to_le_bytes());
        std::fs::write(&p, b).unwrap();
        assert!(
            matches!(c.durable_published_length(), Err(LogError::Corrupt(_))),
            "forged={forged}"
        );
    }
}
#[test]
fn portable_publication_record_vector_replays_without_using_the_encoder() {
    let bytes: [u8; 67] = [
        77, 79, 76, 51, 1, 0, 0, 0, 7, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
        0, 0, 0, 183, 63, 75, 54, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 3, 0, 0, 0, 0, 0,
        0, 0, 246, 122, 252, 246, 97, 98, 99,
    ];
    let h = Header::decode(&bytes[..64]).unwrap();
    assert_eq!(h.published_length, Some(3));
    assert_eq!(h.attempt_tag, Some([7; 16]));
    let (_d, p) = fixture(&[bytes.to_vec()]);
    let c = open(&p);
    assert_eq!(c.read_vec(0, 3).unwrap(), b"abc");
    assert_eq!(c.durable_published_length().unwrap(), 3);
}
