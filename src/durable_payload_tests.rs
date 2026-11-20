// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::{
    sync::{mpsc, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "payload-extents-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("payload.data")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn flip(path: &Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn durable_payload_reused_reads_append_exact_unaligned_ranges_and_validate_arguments() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let data: Vec<_> = (0..10003).map(|n| (n % 251) as u8).collect();
    let extent = store.append(&data).unwrap();
    let prefix = b"retained-prefix";
    let mut output = Vec::with_capacity(data.len() + 2 * SECTOR_BYTES + prefix.len());
    output.extend_from_slice(prefix);
    let buffer = output.as_ptr();
    for (offset, length) in [
        (0, 0),
        (0, data.len()),
        (1, 1),
        (1, 8193),
        (4095, 4097),
        (4096, 4096),
        (data.len() - 1, 1),
        (data.len(), 0),
    ] {
        output.truncate(prefix.len());
        store
            .read_range_append(&extent, offset as u64, length as u64, &mut output)
            .unwrap();
        assert_eq!(&output[..prefix.len()], prefix);
        assert_eq!(&output[prefix.len()..], &data[offset..offset + length]);
        assert_eq!(output.as_ptr(), buffer);
    }
    output.truncate(prefix.len());
    for (offset, length) in [(extent.length + 1, 0), (extent.length, 1), (u64::MAX, 1)] {
        assert!(store
            .read_range_append(&extent, offset, length, &mut output)
            .is_err());
        assert_eq!(output, prefix);
    }
    let mut foreign = extent.clone();
    foreign.store_id[0] ^= 1;
    assert!(store
        .read_range_append(&foreign, 0, 0, &mut output)
        .is_err());
    assert_eq!(output, prefix);
}

#[test]
fn durable_payload_reused_reads_restore_prefix_after_sector_or_padding_damage() {
    for damaged in [0, 4096, 3 * SECTOR_BYTES as u64 - 1] {
        let dir = Directory::new();
        let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
        let extent = store.append(&vec![0x31; 9000]).unwrap();
        flip(&dir.path(), extent.offset + damaged);
        let mut output = b"original".to_vec();
        let error = store
            .read_range_append(&extent, 0, extent.length, &mut output)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(output, b"original");
    }
}

#[test]
fn durable_payload_reused_full_reads_verify_the_logical_digest() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let data = vec![0x52; 9000];
    let mut extent = store.append(&data).unwrap();
    extent.sha256[0] ^= 1;
    let mut output = b"original".to_vec();
    let error = store
        .read_range_append(&extent, 0, extent.length, &mut output)
        .unwrap_err();
    assert_eq!(error.to_string(), "payload checksum mismatch");
    assert_eq!(output, b"original");
    store.read_range_append(&extent, 1, 7, &mut output).unwrap();
    assert_eq!(&output[b"original".len()..], &data[1..8]);
}

#[test]
fn durable_payload_reused_reads_restore_prefix_after_a_short_file_read() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let extent = store.append(&vec![0x31; 9000]).unwrap();
    OpenOptions::new()
        .write(true)
        .open(dir.path())
        .unwrap()
        .set_len(extent.offset + 4096)
        .unwrap();
    let mut output = b"original".to_vec();
    let error = store
        .read_range_append(&extent, 0, extent.length, &mut output)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(output, b"original");
}
#[test]
fn durable_payload_batch_syncs_once_and_binary_descriptors_recover() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let values = [vec![7; 4096], vec![19; 4097], vec![31; MAX_EXTENT_BYTES]];
    let inputs = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let mut syncs = 0;
    let extents = store
        .append_with_sync(&inputs, |file| {
            syncs += 1;
            assert_eq!(
                file.metadata()?.len(),
                4096 + 4096 + 8192 + MAX_EXTENT_BYTES as u64
            );
            file.sync_data()
        })
        .unwrap();
    assert_eq!(syncs, 1);
    assert_eq!(extents.len(), 3);
    let bytes = std::fs::read(dir.path()).unwrap();
    for (extent, value) in extents.iter().zip(&values) {
        assert_eq!(extent.offset % 4096, 0);
        assert_eq!(store.read(extent).unwrap(), *value);
        assert_eq!(
            &bytes[extent.offset as usize..extent.offset as usize + value.len()],
            value
        );
        assert_eq!(
            PayloadExtent::decode(extent.encode_to_vec().as_slice()).unwrap(),
            *extent
        );
    }
    drop(store);
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    for (extent, value) in extents.iter().zip(&values) {
        assert_eq!(store.read(extent).unwrap(), *value);
    }
    let tail = store.append(b"next").unwrap();
    assert_eq!(store.read(&tail).unwrap(), b"next");
    assert_eq!(&std::fs::read(dir.path()).unwrap()[..bytes.len()], bytes);
}
#[test]
fn durable_payload_range_reads_verify_touched_sectors_and_padding() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let value = (0..MAX_EXTENT_BYTES - 7)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    let extent = store.append(&value).unwrap();
    for (offset, length) in [
        (0, 1),
        (4095, 2),
        (4096, 4096),
        (MAX_EXTENT_BYTES - 20, 13),
        (value.len(), 0),
    ] {
        assert_eq!(
            store
                .read_range(&extent, offset as u64, length as u64)
                .unwrap(),
            value[offset..offset + length]
        );
    }
    flip(&dir.path(), extent.offset + 800_000);
    assert_eq!(store.read_range(&extent, 0, 4096).unwrap(), value[..4096]);
    assert!(store.read(&extent).is_err());
    assert!(store.read_range(&extent, 799_000, 4096).is_err());
    flip(&dir.path(), extent.offset + 800_000);
    flip(&dir.path(), extent.offset + extent.length + 2);
    assert!(store.read_range(&extent, extent.length - 1, 1).is_err());
}
#[test]
fn durable_payload_refusals_do_not_write_or_fence() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), 8192).unwrap();
    let extent = store.append(b"kept").unwrap();
    let before = std::fs::read(dir.path()).unwrap();
    let large = vec![0; MAX_EXTENT_BYTES + 1];
    for inputs in [
        vec![&[][..]],
        vec![large.as_slice()],
        vec![&b"x"[..]; 65],
        vec![&b"x"[..]; 2],
    ] {
        assert!(store.append_batch(&inputs).is_err());
    }
    let big = vec![0; MAX_EXTENT_BYTES];
    assert!(store.append_batch(&vec![big.as_slice(); 9]).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    assert_eq!(store.read(&extent).unwrap(), b"kept");
    assert!(store.append_batch(&[]).unwrap().is_empty());
    assert!(store.append(b"last").is_ok());
    assert_eq!(store.remaining_bytes(), 0);
}
#[test]
fn durable_payload_rejects_wrong_identity_versions_ranges_and_digests() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let extent = store.append(b"payload").unwrap();
    let other = Directory::new();
    let other = PayloadExtentStore::open(other.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    assert!(other.read(&extent).is_err());
    let mut bad = extent.clone();
    bad.version = 2;
    assert!(store.read(&bad).is_err());
    bad = extent.clone();
    bad.offset = u64::MAX - 4095;
    assert!(store.read(&bad).is_err());
    bad = extent.clone();
    bad.length = u64::MAX;
    assert!(store.read(&bad).is_err());
    bad = extent.clone();
    bad.offset += 1;
    assert!(store.read(&bad).is_err());
    bad = extent.clone();
    bad.sector_sha256.pop();
    assert!(store.read(&bad).is_err());
    bad = extent.clone();
    bad.sha256[0] ^= 1;
    assert!(store.read(&bad).is_err());
    assert!(store.read_range(&extent, u64::MAX, 1).is_err());
    assert!(store.read_range(&extent, 0, 8).is_err());
}
#[test]
fn durable_payload_sync_and_write_failures_return_no_descriptors_and_fence() {
    let dir = Directory::new();
    let mut store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let extent = store.append(b"published").unwrap();
    let before = store.length();
    assert!(store
        .append_with_sync(&[b"unpublished"], |_| Err(io::Error::other(
            "injected sync failure"
        )))
        .is_err());
    assert_eq!(store.length(), before);
    assert!(store.append(b"refused").is_err());
    assert!(store.read(&extent).is_err());
    drop(store);
    store =
        PayloadExtentStore::open_recovered(dir.path(), DEFAULT_CAPACITY_BYTES, &[extent.clone()])
            .unwrap();
    assert_eq!(store.length(), before);
    assert_eq!(store.read(&extent).unwrap(), b"published");
    store.file = OpenOptions::new().read(true).open(dir.path()).unwrap();
    let bytes = std::fs::read(dir.path()).unwrap();
    assert!(store.append(b"write fails").is_err());
    assert!(store.read(&extent).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), bytes);
}
#[test]
fn durable_payload_verified_read_does_not_wait_for_writer_sync() {
    let dir = Directory::new();
    let store = Arc::new(PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap());
    let old = store.append(b"readable").unwrap();
    let before = store.length();
    let (ready, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let writer = Arc::clone(&store);
    let worker = std::thread::spawn(move || {
        writer.append_with_sync(&[b"new"], |file| {
            ready.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(10)).unwrap();
            file.sync_data()
        })
    });
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let reader = Arc::clone(&store);
    let (done, result) = mpsc::channel();
    let old_read = old.clone();
    let thread = std::thread::spawn(move || done.send(reader.read(&old_read)).unwrap());
    let read = result.recv_timeout(Duration::from_secs(3));
    assert_eq!(store.length(), before);
    release.send(()).unwrap();
    let new = worker.join().unwrap().unwrap().pop().unwrap();
    thread.join().unwrap();
    assert_eq!(read.unwrap().unwrap(), b"readable");
    assert_eq!(store.read(&new).unwrap(), b"new");
}
#[test]
fn durable_payload_recovery_validates_all_roots_before_truncation() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let a = store.append(b"one").unwrap();
    let b = store.append(b"two").unwrap();
    store.append(b"orphan").unwrap();
    drop(store);
    let before = std::fs::read(dir.path()).unwrap();
    let mut bad = b.clone();
    bad.sha256[0] ^= 1;
    assert!(PayloadExtentStore::open_recovered(
        dir.path(),
        DEFAULT_CAPACITY_BYTES,
        &[a.clone(), bad]
    )
    .is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    let store = PayloadExtentStore::open_recovered(
        dir.path(),
        DEFAULT_CAPACITY_BYTES,
        &[b.clone(), a.clone()],
    )
    .unwrap();
    assert_eq!(store.length(), b.offset + 4096);
    assert_eq!(store.read(&a).unwrap(), b"one");
    assert_eq!(store.read(&b).unwrap(), b"two");
}
#[test]
fn durable_payload_identity_lock_and_initialized_file_loss_are_refused() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    assert!(PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).is_err());
    drop(store);
    let before = std::fs::read(dir.path()).unwrap();
    assert!(PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES * 2).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    std::fs::remove_file(dir.path()).unwrap();
    assert!(PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).is_err());
    assert!(!dir.path().exists());
    let other = Directory::new();
    std::fs::write(other.path(), b"unknown existing file").unwrap();
    assert!(PayloadExtentStore::open(other.path(), DEFAULT_CAPACITY_BYTES).is_err());
    assert_eq!(
        std::fs::read(other.path()).unwrap(),
        b"unknown existing file"
    );
}
#[test]
fn durable_payload_initialization_cuts_recover_only_unpublished_headers() {
    for cut in [0, 12, 55, 56] {
        let dir = Directory::new();
        let mut lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(sibling(&dir.path()))
            .unwrap();
        let (id, ready) = marker(&mut lock, &dir.path()).unwrap();
        assert!(!ready);
        if cut < 56 {
            lock.set_len(cut).unwrap();
            lock.sync_all().unwrap();
        } else {
            std::fs::write(
                dir.path(),
                &encode_header(&id, DEFAULT_CAPACITY_BYTES)[..127],
            )
            .unwrap();
        }
        drop(lock);
        let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
        let extent = store.append(b"after initialization").unwrap();
        assert_eq!(store.read(&extent).unwrap(), b"after initialization");
    }
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    drop(store);
    flip(&dir.path(), 12);
    let before = std::fs::read(dir.path()).unwrap();
    assert!(PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
}

#[test]
fn durable_payload_poisoned_writer_refuses_reads_and_writes() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let extent = store.append(b"kept").unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.append_with_sync(&[b"uncertain"], |_| panic!("injected writer panic"))
    }));
    assert!(result.is_err());
    assert!(store.read(&extent).is_err());
    assert!(store.append(b"refused").is_err());
    drop(store);
    let store =
        PayloadExtentStore::open_recovered(dir.path(), DEFAULT_CAPACITY_BYTES, &[extent.clone()])
            .unwrap();
    assert_eq!(store.read(&extent).unwrap(), b"kept");
}

#[test]
fn durable_payload_recovery_shares_identical_roots_and_refuses_overlaps() {
    let dir = Directory::new();
    let store = PayloadExtentStore::open(dir.path(), DEFAULT_CAPACITY_BYTES).unwrap();
    let extent = store.append(&vec![3; 8192]).unwrap();
    store.append(b"orphan").unwrap();
    drop(store);
    let before = std::fs::read(dir.path()).unwrap();
    let mut overlap = extent.clone();
    overlap.offset += 4096;
    overlap.length = 4096;
    overlap.sector_sha256 = extent.sector_sha256[32..].to_vec();
    overlap.sha256 = overlap.sector_sha256.clone();
    assert!(PayloadExtentStore::open_recovered(
        dir.path(),
        DEFAULT_CAPACITY_BYTES,
        &[extent.clone(), overlap]
    )
    .is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    let store = PayloadExtentStore::open_recovered(
        dir.path(),
        DEFAULT_CAPACITY_BYTES,
        &[extent.clone(), extent.clone()],
    )
    .unwrap();
    assert_eq!(store.length(), extent.offset + 8192);
    assert_eq!(store.read(&extent).unwrap(), vec![3; 8192]);
}
#[test]
fn durable_payload_binary_header_and_descriptor_bytes_are_stable() {
    fn unhex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }
    let header = encode_header(&[7; 16], DEFAULT_CAPACITY_BYTES);
    assert_eq!(
        digest::digest(&digest::SHA256, &header).as_ref(),
        unhex("2144e06f1b48f53c7eb1a8c5721f404ca90ee933979e94672ae5b4943e31f247")
    );
    let extent = PayloadExtent {
        version: 1,
        store_id: vec![7; 16],
        offset: 4096,
        length: 3,
        sha256: vec![0xaa; 32],
        sector_sha256: vec![0xbb; 32],
    };
    assert_eq!(extent.encode_to_vec(),unhex("080112100707070707070707070707070707070718802020032a20aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3220bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
}
