// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(unix, any(feature = "replica-server", feature = "meta-server")))]
use matrixobjectstore_rs::durable_payload::{
    PayloadExtent, PayloadExtentStore, DEFAULT_CAPACITY_BYTES,
};
use prost::Message;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "payload-crash-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn payload_process_child() {
    let Some(directory) = std::env::var_os("MATRIXOBJECT_PAYLOAD_CRASH_DIRECTORY") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let path = directory.join("payload.data");
    let store = PayloadExtentStore::open(&path, DEFAULT_CAPACITY_BYTES).unwrap();
    let extent = store.append(&vec![0x5a; 8193]).unwrap();
    let mut root = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join("published.pb"))
        .unwrap();
    root.write_all(&extent.encode_to_vec()).unwrap();
    root.sync_all().unwrap();
    File::open(&directory).unwrap().sync_all().unwrap();
    match std::env::var("MATRIXOBJECT_PAYLOAD_CRASH_MODE")
        .unwrap()
        .as_str()
    {
        "durable" => {}
        "partial" => {
            let mut data = OpenOptions::new().append(true).open(&path).unwrap();
            data.write_all(&[0x71; 123]).unwrap();
            data.sync_all().unwrap();
        }
        "unpublished" => {
            store.append(b"durable but not published").unwrap();
        }
        _ => panic!("unknown crash mode"),
    }
    println!("PAYLOAD_PROCESS_READY");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
fn crash(mode: &str) -> (Directory, PayloadExtent) {
    let directory = Directory::new();
    let mut process = Process(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "payload_process_child",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MATRIXOBJECT_PAYLOAD_CRASH_DIRECTORY", &directory.0)
            .env("MATRIXOBJECT_PAYLOAD_CRASH_MODE", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = process.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) if line.contains("PAYLOAD_PROCESS_READY") => {
                    send.send(true).unwrap();
                    return;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let _ = send.send(false);
    });
    let ready = receive.recv_timeout(Duration::from_secs(20));
    let _ = process.0.kill();
    let status = process.0.wait().unwrap();
    reader.join().unwrap();
    if !matches!(ready, Ok(true)) {
        let mut errors = String::new();
        process
            .0
            .stderr
            .take()
            .unwrap()
            .take(16384)
            .read_to_string(&mut errors)
            .unwrap();
        panic!("payload child did not reach its durable point: {ready:?}; {errors}");
    }
    assert!(
        !status.success(),
        "child exited cleanly instead of being killed"
    );
    let extent = PayloadExtent::decode(
        std::fs::read(directory.0.join("published.pb"))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    (directory, extent)
}
#[test]
fn synchronized_payload_and_binary_descriptor_survive_process_kill() {
    let (dir, extent) = crash("durable");
    let store =
        PayloadExtentStore::open(dir.0.join("payload.data"), DEFAULT_CAPACITY_BYTES).unwrap();
    assert_eq!(store.read(&extent).unwrap(), vec![0x5a; 8193]);
    assert_eq!(store.read_range(&extent, 4095, 3).unwrap(), vec![0x5a; 3]);
}
#[test]
fn partial_unpublished_tail_requires_validated_roots_after_process_kill() {
    let (dir, extent) = crash("partial");
    let path = dir.0.join("payload.data");
    let before = std::fs::read(&path).unwrap();
    assert!(PayloadExtentStore::open(&path, DEFAULT_CAPACITY_BYTES).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let mut invalid = extent.clone();
    invalid.sha256[0] ^= 1;
    assert!(PayloadExtentStore::open_recovered(&path, DEFAULT_CAPACITY_BYTES, &[invalid]).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let store =
        PayloadExtentStore::open_recovered(&path, DEFAULT_CAPACITY_BYTES, &[extent.clone()])
            .unwrap();
    assert_eq!(store.read(&extent).unwrap(), vec![0x5a; 8193]);
    assert_eq!(store.length(), extent.offset + 12288);
    let next = store.append(b"new").unwrap();
    assert_eq!(next.offset, store.length() - 4096);
    assert_eq!(store.read(&extent).unwrap(), vec![0x5a; 8193]);
}
#[test]
fn complete_unpublished_tail_is_reclaimed_without_overwriting_published_bytes() {
    let (dir, extent) = crash("unpublished");
    let path = dir.0.join("payload.data");
    let before = std::fs::read(&path).unwrap();
    let store =
        PayloadExtentStore::open_recovered(&path, DEFAULT_CAPACITY_BYTES, &[extent.clone()])
            .unwrap();
    let retained = store.length() as usize;
    assert_eq!(std::fs::read(&path).unwrap(), before[..retained]);
    assert_eq!(store.read(&extent).unwrap(), vec![0x5a; 8193]);
    let next = store.append(b"replacement").unwrap();
    assert_eq!(next.offset, retained as u64);
}
