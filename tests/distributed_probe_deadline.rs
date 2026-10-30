// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]

use matrixobjectstore_rs::cluster::meta_message::{encode_meta_reply, MetaReply, MetaRequest};
use matrixobjectstore_rs::cluster::{MetaGroupIdentity, MetaRequestCode, StatusCode};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

fn accept(listener: &TcpListener) -> TcpStream {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                return stream;
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < until =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            result => panic!("missing probe connection: {result:?}"),
        }
    }
}

fn receive(stream: &mut TcpStream) -> MetaRequest {
    let mut header = [0; 6];
    stream.read_exact(&mut header).unwrap();
    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    assert!(length <= 4096);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).unwrap();
    MetaRequest::decode(MetaRequestCode::from_u8(header[1]).unwrap(), &payload).unwrap()
}

#[test]
fn probe_object_budget_bounds_multiple_metadata_stages_without_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let worker = std::thread::spawn(move || {
        let identity = encode_meta_reply(&MetaReply::GroupIdentity(MetaGroupIdentity {
            version: 1,
            group_id: 4,
            node_id: 1,
            voters: vec![1, 2, 3],
            peer_endpoints: vec![
                "127.0.0.1:12001".into(),
                "127.0.0.1:12002".into(),
                "127.0.0.1:12003".into(),
            ],
            bootstrap_id: 9,
        }));
        let mut discovery = accept(&listener);
        for _ in 0..2 {
            assert!(matches!(
                receive(&mut discovery),
                MetaRequest::GroupIdentity
            ));
            discovery.write_all(&identity).unwrap();
        }
        let mut stream = accept(&listener);
        assert!(matches!(receive(&mut stream), MetaRequest::GroupIdentity));
        stream.write_all(&identity).unwrap();
        assert!(matches!(
            receive(&mut stream),
            MetaRequest::ReadManifest { .. }
        ));
        std::thread::sleep(Duration::from_millis(150));
        stream
            .write_all(&matrixobjectstore_rs::cluster::message::encode_failure(
                StatusCode::NotFound,
                "object missing",
            ))
            .unwrap();
        assert!(matches!(
            receive(&mut stream),
            MetaRequest::PlanObjectPlacement { .. }
        ));
        let mut byte = [0];
        assert_eq!(
            stream.read(&mut byte).unwrap(),
            0,
            "expired object must close without allocation or publication"
        );
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    });
    let began = Instant::now();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_distributed_probe"))
        .args([
            "write",
            "--metadata",
            &endpoint,
            "--group",
            "4",
            "--bucket",
            "probe-bucket",
            "--run",
            "deadline",
            "--objects",
            "1",
            "--bytes",
            "1",
            "--workers",
            "1",
            "--groups",
            "1",
            "--timeout-ms",
            "5000",
            "--operation-timeout-ms",
            "500",
        ])
        .output()
        .unwrap();
    worker.join().unwrap();
    assert!(!output.status.success());
    assert!(
        began.elapsed() < Duration::from_secs(3),
        "probe waited for a per-frame timeout"
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("operation_timeout_ms=500"), "{stdout}");
    assert!(
        stdout.contains("phase=write completed=0 requested=1"),
        "{stdout}"
    );
    assert!(stdout.contains("failed=true"), "{stdout}");
    assert!(
        stderr.contains("outcome must be inspected, not replayed"),
        "{stderr}"
    );
}
