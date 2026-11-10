// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::net::TcpListener;

#[test]
fn atomic_copy_lost_reply_remains_unconfirmed_without_mutation_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let worker = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut header = [0; HEADER_BYTES];
        stream.read_exact(&mut header).unwrap();
        assert_eq!(header[1], MetaRequestCode::CopyManifest as u8);
        let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
        assert!(length < 8192);
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).unwrap();
        assert!(matches!(
            MetaRequest::decode(MetaRequestCode::CopyManifest, &payload).unwrap(),
            MetaRequest::CopyManifest(_)
        ));
        drop(stream);
        listener.set_nonblocking(true).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    });
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    assert!(matches!(
        client.copy_object_atomic("source", "key", "target", "key", None, 64, 0),
        Err(crate::manifest_copy::AtomicCopyFailure::Unconfirmed(_))
    ));
    drop(client);
    worker.join().unwrap();
}
