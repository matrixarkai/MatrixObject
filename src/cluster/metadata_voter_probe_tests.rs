// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};
fn accept(listener: &TcpListener) -> TcpStream {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                return stream;
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < until =>
            {
                std::thread::sleep(Duration::from_millis(2))
            }
            other => panic!("missing metadata connection: {other:?}"),
        }
    }
}
fn receive(stream: &mut TcpStream) -> MetaRequest {
    let mut header = [0; HEADER_BYTES];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0], super::super::PROTOCOL_STAMP);
    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    assert!(length <= 4096);
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).unwrap();
    MetaRequest::decode(MetaRequestCode::from_u8(header[1]).unwrap(), &bytes).unwrap()
}
fn reply(stream: &mut TcpStream, value: MetaReply) {
    stream
        .write_all(&super::super::meta_message::encode_meta_reply(&value))
        .unwrap();
}
#[test]
fn verified_metadata_reconnect_skips_typed_follower_without_skipping_ordinary_refusal() {
    for typed in [true, false] {
        let listeners: Vec<_> = (0..3)
            .map(|_| {
                let l = TcpListener::bind("127.0.0.1:0").unwrap();
                l.set_nonblocking(true).unwrap();
                l
            })
            .collect();
        let endpoints: Vec<_> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().to_string())
            .collect();
        let expected = super::super::MetaGroupIdentity {
            version: 1,
            group_id: 71,
            node_id: 1,
            voters: vec![1, 2, 3],
            peer_endpoints: endpoints.clone(),
            bootstrap_id: 17,
            bootstrap_voters: Vec::new(),
        };
        expected.validate().unwrap();
        let mut iter = listeners.into_iter();
        let first = iter.next().unwrap();
        let follower = iter.next().unwrap();
        let leader = iter.next().unwrap();
        let old = std::thread::spawn(move || {
            let mut stream = accept(&first);
            assert!(matches!(receive(&mut stream), MetaRequest::Ping));
            reply(&mut stream, MetaReply::NotLeader);
        });
        let skipped = std::thread::spawn(move || {
            let mut stream = accept(&follower);
            assert!(matches!(receive(&mut stream), MetaRequest::GroupIdentity));
            if typed {
                reply(&mut stream, MetaReply::NotLeader);
            } else {
                stream
                    .write_all(&super::super::message::encode_failure(
                        super::super::StatusCode::PreconditionFailed,
                        "metadata request was not admitted by a leader",
                    ))
                    .unwrap();
            }
            stream
                .set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            let mut byte = [0];
            match stream.read(&mut byte) {
                Ok(0) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                other => panic!("unverified follower received application bytes: {other:?}"),
            }
        });
        let options = MetaClientOptions {
            connect_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let mut client = RemoteMetaClient::connect(&endpoints[0], &options).unwrap();
        client.catalog = Some((endpoints.clone(), options));
        client.group_identity = Some(expected.clone());
        let worker = if typed {
            Some(std::thread::spawn(move || {
                let mut stream = accept(&leader);
                assert!(matches!(receive(&mut stream), MetaRequest::GroupIdentity));
                let mut actual = expected;
                actual.node_id = 3;
                reply(&mut stream, MetaReply::GroupIdentity(actual));
                assert!(matches!(receive(&mut stream), MetaRequest::Ping));
                reply(
                    &mut stream,
                    MetaReply::Pong {
                        node_count: 3,
                        placement_count: 1,
                        commit_index: 91,
                    },
                );
            }))
        } else {
            None
        };
        let result = client.probe();
        drop(client);
        old.join().unwrap();
        skipped.join().unwrap();
        if let Some(worker) = worker {
            worker.join().unwrap();
            assert!(matches!(
                result,
                MetaCallOutcome::Answered(MetaReply::Pong {
                    commit_index: 91,
                    ..
                })
            ));
        } else {
            assert!(matches!(
                result,
                MetaCallOutcome::Refused(ObjectError::PreconditionFailed(_))
            ));
        }
    }
}
