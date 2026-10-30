// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(target_os = "linux", feature = "replica-server"))]
mod node_regime;
use matrixobjectstore_rs::cluster::{RemoteReplicaClient, RemoteReplicaSet, ReplicaClientOptions};
use matrixobjectstore_rs::{ChunkId, ObjectError, PageId};
use node_regime::processes;

fn page(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 2).unwrap(),
        page_index: index,
    }
}
fn start() -> processes::NodeProcess {
    processes::start(
        env!("CARGO_BIN_EXE_chunkserver"),
        &[("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0")],
    )
}
fn client(node: &processes::NodeProcess) -> RemoteReplicaClient {
    RemoteReplicaClient::connect(&node.endpoint, &ReplicaClientOptions::default()).unwrap()
}

#[test]
fn page_state_transfer_preserves_discarded_ranges_and_refuses_stale_chunks() {
    let node = start();
    let mut writer = client(&node);
    let data = vec![23; 17 * 1024];
    writer.write_chunk(page(0), 0, &data).unwrap();
    writer.discard_chunk(&page(0), 3000, 5000).unwrap();
    let head = writer.head_chunk(&page(0)).unwrap();
    let mut reader = RemoteReplicaClient::connect(
        &node.endpoint,
        &ReplicaClientOptions {
            max_payload_bytes: 4096,
            ..Default::default()
        },
    )
    .unwrap();
    let state = reader.read_chunk_state(&page(0)).unwrap();
    assert_eq!(state.data.as_slice(), data);
    assert_eq!(state.logical_length, head.logical_length);
    assert_eq!(state.write_sequence, head.write_sequence);
    assert_eq!(state.discarded_ranges, head.discarded_ranges);
    assert_eq!(state.checksum(), head.checksum);
    assert!(reader.read_chunk(&page(0), 3000, 1).is_err());
    let first = reader
        .read_chunk_state_range(&page(0), &head.state_checksum, 0, 1024)
        .unwrap();
    assert_eq!(first.data, vec![23; 1024]);
    writer.write_chunk(page(0), 16000, b"changed").unwrap();
    assert!(matches!(
        reader.read_chunk_state_range(&page(0), &head.state_checksum, 1024, 1024),
        Err(ObjectError::PreconditionFailed(_))
    ));
    let current = reader.read_chunk_state(&page(0)).unwrap();
    assert_eq!(&current.data[16000..16007], b"changed");
    writer.write_chunk(page(1), 0, &[]).unwrap();
    let empty = reader.read_chunk_state(&page(1)).unwrap();
    assert!(empty.data.is_empty());
    assert_eq!(empty.logical_length, 0);
}

#[test]
fn star_state_transfer_requires_valid_bytes_from_a_read_quorum() {
    use matrixobjectstore_rs::cluster::{
        message::encode_reply, state_transfer::ChunkStateRange, ReplicaReply, ReplicaRequest,
        RequestCode,
    };
    use matrixobjectstore_rs::MatrixReplicaService;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};
    let mut service = MatrixReplicaService::new();
    service.write_chunk(page(0), 0, &vec![7; 8192]).unwrap();
    service.discard_chunk(&page(0), 100, 20).unwrap();
    let head = service.head_chunk(&page(0)).unwrap();
    for corrupt in [false, true] {
        let listeners: Vec<_> = (0..2)
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
        let workers: Vec<_> = listeners
            .into_iter()
            .enumerate()
            .map(|(index, listener)| {
                let head = head.clone();
                std::thread::spawn(move || {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    let mut stream = loop {
                        match listener.accept() {
                            Ok((stream, _)) => break stream,
                            Err(error)
                                if error.kind() == std::io::ErrorKind::WouldBlock
                                    && Instant::now() < deadline =>
                            {
                                std::thread::sleep(Duration::from_millis(1))
                            }
                            other => panic!("state client did not connect: {other:?}"),
                        }
                    };
                    stream
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    loop {
                        let mut header = [0; 6];
                        if stream.read_exact(&mut header).is_err() {
                            break;
                        }
                        let len = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                        assert!(len <= 512);
                        let mut bytes = vec![0; len];
                        stream.read_exact(&mut bytes).unwrap();
                        let request = ReplicaRequest::decode(
                            RequestCode::from_u8(header[1]).unwrap(),
                            &bytes,
                        )
                        .unwrap();
                        let reply = match request {
                            ReplicaRequest::HeadChunk { .. } => ReplicaReply::Head(head.clone()),
                            ReplicaRequest::ReadChunkStateRange {
                                offset,
                                length,
                                expected_state_checksum,
                                ..
                            } => {
                                assert_eq!(expected_state_checksum, head.state_checksum);
                                let mut data = vec![7; length.min(8192 - offset) as usize];
                                if corrupt && index == 1 && !data.is_empty() {
                                    data[0] ^= 1;
                                }
                                ReplicaReply::StateRange(ChunkStateRange {
                                    version: 1,
                                    offset,
                                    data_length: 8192,
                                    data,
                                    state_checksum: head.state_checksum.clone(),
                                })
                            }
                            _ => panic!("unexpected transfer request"),
                        };
                        if stream.write_all(&encode_reply(&reply)).is_err() {
                            break;
                        }
                    }
                })
            })
            .collect();
        let mut set = RemoteReplicaSet::connect(
            &endpoints,
            1,
            2,
            &ReplicaClientOptions {
                max_payload_bytes: 4096,
                ..Default::default()
            },
        )
        .unwrap();
        let result = set.read_chunk_state(&page(0));
        if corrupt {
            assert!(
                result.is_err(),
                "matching heads alone cannot supply a byte quorum"
            );
            assert!(set.repair_chunk(&page(0)).is_err());
        } else {
            let state = result.unwrap();
            assert_eq!(state.data.as_slice(), vec![7; 8192]);
            assert_eq!(state.discarded_ranges, head.discarded_ranges);
        }
        drop(set);
        for worker in workers {
            worker.join().unwrap();
        }
    }
}

#[test]
#[ignore = "256 MiB page over bounded network frames; run explicitly in release mode"]
fn page_state_transfer_reads_a_full_256_mib_page() {
    let node = start();
    let mut writer = client(&node);
    let block = vec![17; 7 * 1024 * 1024];
    let total = 256 * 1024 * 1024;
    let mut offset = 0;
    while offset < total {
        let count = block.len().min(total - offset);
        writer
            .write_chunk(page(0), offset as u64, &block[..count])
            .unwrap();
        offset += count;
    }
    writer
        .write_chunk(page(0), total as u64 - 1, &[19])
        .unwrap();
    writer.discard_chunk(&page(0), 1024 * 1024 - 3, 9).unwrap();
    let head = writer.head_chunk(&page(0)).unwrap();
    let mut reader = client(&node);
    let state = reader.read_chunk_state(&page(0)).unwrap();
    assert_eq!(state.data.len(), total);
    assert!(state.data[..total - 1].iter().all(|b| *b == 17));
    assert_eq!(state.data[total - 1], 19);
    assert_eq!(state.write_sequence, head.write_sequence);
    assert_eq!(state.discarded_ranges, head.discarded_ranges);
    assert_eq!(state.checksum(), head.checksum);
    println!("transferred {total} page bytes through frames bounded to one MiB");
}


#[test]
fn star_repair_aborts_changed_sources_and_never_replays_uncertain_commits() {
    use matrixobjectstore_rs::cluster::{
        message::{encode_failure, encode_reply},
        state_transfer::ChunkStateRange,
        PageRepairStatus, ReplicaReply, ReplicaRequest, RequestCode, StatusCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};
    let mut source = matrixobjectstore_rs::MatrixReplicaService::new();
    source.write_chunk(page(0), 0, &vec![7; 8192]).unwrap();
    let original = source.head_chunk(&page(0)).unwrap();
    source.write_chunk(page(0), 0, b"updated").unwrap();
    let newer = source.head_chunk(&page(0)).unwrap();
    for (change_source, batch_mode) in [(true, false), (false, false), (true, true), (false, true)]
    {
        let changed = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let commits = Arc::new(AtomicUsize::new(0));
        let aborts = Arc::new(AtomicUsize::new(0));
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
        let workers: Vec<_> = listeners
            .into_iter()
            .enumerate()
            .map(|(index, listener)| {
                let original = original.clone();
                let newer = newer.clone();
                let changed = changed.clone();
                let finished = finished.clone();
                let commits = commits.clone();
                let aborts = aborts.clone();
                std::thread::spawn(move || {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    let mut stream = loop {
                        match listener.accept() {
                            Ok((stream, _)) => break stream,
                            Err(error)
                                if error.kind() == std::io::ErrorKind::WouldBlock
                                    && Instant::now() < deadline =>
                            {
                                std::thread::sleep(Duration::from_millis(1))
                            }
                            other => panic!("repair connection failed: {other:?}"),
                        }
                    };
                    stream
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    loop {
                        let mut header = [0; 6];
                        if stream.read_exact(&mut header).is_err() {
                            break;
                        }
                        let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                        assert!(length <= 4096);
                        let mut bytes = vec![0; length];
                        stream.read_exact(&mut bytes).unwrap();
                        let request = ReplicaRequest::decode(
                            RequestCode::from_u8(header[1]).unwrap(),
                            &bytes,
                        )
                        .unwrap();
                        let reply = match request {
                            ReplicaRequest::PageInventory { after, limit } => {
                                assert!(after.is_none());assert_eq!(limit,1);
                                encode_reply(&ReplicaReply::PageInventory(matrixobjectstore_rs::cluster::message::PageInventory {entries:if index==2 {vec![]} else {vec![(page(0),1)]},has_more:false}))
                            }
                            ReplicaRequest::HeadChunk { .. } if index == 2 => {
                                encode_failure(StatusCode::NotFound, "absent")
                            }
                            ReplicaRequest::HeadChunk { .. } => encode_reply(&ReplicaReply::Head(
                                if changed.load(Ordering::SeqCst) {
                                    newer.clone()
                                } else {
                                    original.clone()
                                },
                            )),
                            ReplicaRequest::ReadChunkStateRange {
                                offset,
                                length,
                                expected_state_checksum,
                                ..
                            } => {
                                assert!(index < 2);
                                assert_eq!(expected_state_checksum, original.state_checksum);
                                encode_reply(&ReplicaReply::StateRange(ChunkStateRange {
                                    version: 1,
                                    offset,
                                    data_length: 8192,
                                    data: vec![7; length.min(8192 - offset) as usize],
                                    state_checksum: original.state_checksum.clone(),
                                }))
                            }
                            ReplicaRequest::StateInstall(request) => {
                                assert_eq!(index, 2);
                                match request.operation {
                                    1 => {
                                        assert_eq!(
                                            request.head.unwrap().state_checksum,
                                            original.state_checksum
                                        );
                                    }
                                    2 => {
                                        if change_source {
                                            changed.store(true, Ordering::SeqCst);
                                        }
                                    }
                                    3 => {
                                        commits.fetch_add(1, Ordering::SeqCst);
                                    drop(stream);
                                    loop {
                                        match listener.accept() {
                                            Ok(_) => panic!("uncertain commit was reconnected for replay"),
                                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                                if finished.load(Ordering::SeqCst) { break; }
                                                assert!(Instant::now() < deadline, "repair did not return after lost commit reply");
                                                std::thread::sleep(Duration::from_millis(1));
                                            }
                                            Err(error) => panic!("repair listener failed: {error}"),
                                        }
                                    }
                                    return;
                                    }
                                    4 => {
                                        aborts.fetch_add(1, Ordering::SeqCst);
                                    }
                                    _ => panic!("unexpected stage operation"),
                                }
                                encode_reply(&ReplicaReply::Applied)
                            }
                            _ => panic!("unexpected repair request"),
                        };
                        if stream.write_all(&reply).is_err() {
                            break;
                        }
                    }
                })
            })
            .collect();
        let mut set = RemoteReplicaSet::connect(
            &endpoints,
            2,
            2,
            &ReplicaClientOptions {
                max_payload_bytes: 4096,
                ..Default::default()
            },
        )
        .unwrap();
        let report = if batch_mode {
            let batch = set.repair_inventory_batch(None, 1).unwrap();
            assert!(batch.blocked);
            assert!(batch.has_more);
            assert_eq!(batch.resume_after, None);
            assert_eq!(batch.pages.len(), 1);
            batch.pages.into_iter().next().unwrap().result.unwrap()
        } else {
            set.repair_chunk(&page(0)).unwrap()
        };
        finished.store(true, Ordering::SeqCst);
        assert_eq!(report.source, original);
        assert_eq!(
            &report.members[..2],
            &[PageRepairStatus::Current, PageRepairStatus::Current]
        );
        if change_source {
            assert!(matches!(report.members[2], PageRepairStatus::Deferred(_)));
            assert_eq!(commits.load(Ordering::SeqCst), 0);
            assert_eq!(aborts.load(Ordering::SeqCst), 1);
        } else {
            assert!(matches!(
                report.members[2],
                PageRepairStatus::Unconfirmed(_)
            ));
            assert_eq!(commits.load(Ordering::SeqCst), 1);
            assert_eq!(aborts.load(Ordering::SeqCst), 0);
        }
        drop(set);
        for worker in workers {
            worker.join().unwrap();
        }
    }
}

#[test]
fn star_reads_writes_and_discards_send_all_members_before_waiting_for_replies() {
    use matrixobjectstore_rs::cluster::RequestCode;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    let nodes: Vec<_> = (0..3).map(|_| start()).collect();
    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().to_string())
        .collect();
    let (arrived, arrivals) = mpsc::channel();
    let mut releases = Vec::new();
    let mut workers = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        listener.set_nonblocking(true).unwrap();
        let endpoint = nodes[index].endpoint.clone();
        let arrived = arrived.clone();
        let (release, released) = mpsc::channel();
        releases.push(release);
        workers.push(std::thread::spawn(move || {
            let until = Instant::now() + Duration::from_secs(5);
            let mut front = loop {
                match listener.accept() {
                    Ok((s, _)) => break s,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < until =>
                    {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    other => panic!("proxy accept: {other:?}"),
                }
            };
            let mut back = TcpStream::connect(endpoint).unwrap();
            for s in [&front, &back] {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
                s.set_nodelay(true).unwrap();
            }
            for phase in 0..8 {
                let mut header = [0; 6];
                front.read_exact(&mut header).unwrap();
                let size = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
                assert!(size <= 4096);
                let mut body = vec![0; size];
                front.read_exact(&mut body).unwrap();
                arrived.send((phase, index, header[1])).unwrap();
                // No member can answer until the whole phase has been sent.
                released.recv_timeout(Duration::from_secs(5)).unwrap();
                back.write_all(&header).unwrap();
                back.write_all(&body).unwrap();
                back.read_exact(&mut header).unwrap();
                let size = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
                assert!(size <= 4096);
                body.resize(size, 0);
                back.read_exact(&mut body).unwrap();
                front.write_all(&header).unwrap();
                front.write_all(&body).unwrap();
            }
        }));
    }
    drop(arrived);
    let coordinator = std::thread::spawn(move || {
        let codes = [
            RequestCode::HeadChunk,
            RequestCode::WriteChunkIfSequence,
            RequestCode::ReadChunk,
            RequestCode::HeadChunk,
            RequestCode::DiscardChunkIfSequence,
            RequestCode::ReadChunk,
            RequestCode::HeadChunk,
            RequestCode::HeadChunk,
        ];
        for (phase, code) in codes.into_iter().enumerate() {
            let mut members = std::collections::BTreeSet::new();
            for _ in 0..3 {
                let (actual, index, actual_code) =
                    arrivals.recv_timeout(Duration::from_secs(5)).unwrap();
                assert_eq!(actual, phase);
                assert_eq!(actual_code, code as u8);
                assert!(members.insert(index));
            }
            for release in &releases {
                release.send(()).unwrap();
            }
        }
    });
    let options = ReplicaClientOptions {
        read_timeout: Duration::from_secs(8),
        ..Default::default()
    };
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap();
    assert_eq!(set.write_chunk(&page(0), 0, b"abcdefgh").unwrap(), 3);
    assert_eq!(set.read_chunk(&page(0), 0, 8).unwrap().data, b"abcdefgh");
    assert_eq!(set.discard_chunk(&page(0), 2, 2).unwrap(), 3);
    assert_eq!(set.read_chunk(&page(0), 0, 2).unwrap().data, b"ab");
    let head = set.head_chunk(&page(0)).unwrap();
    assert_eq!(head.write_sequence, 2);
    assert_eq!(head.discarded_ranges.len(), 1);
    assert!(matches!(
        set.head_chunk(&page(1)),
        Err(ObjectError::NotFound(_))
    ));
    drop(set);
    coordinator.join().unwrap();
    for worker in workers {
        worker.join().unwrap();
    }
    for node in &nodes {
        let mut c = client(node);
        assert_eq!(c.head_chunk(&page(0)).unwrap().write_sequence, 2);
        assert!(c.read_chunk(&page(0), 2, 2).is_err());
    }
}

#[test]
fn star_slow_first_member_does_not_expire_healthy_quorum_replies() {
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};
    let nodes: Vec<_> = (0..2).map(|_| start()).collect();
    for node in &nodes {
        client(node).write_chunk(page(0), 0, b"healthy").unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut endpoints = vec![listener.local_addr().unwrap().to_string()];
    endpoints.extend(nodes.iter().map(|n| n.endpoint.clone()));
    let slow = std::thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < until => {
                    std::thread::sleep(Duration::from_millis(1))
                }
                other => panic!("slow member accept: {other:?}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut received = Vec::new();
        stream.read_to_end(&mut received).unwrap();
        assert!(!received.is_empty());
    });
    let mut set = RemoteReplicaSet::connect(
        &endpoints,
        2,
        2,
        &ReplicaClientOptions {
            read_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    )
    .unwrap();
    let read = set.read_chunk(&page(0), 0, 7).unwrap();
    assert_eq!(read.data, b"healthy");
    drop(set);
    slow.join().unwrap();
}

#[test]
#[ignore = "controlled three-replica latency measurement; run explicitly in release mode"]
fn star_fanout_measures_reads_and_writes_with_delayed_replies() {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};
    fn run(pipelined: bool, round: usize) {
        let nodes: Vec<_> = (0..3).map(|_| start()).collect();
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let endpoints: Vec<_> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().to_string())
            .collect();
        let workers: Vec<_> = listeners
            .into_iter()
            .enumerate()
            .map(|(index, listener)| {
                let endpoint = nodes[index].endpoint.clone();
                listener.set_nonblocking(true).unwrap();
                std::thread::spawn(move || {
                    let until = Instant::now() + Duration::from_secs(5);
                    let mut front = loop {
                        match listener.accept() {
                            Ok((s, _)) => break s,
                            Err(e)
                                if e.kind() == std::io::ErrorKind::WouldBlock
                                    && Instant::now() < until =>
                            {
                                std::thread::sleep(Duration::from_millis(1))
                            }
                            other => panic!("latency proxy accept: {other:?}"),
                        }
                    };
                    let mut back = TcpStream::connect(endpoint).unwrap();
                    for socket in [&front, &back] {
                        socket.set_nodelay(true).unwrap();
                        socket
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        socket
                            .set_write_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                    }
                    loop {
                        let mut header = [0; 6];
                        if front.read_exact(&mut header).is_err() {
                            break;
                        }
                        let size = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
                        assert!(size <= 8192);
                        let mut body = vec![0; size];
                        front.read_exact(&mut body).unwrap();
                        back.write_all(&header).unwrap();
                        back.write_all(&body).unwrap();
                        back.read_exact(&mut header).unwrap();
                        let size = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
                        assert!(size <= 8192);
                        body.resize(size, 0);
                        back.read_exact(&mut body).unwrap();
                        std::thread::sleep(Duration::from_millis(5));
                        front.write_all(&header).unwrap();
                        front.write_all(&body).unwrap();
                    }
                })
            })
            .collect();
        let options = ReplicaClientOptions::default();
        let mut set = if pipelined {
            Some(RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap())
        } else {
            None
        };
        let mut serial: Vec<_> = if pipelined {
            vec![]
        } else {
            endpoints
                .iter()
                .map(|e| RemoteReplicaClient::connect(e, &options).unwrap())
                .collect()
        };
        let mut writes = Vec::new();
        let mut reads = Vec::new();
        let mut heads = Vec::new();
        for index in 0..24 {
            let id = page(index);
            let data = vec![index as u8; 1024];
            let began = Instant::now();
            if let Some(set) = &mut set {
                assert_eq!(set.write_chunk(&id, 0, &data).unwrap(), 3);
            } else {
                // Same healthy-member admission and conditional mutation phases.
                for c in &mut serial {
                    assert!(matches!(c.head_chunk(&id), Err(ObjectError::NotFound(_))));
                }
                for c in &mut serial {
                    c.write_chunk_if_sequence(id.clone(), 0, &data, 0).unwrap();
                }
            }
            writes.push(began.elapsed().as_micros() as u64);
            let began = Instant::now();
            if let Some(set) = &mut set {
                let read = set.read_chunk(&id, 0, 1024).unwrap();
                assert_eq!(read.data, data);
                assert_eq!(read.write_sequence, 1);
            } else {
                let mut results = Vec::new();
                for c in &mut serial {
                    results.push(c.read_chunk(&id, 0, 1024).unwrap());
                }
                for read in &results {
                    assert_eq!(read.data, data);
                    assert_eq!(read.write_sequence, 1);
                    assert_eq!(read.checksum, results[0].checksum);
                }
            }
            reads.push(began.elapsed().as_micros() as u64);
            let began = Instant::now();
            if let Some(set) = &mut set {
                let head = set.head_chunk(&id).unwrap();
                assert_eq!(head.write_sequence, 1);
                assert_eq!(head.logical_length, 1024);
                assert!(head.discarded_ranges.is_empty());
            } else {
                let results: Vec<_> = serial
                    .iter_mut()
                    .map(|c| c.head_chunk(&id).unwrap())
                    .collect();
                assert!(results.iter().all(|head| head == &results[0]));
                assert_eq!(results[0].write_sequence, 1);
                assert_eq!(results[0].logical_length, 1024);
                assert!(results[0].discarded_ranges.is_empty());
            }
            heads.push(began.elapsed().as_micros() as u64);
        }
        drop(set);
        drop(serial);
        for worker in workers {
            worker.join().unwrap();
        }
        // Validate every stored copy outside the measured request interval.
        for node in &nodes {
            let mut c = client(node);
            for index in 0..24 {
                let read = c.read_chunk(&page(index), 0, 1024).unwrap();
                assert_eq!(read.data, vec![index as u8; 1024]);
                assert_eq!(read.write_sequence, 1);
            }
        }
        for (operation, mut samples) in [("write", writes), ("read", reads), ("head", heads)] {
            let total: u64 = samples.iter().sum();
            samples.sort_unstable();
            eprintln!("STAR_DELAY round={round} pipelined={pipelined} operation={operation} replicas=3 bytes=1024 delay_ms=5 samples=24 total_us={total} p50_us={} p95_us={} operations_per_second={:.1}", samples[11], samples[22], 24_000_000.0 / total as f64);
        }
    }
    // Alternate order to reduce systematic warm-up bias. Do not assert timing ratios.
    for round in 0..3 {
        for pipelined in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            run(pipelined, round);
        }
    }
}

#[test]
fn star_stalled_members_share_reply_wait_budget_without_replaying() {
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};
    let listeners: Vec<_> = (0..3).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let endpoints: Vec<_> = listeners.iter().map(|s| s.local_addr().unwrap().to_string()).collect();
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions {
        read_timeout: Duration::from_millis(600), ..Default::default()
    }).unwrap();
    let mut sockets: Vec<_> = listeners.iter().map(|l| l.accept().unwrap().0).collect();
    for socket in &sockets {
        socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    }
    let began = Instant::now();
    assert!(set.head_chunk(&page(0)).is_err());
    assert!(began.elapsed() < Duration::from_millis(1200), "three reply budgets accumulated: {:?}", began.elapsed());
    for socket in &mut sockets {
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).unwrap();
        assert!(bytes.len() >= 6);
        assert_eq!(bytes[1], matrixobjectstore_rs::cluster::RequestCode::HeadChunk as u8);
        let length = u32::from_be_bytes(bytes[2..6].try_into().unwrap()) as usize;
        assert_eq!(bytes.len(), 6 + length, "exactly one request must be sent per member");
    }
}
