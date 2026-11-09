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
fn star_operations_send_each_wave_before_waiting_for_replies() {
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
    let step_timeout = Duration::from_secs(20);
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
            let until = Instant::now() + step_timeout;
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
                s.set_read_timeout(Some(step_timeout)).unwrap();
                s.set_write_timeout(Some(step_timeout)).unwrap();
                s.set_nodelay(true).unwrap();
            }
            loop {
                let mut header = [0; 6];
                match front.read_exact(&mut header) {
                    Ok(()) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::UnexpectedEof
                                | std::io::ErrorKind::ConnectionReset
                        ) =>
                    {
                        break;
                    }
                    Err(error) => panic!("proxy read: {error}"),
                }
                let size = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
                assert!(size <= 4096);
                let mut body = vec![0; size];
                front.read_exact(&mut body).unwrap();
                arrived.send((index, header[1])).unwrap();
                // No selected member can answer until the whole wave has been sent.
                released.recv_timeout(step_timeout).unwrap();
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
            RequestCode::Capabilities,
            RequestCode::HeadChunk,
            RequestCode::WriteChunkIfState,
            RequestCode::ReadChunk,
            RequestCode::HeadChunk,
            RequestCode::DiscardChunkIfSequence,
            RequestCode::ReadChunk,
            RequestCode::HeadChunk,
            RequestCode::HeadChunk,
        ];
        for (phase, code) in codes.into_iter().enumerate() {
            let expected_members = if matches!(phase, 1 | 3 | 6 | 7 | 8) {
                2
            } else {
                3
            };
            let mut members = std::collections::BTreeSet::new();
            for _ in 0..expected_members {
                let (index, actual_code) = arrivals
                    .recv_timeout(step_timeout)
                    .unwrap_or_else(|error| panic!("waiting for {code:?}: {error}"));
                assert_eq!(actual_code, code as u8);
                assert!(members.insert(index));
            }
            for index in members {
                releases[index].send(()).unwrap();
            }
        }
    });
    let options = ReplicaClientOptions {
        read_timeout: step_timeout,
        ..Default::default()
    };
    let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &options).unwrap();
    assert!((2..=3).contains(&set.write_chunk(&page(0), 0, b"abcdefgh").unwrap()));
    assert_eq!(set.read_chunk(&page(0), 0, 8).unwrap().data, b"abcdefgh");
    assert!((2..=3).contains(&set.discard_chunk(&page(0), 2, 2).unwrap()));
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
fn sixteen_default_pages_use_bounded_star_frames() {
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
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let (observed, observations) = mpsc::channel();
    let workers: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(index, listener)| {
            let endpoint = nodes[index].endpoint.clone();
            let observed = observed.clone();
            std::thread::spawn(move || {
                listener.set_nonblocking(true).unwrap();
                let mut first_connection = true;
                loop {
                    let until = Instant::now()
                        + if first_connection {
                            Duration::from_secs(5)
                        } else {
                            Duration::from_secs(1)
                        };
                    let front = loop {
                        match listener.accept() {
                            Ok((stream, _)) => break Some(stream),
                            Err(error)
                                if error.kind() == std::io::ErrorKind::WouldBlock
                                    && Instant::now() < until =>
                            {
                                std::thread::sleep(Duration::from_millis(1));
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                break None
                            }
                            other => panic!("batch proxy accept: {other:?}"),
                        }
                    };
                    let Some(mut front) = front else {
                        assert!(!first_connection, "batch proxy accepted no connection");
                        break;
                    };
                    first_connection = false;
                    let mut back = TcpStream::connect(&endpoint).unwrap();
                    for socket in [&front, &back] {
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
                        let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                        let mut body = vec![0; size];
                        if front.read_exact(&mut body).is_err() {
                            break;
                        }
                        observed.send((index, header[1])).unwrap();
                        if back.write_all(&header).is_err()
                            || back.write_all(&body).is_err()
                            || back.read_exact(&mut header).is_err()
                        {
                            break;
                        }
                        let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                        body.resize(size, 0);
                        if back.read_exact(&mut body).is_err()
                            || front.write_all(&header).is_err()
                            || front.write_all(&body).is_err()
                        {
                            break;
                        }
                    }
                }
            })
        })
        .collect();
    drop(observed);

    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    let data = vec![7u8; 1024 * 1024];
    let writes: Vec<_> = (10..26)
        .map(|index| (page(index), 0, data.as_slice()))
        .collect();
    let acknowledgements = set.write_chunks(&writes).unwrap();
    assert!(
        acknowledgements
            .iter()
            .all(|count| (2..=3).contains(count))
    );
    let ranges: Vec<_> = (10..26)
        .map(|index| (page(index), 0, data.len() as u64))
        .collect();
    let reads: Vec<_> = ranges
        .chunks(8)
        .flat_map(|batch| set.read_chunks(batch).unwrap())
        .collect();
    assert_eq!(reads.len(), 16);
    assert!(reads.iter().all(|read| read.data == data));
    drop(set);
    for worker in workers {
        worker.join().unwrap();
    }

    let mut codes = vec![Vec::new(); 3];
    for (index, code) in observations {
        codes[index].push(code);
    }
    const HEAD_CHUNK_BATCH_CODE: u8 = 18;
    const CONDITIONAL_BATCH_CODE: u8 = 14;
    const READ_BATCH_CODE: u8 = 17;
    let mut read_participants = 0usize;
    let mut read_frames = 0usize;
    let mut complete_writes = 0usize;
    for member in codes {
        assert_eq!(member.first(), Some(&HEAD_CHUNK_BATCH_CODE));
        assert_eq!(
            member
                .iter()
                .filter(|code| **code == HEAD_CHUNK_BATCH_CODE)
                .count(),
            1
        );
        let write_frames = member
            .iter()
            .filter(|code| **code == CONDITIONAL_BATCH_CODE)
            .count();
        assert!((1..=4).contains(&write_frames));
        complete_writes += usize::from(write_frames == 4);
        let member_read_frames = member
            .iter()
            .filter(|code| **code == READ_BATCH_CODE)
            .count();
        read_participants += usize::from(member_read_frames > 0);
        read_frames += member_read_frames;
    }
    assert!(complete_writes >= 2);
    assert!(read_participants >= 2);
    assert!(read_frames >= 8);
    let mut final_page_copies = 0usize;
    for node in &nodes {
        let mut replica = client(node);
        assert_eq!(
            replica.read_chunk(&page(10), 0, data.len() as u64).unwrap().data,
            data
        );
        if let Ok(read) = replica.read_chunk(&page(25), 0, data.len() as u64) {
            assert_eq!(read.data, data);
            final_page_copies += 1;
        }
    }
    assert!(final_page_copies >= 2);
}

#[test]
fn star_batch_continues_on_quorum_when_one_member_stalls() {
    use matrixobjectstore_rs::cluster::{
        message::encode_reply, ReplicaReply, ReplicaRequest, RequestCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let (release_slow, slow_released) = mpsc::channel();
    let (advanced, observed_advance) = mpsc::channel();
    let mut slow_released = Some(slow_released);
    let workers: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(index, listener)| {
            let release = if index == 2 {
                slow_released.take()
            } else {
                None
            };
            let advanced = advanced.clone();
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                for phase in 0..4 {
                    let mut header = [0; 6];
                    stream.read_exact(&mut header).unwrap();
                    let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut body = vec![0; size];
                    stream.read_exact(&mut body).unwrap();
                    let code = RequestCode::from_u8(header[1]).unwrap();
                    match (phase, ReplicaRequest::decode(code, &body).unwrap()) {
                        (0, ReplicaRequest::HeadChunkBatch { pages }) => {
                            assert_eq!(pages.len(), 16);
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Heads(vec![
                                    None;
                                    pages.len()
                                ])))
                                .unwrap();
                        }
                        (1, ReplicaRequest::ConditionalWriteLimits) => stream
                            .write_all(&encode_reply(&ReplicaReply::ConditionalWriteLimits {
                                max_writes: 8,
                                max_payload_bytes: 4 * 1024 * 1024,
                            }))
                            .unwrap(),
                        (2, ReplicaRequest::WriteChunkBatchIfSequence { writes }) => {
                            assert_eq!(writes.len(), 8);
                            if let Some(release) = release.as_ref() {
                                release.recv_timeout(Duration::from_secs(5)).unwrap();
                                let _ = stream.write_all(&encode_reply(&ReplicaReply::Applied));
                                return;
                            }
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Applied))
                                .unwrap();
                        }
                        (3, ReplicaRequest::WriteChunkBatchIfSequence { writes }) => {
                            assert_eq!(writes.len(), 8);
                            advanced.send(index).unwrap();
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Applied))
                                .unwrap();
                        }
                        other => panic!("unexpected stalled-member batch phase: {other:?}"),
                    }
                }
            })
        })
        .collect();
    drop(advanced);

    let mut set = RemoteReplicaSet::connect(
        &endpoints,
        2,
        2,
        &ReplicaClientOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .unwrap();
    let writer = std::thread::spawn(move || {
        let writes: Vec<_> = (100..116)
            .map(|index| (page(index), 0, b"x".as_slice()))
            .collect();
        set.write_chunks(&writes)
    });

    let continued = observed_advance.recv_timeout(Duration::from_secs(2));
    release_slow.send(()).unwrap();
    let acknowledgements = writer.join().unwrap().unwrap();
    assert!(
        continued.is_ok(),
        "a slow optional member delayed the next star write frame"
    );
    assert_eq!(acknowledgements, vec![2; 16]);
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn star_head_fence_falls_back_to_eight_page_frames_after_bounded_refusal() {
    use matrixobjectstore_rs::cluster::{
        message::{encode_failure, encode_reply},
        ReplicaReply, ReplicaRequest, RequestCode, StatusCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let workers: Vec<_> = listeners
        .into_iter()
        .map(|listener| {
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                for phase in 0..5 {
                    let mut header = [0; 6];
                    stream.read_exact(&mut header).unwrap();
                    let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut body = vec![0; size];
                    stream.read_exact(&mut body).unwrap();
                    let code = RequestCode::from_u8(header[1]).unwrap();
                    match (phase, ReplicaRequest::decode(code, &body).unwrap()) {
                        (0, ReplicaRequest::HeadChunkBatch { pages }) => {
                            assert_eq!(pages.len(), 16);
                            stream
                                .write_all(&encode_failure(
                                    StatusCode::InvalidArgument,
                                    "bounded head batch",
                                ))
                                .unwrap();
                        }
                        (1 | 2, ReplicaRequest::HeadChunkBatch { pages }) => {
                            assert_eq!(pages.len(), 8);
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Heads(vec![
                                    None;
                                    pages.len()
                                ])))
                                .unwrap();
                        }
                        (3, ReplicaRequest::ConditionalWriteLimits) => stream
                            .write_all(&encode_reply(&ReplicaReply::ConditionalWriteLimits {
                                max_writes: 32,
                                max_payload_bytes: 4 * 1024 * 1024,
                            }))
                            .unwrap(),
                        (4, ReplicaRequest::WriteChunkBatchIfSequence { writes }) => {
                            assert_eq!(writes.len(), 16);
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Applied))
                                .unwrap();
                        }
                        other => panic!("unexpected bounded-fallback phase: {other:?}"),
                    }
                }
            })
        })
        .collect();

    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    let writes: Vec<_> = (200..216)
        .map(|index| (page(index), 0, b"x".as_slice()))
        .collect();
    assert_eq!(set.write_chunks(&writes).unwrap(), vec![3; 16]);
    drop(set);
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn fresh_star_pages_use_conditional_writes_as_the_absence_fence() {
    use matrixobjectstore_rs::cluster::{
        message::{encode_failure, encode_reply},
        ReplicaReply, ReplicaRequest, RequestCode, StatusCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let workers: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(index, listener)| {
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                for phase in 0..2 {
                    let mut header = [0; 6];
                    stream.read_exact(&mut header).unwrap();
                    let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut body = vec![0; size];
                    stream.read_exact(&mut body).unwrap();
                    let code = RequestCode::from_u8(header[1]).unwrap();
                    match (phase, ReplicaRequest::decode(code, &body).unwrap()) {
                        (0, ReplicaRequest::ConditionalWriteLimits) => stream
                            .write_all(&encode_reply(&ReplicaReply::ConditionalWriteLimits {
                                max_writes: 32,
                                max_payload_bytes: 4 * 1024 * 1024,
                            }))
                            .unwrap(),
                        (1, ReplicaRequest::WriteChunkBatchIfSequence { writes }) => {
                            assert_eq!(writes.len(), 16);
                            assert!(writes.iter().all(|write| {
                                write.offset == 0 && write.expected_sequence == 0
                            }));
                            assert_eq!(writes[0].page_id, page(300));
                            if index == 2 {
                                stream
                                    .write_all(&encode_failure(
                                        StatusCode::PreconditionFailed,
                                        "page already exists",
                                    ))
                                    .unwrap();
                            } else {
                                stream
                                    .write_all(&encode_reply(&ReplicaReply::Applied))
                                    .unwrap();
                            }
                        }
                        other => panic!("fresh star page write sent {other:?}"),
                    }
                }
            })
        })
        .collect();

    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    let payloads = vec![vec![7u8]; 16];
    let writes: Vec<_> = payloads
        .iter()
        .enumerate()
        .map(|(index, data)| (page(300 + index as u64), 0, data.as_slice(), 0))
        .collect();
    assert_eq!(set.write_chunks_if_sequence(&writes).unwrap(), vec![2; 16]);
    drop(set);
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn star_batch_sequences_thirty_two_repeated_page_appends_after_one_head_fence() {
    use matrixobjectstore_rs::cluster::{
        message::encode_reply, ReplicaReply, ReplicaRequest, RequestCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let expected_page = page(19);
    let workers: Vec<_> = listeners
        .into_iter()
        .map(|listener| {
            let expected_page = expected_page.clone();
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                for phase in 0..3 {
                    let mut header = [0; 6];
                    stream.read_exact(&mut header).unwrap();
                    let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut body = vec![0; size];
                    stream.read_exact(&mut body).unwrap();
                    let code = RequestCode::from_u8(header[1]).unwrap();
                    match (phase, ReplicaRequest::decode(code, &body).unwrap()) {
                        (0, ReplicaRequest::HeadChunkBatch { pages }) => {
                            assert_eq!(pages, vec![expected_page.clone()]);
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Heads(vec![None])))
                                .unwrap();
                        }
                        (1, ReplicaRequest::ConditionalWriteLimits) => stream
                            .write_all(&encode_reply(&ReplicaReply::ConditionalWriteLimits {
                                max_writes: 32,
                                max_payload_bytes: 4 * 1024 * 1024,
                            }))
                            .unwrap(),
                        (2, ReplicaRequest::WriteChunkBatchIfSequence { writes }) => {
                            assert_eq!(writes.len(), 32);
                            assert!(writes.iter().all(|write| write.page_id == expected_page));
                            assert_eq!(
                                writes
                                    .iter()
                                    .map(|write| write.expected_sequence)
                                    .collect::<Vec<_>>(),
                                (0..32).map(|value| value as u64).collect::<Vec<_>>()
                            );
                            assert_eq!(
                                writes.iter().map(|write| write.offset).collect::<Vec<_>>(),
                                (0..32).map(|value| value as u64).collect::<Vec<_>>()
                            );
                            assert!(writes.iter().all(|write| write.data == b"x"));
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Applied))
                                .unwrap();
                        }
                        other => panic!("unexpected repeated-page batch phase: {other:?}"),
                    }
                }
            })
        })
        .collect();

    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    let writes: Vec<_> = (0..32)
        .map(|offset| (expected_page.clone(), offset, b"x".as_slice()))
        .collect();
    let acknowledgements = set.write_chunks(&writes).unwrap();
    assert_eq!(acknowledgements.len(), 32);
    assert!(
        acknowledgements
            .iter()
            .all(|count| (2..=3).contains(count))
    );
    let too_many: Vec<_> = (0..33)
        .map(|offset| (expected_page.clone(), offset, b"x".as_slice()))
        .collect();
    assert!(matches!(
        set.write_chunks(&too_many),
        Err(ObjectError::InvalidArgument(message))
            if message == "star write batch exceeds thirty-two writes"
    ));
    drop(set);
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn older_member_keeps_star_write_frames_at_eight_writes() {
    use matrixobjectstore_rs::cluster::{
        message::{encode_failure, encode_reply},
        ReplicaReply, ReplicaRequest, RequestCode, StatusCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let workers: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(index, listener)| {
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                for phase in 0..5 {
                    let mut header = [0; 6];
                    stream.read_exact(&mut header).unwrap();
                    let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut body = vec![0; size];
                    stream.read_exact(&mut body).unwrap();
                    let code = RequestCode::from_u8(header[1]).unwrap();
                    match (phase, ReplicaRequest::decode(code, &body).unwrap()) {
                        (0, ReplicaRequest::ConditionalWriteLimits) if index == 2 => stream
                            .write_all(&encode_failure(
                                StatusCode::UnknownRequest,
                                "unsupported request",
                            ))
                            .unwrap(),
                        (0, ReplicaRequest::ConditionalWriteLimits) => stream
                            .write_all(&encode_reply(&ReplicaReply::ConditionalWriteLimits {
                                max_writes: 32,
                                max_payload_bytes: 4 * 1024 * 1024,
                            }))
                            .unwrap(),
                        (1..=4, ReplicaRequest::WriteChunkBatchIfSequence { writes }) => {
                            assert_eq!(writes.len(), 8);
                            let first = (phase - 1) * 8;
                            assert_eq!(writes[0].page_id, page(400 + first as u64));
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Applied))
                                .unwrap();
                        }
                        other => panic!("unexpected mixed-version phase: {other:?}"),
                    }
                }
            })
        })
        .collect();

    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    let payloads = vec![vec![5u8]; 32];
    let writes: Vec<_> = payloads
        .iter()
        .enumerate()
        .map(|(index, data)| (page(400 + index as u64), 0, data.as_slice(), 0))
        .collect();
    assert_eq!(set.write_chunks_if_sequence(&writes).unwrap(), vec![3; 32]);
    drop(set);
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
#[ignore = "controlled star batch latency measurement; run explicitly in release mode"]
fn star_write_frame_width_measurement() {
    use matrixobjectstore_rs::cluster::{
        message::encode_reply, ReplicaReply, ReplicaRequest, RequestCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    fn run(max_writes: u32, delay: Duration) -> (u128, usize) {
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let endpoints: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let expected_frames = 32usize.div_ceil(max_writes as usize);
        let workers: Vec<_> = listeners
            .into_iter()
            .map(|listener| {
                std::thread::spawn(move || {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut mutations = 0usize;
                    for phase in 0..=expected_frames {
                        let mut header = [0; 6];
                        stream.read_exact(&mut header).unwrap();
                        let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                        let mut body = vec![0; size];
                        stream.read_exact(&mut body).unwrap();
                        let code = RequestCode::from_u8(header[1]).unwrap();
                        let reply = match (phase, ReplicaRequest::decode(code, &body).unwrap()) {
                            (0, ReplicaRequest::ConditionalWriteLimits) => {
                                ReplicaReply::ConditionalWriteLimits {
                                    max_writes,
                                    max_payload_bytes: 4 * 1024 * 1024,
                                }
                            }
                            (_, ReplicaRequest::WriteChunkBatchIfSequence { writes }) => {
                                assert!(!writes.is_empty());
                                assert!(writes.len() <= max_writes as usize);
                                mutations += 1;
                                ReplicaReply::Applied
                            }
                            other => panic!("unexpected measured star phase: {other:?}"),
                        };
                        std::thread::sleep(delay);
                        stream.write_all(&encode_reply(&reply)).unwrap();
                    }
                    mutations
                })
            })
            .collect();

        let mut set =
            RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
        let payloads = vec![vec![9u8; 256]; 32];
        let writes: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(index, data)| (page(500 + index as u64), 0, data.as_slice(), 0))
            .collect();
        let began = Instant::now();
        assert_eq!(set.write_chunks_if_sequence(&writes).unwrap(), vec![3; 32]);
        let elapsed = began.elapsed().as_micros();
        drop(set);
        let mutation_frames: usize = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .sum();
        assert_eq!(mutation_frames, expected_frames * 3);
        (elapsed, mutation_frames)
    }

    let delay = Duration::from_millis(5);
    for max_writes in [8, 32] {
        let mut samples = Vec::new();
        let mut mutation_frames = 0usize;
        for _ in 0..7 {
            let (elapsed, frames) = run(max_writes, delay);
            samples.push(elapsed);
            mutation_frames = frames;
        }
        samples.sort_unstable();
        eprintln!(
            "STAR_BATCH max_writes={max_writes} writes=32 replicas=3 delay_ms=5 mutation_frames={mutation_frames} p50_us={}",
            samples[3]
        );
    }
}

#[test]
fn star_batches_fall_back_to_scalar_requests_on_older_members() {
    use matrixobjectstore_rs::cluster::{
        message::{encode_failure, encode_reply},
        ReplicaReply, ReplicaRequest, RequestCode, StatusCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let workers: Vec<_> = listeners
        .into_iter()
        .map(|listener| {
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut codes = Vec::new();
                loop {
                    let mut header = [0; 6];
                    match stream.read_exact(&mut header) {
                        Ok(()) => {}
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::UnexpectedEof
                                    | std::io::ErrorKind::ConnectionReset
                            ) =>
                        {
                            break;
                        }
                        Err(error) => panic!("older member read failed: {error}"),
                    }
                    let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut body = vec![0; size];
                    stream.read_exact(&mut body).unwrap();
                    let code = RequestCode::from_u8(header[1]).unwrap();
                    let request = ReplicaRequest::decode(code, &body).unwrap();
                    codes.push(code);
                    match request {
                        ReplicaRequest::HeadChunkBatch { pages } => {
                            assert_eq!(pages.len(), 2);
                            stream
                                .write_all(&encode_failure(
                                    StatusCode::UnknownRequest,
                                    "older peer",
                                ))
                                .unwrap();
                        }
                        ReplicaRequest::HeadChunk { .. } => stream
                            .write_all(&encode_failure(StatusCode::NotFound, "absent"))
                            .unwrap(),
                        ReplicaRequest::WriteChunkBatchIfSequence { writes } => {
                            assert_eq!(writes.len(), 2);
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Applied))
                                .unwrap();
                        }
                        ReplicaRequest::ReadChunkBatch { reads } => {
                            assert_eq!(reads.len(), 2);
                            stream
                                .write_all(&encode_failure(
                                    StatusCode::UnknownRequest,
                                    "older peer",
                                ))
                                .unwrap();
                        }
                        ReplicaRequest::ReadChunk { page_id, .. } => {
                            let data = if page_id.page_index == 30 {
                                b"first".to_vec()
                            } else {
                                b"second".to_vec()
                            };
                            stream
                                .write_all(&encode_reply(&ReplicaReply::Read(
                                    matrixobjectstore_rs::ReplicaReadResult {
                                        data,
                                        write_sequence: 1,
                                        checksum: "same".into(),
                                    },
                                )))
                                .unwrap();
                        }
                        other => panic!("unexpected fallback request: {other:?}"),
                    }
                }
                codes
            })
        })
        .collect();
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(
        set.write_chunks(&[(page(30), 0, b"first"), (page(31), 0, b"second")])
            .unwrap(),
        vec![3, 3]
    );
    let reads = set
        .read_chunks(&[(page(30), 0, 5), (page(31), 0, 6)])
        .unwrap();
    assert_eq!(reads[0].data, b"first");
    assert_eq!(reads[1].data, b"second");
    assert!(set.read_chunks(&[]).unwrap().is_empty());
    assert!(set.write_chunks(&[]).unwrap().is_empty());
    drop(set);
    let mut codes = Vec::new();
    for worker in workers {
        codes.extend(worker.join().unwrap());
    }
    assert_eq!(
        codes
            .iter()
            .filter(|code| **code == RequestCode::HeadChunkBatch)
            .count(),
        3
    );
    assert_eq!(
        codes
            .iter()
            .filter(|code| **code == RequestCode::HeadChunk)
            .count(),
        6
    );
    assert_eq!(
        codes
            .iter()
            .filter(|code| **code == RequestCode::WriteChunkBatchIfSequence)
            .count(),
        3
    );
    assert_eq!(
        codes
            .iter()
            .filter(|code| **code == RequestCode::ReadChunkBatch)
            .count(),
        2
    );
    assert_eq!(
        codes
            .iter()
            .filter(|code| **code == RequestCode::ReadChunk)
            .count(),
        4
    );
}

#[test]
fn star_write_batch_keeps_different_matching_member_sets_separate() {
    let nodes: Vec<_> = (0..3).map(|_| start()).collect();
    for index in [0, 1] {
        client(&nodes[index])
            .write_chunk(page(20), 0, b"left")
            .unwrap();
    }
    for index in [1, 2] {
        client(&nodes[index])
            .write_chunk(page(21), 0, b"right")
            .unwrap();
    }
    let endpoints: Vec<_> = nodes.iter().map(|node| node.endpoint.clone()).collect();
    let mut set =
        RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(
        set.write_chunks(&[(page(20), 4, b"!"), (page(21), 5, b"?")])
            .unwrap(),
        vec![2, 2]
    );
    let reads = set
        .read_chunks(&[(page(20), 0, 5), (page(21), 0, 6)])
        .unwrap();
    assert_eq!(reads[0].data, b"left!");
    assert_eq!(reads[1].data, b"right?");
    drop(set);

    let mut first = client(&nodes[0]);
    assert_eq!(first.read_chunk(&page(20), 0, 5).unwrap().data, b"left!");
    assert!(matches!(
        first.read_chunk(&page(21), 0, 6),
        Err(ObjectError::NotFound(_))
    ));
    let mut middle = client(&nodes[1]);
    assert_eq!(middle.read_chunk(&page(20), 0, 5).unwrap().data, b"left!");
    assert_eq!(middle.read_chunk(&page(21), 0, 6).unwrap().data, b"right?");
    let mut last = client(&nodes[2]);
    assert!(matches!(
        last.read_chunk(&page(20), 0, 5),
        Err(ObjectError::NotFound(_))
    ));
    assert_eq!(last.read_chunk(&page(21), 0, 6).unwrap().data, b"right?");
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
fn star_write_returns_after_quorum_without_replaying_a_slow_member() {
    use matrixobjectstore_rs::cluster::{
        message::{encode_failure, encode_reply}, ReplicaReply, ReplicaRequest, RequestCode,
        StatusCode,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{mpsc, Arc, Condvar, Mutex};
    use std::time::Duration;

    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let (slow_arrived, slow_request) = mpsc::channel();
    let slow_release = Arc::new((Mutex::new(false), Condvar::new()));
    let workers: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(index, listener)| {
            let slow_arrived = slow_arrived.clone();
            let slow_release = Arc::clone(&slow_release);
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                for phase in 0..3 {
                    let mut header = [0; 6];
                    stream.read_exact(&mut header).unwrap();
                    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).unwrap();
                    let request = ReplicaRequest::decode(
                        RequestCode::from_u8(header[1]).unwrap(),
                        &body,
                    )
                    .unwrap();
                    match (phase, request) {
                        (0, ReplicaRequest::Capabilities) => stream
                            .write_all(&encode_failure(
                                StatusCode::UnknownRequest,
                                "capabilities",
                            ))
                            .unwrap(),
                        (1, ReplicaRequest::HeadChunk { .. }) => stream
                            .write_all(&encode_failure(StatusCode::NotFound, "absent"))
                            .unwrap(),
                        (2, ReplicaRequest::WriteChunkIfSequence { expected_sequence, .. }) => {
                            assert_eq!(expected_sequence, 0);
                            if index == 0 {
                                slow_arrived.send(()).unwrap();
                                let (released, ready) = &*slow_release;
                                let released = released.lock().unwrap();
                                drop(ready.wait_while(released, |released| !*released).unwrap());
                            }
                            let _ = stream.write_all(&encode_reply(&ReplicaReply::Applied));
                        }
                        other => panic!("unexpected star write phase: {other:?}"),
                    }
                }
                let mut trailing = Vec::new();
                stream.read_to_end(&mut trailing).unwrap();
                assert!(trailing.is_empty(), "a mutation was replayed");
            })
        })
        .collect();
    drop(slow_arrived);

    let (completed, completion) = mpsc::channel();
    let coordinator = std::thread::spawn(move || {
        let mut set = RemoteReplicaSet::connect(
            &endpoints,
            2,
            2,
            &ReplicaClientOptions {
                read_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .unwrap();
        completed
            .send(set.write_chunk(&page(40), 0, b"quorum"))
            .unwrap();
    });

    slow_request.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        completion.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
        2
    );
    let (released, ready) = &*slow_release;
    *released.lock().unwrap() = true;
    ready.notify_one();
    coordinator.join().unwrap();
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn star_discard_returns_after_quorum_without_replaying_a_slow_member() {
    use matrixobjectstore_rs::cluster::{
        message::encode_reply, ReplicaReply, ReplicaRequest, RequestCode,
    };
    use matrixobjectstore_rs::MatrixReplicaService;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{mpsc, Arc, Condvar, Mutex};
    use std::time::Duration;

    let mut service = MatrixReplicaService::new();
    service.write_chunk(page(42), 0, b"discard").unwrap();
    let head = service.head_chunk(&page(42)).unwrap();
    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let endpoints: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let (slow_arrived, slow_request) = mpsc::channel();
    let slow_release = Arc::new((Mutex::new(false), Condvar::new()));
    let workers: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(index, listener)| {
            let head = head.clone();
            let slow_arrived = slow_arrived.clone();
            let slow_release = Arc::clone(&slow_release);
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                for phase in 0..2 {
                    let mut header = [0; 6];
                    stream.read_exact(&mut header).unwrap();
                    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).unwrap();
                    let request = ReplicaRequest::decode(
                        RequestCode::from_u8(header[1]).unwrap(),
                        &body,
                    )
                    .unwrap();
                    match (phase, request) {
                        (0, ReplicaRequest::HeadChunk { .. }) => stream
                            .write_all(&encode_reply(&ReplicaReply::Head(head.clone())))
                            .unwrap(),
                        (
                            1,
                            ReplicaRequest::DiscardChunkIfSequence {
                                expected_sequence,
                                ..
                            },
                        ) => {
                            assert_eq!(expected_sequence, head.write_sequence);
                            if index == 0 {
                                slow_arrived.send(()).unwrap();
                                let (released, ready) = &*slow_release;
                                let released = released.lock().unwrap();
                                drop(ready.wait_while(released, |released| !*released).unwrap());
                            }
                            let _ = stream.write_all(&encode_reply(&ReplicaReply::Applied));
                        }
                        other => panic!("unexpected star discard phase: {other:?}"),
                    }
                }
                let mut trailing = Vec::new();
                stream.read_to_end(&mut trailing).unwrap();
                assert!(trailing.is_empty(), "a discard was replayed");
            })
        })
        .collect();
    drop(slow_arrived);

    let (completed, completion) = mpsc::channel();
    let coordinator = std::thread::spawn(move || {
        let mut set = RemoteReplicaSet::connect(
            &endpoints,
            2,
            2,
            &ReplicaClientOptions {
                read_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .unwrap();
        completed
            .send(set.discard_chunk(&page(42), 1, 2))
            .unwrap();
    });

    slow_request.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        completion.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
        2
    );
    let (released, ready) = &*slow_release;
    *released.lock().unwrap() = true;
    ready.notify_one();
    coordinator.join().unwrap();
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
#[ignore = "controlled three-replica latency measurement; run explicitly in release mode"]
fn star_fanout_measures_reads_and_writes_with_delayed_replies() {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};
    fn run(pipelined: bool, round: usize, delay_ms: u64) {
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
                        std::thread::sleep(Duration::from_millis(delay_ms));
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
            eprintln!("STAR_DELAY round={round} pipelined={pipelined} operation={operation} replicas=3 bytes=1024 delay_ms={delay_ms} samples=24 total_us={total} p50_us={} p95_us={} operations_per_second={:.1}", samples[11], samples[22], 24_000_000.0 / total as f64);
        }
    }
    let delay_ms = std::env::var("MATRIXOBJECT_STAR_REPLY_DELAY_MS")
        .ok()
        .map(|value| value.parse::<u64>().unwrap())
        .unwrap_or(5);
    assert!(delay_ms <= 100, "reply delay must be at most 100 ms");
    // Alternate order to reduce systematic warm-up bias. Do not assert timing ratios.
    for round in 0..3 {
        for pipelined in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            run(pipelined, round, delay_ms);
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
    assert!(
        began.elapsed() < Duration::from_millis(1000),
        "read waves exceeded one shared reply budget: {:?}",
        began.elapsed()
    );
    drop(set);
    let mut request_counts = Vec::new();
    for socket in &mut sockets {
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).unwrap();
        if bytes.is_empty() {
            request_counts.push(0);
            continue;
        }
        assert!(bytes.len() >= 6);
        assert_eq!(
            bytes[1],
            matrixobjectstore_rs::cluster::RequestCode::HeadChunk as u8
        );
        let length = u32::from_be_bytes(bytes[2..6].try_into().unwrap()) as usize;
        assert_eq!(
            bytes.len(),
            6 + length,
            "a selected member must receive exactly one request"
        );
        request_counts.push(1);
    }
    request_counts.sort_unstable();
    assert_eq!(request_counts, vec![0, 1, 1]);
}
