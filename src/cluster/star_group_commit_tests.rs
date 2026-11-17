// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::super::super::message::ConditionalChunkWrite;
use super::super::append_outcome_tests::{page, Fixture};
use super::*;

fn job(request: ReplicaRequest, wire_bytes: usize) -> (Job, Receiver<ResultReply>) {
    let (reply, receive) = mpsc::sync_channel(1);
    (
        Job {
            request: Some(request),
            result: None,
            wire_bytes,
            reply,
        },
        receive,
    )
}

fn write(index: u64, offset: u64, data: &[u8], expected_sequence: u64) -> ReplicaRequest {
    let mut page_id = page();
    page_id.page_index = index;
    ReplicaRequest::WriteChunkIfSequence {
        page_id,
        offset,
        data: data.to_vec(),
        expected_sequence,
    }
}

fn base(fixture: &Fixture) {
    fixture
        .request(ReplicaRequest::WriteChunk {
            page_id: page(),
            offset: 0,
            data: b"base".to_vec(),
        })
        .unwrap();
}

#[test]
fn shared_sync_preserves_order_batch_digests_refusals_and_recovery() {
    let fixture = Fixture::new();
    base(&fixture);
    let before = fixture
        .node
        .lock()
        .unwrap()
        .journal
        .as_ref()
        .unwrap()
        .sync_count();
    let mut jobs = Vec::new();
    let mut receivers = Vec::new();
    let requests = [
        ReplicaRequest::WriteChunkBatchIfSequence {
            writes: vec![
                ConditionalChunkWrite {
                    page_id: page(),
                    offset: 4,
                    data: b"one".to_vec(),
                    expected_sequence: 1,
                },
                ConditionalChunkWrite {
                    page_id: page(),
                    offset: 7,
                    data: b"two".to_vec(),
                    expected_sequence: 2,
                },
            ],
        },
        ReplicaRequest::WriteChunkBatchIfSequenceDigest {
            writes: vec![ConditionalChunkWrite {
                page_id: page(),
                offset: 10,
                data: b"last".to_vec(),
                expected_sequence: 3,
            }],
        },
        write(0, 14, b"bad", 1),
        write(1, 0, b"other", 0),
    ];
    for request in requests {
        let (job, receive) = job(request, 256);
        jobs.push(job);
        receivers.push(receive);
    }
    commit_jobs(&fixture.node, &mut jobs, MAX_PAYLOAD_BYTES, |guard| {
        assert_eq!(guard.journal.as_ref().unwrap().sync_count(), before);
        for receive in &receivers {
            assert!(
                matches!(receive.try_recv(), Err(mpsc::TryRecvError::Empty)),
                "reply escaped before sync"
            );
        }
        finish_deferred_journal(guard)
    });
    let results = receivers
        .iter()
        .map(|rx| rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .collect::<Vec<_>>();
    assert!(matches!(results[0], Ok(ReplicaReply::Applied)));
    assert!(
        matches!(&results[1], Ok(ReplicaReply::Digests(values)) if values.len() == 1 && values[0].0 == 4 && values[0].1 == 4)
    );
    assert!(matches!(
        results[2],
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert!(matches!(results[3], Ok(ReplicaReply::Applied)));
    {
        let mut guard = fixture.node.lock().unwrap();
        assert_eq!(guard.journal.as_ref().unwrap().sync_count(), before + 1);
        assert_eq!(guard.used_bytes, 19);
        guard.journal = None;
    }
    let mut recovered = MatrixReplicaService::new();
    recovered.set_keep_ec_sidecar(false);
    drop(ReplicaJournal::open(&fixture.path, &mut recovered).unwrap());
    assert_eq!(
        recovered.read_chunk(&page(), 0, 14).unwrap().data,
        b"baseonetwolast"
    );
    assert_eq!(recovered.head_chunk(&page()).unwrap().write_sequence, 4);
    let mut second = page();
    second.page_index = 1;
    assert_eq!(recovered.read_chunk(&second, 0, 5).unwrap().data, b"other");
}

#[test]
fn shared_sync_failure_releases_no_successes_and_fences_later_reads() {
    let fixture = Fixture::new();
    base(&fixture);
    let mut jobs = Vec::new();
    let mut receivers = Vec::new();
    for request in [write(0, 4, b"tail", 1), write(0, 8, b"more", 2)] {
        let (job, receive) = job(request, 128);
        jobs.push(job);
        receivers.push(receive);
    }
    commit_jobs(&fixture.node, &mut jobs, MAX_PAYLOAD_BYTES, |_| {
        assert!(receivers
            .iter()
            .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
        Err(ObjectError::Corruption(
            "injected synchronization failure".into(),
        ))
    });
    for receive in receivers {
        assert!(matches!(
            receive.recv_timeout(Duration::from_secs(5)).unwrap(),
            Err(ObjectError::Corruption(_))
        ));
    }
    assert!(fixture.node.lock().unwrap().failed);
    assert!(fixture.request(ReplicaRequest::Ping).is_err());
}

#[test]
fn shared_sync_append_failure_does_not_flush_or_ack_a_failed_group() {
    let fixture = Fixture::new();
    base(&fixture);
    let disk = std::fs::read(&fixture.path).unwrap();
    fixture.node.lock().unwrap().journal = Some(ReplicaJournal::read_only_for_test(&fixture.path));
    let (first, first_reply) = job(write(0, 4, b"tail", 1), 128);
    let (second, second_reply) = job(write(1, 0, b"other", 0), 128);
    commit_jobs(
        &fixture.node,
        &mut vec![first, second],
        MAX_PAYLOAD_BYTES,
        |_| panic!("failed group must not synchronize or checkpoint"),
    );
    for reply in [first_reply, second_reply] {
        assert!(matches!(
            reply.recv_timeout(Duration::from_secs(5)).unwrap(),
            Err(ObjectError::Corruption(_))
        ));
    }
    assert!(fixture.node.lock().unwrap().failed);
    assert_eq!(std::fs::read(&fixture.path).unwrap(), disk);
}

#[test]
fn shared_sync_refused_requests_do_not_force_a_journal_sync() {
    let fixture = Fixture::new();
    base(&fixture);
    let before = fixture
        .node
        .lock()
        .unwrap()
        .journal
        .as_ref()
        .unwrap()
        .sync_count();
    let (request, receive) = job(write(0, 4, b"stale", 0), 128);
    commit_jobs(&fixture.node, &mut vec![request], MAX_PAYLOAD_BYTES, |_| {
        panic!("refused request must not sync")
    });
    assert!(matches!(
        receive.recv_timeout(Duration::from_secs(5)).unwrap(),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_eq!(
        fixture
            .node
            .lock()
            .unwrap()
            .journal
            .as_ref()
            .unwrap()
            .sync_count(),
        before
    );
}

#[test]
fn shared_sync_groups_bound_request_count_and_payload_bytes() {
    for (count, weight, limit, expected_syncs) in [(70, 1, MAX_PAYLOAD_BYTES, 2), (4, 600, 1024, 4)]
    {
        let fixture = Fixture::new();
        fixture.node.lock().unwrap().max_pages = 100;
        let (send, receive) = mpsc::sync_channel(count);
        let mut replies = Vec::new();
        for index in 0..count {
            let (request, reply) = job(write(index as u64, 0, b"data", 0), weight);
            send.send(request).unwrap();
            replies.push(reply);
        }
        drop(send);
        run(receive, Arc::clone(&fixture.node), limit);
        for reply in replies {
            assert!(reply.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
        }
        assert_eq!(
            fixture
                .node
                .lock()
                .unwrap()
                .journal
                .as_ref()
                .unwrap()
                .sync_count(),
            expected_syncs
        );
    }
}

#[test]
fn shared_sync_idle_worker_completes_one_request_and_joins() {
    let fixture = Fixture::new();
    let committer = Committer::start(Arc::clone(&fixture.node), MAX_PAYLOAD_BYTES).unwrap();
    assert!(committer.request(write(0, 0, b"data", 0), 128).is_ok());
    drop(committer);
    assert_eq!(
        fixture
            .node
            .lock()
            .unwrap()
            .journal
            .as_ref()
            .unwrap()
            .sync_count(),
        1
    );
}

#[test]
fn shared_sync_counts_mutations_inside_each_request_batch() {
    let fixture = Fixture::new();
    fixture.node.lock().unwrap().max_pages = 100;
    let (send, receive) = mpsc::sync_channel(3);
    let mut replies = Vec::new();
    for batch in 0..3 {
        let writes = (0..32)
            .map(|index| {
                let mut page_id = page();
                page_id.page_index = batch * 32 + index;
                ConditionalChunkWrite {
                    page_id,
                    offset: 0,
                    data: b"data".to_vec(),
                    expected_sequence: 0,
                }
            })
            .collect();
        let (request, reply) = job(ReplicaRequest::WriteChunkBatchIfSequence { writes }, 2048);
        send.send(request).unwrap();
        replies.push(reply);
    }
    drop(send);
    run(receive, Arc::clone(&fixture.node), MAX_PAYLOAD_BYTES);
    for reply in replies {
        assert!(reply.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
    }
    let guard = fixture.node.lock().unwrap();
    assert_eq!(guard.journal.as_ref().unwrap().sync_count(), 2);
    assert_eq!(guard.service.page_count(), 96);
}

#[test]
fn shared_sync_failed_service_cannot_publish_a_checkpoint() {
    let fixture = Fixture::new();
    base(&fixture);
    let disk = std::fs::read(&fixture.path).unwrap();
    let mut guard = fixture.node.lock().unwrap();
    let before = guard.journal.as_ref().unwrap().sync_count();
    guard
        .service
        .write_chunk(page(), 4, b"unjournaled")
        .unwrap();
    guard.journal_checkpoint_after_bytes = 1;
    guard.failed = true;
    assert!(finish_deferred_journal(&mut guard).is_err());
    assert_eq!(guard.journal.as_ref().unwrap().sync_count(), before);
    drop(guard);
    assert_eq!(std::fs::read(&fixture.path).unwrap(), disk);
}

#[test]
fn shared_sync_busy_service_does_not_block_server_startup_shutdown() {
    let fixture = Fixture::new();
    let path = fixture.path.with_file_name("shutdown.journal");
    let server =
        ReplicaNodeServer::bind_with_journal("127.0.0.1:0", ReplicaNodeOptions::default(), &path)
            .unwrap();
    let state = Arc::clone(&server.replica);
    let stop = server.shutdown_handle();
    let guard = state.lock().unwrap();
    let (send, receive) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let result = server.serve();
        send.send(()).unwrap();
        result
    });
    stop.stop();
    let finished = receive.recv_timeout(Duration::from_secs(5)).is_ok();
    drop(guard);
    worker.join().unwrap().unwrap();
    assert!(
        finished,
        "journal worker startup or shutdown waited for service state"
    );
}

#[test]
fn shared_sync_concurrent_conditional_requests_have_one_durable_winner() {
    let fixture = Fixture::new();
    base(&fixture);
    let before = fixture
        .node
        .lock()
        .unwrap()
        .journal
        .as_ref()
        .unwrap()
        .sync_count();
    let committer = Committer::start(Arc::clone(&fixture.node), MAX_PAYLOAD_BYTES).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(17));
    let (done, results) = mpsc::channel();
    let mut workers = Vec::new();
    for _ in 0..16 {
        let committer = Arc::clone(&committer);
        let barrier = Arc::clone(&barrier);
        let done = done.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            done.send(committer.request(write(0, 4, b"ok", 1), 128))
                .unwrap();
        }));
    }
    drop(done);
    barrier.wait();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let values = (0..16)
        .map(|_| {
            results
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .expect("conditional requests stalled")
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(values.iter().filter(|value| value.is_ok()).count(), 1);
    assert_eq!(
        values
            .iter()
            .filter(|value| matches!(value, Err(ObjectError::PreconditionFailed(_))))
            .count(),
        15
    );
    assert_eq!(committer.offered_requests.load(Ordering::Acquire), 0);
    let guard = fixture.node.lock().unwrap();
    assert_eq!(guard.journal.as_ref().unwrap().sync_count(), before + 1);
    assert_eq!(
        guard.service.read_chunk(&page(), 0, 6).unwrap().data,
        b"baseok"
    );
    assert_eq!(guard.service.head_chunk(&page()).unwrap().write_sequence, 2);
}

#[test]
fn shared_sync_idle_path_refuses_failed_persistence() {
    let fixture = Fixture::new();
    base(&fixture);
    let disk = std::fs::read(&fixture.path).unwrap();
    let committer = Committer::start(Arc::clone(&fixture.node), MAX_PAYLOAD_BYTES).unwrap();
    fixture.node.lock().unwrap().journal = Some(ReplicaJournal::read_only_for_test(&fixture.path));
    assert!(matches!(
        committer.request(write(0, 4, b"tail", 1), 128),
        Err(ObjectError::Corruption(_))
    ));
    assert!(fixture.node.lock().unwrap().failed);
    assert_eq!(committer.offered_requests.load(Ordering::Acquire), 0);
    assert!(fixture.request(ReplicaRequest::Ping).is_err());
    assert_eq!(std::fs::read(&fixture.path).unwrap(), disk);
}

#[test]
fn shared_sync_stopped_worker_prevents_idle_writes() {
    let fixture = Fixture::new();
    base(&fixture);
    let disk = std::fs::read(&fixture.path).unwrap();
    let mut committer = match Arc::try_unwrap(
        Committer::start(Arc::clone(&fixture.node), MAX_PAYLOAD_BYTES).unwrap(),
    ) {
        Ok(committer) => committer,
        Err(_) => panic!("unexpected committer owner"),
    };
    let (closed_sender, closed_receiver) = mpsc::sync_channel(0);
    drop(closed_receiver);
    drop(committer.sender.replace(closed_sender));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !committer.worker.as_ref().unwrap().is_finished() {
        assert!(
            std::time::Instant::now() < deadline,
            "journal worker did not stop"
        );
        std::thread::yield_now();
    }
    assert!(matches!(
        committer.request(write(0, 4, b"tail", 1), 128),
        Err(ObjectError::Corruption(_))
    ));
    assert!(fixture.node.lock().unwrap().failed);
    assert!(fixture.request(ReplicaRequest::Ping).is_err());
    assert_eq!(std::fs::read(&fixture.path).unwrap(), disk);
}
