// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn write(index: u64, value: u8, size: usize) -> ReplicaRequest {
    ReplicaRequest::WriteChunk {
        page_id: crate::PageId {
            chunk_id: crate::ChunkId::new(1, 1).unwrap(),
            page_index: index,
        },
        offset: 0,
        data: vec![value; size],
    }
}
fn request(command: ReplicaRequest) -> (Request, Receiver<Result<ReplicaReply, ObjectError>>) {
    let (reply, response) = mpsc::sync_channel(1);
    (
        Request {
            progress: RequestProgress::new(read_only(&command)),
            request: command,
            reply,
            deadline: Instant::now() + Duration::from_secs(30),
        },
        response,
    )
}
fn service() -> MatrixReplicaService {
    let mut service = MatrixReplicaService::new();
    service.set_capacity_bytes(32 * 1024 * 1024).unwrap();
    service
}

#[test]
fn write_batch_preserves_live_state_and_replays_same_page_in_order() {
    let live = service();
    let original = live.clone();
    let mut expected = live.clone();
    let commands = [
        write(0, 1, 64),
        write(0, 2, 64),
        ReplicaRequest::WriteChunkIfSequence {
            page_id: crate::PageId {
                chunk_id: crate::ChunkId::new(1, 1).unwrap(),
                page_index: 0,
            },
            offset: 0,
            data: vec![3; 64],
            expected_sequence: 99,
        },
        write(1, 4, 64),
    ];
    let expected_replies: Vec<_> = commands
        .iter()
        .map(|command| {
            checked_apply(&mut expected, command.clone(), 64, 8 * 1024 * 1024)
                .map_err(|error| error.to_string())
        })
        .collect();
    let (tx, rx) = mpsc::sync_channel(8);
    let (first, first_reply) = request(commands[0].clone());
    let mut receivers = vec![first_reply];
    for command in &commands[1..] {
        let (request, reply) = request(command.clone());
        tx.send(request).unwrap();
        receivers.push(reply);
    }
    let batch = prepare_write_batch(&live, 64, 8 * 1024 * 1024, first, &rx, &mut None).unwrap();
    assert_eq!(batch.len(), commands.len());
    assert_eq!(live, original);
    assert!(receivers
        .iter()
        .all(|reply| matches!(reply.try_recv(), Err(mpsc::TryRecvError::Empty))));
    let mut applied = live.clone();
    for ((payload, (_, reply)), expected_reply) in batch.iter().zip(expected_replies) {
        assert_eq!(
            reply.as_ref().unwrap().as_ref().map_err(|e| e.to_string()),
            expected_reply.as_ref().map_err(Clone::clone)
        );
        let entry = Entry::decode(payload.as_slice()).unwrap();
        if !entry.outcome.is_empty() {
            PageOutcome::from_payload(&entry.outcome)
                .unwrap()
                .apply(&mut applied, 64)
                .unwrap();
        }
    }
    assert_eq!(applied.export_snapshot(), expected.export_snapshot());
    drop(batch);
    assert_eq!(live, original);
}

#[test]
fn write_batch_stops_at_reads_and_expires_unadmitted_requests() {
    for barrier in [
        ReplicaRequest::Ping,
        ReplicaRequest::Membership(crate::cluster::MembershipRequest {
            version: 1,
            bootstrap_id: 7,
            operation: crate::cluster::MembershipOperation::Promote as i32,
            node_id: 4,
        }),
    ] {
        let live = service();
        let (tx, rx) = mpsc::sync_channel(8);
        let (first, _reply) = request(write(0, 1, 16));
        let (mut expired, expired_reply) = request(write(1, 2, 16));
        expired.deadline = Instant::now() - Duration::from_secs(1);
        tx.send(expired).unwrap();
        tx.send(request(barrier.clone()).0).unwrap();
        tx.send(request(write(2, 3, 16)).0).unwrap();
        let mut deferred = None;
        let batch = prepare_write_batch(&live, 64, 65536, first, &rx, &mut deferred).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(expired_reply.try_recv().unwrap().is_err());
        assert_eq!(deferred.unwrap().request, barrier);
        assert!(matches!(
            rx.try_recv().unwrap().request,
            ReplicaRequest::WriteChunk { .. }
        ));
    }
}

#[test]
fn write_batch_bounds_count_and_bytes_without_consuming_the_next_request() {
    for size in [16, 1024 * 1024] {
        let live = service();
        let original = live.clone();
        let (tx, rx) = mpsc::sync_channel(40);
        for index in 1..40 {
            tx.send(request(write(index, 1, size)).0).unwrap();
        }
        let mut deferred = None;
        let batch = prepare_write_batch(
            &live,
            64,
            8 * 1024 * 1024,
            request(write(0, 1, size)).0,
            &rx,
            &mut deferred,
        )
        .unwrap();
        assert!(
            batch
                .iter()
                .map(|(payload, _)| payload.len())
                .sum::<usize>()
                <= 4 * 1024 * 1024
        );
        if size == 16 {
            assert_eq!(batch.len(), 32);
        } else {
            assert!(!batch.is_empty() && batch.len() < 4);
            assert!(deferred.is_some());
        }
        assert_eq!(
            batch.len() + usize::from(deferred.is_some()) + rx.try_iter().count(),
            40
        );
        assert_eq!(live, original);
    }
}
