// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::chunk_catalog::ChunkOutcome;
use crate::cluster::MetaRequestCode;
use crate::chunk_manifest::{ChunkExtent, ChunkManifest, CHUNK_FLAG_SEALED, CHUNK_KIND_PAGED, CHUNK_MANIFEST_FORMAT};

fn chunk_manifest(key: &str, generation: u64, byte: u8) -> ChunkManifest {
    ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: key.into(),
        generation,
        length: 4096,
        extents: vec![ChunkExtent {
            group: "group-0".into(),
            volume_id: 1,
            chunk_id: 9,
            kind: CHUNK_KIND_PAGED,
            object_offset: 0,
            chunk_offset: 0,
            length: 4096,
            flags: CHUNK_FLAG_SEALED,
            digest: vec![byte; 32],
            ..Default::default()
        }],
        deleted: false,
        kind: CHUNK_KIND_PAGED,
        page_bytes: 1 << 20,
        ..Default::default()
    }
}

fn chunk_outcome(before: Option<&ChunkManifest>, after: ChunkManifest) -> ChunkOutcome {
    ChunkOutcome::decide(before.cloned(), after).unwrap()
}

fn chunk_publication(before: Option<&ChunkManifest>, after: ChunkManifest) -> MetaRequest {
    MetaRequest::PublishChunkManifest(chunk_outcome(before, after))
}

fn chunk_catalog_state(service: &MatrixMetaService) -> (Vec<u8>, u64) {
    (service.chunk_catalog.to_bytes().unwrap(), service.chunk_catalog.revision())
}

fn chunk_queue_of(requests: Vec<MetaRequest>) -> (VecDeque<Request>, Vec<mpsc::Receiver<Result<MetaReply, ObjectError>>>) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut queue = VecDeque::new();
    let mut receivers = Vec::new();
    for request in requests {
        let (request, receiver) = queued(request, deadline);
        queue.push_back(request);
        receivers.push(receiver);
    }
    (queue, receivers)
}

#[test]
fn a_chunk_publication_and_its_reply_round_trip_the_wire_and_a_malformed_one_is_refused() {
    assert_eq!(MetaRequestCode::from_u8(98), Some(MetaRequestCode::PublishChunkManifest));
    assert!(MetaRequestCode::all().contains(&MetaRequestCode::PublishChunkManifest));
    let manifest = chunk_manifest("a", 1, 1);
    let request = chunk_publication(None, manifest.clone());
    assert_eq!(request.code(), MetaRequestCode::PublishChunkManifest);
    let payload = request.encode_payload();
    assert_eq!(MetaRequest::decode(MetaRequestCode::PublishChunkManifest, &payload).unwrap(), request);
    // a payload that is not an outcome, one cut short and one with a byte changed are refused
    assert!(MetaRequest::decode(MetaRequestCode::PublishChunkManifest, &[]).is_err());
    assert!(MetaRequest::decode(MetaRequestCode::PublishChunkManifest, &payload[..payload.len() - 3]).is_err());
    let mut damaged = payload.clone();
    let middle = damaged.len() / 2;
    damaged[middle] ^= 0xFF;
    assert!(MetaRequest::decode(MetaRequestCode::PublishChunkManifest, &damaged).is_err());

    let reply = MetaReply::ChunkManifest(manifest);
    let bytes = reply.encode_payload();
    assert_eq!(reply.payload_len(), bytes.len());
    assert_eq!(MetaReply::decode(MetaRequestCode::PublishChunkManifest, &bytes).unwrap(), reply);
    assert!(MetaReply::decode(MetaRequestCode::PublishChunkManifest, &bytes[..bytes.len() - 1]).is_err());
}

#[test]
fn a_batch_of_publications_is_prepared_against_the_prefix_and_applied_only_when_committed() {
    let (_files, mut peer) = batch_peer();
    let (mut service, _groups) = fixture(1, 0);
    let groups = ChunkGroupCatalog::default();
    let first = chunk_manifest("a", 1, 1);
    let second = chunk_manifest("b", 1, 2);
    let first_next = chunk_manifest("a", 2, 3);
    // the third publication is decided from the first one's result, which is not committed yet
    let (mut queue, receivers) = chunk_queue_of(vec![
        chunk_publication(None, first.clone()),
        chunk_publication(None, second.clone()),
        chunk_publication(Some(&first), first_next.clone()),
    ]);
    let before = chunk_catalog_state(&service);
    let mut pending = VecDeque::new();
    propose_chunk_manifest_batch(&mut peer, &mut service, &mut queue, &mut pending).unwrap();
    assert_eq!(pending.len(), 3);
    assert!(queue.is_empty());
    assert_eq!(chunk_catalog_state(&service), before, "nothing is visible until the entries commit");
    assert!(receivers.iter().all(|receiver| receiver.try_recv().is_err()));

    peer.process_ready(|id, bytes| apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)).unwrap();
    assert!(pending.is_empty());
    let published = |index: usize| match receivers[index].try_recv().unwrap().unwrap() {
        MetaReply::ChunkManifest(manifest) => manifest,
        other => panic!("a chunk manifest was expected: {other:?}"),
    };
    assert_eq!(published(0), first);
    assert_eq!(published(1), second);
    assert_eq!(published(2), first_next);
    assert_eq!(**service.chunk_catalog.get(&first.object_id()).unwrap(), first_next);
    assert_eq!(**service.chunk_catalog.get(&second.object_id()).unwrap(), second);
}

#[test]
fn a_publication_the_catalog_refuses_is_answered_alone_and_the_others_still_commit() {
    let (_files, mut peer) = batch_peer();
    let (mut service, _groups) = fixture(1, 0);
    let groups = ChunkGroupCatalog::default();
    let first = chunk_manifest("a", 1, 1);
    // decided against an empty catalog too, but for different bytes: once the first is applied to the
    // draft, this one was decided from a state that no longer exists
    let rival = chunk_manifest("a", 1, 9);
    let other = chunk_manifest("b", 1, 2);
    let (mut queue, receivers) = chunk_queue_of(vec![
        chunk_publication(None, first.clone()),
        chunk_publication(None, rival),
        chunk_publication(None, other.clone()),
    ]);
    let mut pending = VecDeque::new();
    propose_chunk_manifest_batch(&mut peer, &mut service, &mut queue, &mut pending).unwrap();
    assert_eq!(pending.len(), 3, "the refusal waits behind the commit of the prefix it was decided against");
    peer.process_ready(|id, bytes| apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)).unwrap();
    assert!(receivers[0].try_recv().unwrap().is_ok());
    assert!(receivers[1].try_recv().unwrap().is_err(), "the rival is refused");
    assert!(receivers[2].try_recv().unwrap().is_ok());
    assert_eq!(**service.chunk_catalog.get(&first.object_id()).unwrap(), first, "the first writer won");
    assert_eq!(**service.chunk_catalog.get(&other.object_id()).unwrap(), other);
}

#[test]
fn a_replayed_publication_changes_nothing_and_answers_with_the_stored_manifest() {
    let (_files, mut peer) = batch_peer();
    let (mut service, _groups) = fixture(1, 0);
    let groups = ChunkGroupCatalog::default();
    let manifest = chunk_manifest("a", 1, 1);
    for _ in 0..2 {
        let (mut queue, receivers) = chunk_queue_of(vec![chunk_publication(None, manifest.clone())]);
        let mut pending = VecDeque::new();
        propose_chunk_manifest_batch(&mut peer, &mut service, &mut queue, &mut pending).unwrap();
        peer.process_ready(|id, bytes| apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)).unwrap();
        match receivers[0].try_recv().unwrap().unwrap() {
            MetaReply::ChunkManifest(answer) => assert_eq!(answer, manifest),
            other => panic!("a chunk manifest was expected: {other:?}"),
        }
        assert_eq!(service.chunk_catalog.revision(), 1, "the second delivery is a replay and moves nothing");
    }
}

/// The rule for every new command (the refuse-not-abort path): a prepared outcome that fails its
/// draft apply is refused for its own request only, with an error that says nothing was published;
/// the worker survives, the outcome before it is still proposed, the live catalog is exactly as it
/// was, and the request behind it stays queued for the next round.
#[test]
fn a_chunk_outcome_that_fails_to_apply_after_preparation_is_refused_alone_and_the_worker_survives() {
    let (_files, mut peer) = batch_peer();
    let (mut service, _groups) = fixture(1, 0);
    let before = chunk_catalog_state(&service);
    let (mut queue, receivers) = chunk_queue_of(vec![
        chunk_publication(None, chunk_manifest("a", 1, 1)),
        chunk_publication(None, chunk_manifest("b", 1, 2)),
        chunk_publication(None, chunk_manifest("c", 1, 3)),
    ]);
    CHUNK_APPLY_FAULT.with(|fault| fault.set(Some(1)));
    let mut pending = VecDeque::new();
    let result = propose_chunk_manifest_batch(&mut peer, &mut service, &mut queue, &mut pending);
    CHUNK_APPLY_FAULT.with(|fault| fault.set(None));
    result.expect("a failing apply of one outcome must not abort the worker");
    assert_eq!(pending.len(), 1, "only the outcome before the failure is proposed");
    match receivers[1].try_recv() {
        Ok(Err(ObjectError::PreconditionFailed(message))) => {
            assert!(message.contains("nothing was published"), "{message}");
        }
        other => panic!("the failing request must be refused with a typed error: {other:?}"),
    }
    assert!(receivers[0].try_recv().is_err() && receivers[2].try_recv().is_err(), "nobody else was answered");
    assert_eq!(queue.len(), 1, "the request behind the failure stays queued");
    assert_eq!(chunk_catalog_state(&service), before, "the live catalog is restored exactly");
    // the next round prepares the remaining request against the clean state
    let mut next = VecDeque::new();
    propose_chunk_manifest_batch(&mut peer, &mut service, &mut queue, &mut next).unwrap();
    assert_eq!(next.len(), 1);
    assert!(queue.is_empty());
    assert_eq!(chunk_catalog_state(&service), before);
}

/// `prepare` validates everything `apply` will check, so a failing apply is a defect and not a
/// normal answer: for every outcome here, the leader's decision and a voter's apply agree.
#[test]
fn what_the_leader_accepts_every_voter_applies_and_what_it_refuses_no_voter_would() {
    let (mut service, _groups) = fixture(1, 0);
    let first = chunk_manifest("a", 1, 1);
    super::super::chunk_manifest_command::apply(&mut service, &chunk_outcome(None, first.clone())).unwrap();
    let deleted = ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: "a".into(),
        generation: 2,
        deleted: true,
        ..Default::default()
    };
    let cases = vec![
        // decided from the current manifest: accepted
        chunk_outcome(Some(&first), chunk_manifest("a", 2, 2)),
        // a new object: accepted
        chunk_outcome(None, chunk_manifest("b", 1, 3)),
        // a replay of what is stored: nothing to change, nothing to refuse
        chunk_outcome(None, first.clone()),
        // decided from a generation that is not the stored one
        chunk_outcome(Some(&chunk_manifest("a", 1, 7)), chunk_manifest("a", 2, 2)),
        // a new object decided as if it replaced one
        chunk_outcome(Some(&chunk_manifest("c", 1, 7)), chunk_manifest("c", 2, 2)),
        // a delete of the stored object: accepted
        chunk_outcome(Some(&first), deleted),
    ];
    for case in cases {
        let decided = super::super::chunk_manifest_command::prepare(&service, &case);
        let mut voter = service.clone();
        let applied = super::super::chunk_manifest_command::apply(&mut voter, &case);
        assert_eq!(decided.is_ok(), applied.is_ok(), "the leader and a voter disagree about {case:?}");
        if let (Ok(super::super::chunk_manifest_command::Decision::Change(..)), Ok(changed)) = (&decided, &applied) {
            assert!(*changed);
        }
        if let (Ok(super::super::chunk_manifest_command::Decision::Unchanged(..)), Ok(changed)) = (&decided, &applied) {
            assert!(!*changed);
        }
    }
}

#[test]
fn an_entry_that_names_more_than_one_kind_of_change_is_refused_by_every_voter() {
    let (mut service, _groups) = fixture(1, 0);
    let groups = ChunkGroupCatalog::default();
    let encoded = chunk_outcome(None, chunk_manifest("a", 1, 1)).to_bytes().unwrap();
    let id = || LogId { index: 1, term: 1 };
    let entry = |version: u32, chunk_outcome: Vec<u8>, outcome: Vec<u8>| {
        Entry { version, chunk_outcome, outcome, catalog_outcome: Vec::new() }.encode_to_vec()
    };
    let mut pending = VecDeque::new();
    // a chunk entry that also carries a placement outcome, one that carries nothing, and an old kind
    // that carries a chunk outcome are all refused, and the catalog does not move
    for bytes in [
        entry(3, encoded.clone(), vec![1, 2, 3]),
        entry(3, Vec::new(), Vec::new()),
        entry(1, encoded.clone(), Vec::new()),
    ] {
        assert!(apply_committed(&mut service, &groups, 65536, &mut pending, id(), &bytes).is_err());
        assert!(service.chunk_catalog.is_empty());
    }
    // the same outcome in a well-formed entry is applied
    apply_committed(&mut service, &groups, 65536, &mut pending, id(), &entry(3, encoded, Vec::new())).unwrap();
    assert_eq!(service.chunk_catalog.len(), 1);
}
