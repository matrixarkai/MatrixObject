// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The client against a door that can lose a reply after committing, refuse before committing, and
//! answer a resolve the way the gateway documents. The same contract is run against the real
//! gateway handler in the gateway's own tests.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use crate::transport::{Reply, Transport, TransportError};
use crate::wire::{
    AttemptId, Resolution, ResolveRequest, ATTEMPT_HEADER, OBJECT_SIZE_HEADER, RESOLUTION_HEADER,
    WRITE_OFFSET_HEADER,
};
use crate::{AppendResolutions, ClientError, S3Client, TcpTransport};

/// What the fake door does with the next append it receives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    /// Commit, then fail to answer at all (a dropped connection).
    DropReplyAfterCommit,
    /// Commit, then answer 503.
    Answer503AfterCommit,
    /// Answer 503 without committing.
    Answer503Uncommitted,
}

#[derive(Default, Debug)]
struct State {
    objects: HashMap<String, Vec<u8>>,
    /// Attempts this door committed, by id: where and how long.
    landed: HashMap<[u8; 16], (String, u64, u64)>,
    /// Attempts fenced out by an `absent` answer: they may never land.
    fenced: HashSet<[u8; 16]>,
    faults: Vec<Fault>,
    puts: usize,
    resolves: usize,
    resolve_supported: bool,
    /// Answer every resolve as ambiguous.
    always_ambiguous: bool,
}

#[derive(Clone, Debug)]
struct FakeDoor(Arc<Mutex<State>>);

impl FakeDoor {
    fn new() -> Self {
        FakeDoor(Arc::new(Mutex::new(State { resolve_supported: true, ..State::default() })))
    }

    fn fault(&self, fault: Fault) {
        self.0.lock().unwrap().faults.push(fault);
    }

    fn object(&self, key: &str) -> Vec<u8> {
        self.0.lock().unwrap().objects.get(key).cloned().unwrap_or_default()
    }

    fn puts(&self) -> usize {
        self.0.lock().unwrap().puts
    }
}

fn reply(status: u16, headers: Vec<(&str, String)>, body: &[u8]) -> Reply {
    Reply {
        status,
        headers: headers.into_iter().map(|(n, v)| (n.to_string(), v)).collect(),
        body: body.to_vec(),
    }
}

fn split(target: &str) -> (&str, &str) {
    target.split_once('?').unwrap_or((target, ""))
}

fn key_of(path: &str) -> String {
    path.trim_start_matches('/').split_once('/').map(|(_, key)| key.to_string()).unwrap_or_default()
}

fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').filter_map(|pair| pair.split_once('=')).find(|(k, _)| *k == name).map(|(_, v)| v.to_string())
}

impl Transport for FakeDoor {
    fn send(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply, TransportError> {
        let header = |name: &str| headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.clone());
        let (path, query) = split(target);
        let mut state = self.0.lock().unwrap();
        match method {
            "HEAD" => Ok(match state.objects.get(&key_of(path)) {
                Some(object) => reply(200, vec![(OBJECT_SIZE_HEADER, object.len().to_string())], b""),
                None => reply(404, vec![], b""),
            }),
            "GET" if query.contains("list-type=2") => {
                let prefix = query_param(query, "prefix").unwrap_or_default().replace("%2F", "/");
                let after = query_param(query, "continuation-token").or_else(|| query_param(query, "start-after"));
                let mut keys: Vec<&String> = state.objects.keys().filter(|k| k.starts_with(&prefix)).collect();
                keys.sort();
                let keys: Vec<&String> = keys.into_iter().filter(|k| after.as_ref().map_or(true, |a| k.as_str() > a.replace("%2F", "/").as_str())).collect();
                let page: Vec<&&String> = keys.iter().take(2).collect();
                let truncated = keys.len() > 2;
                let mut xml = String::from("<ListBucketResult>");
                for key in &page {
                    xml.push_str(&format!("<Contents><Key>{key}</Key></Contents>"));
                }
                xml.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
                if truncated {
                    xml.push_str(&format!("<NextContinuationToken>{}</NextContinuationToken>", page.last().unwrap()));
                }
                Ok(reply(200, vec![], xml.as_bytes()))
            }
            "GET" => {
                let Some(object) = state.objects.get(&key_of(path)) else { return Ok(reply(404, vec![], b"")) };
                match header("range").and_then(|r| r.strip_prefix("bytes=").map(str::to_string)) {
                    None => Ok(reply(200, vec![], object)),
                    Some(range) => {
                        let (from, to) = range.split_once('-').unwrap();
                        let from: usize = from.parse().unwrap();
                        if from >= object.len() {
                            return Ok(reply(416, vec![("content-range", format!("bytes */{}", object.len()))], b""));
                        }
                        let to = to.parse::<usize>().map_or(object.len(), |t| (t + 1).min(object.len()));
                        Ok(reply(206, vec![], &object[from..to]))
                    }
                }
            }
            "DELETE" => {
                state.objects.remove(&key_of(path));
                Ok(reply(204, vec![], b""))
            }
            "POST" => {
                state.resolves += 1;
                if !state.resolve_supported {
                    return Ok(reply(405, vec![], b""));
                }
                let request = ResolveRequest::parse(|name| query_param(query, name)).expect("a well-formed resolve");
                let id = request.attempt.into_bytes();
                let key = key_of(path);
                let word = if state.always_ambiguous {
                    Resolution::Ambiguous
                } else if state.landed.get(&id) == Some(&(key.clone(), request.offset, request.length)) {
                    Resolution::Landed
                } else {
                    // Not this attempt's: equal bytes there are somebody else's and cannot be proved
                    // otherwise; anything else is absent, and the attempt is fenced from now on.
                    let object = state.objects.get(&key).cloned().unwrap_or_default();
                    let (from, to) = (request.offset as usize, (request.offset + request.length) as usize);
                    let equal = object.len() >= to && sha(&object[from..to]) == request.sha256.unwrap();
                    if equal {
                        Resolution::Ambiguous
                    } else {
                        state.fenced.insert(id);
                        Resolution::Absent
                    }
                };
                Ok(reply(200, vec![(RESOLUTION_HEADER, word.word().to_string())], word.word().as_bytes()))
            }
            "PUT" => {
                let key = key_of(path);
                let Some(offset) = header(WRITE_OFFSET_HEADER) else {
                    state.objects.insert(key, body.to_vec());
                    return Ok(reply(200, vec![], b""));
                };
                state.puts += 1;
                let offset: u64 = offset.parse().unwrap();
                let id = AttemptId::parse(&header(ATTEMPT_HEADER).expect("every append carries an attempt id"))
                    .expect("a well-formed attempt id")
                    .into_bytes();
                let fault = if state.faults.is_empty() { None } else { Some(state.faults.remove(0)) };
                if fault == Some(Fault::Answer503Uncommitted) || state.fenced.contains(&id) {
                    return Ok(reply(503, vec![], b"busy"));
                }
                let length = state.objects.get(&key).map_or(0, |o| o.len() as u64);
                if offset != length {
                    return Ok(reply(412, vec![], b"offset is not the end"));
                }
                state.objects.entry(key.clone()).or_default().extend_from_slice(body);
                state.landed.insert(id, (key.clone(), offset, body.len() as u64));
                let size = offset + body.len() as u64;
                match fault {
                    Some(Fault::DropReplyAfterCommit) => Err(TransportError("connection reset".into())),
                    Some(Fault::Answer503AfterCommit) => Ok(reply(503, vec![], b"busy")),
                    _ => Ok(reply(200, vec![(OBJECT_SIZE_HEADER, size.to_string())], b"")),
                }
            }
            other => panic!("unexpected {other}"),
        }
    }
}

fn sha(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

fn client(door: &FakeDoor) -> S3Client<FakeDoor> {
    S3Client::new(door.clone(), "b")
}

#[test]
fn an_append_creates_the_object_and_reports_where_it_landed() {
    let door = FakeDoor::new();
    let c = client(&door);
    let first = c.append("log", b"abc").unwrap();
    assert_eq!((first.start_offset, first.end_offset, first.object_length), (0, 3, 3));
    let second = c.append("log", b"de").unwrap();
    assert_eq!((second.start_offset, second.end_offset, second.object_length), (3, 5, 5));
    assert_eq!(door.object("log"), b"abcde");
}

#[test]
fn an_append_committed_before_the_reply_was_lost_is_landed_and_is_not_repeated() {
    for fault in [Fault::DropReplyAfterCommit, Fault::Answer503AfterCommit] {
        let door = FakeDoor::new();
        door.fault(fault);
        let receipt = client(&door).append("log", b"abc").unwrap();
        assert_eq!((receipt.start_offset, receipt.end_offset, receipt.object_length), (0, 3, 3), "{fault:?}");
        assert_eq!(door.object("log"), b"abc", "{fault:?}: the bytes are in the object once");
        assert_eq!(door.puts(), 1, "{fault:?}: the append was not sent again");
    }
}

#[test]
fn an_append_refused_before_it_committed_is_retried_once_it_is_known_absent() {
    let door = FakeDoor::new();
    door.fault(Fault::Answer503Uncommitted);
    let receipt = client(&door).append("log", b"abc").unwrap();
    assert_eq!((receipt.start_offset, receipt.object_length), (0, 3));
    assert_eq!(door.object("log"), b"abc");
    assert_eq!(door.puts(), 2, "the first attempt was refused and the second landed");
}

#[test]
fn a_late_copy_of_an_attempt_the_door_called_absent_can_never_land() {
    let door = FakeDoor::new();
    door.fault(Fault::Answer503Uncommitted);
    let c = client(&door);
    c.append("log", b"abc").unwrap();
    // The attempt the door fenced is refused if it shows up again; the object holds one copy.
    assert_eq!(door.0.lock().unwrap().fenced.len(), 1);
    assert_eq!(door.object("log"), b"abc");
}

#[test]
fn a_door_that_cannot_resolve_leaves_the_outcome_unknown_and_nothing_is_written_again() {
    let door = FakeDoor::new();
    door.0.lock().unwrap().resolve_supported = false;
    door.fault(Fault::DropReplyAfterCommit);
    let error = client(&door).append("log", b"abc").unwrap_err();
    assert!(matches!(error, ClientError::OutcomeUnknown { .. }), "{error:?}");
    assert_eq!(door.puts(), 1);
    assert_eq!(door.object("log"), b"abc");
}

#[test]
fn an_ambiguous_resolution_is_unconfirmed_and_never_landed() {
    let door = FakeDoor::new();
    door.0.lock().unwrap().always_ambiguous = true;
    door.fault(Fault::DropReplyAfterCommit);
    let error = client(&door).append("log", b"abc").unwrap_err();
    assert!(matches!(error, ClientError::OutcomeUnknown { .. }), "{error:?}");
    assert_eq!(door.puts(), 1);
}

#[test]
fn every_resolve_answer_is_counted_by_its_word() {
    let door = FakeDoor::new();
    let c = client(&door);
    assert_eq!(c.append_resolutions(), AppendResolutions::default());
    door.fault(Fault::DropReplyAfterCommit);
    c.append("log", b"a").unwrap();
    assert_eq!(c.append_resolutions(), AppendResolutions { landed: 1, absent: 0, ambiguous: 0 });
    door.fault(Fault::Answer503Uncommitted);
    c.append("log", b"b").unwrap();
    assert_eq!(c.append_resolutions(), AppendResolutions { landed: 1, absent: 1, ambiguous: 0 });
    door.0.lock().unwrap().always_ambiguous = true;
    door.fault(Fault::DropReplyAfterCommit);
    assert!(c.append("log", b"c").is_err());
    assert_eq!(c.append_resolutions(), AppendResolutions { landed: 1, absent: 1, ambiguous: 1 });
    // A clone reports what its original saw: the counts belong to the client, not the handle.
    assert_eq!(c.clone().append_resolutions(), c.append_resolutions());
}

#[test]
fn two_writers_of_identical_bytes_at_one_offset_are_not_both_told_they_landed() {
    // Writer A's append commits and its reply is lost. Writer B, with a stale length, appends the
    // SAME bytes at the SAME offset and is refused. Equal bytes are at that offset, but they are
    // A's: B must not be told its own copy landed, and A must be.
    let door = FakeDoor::new();
    let b = client(&door);
    b.put("log", b"").unwrap();
    let a = client(&door);
    door.fault(Fault::DropReplyAfterCommit);
    let landed = a.append("log", b"same").unwrap();
    assert_eq!((landed.start_offset, landed.end_offset), (0, 4));
    let other = b.append("log", b"same").unwrap_err();
    assert!(matches!(other, ClientError::OutcomeUnknown { .. }), "{other:?}");
    assert_eq!(door.object("log"), b"same", "one physical copy, claimed by exactly one writer");
}

#[test]
fn every_attempt_has_its_own_identity() {
    let door = FakeDoor::new();
    let c = client(&door);
    for _ in 0..4 {
        c.append("log", b"x").unwrap();
    }
    assert_eq!(door.0.lock().unwrap().landed.len(), 4, "four appends, four attempt ids");
}

#[test]
fn concurrent_appends_of_one_object_in_one_process_are_serialised_and_land_apart() {
    let door = FakeDoor::new();
    let c = Arc::new(client(&door));
    let mut threads = Vec::new();
    for t in 0..8u8 {
        let c = c.clone();
        threads.push(std::thread::spawn(move || {
            (0..20).map(|i| c.append("log", &[t, i]).unwrap().start_offset).collect::<Vec<u64>>()
        }));
    }
    let mut offsets: Vec<u64> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
    offsets.sort();
    assert_eq!(offsets, (0..160).map(|i| i * 2).collect::<Vec<u64>>());
    assert_eq!(door.object("log").len(), 320);
    // Serialised here, so no append lost its offset to a sibling of this process.
    assert_eq!(door.puts(), 160);
}

#[test]
fn reads_are_ranged_and_a_tail_past_the_end_is_not_an_error() {
    let door = FakeDoor::new();
    let c = client(&door);
    c.put("k", b"0123456789").unwrap();
    assert_eq!(c.get("k").unwrap(), b"0123456789");
    assert_eq!(c.get_range("k", 2, 3).unwrap(), b"234");
    assert_eq!(c.get_range("k", 2, 0).unwrap(), b"");
    assert_eq!(c.get_range_from("k", 7).unwrap(), Some(b"789".to_vec()));
    assert_eq!(c.get_range_from("k", 10).unwrap(), Some(Vec::new()));
    assert_eq!(c.get_range_from("k", 11).unwrap(), None);
    assert_eq!(c.get("missing").unwrap_err(), ClientError::NotFound("missing".into()));
    assert_eq!(c.length_or_zero("missing").unwrap(), 0);
    assert_eq!(c.length_or_zero("k").unwrap(), 10);
    c.delete("k").unwrap();
    c.delete("k").unwrap();
    assert_eq!(c.length_or_zero("k").unwrap(), 0);
}

#[test]
fn a_listing_follows_its_continuation_tokens_to_the_end() {
    let door = FakeDoor::new();
    let c = client(&door);
    for key in ["w/1", "w/2", "w/3", "w/4", "w/5", "x/1"] {
        c.put(key, b"v").unwrap();
    }
    assert_eq!(c.list("w/", None).unwrap(), vec!["w/1", "w/2", "w/3", "w/4", "w/5"]);
    assert_eq!(c.list("w/", Some("w/3")).unwrap(), vec!["w/4", "w/5"]);
}

#[test]
fn an_attempt_id_drawn_at_random_is_well_formed_and_unrepeated() {
    let ids: HashSet<String> = (0..64).map(|_| AttemptId::random().hex()).collect();
    assert_eq!(ids.len(), 64);
    assert!(ids.iter().all(|id| AttemptId::parse(id).is_some()));
}

#[test]
fn the_tcp_transport_reuses_its_socket_and_counts_what_crossed_the_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        for _ in 0..3 {
            let n = stream.read(&mut buf).unwrap();
            assert!(n > 0);
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nx-amz-object-size: 2\r\n\r\nhi").unwrap();
        }
    });
    let c = S3Client::new(TcpTransport::new(addr), "b");
    for _ in 0..3 {
        assert_eq!(c.get("k").unwrap(), b"hi");
    }
    server.join().unwrap();
    let stats = c.transport().wire_stats();
    assert_eq!(stats.connections_opened, 1, "three requests on one socket");
    assert_eq!(c.requests_issued(), 3);
    assert!(stats.bytes_sent > 0 && stats.bytes_received > 3 * 2);
}
