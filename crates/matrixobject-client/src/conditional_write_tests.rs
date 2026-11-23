// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::{ClientError, Reply, S3Client, Transport, TransportError};
use std::sync::{Arc, Barrier, Mutex};

#[derive(Debug, Default)]
struct State { value: Option<Vec<u8>>, generation: u64, writes: usize, drop_reply: bool, conflicts: usize, status: Option<u16>, tag: Option<String> }
#[derive(Clone, Debug, Default)]
struct Door { state: Arc<Mutex<State>>, first_read_barrier: Option<Arc<Barrier>> }
fn reply(status: u16, body: Vec<u8>, headers: Vec<(String, String)>) -> Reply { Reply { status, body, headers } }
impl Transport for Door {
    fn send(&self, method: &str, _: &str, headers: &[(&str, String)], body: &[u8]) -> Result<Reply, TransportError> {
        let mut state = self.state.lock().unwrap();
        if method == "GET" {
            let response = match &state.value {
                Some(value) => reply(200, value.clone(), vec![("etag".into(), state.tag.clone().unwrap_or_else(|| format!("\"gen-{}\"", state.generation)))]),
                None => reply(404, vec![], vec![]),
            };
            drop(state);
            if let Some(barrier) = &self.first_read_barrier {
                // Only the initial read joins; a conflict reread must be free to finish.
                if response.header("etag") == Some("\"gen-1\"") { barrier.wait(); }
            }
            return Ok(response);
        }
        assert_eq!(method, "PUT"); state.writes += 1;
        assert_eq!(headers.len(), 1, "every write must carry exactly one condition");
        let (name, value) = &headers[0];
        let allowed = match *name {
            "if-none-match" => { assert_eq!(value, "*"); state.value.is_none() },
            "if-match" => state.value.is_some() && value == &format!("\"gen-{}\"", state.generation),
            _ => panic!("unconditional mutation"),
        };
        if state.conflicts > 0 { state.conflicts -= 1; state.generation += 1; return Ok(reply(412, vec![], vec![])); }
        if !allowed { return Ok(reply(412, vec![], vec![])); }
        state.value = Some(body.to_vec()); state.generation += 1;
        if state.drop_reply { return Err(TransportError("reply lost after commit".into())); }
        Ok(reply(state.status.unwrap_or(200), vec![], vec![]))
    }
}
fn existing() -> Door {
    let door = Door::default(); { let mut s = door.state.lock().unwrap(); s.value = Some(b"old".to_vec()); s.generation = 1; } door
}
#[test]
fn conditional_create_then_reject_duplicate() {
    let door = Door::default(); let c = S3Client::new(door.clone(), "b");
    c.compare_and_swap("k", None, b"first").unwrap();
    assert!(matches!(c.compare_and_swap("k", None, b"second"), Err(ClientError::ConditionFailed { .. })));
    assert_eq!(door.state.lock().unwrap().value.as_deref(), Some(b"first".as_slice()));
    assert_eq!(c.requests_issued(), 2);
}
#[test]
fn conditional_replacement_uses_the_observed_generation() {
    let door = existing(); let c = S3Client::new(door.clone(), "b");
    c.compare_and_swap("k", Some(b"old"), b"new").unwrap();
    assert_eq!(door.state.lock().unwrap().value.as_deref(), Some(b"new".as_slice()));
    assert_eq!(c.requests_issued(), 2);
}
#[test]
fn conditional_mismatch_and_missing_do_not_write() {
    for door in [existing(), Door::default()] {
        let c = S3Client::new(door.clone(), "b");
        assert!(matches!(c.compare_and_swap("k", Some(b"different"), b"new"), Err(ClientError::ConditionFailed { .. })));
        assert_eq!(door.state.lock().unwrap().writes, 0);
    }
}
#[test]
fn conditional_replacement_refuses_unsafe_tags() {
    for tag in ["", "gen-1", "W/\"gen-1\"", "\"a\r\nb\"", "\"a\"b\""] {
        let door = existing(); door.state.lock().unwrap().tag = Some(tag.into());
        assert!(matches!(S3Client::new(door.clone(), "b").compare_and_swap("k", Some(b"old"), b"new"), Err(ClientError::Http(_))));
        assert_eq!(door.state.lock().unwrap().writes, 0);
    }
}
#[test]
fn conditional_generation_conflict_rechecks_equal_bytes() {
    let door = existing(); door.state.lock().unwrap().conflicts = 1;
    S3Client::new(door.clone(), "b").compare_and_swap("k", Some(b"old"), b"new").unwrap();
    assert_eq!(door.state.lock().unwrap().writes, 2);
}
#[test]
fn conditional_contention_is_bounded_without_claiming_value_mismatch() {
    let door = existing(); door.state.lock().unwrap().conflicts = 100;
    assert!(matches!(S3Client::new(door.clone(), "b").compare_and_swap("k", Some(b"old"), b"new"), Err(ClientError::Http(_))));
    assert_eq!(door.state.lock().unwrap().writes, 8);
    assert_eq!(door.state.lock().unwrap().value.as_deref(), Some(b"old".as_slice()));
}
#[test]
fn conditional_lost_reply_is_unknown_and_never_resent() {
    for expected in [None, Some(b"old".as_slice())] {
        let door = if expected.is_some() { existing() } else { Door::default() };
        door.state.lock().unwrap().drop_reply = true;
        assert!(matches!(S3Client::new(door.clone(), "b").compare_and_swap("k", expected, b"new"), Err(ClientError::ConditionalWriteUnknown { .. })));
        assert_eq!(door.state.lock().unwrap().writes, 1);
        assert_eq!(door.state.lock().unwrap().value.as_deref(), Some(b"new".as_slice()));
    }
}
#[test]
fn conditional_uncertain_server_reply_is_not_resent() {
    for status in [408, 500, 503] {
        let door = existing(); door.state.lock().unwrap().status = Some(status);
        assert!(matches!(S3Client::new(door.clone(), "b").compare_and_swap("k", Some(b"old"), b"new"), Err(ClientError::ConditionalWriteUnknown { .. })));
        assert_eq!(door.state.lock().unwrap().writes, 1);
    }
}
#[test]
fn conditional_independent_clients_have_only_one_winner() {
    let mut door = existing(); door.first_read_barrier = Some(Arc::new(Barrier::new(2)));
    let joins: Vec<_> = [b"one".as_slice(), b"two".as_slice()].into_iter().map(|value| {
        let c = S3Client::new(door.clone(), "b"); std::thread::spawn(move || c.compare_and_swap("k", Some(b"old"), value))
    }).collect();
    let results: Vec<_> = joins.into_iter().map(|j| j.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|r| matches!(r, Err(ClientError::ConditionFailed { .. }))).count(), 1);
    assert_eq!(door.state.lock().unwrap().generation, 2);
}

#[derive(Debug)]
struct UnusableTagDoor { door: Door, duplicate: bool }
impl Transport for UnusableTagDoor {
    fn send(&self, method: &str, target: &str, headers: &[(&str, String)], body: &[u8]) -> Result<Reply, TransportError> {
        let mut reply = self.door.send(method, target, headers, body)?;
        if method == "GET" {
            if self.duplicate { reply.headers.push(("ETag".into(), "\"another\"".into())); }
            else { reply.headers.clear(); }
        }
        Ok(reply)
    }
}
#[test]
fn conditional_missing_or_duplicate_tags_never_allow_unconditional_write() {
    for duplicate in [false, true] {
        let door = existing(); let transport = UnusableTagDoor { door: door.clone(), duplicate };
        assert!(matches!(S3Client::new(transport, "b").compare_and_swap("k", Some(b"old"), b"new"), Err(ClientError::Http(_))));
        assert_eq!(door.state.lock().unwrap().writes, 0);
    }
}
