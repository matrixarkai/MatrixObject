// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A client for the S3 door of MatrixObject: whole-object and ranged reads, put, delete, listing,
//! and an append that settles its own uncertain outcomes.
//!
//! Everything a caller needs to use the door correctly is here once. A caller does not frame a
//! request, choose a header, read an append's outcome off a status code, or decide whether a lost
//! reply means the bytes landed: the client does, by the contract in [`wire`](crate::wire).

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};

use crate::transport::{Reply, Transport, TransportError};
use crate::wire::{
    classify_append_status, AppendStatus, AttemptId, Resolution, ResolveRequest, ATTEMPT_HEADER,
    OBJECT_SIZE_HEADER, RESOLUTION_HEADER, WRITE_OFFSET_HEADER,
};

/// How many times an append may lose its offset, or fail to be answered, before the client gives up
/// and reports the last failure. Contention worth seeing, not something to spin on for ever.
const APPEND_ATTEMPTS: usize = 8;

/// Locks one process keeps per object before it drops the ones nobody holds.
const MAX_APPEND_LOCKS: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientError {
    /// The object does not exist.
    NotFound(String),
    /// A range that cannot be asked for or answered.
    InvalidRange(String),
    /// An append whose outcome is not decided: it may or may not be in the object, and the client
    /// has not written it again. The caller decides from its own record of what it acknowledged.
    OutcomeUnknown { key: String, detail: String },
    /// The door answered with a status the operation does not accept.
    Http(String),
    /// The door could not be reached or did not finish answering.
    Transport(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::NotFound(key) => write!(f, "{key}: no such object"),
            ClientError::InvalidRange(detail) => f.write_str(detail),
            ClientError::OutcomeUnknown { key, detail } => {
                write!(f, "{key}: the outcome of the append is unknown: {detail}")
            }
            ClientError::Http(detail) | ClientError::Transport(detail) => f.write_str(detail),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<TransportError> for ClientError {
    fn from(error: TransportError) -> Self {
        ClientError::Transport(error.0)
    }
}

/// How many times the gateway has answered a resolve with each word, as seen by one client.
///
/// A resolve is the only place a client learns what became of an append whose reply was lost, and
/// a gateway logs only its failures. These counts let a caller see that a landed and an absent
/// answer were both really given, rather than assume the path was exercised.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AppendResolutions {
    pub landed: u64,
    pub absent: u64,
    pub ambiguous: u64,
}

/// Where an append landed, as the door reports it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendReceipt {
    /// The offset the bytes landed at, which is the one a reader finds them by.
    pub start_offset: u64,
    pub end_offset: u64,
    pub bytes_written: u64,
    /// The object's length after the append. Later appends may already follow it.
    pub object_length: u64,
}

/// A client of one bucket of the S3 door, over any [`Transport`].
///
/// Cloning shares the length cache, the append locks and the request counter.
#[derive(Debug)]
pub struct S3Client<T: Transport> {
    transport: T,
    bucket: String,
    /// Object lengths this client last observed, so an append can skip asking.
    ///
    /// A cached offset is safe where it normally would not be: an append sends the offset it
    /// believes is the end and the door appends THERE or refuses, so the offset is a condition and
    /// not a destination. A stale entry earns a 412 that the append path already re-reads and
    /// retries; being wrong costs a round trip, not an object.
    known_lengths: Arc<Mutex<HashMap<String, u64>>>,
    /// One lock per object key, held around an append and its settlement.
    ///
    /// Concurrent callers of ONE process that append to the same object would otherwise read the
    /// same end, race for it, and lose with a 412 over and over: contention of this process's own
    /// making. It does NOT replace the offset condition, which still arbitrates between processes.
    append_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    requests: Arc<AtomicU64>,
    /// Resolve answers seen, indexed landed, absent, ambiguous.
    resolutions: Arc<[AtomicU64; 3]>,
}

impl<T: Transport + Clone> Clone for S3Client<T> {
    fn clone(&self) -> Self {
        S3Client {
            transport: self.transport.clone(),
            bucket: self.bucket.clone(),
            known_lengths: self.known_lengths.clone(),
            append_locks: self.append_locks.clone(),
            requests: self.requests.clone(),
            resolutions: self.resolutions.clone(),
        }
    }
}

impl<T: Transport> S3Client<T> {
    pub fn new(transport: T, bucket: impl Into<String>) -> Self {
        S3Client {
            transport,
            bucket: bucket.into(),
            known_lengths: Arc::default(),
            append_locks: Arc::default(),
            requests: Arc::default(),
            resolutions: Arc::default(),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// How many requests this client has issued. Counted at the one place requests are sent, so
    /// nothing can route around it.
    pub fn requests_issued(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// The resolve answers this client has been given, over all its objects.
    pub fn append_resolutions(&self) -> AppendResolutions {
        let [landed, absent, ambiguous] = &*self.resolutions;
        AppendResolutions {
            landed: landed.load(Ordering::Relaxed),
            absent: absent.load(Ordering::Relaxed),
            ambiguous: ambiguous.load(Ordering::Relaxed),
        }
    }

    fn object_target(&self, key: &str) -> String {
        format!("/{}/{}", self.bucket, encode_key(key))
    }

    fn send(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply, ClientError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        Ok(self.transport.send(method, target, headers, body)?)
    }

    fn cached_length(&self, key: &str) -> Option<u64> {
        self.known_lengths.lock().expect("known lengths poisoned").get(key).copied()
    }

    fn remember_length(&self, key: &str, length: u64) {
        self.known_lengths.lock().expect("known lengths poisoned").insert(key.to_string(), length);
    }

    /// Drop a remembered length because something other than an append changed the object. A
    /// remembered end that describes an object that no longer has one costs the next append a
    /// refusal and a retry, which is slower and noisier than asking.
    fn forget_length(&self, key: &str) {
        self.known_lengths.lock().expect("known lengths poisoned").remove(key);
    }

    fn append_lock(&self, key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.append_locks.lock().expect("append locks poisoned");
        // Bound the table: drop locks nobody holds or waits on once it has grown.
        if locks.len() >= MAX_APPEND_LOCKS {
            locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        locks.entry(key.to_string()).or_default().clone()
    }

    /// The error a non-success status becomes, carrying the server's own words: a bare status turns
    /// a refused append into "something went wrong" when the server said exactly what was wrong.
    fn failure(reply: &Reply, what: &str) -> ClientError {
        if reply.status == 404 {
            return ClientError::NotFound(what.to_string());
        }
        let text = String::from_utf8_lossy(&reply.body);
        let detail = text.trim();
        if detail.is_empty() {
            ClientError::Http(format!("{what}: the store answered {}", reply.status))
        } else {
            ClientError::Http(format!("{what}: the store answered {} -- {detail}", reply.status))
        }
    }

    /// Replace the object.
    pub fn put(&self, key: &str, bytes: &[u8]) -> Result<(), ClientError> {
        let reply = self.send("PUT", &self.object_target(key), &[], bytes)?;
        match reply.status {
            200 | 201 | 204 => {
                // A put REPLACES, so any remembered end is now wrong.
                self.remember_length(key, bytes.len() as u64);
                Ok(())
            }
            _ => Err(Self::failure(&reply, key)),
        }
    }

    /// The whole object.
    pub fn get(&self, key: &str) -> Result<Vec<u8>, ClientError> {
        let reply = self.send("GET", &self.object_target(key), &[], &[])?;
        match reply.status {
            200 => Ok(reply.body),
            _ => Err(Self::failure(&reply, key)),
        }
    }

    /// `length` bytes from `offset`.
    pub fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Vec<u8>, ClientError> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let last = offset
            .checked_add(length)
            .and_then(|end| end.checked_sub(1))
            .ok_or_else(|| ClientError::InvalidRange(format!("{key}: range overflow")))?;
        let reply = self.send(
            "GET",
            &self.object_target(key),
            &[("range", format!("bytes={offset}-{last}"))],
            &[],
        )?;
        match reply.status {
            // 200 means the server ignored the range and sent the whole object, which is a legal
            // answer to any range request: slice it here rather than trust the body.
            200 => {
                let start = usize::try_from(offset)
                    .map_err(|_| ClientError::InvalidRange(format!("{key}: range offset too large")))?;
                let len = usize::try_from(length)
                    .map_err(|_| ClientError::InvalidRange(format!("{key}: range length too large")))?;
                let end = start
                    .checked_add(len)
                    .ok_or_else(|| ClientError::InvalidRange(format!("{key}: range overflow")))?;
                if end > reply.body.len() {
                    return Err(ClientError::InvalidRange(format!(
                        "{key}: range {offset}..{} exceeds object length {}",
                        offset.saturating_add(length),
                        reply.body.len()
                    )));
                }
                Ok(reply.body[start..end].to_vec())
            }
            206 => Ok(reply.body),
            _ => Err(Self::failure(&reply, key)),
        }
    }

    /// Everything from `offset` to the end, in one request whatever the object's length is.
    ///
    /// `None` when `offset` is past the end: the object is no longer the one being followed.
    pub fn get_range_from(&self, key: &str, offset: u64) -> Result<Option<Vec<u8>>, ClientError> {
        let reply = self.send(
            "GET",
            &self.object_target(key),
            &[("range", format!("bytes={offset}-"))],
            &[],
        )?;
        match reply.status {
            206 => Ok(Some(reply.body)),
            // THE SERVER IGNORED THE RANGE and sent the whole object: slice it here rather than
            // trust the body to be the tail.
            200 => {
                let start = usize::try_from(offset)
                    .map_err(|_| ClientError::InvalidRange(format!("{key}: range offset too large")))?;
                if start > reply.body.len() {
                    return Ok(None);
                }
                Ok(Some(reply.body[start..].to_vec()))
            }
            // UNSATISFIABLE says the offset is at or past the end, and `Content-Range: bytes */len`
            // says where the end is: AT it is an empty tail, PAST it is an object that is no longer
            // the one being followed. A reply that does not say is treated as the second, because
            // starting over is the safe reading.
            416 => {
                let length = reply
                    .header("content-range")
                    .and_then(|value| value.trim().strip_prefix("bytes */"))
                    .and_then(|length| length.trim().parse::<u64>().ok());
                Ok(if length == Some(offset) { Some(Vec::new()) } else { None })
            }
            _ => Err(Self::failure(&reply, key)),
        }
    }

    /// The object's length, or zero when it does not exist yet.
    ///
    /// An append to a key nothing has written is how an append-only object is created, so a
    /// missing key is a length here and not an error.
    pub fn length_or_zero(&self, key: &str) -> Result<u64, ClientError> {
        let reply = self.send("HEAD", &self.object_target(key), &[], &[])?;
        match reply.status {
            200 => Ok(reply
                .header(OBJECT_SIZE_HEADER)
                .or_else(|| reply.header("content-length"))
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(0)),
            404 => Ok(0),
            _ => Err(Self::failure(&reply, key)),
        }
    }

    /// Delete the object. Deleting something already gone succeeds, as S3 has it.
    pub fn delete(&self, key: &str) -> Result<(), ClientError> {
        let reply = self.send("DELETE", &self.object_target(key), &[], &[])?;
        match reply.status {
            200 | 202 | 204 | 404 => {
                self.forget_length(key);
                Ok(())
            }
            _ => Err(Self::failure(&reply, key)),
        }
    }

    /// `ListObjectsV2`: every key under `prefix` (after `after`, when given), following
    /// continuation tokens, sorted.
    ///
    /// Paged rather than assumed to fit: the door answers a bounded number of keys and says so with
    /// `IsTruncated`, and a caller reading only the first page would silently see a prefix of its
    /// own log.
    pub fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>, ClientError> {
        let mut keys: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut target = format!("/{}?list-type=2", self.bucket);
            if !prefix.is_empty() {
                target.push_str("&prefix=");
                target.push_str(&encode_query(prefix));
            }
            if let Some(after) = after {
                target.push_str("&start-after=");
                target.push_str(&encode_query(after));
            }
            if let Some(token) = &token {
                target.push_str("&continuation-token=");
                target.push_str(&encode_query(token));
            }
            let reply = self.send("GET", &target, &[], &[])?;
            if reply.status != 200 {
                return Err(Self::failure(&reply, prefix));
            }
            let body = String::from_utf8_lossy(&reply.body);
            keys.extend(xml_values(&body, "Key"));
            let truncated = xml_values(&body, "IsTruncated")
                .first()
                .is_some_and(|value| value.trim() == "true");
            if !truncated {
                break;
            }
            match xml_values(&body, "NextContinuationToken").into_iter().find(|value| !value.is_empty()) {
                Some(next) => token = Some(next),
                // Truncated with nothing to continue from: stop rather than loop for ever, and say
                // the listing is short instead of pretending it is whole.
                None => {
                    return Err(ClientError::Http(format!(
                        "the listing of {prefix:?} is truncated and gave no continuation token, so it cannot be read to the end"
                    )))
                }
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// Append `bytes` to the object and report where they landed.
    ///
    /// # THE OFFSET IS A CONDITION, AND LOSING ON IT IS NORMAL UNDER CONCURRENCY
    ///
    /// Appending takes two round trips -- learn the end, then write at it -- and those are not one
    /// atomic step, so two writers against one object can read the same end and one of them loses
    /// with a 412. The door is right to refuse: it appends where it was told or not at all. The
    /// loser re-reads the end and tries again, which is safe for exactly one reason: **the receipt
    /// reports where the bytes LANDED, not where this call first intended to put them.** A caller
    /// that indexes by the receipt finds its bytes; reporting the intended offset after landing
    /// elsewhere is the thing that would corrupt it.
    ///
    /// # AN ATTEMPT THAT MAY HAVE LANDED IS SETTLED BY ITS IDENTITY
    ///
    /// A refused offset (the refusal may be about OUR OWN bytes, landed by an earlier send), a
    /// connection that failed after the request left, a 409 and a server error all leave the outcome
    /// unknown. Each attempt carries its own [`AttemptId`], and an unknown outcome is settled by
    /// asking the door about THAT attempt before anything is written again. The object is never read
    /// back to decide: two writers that append identical bytes at one offset leave one copy, and a
    /// comparison would tell both that theirs landed.
    pub fn append(&self, key: &str, bytes: &[u8]) -> Result<AppendReceipt, ClientError> {
        let bytes_written = bytes.len() as u64;
        let mut last_failure: Option<ClientError> = None;
        let mut unsettled: Option<(u64, AttemptId)> = None;

        // ONE APPEND PER OBJECT AT A TIME WITHIN THIS PROCESS: see `append_locks`.
        let lock = self.append_lock(key);
        let _serialised = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        for attempt in 0..APPEND_ATTEMPTS {
            if let Some((start_offset, id)) = unsettled.take() {
                if let Some(receipt) = self.settle(key, id, start_offset, bytes)? {
                    return Ok(receipt);
                }
            }
            // A FRESH IDENTITY PER ATTEMPT, reused only by the transport's own repeats of this one
            // send.
            let id = AttemptId::random();
            // ONE ROUND TRIP INSTEAD OF TWO, when this client already knows the end. The first
            // attempt may use the cached length; a retry never does, because a retry exists
            // precisely because the end moved.
            let start_offset = match self.cached_length(key) {
                Some(length) if attempt == 0 => length,
                _ => self.length_or_zero(key)?,
            };
            let reply = match self.send(
                "PUT",
                &self.object_target(key),
                &[(WRITE_OFFSET_HEADER, start_offset.to_string()), (ATTEMPT_HEADER, id.hex())],
                bytes,
            ) {
                Ok(reply) => reply,
                Err(error) => {
                    // The request may have been served before the connection failed.
                    self.forget_length(key);
                    unsettled = Some((start_offset, id));
                    last_failure = Some(error);
                    continue;
                }
            };
            match classify_append_status(reply.status) {
                AppendStatus::Appended => {
                    // The server reports what the object is NOW. Deriving it from start + written
                    // would paper over a server that appended somewhere else, which is the one
                    // thing the receipt exists to rule out.
                    let object_length = reply
                        .header(OBJECT_SIZE_HEADER)
                        .and_then(|value| value.trim().parse::<u64>().ok())
                        .ok_or_else(|| {
                            ClientError::Http(format!(
                                "{key}: the append gave no usable {OBJECT_SIZE_HEADER}, so where the bytes landed is unknown"
                            ))
                        })?;
                    let end_offset = start_offset.saturating_add(bytes_written);
                    if object_length != end_offset {
                        return Err(ClientError::Http(format!(
                            "{key}: appended {bytes_written} bytes at {start_offset}, so the object should be {end_offset} bytes and the server reports {object_length}"
                        )));
                    }
                    self.remember_length(key, object_length);
                    return Ok(AppendReceipt { start_offset, end_offset, bytes_written, object_length });
                }
                // The end moved, the gateway could not decide, or the server failed: none of those
                // says whether this append was committed. Settle before writing again.
                AppendStatus::OffsetRefused | AppendStatus::Ambiguous | AppendStatus::Unknown => {
                    self.forget_length(key);
                    unsettled = Some((start_offset, id));
                    last_failure = Some(Self::failure(&reply, key));
                }
                AppendStatus::Refused => return Err(Self::failure(&reply, key)),
            }
        }

        // The last attempt may have left something unsettled; a failure must not be reported as
        // "not written" when it was.
        if let Some((start_offset, id)) = unsettled.take() {
            if let Some(receipt) = self.settle(key, id, start_offset, bytes)? {
                return Ok(receipt);
            }
        }
        Err(last_failure.unwrap_or_else(|| {
            ClientError::Http(format!("{key}: gave up appending after {APPEND_ATTEMPTS} attempts"))
        }))
    }

    /// Did the append identified by `id` land at `start_offset` of `key`? A receipt if so, `None`
    /// if it did not and cannot still, and `OutcomeUnknown` when that is not decided.
    fn settle(
        &self,
        key: &str,
        id: AttemptId,
        start_offset: u64,
        bytes: &[u8],
    ) -> Result<Option<AppendReceipt>, ClientError> {
        if bytes.is_empty() {
            return Ok(None);
        }
        let length = bytes.len() as u64;
        // A read can say the bytes are in the object, but it cannot say that an attempt still in
        // flight inside a slow server will never commit. A resolve answers landed, or absent with
        // the late attempt fenced out, so an absent answer is safe to retry on.
        match self.resolve(key, id, start_offset, bytes)? {
            Resolution::Landed => {
                // The append is in the object; the object may be longer than its end, because
                // later appends follow it.
                let object_length = self.length_or_zero(key).map_err(|error| ClientError::OutcomeUnknown {
                    key: key.to_string(),
                    detail: format!("the append is present but the length could not be read: {error}"),
                })?;
                self.remember_length(key, object_length);
                Ok(Some(AppendReceipt {
                    start_offset,
                    end_offset: start_offset + length,
                    bytes_written: length,
                    object_length,
                }))
            }
            Resolution::Absent => Ok(None),
            Resolution::Ambiguous => Err(ClientError::OutcomeUnknown {
                key: key.to_string(),
                detail: "the gateway found equal bytes at the offset but cannot prove they are this attempt's"
                    .to_string(),
            }),
        }
    }

    /// Ask the gateway whether an append whose reply was lost landed. Anything the gateway says
    /// other than a resolution, including not knowing the route, is an undecided outcome and never
    /// a reason to write again.
    fn resolve(
        &self,
        key: &str,
        id: AttemptId,
        start_offset: u64,
        bytes: &[u8],
    ) -> Result<Resolution, ClientError> {
        let unknown = |detail: String| ClientError::OutcomeUnknown { key: key.to_string(), detail };
        let request = ResolveRequest {
            attempt: id,
            offset: start_offset,
            length: bytes.len() as u64,
            sha256: Some(Sha256::digest(bytes).into()),
        };
        let target = format!("{}?{}", self.object_target(key), request.encode_query());
        let reply = self
            .send("POST", &target, &[], &[])
            .map_err(|error| unknown(format!("the append could not be resolved: {error}")))?;
        match (reply.status, reply.header(RESOLUTION_HEADER)) {
            (200, Some(word)) => {
                let resolution = Resolution::from_word(word)
                    .ok_or_else(|| unknown(format!("the gateway resolved the append as {word:?}")))?;
                let counter = match resolution {
                    Resolution::Landed => 0,
                    Resolution::Absent => 1,
                    Resolution::Ambiguous => 2,
                };
                self.resolutions[counter].fetch_add(1, Ordering::Relaxed);
                Ok(resolution)
            }
            (503, _) => Err(unknown("the gateway could not decide whether the append landed".to_string())),
            (status, _) => Err(unknown(format!(
                "the gateway cannot resolve an append by attempt (it answered {status}), so the outcome is undecided"
            ))),
        }
    }
}

/// Percent-encode an object key for a request target, LEAVING `/` ALONE.
///
/// A key with slashes in it is one object whose name contains them, exactly as S3 has it: the door
/// takes everything after the bucket as the key. Encoding them would address a different object,
/// and the bug would show only on keys that happen to contain one.
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' | b':' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Percent-encode a query value, where `/` goes too: it is data here, not structure.
fn encode_query(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Every `<tag>` value in an S3 listing, with the five XML entities put back.
fn xml_values(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(
            after[..end]
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                // LAST, so a literal `&amp;lt;` does not come back as `<`.
                .replace("&amp;", "&"),
        );
        rest = &after[end + close.len()..];
    }
    out
}
