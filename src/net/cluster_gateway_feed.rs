// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The change feed: an ordered record of what happened to the objects of a bucket, read by cursor.
//!
//! A bucket opts in (`PUT /{bucket}?changefeed` with `<ChangeFeedConfiguration><Status>Enabled</Status>...`). From
//! then on every put, append, copy, delete and version delete in it writes one small entry, an ordinary object under
//! the hidden prefix `.changes/`. The entry's key is its cursor: the writer's clock in milliseconds, then an id
//! that is unique per process and call, so entries sort by time and never collide.
//! `GET /{bucket}?changes&after=<cursor>&limit=N` returns the entries after a cursor and the cursor to continue
//! from; `DELETE /{bucket}?changes&before=<cursor>` drops the entries up to and including one, so a consumer that
//! has processed them frees them.
//!
//! # WHAT THE FEED PROMISES
//!
//! An entry is written after its mutation succeeded, so a gateway that dies between the two leaves a mutation
//! with no entry: the feed is at-most-once per mutation, never invents one, and a failed write of an entry is
//! logged and does not fail the request. Entries are ordered by the clock of the gateway that wrote them, so a
//! consumer that must see every entry reads from a cursor a little behind the last one it saw (the entries are
//! idempotent to replay: they name the key and the version). Nothing of this costs a request in a bucket that did
//! not opt in.
use super::*;
use crate::bucket_config as bc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Where the entries of a bucket live.
pub(super) const FEED_PREFIX: &str = ".changes/";
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
const MAX_PAGE: usize = 1000;

/// One entry as the feed returns it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub cursor: String,
    pub time_ms: u64,
    pub operation: String,
    pub key: String,
    pub version: u64,
    pub size: u64,
}

fn encode(change: &Change) -> String {
    format!("{}\t{}\t{}\t{}\n{}", change.time_ms, change.operation, change.version, change.size, change.key)
}

fn decode(cursor: &str, text: &str) -> Option<Change> {
    let (head, key) = text.split_once('\n')?;
    let mut fields = head.split('\t');
    Some(Change {
        cursor: cursor.to_string(),
        time_ms: fields.next()?.parse().ok()?,
        operation: fields.next()?.to_string(),
        version: fields.next()?.parse().ok()?,
        size: fields.next()?.parse().ok()?,
        key: key.to_string(),
    })
}

impl ClusterGateway {
    /// Record that `operation` happened to `key`, if the bucket keeps a feed. Never fails the request.
    pub(super) fn note_change(&self, bucket: &str, operation: &str, key: &str, version: u64, size: u64) {
        if s3_verbs::is_reserved(key) {
            return;
        }
        match self.bucket_rules(bucket) {
            Ok(Some(rules)) if rules.change_feed => {}
            _ => return,
        }
        let time_ms = now_ms();
        let cursor = format!(
            "{FEED_PREFIX}{time_ms:020}-{:08x}{:08x}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed) & 0xffff_ffff
        );
        let entry = encode(&Change { cursor: cursor.clone(), time_ms, operation: operation.to_string(), key: key.to_string(), version, size });
        let written = self.with_plane(|plane| plane.write(bucket, &cursor, None, entry.as_bytes()).map(|_| ()).map_err(|failure| failure.error));
        if let Err(error) = written {
            eprintln!("change feed: the entry for {operation} {bucket}/{key} was not written: {error}");
        }
    }

    /// The entries of `bucket` after `after`, at most `limit`, and whether more follow.
    pub(super) fn read_changes(&self, bucket: &str, after: &str, limit: usize) -> Result<(Vec<Change>, bool), ObjectError> {
        let start = if after.is_empty() { format!("{FEED_PREFIX}.") } else { after.to_string() };
        let keys = self.with_plane(|plane| {
            let mut found: Vec<String> = Vec::new();
            let mut cursor = Some(start.clone());
            loop {
                let page = plane.list(bucket, cursor.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    cursor = Some(summary.key.clone());
                    if !summary.key.starts_with(FEED_PREFIX) {
                        return Ok(found);
                    }
                    if !summary.deleted {
                        found.push(summary.key.clone());
                        if found.len() > limit {
                            return Ok(found);
                        }
                    }
                }
                if !page.has_more || page.entries.is_empty() {
                    return Ok(found);
                }
            }
        })?;
        let more = keys.len() > limit;
        let mut changes = Vec::new();
        for cursor in keys.into_iter().take(limit) {
            match self.with_plane(|plane| plane.read(bucket, &cursor)) {
                Ok((_, bytes)) => {
                    if let Some(change) = decode(&cursor, &String::from_utf8_lossy(&bytes)) {
                        changes.push(change);
                    }
                }
                // trimmed while it was being read
                Err(ObjectError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok((changes, more))
    }

    /// `GET /{bucket}?changes` and `DELETE /{bucket}?changes`.
    pub(super) fn change_feed_request(&self, request: &Request, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        match self.bucket_rules(bucket) {
            Ok(Some(rules)) if rules.change_feed => {}
            Ok(Some(_)) => {
                return s3::error_response(s3::S3ErrorCode::InvalidRequest, "the bucket does not keep a change feed", &resource)
            }
            Ok(None) => return s3::error_response(s3::S3ErrorCode::NoSuchBucket, "the bucket does not exist", &resource),
            Err(error) => return error_response(error, &resource, true),
        }
        match request.method.as_str() {
            "GET" => {
                let after = request.query_param("after").unwrap_or_default();
                let limit = request.query_param("limit").and_then(|raw| raw.parse::<usize>().ok()).unwrap_or(100).clamp(1, MAX_PAGE);
                let (changes, more) = match self.read_changes(bucket, &after, limit) {
                    Ok(found) => found,
                    Err(error) => return error_response(error, &resource, true),
                };
                let mut body = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ChangeFeed><Bucket>{bucket}</Bucket><HasMore>{more}</HasMore>");
                if let Some(last) = changes.last() {
                    body.push_str(&format!("<NextCursor>{}</NextCursor>", bc::xml_escape(&last.cursor)));
                }
                for change in &changes {
                    body.push_str(&format!(
                        "<Change><Cursor>{}</Cursor><Time>{}</Time><Operation>{}</Operation><Key>{}</Key><VersionId>{}</VersionId><Size>{}</Size></Change>",
                        bc::xml_escape(&change.cursor),
                        s3::iso8601(change.time_ms),
                        change.operation,
                        bc::xml_escape(&change.key),
                        change.version,
                        change.size
                    ));
                }
                body.push_str("</ChangeFeed>\n");
                s3::xml_response(200, body)
            }
            "DELETE" => {
                let Some(before) = request.query_param("before").filter(|cursor| cursor.starts_with(FEED_PREFIX)) else {
                    return s3::error_response(s3::S3ErrorCode::InvalidArgument, "before names a cursor the feed returned", &resource);
                };
                let mut dropped = 0u64;
                loop {
                    let (changes, more) = match self.read_changes(bucket, "", 200) {
                        Ok(found) => found,
                        Err(error) => return error_response(error, &resource, true),
                    };
                    let due: Vec<_> = changes.iter().filter(|change| change.cursor.as_str() <= before.as_str()).collect();
                    for change in &due {
                        match self.with_plane(|plane| plane.delete(bucket, &change.cursor)) {
                            Ok(_) | Err(ObjectError::NotFound(_)) => dropped += 1,
                            Err(error) => return error_response(error, &resource, true),
                        }
                    }
                    if due.len() < changes.len() || !more || due.is_empty() {
                        break;
                    }
                }
                Response::status(204).with_header("x-matrixobject-entries-dropped", dropped.to_string())
            }
            _ => method_not_allowed("GET, DELETE", &resource),
        }
    }
}
