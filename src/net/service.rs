// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Routing one HTTP request onto the store's own object API.
//!
//! # WHAT THIS SURFACE IS, AND WHAT IT IS NOT
//!
//! It is an HTTP object API shaped like the one this store already models: the bucket and key come
//! from the path, the conditional headers are the ones the store enforces internally
//! (`If-None-Match`, `If-Match`), user metadata travels as `x-amz-meta-*`, and the response
//! headers are the ones this crate's own default CORS policy already advertises as exposed --
//! `ETag`, `Content-Type`, `Content-Length`, `Last-Modified`, `Accept-Ranges`, `Content-Range`.
//!
//! It is NOT an S3-compatible endpoint, and saying so plainly is the point of this paragraph. It
//! does not implement request signing, multipart upload, versioning verbs, tagging verbs, or the
//! XML `ListObjects` body -- a bucket listing here answers newline-delimited keys as
//! `text/plain`, which is enough for a client that wants to enumerate and is not what an S3
//! client parses. Anything a caller might reasonably expect and not get is answered with a status
//! rather than a plausible-looking wrong answer.
//!
//! # IT IS UNAUTHENTICATED
//!
//! There is no signing, no token check and no transport security here. Every request that reaches
//! it can read, overwrite and delete any object in the bucket. That is why the binary that serves
//! it binds to loopback unless told otherwise, and why this is stated at the top of the module
//! rather than in a footnote: a store with an open HTTP port is the whole store, open.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::http::{self, Request, Response, ResolvedRange};
use crate::{MatrixObjectStore, ObjectError, ObjectMetadata, PutOptions};

/// Default `Content-Type` recorded for a body that arrives without one.
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Header prefix carrying caller-supplied object metadata.
pub const USER_METADATA_PREFIX: &str = "x-amz-meta-";

/// Most keys one bucket listing will answer with, when the caller names no limit.
pub const DEFAULT_LIST_LIMIT: usize = 1_000;

/// Most keys one bucket listing will answer with, whatever the caller asks for.
pub const MAX_LIST_LIMIT: usize = 10_000;

/// Serves object requests against one store behind one lock.
///
/// # WHY ONE LOCK, AND WHAT THAT MEANS FOR THROUGHPUT
///
/// [`MatrixObjectStore`] keeps interior mutability (`Cell`/`RefCell`) for its caches and durable
/// stamps, so it is `Send` but not `Sync`: it can move between threads but cannot be shared by
/// them. A mutex is therefore not a conservative choice here, it is the only sound one, and it
/// means requests are serialised however many connection threads are accepting. Stated here
/// rather than discovered from a flat throughput curve: this surface exists so data can reach the
/// store from another machine, not so it can reach it faster.
pub struct ObjectService {
    store: Arc<Mutex<MatrixObjectStore>>,
    bucket_hint: Option<String>,
}

impl ObjectService {
    /// Serve requests against `store`.
    pub fn new(store: Arc<Mutex<MatrixObjectStore>>) -> Self {
        Self {
            store,
            bucket_hint: None,
        }
    }

    /// Record which bucket this server was started for, for the readiness answer only.
    ///
    /// It narrows nothing: a request names its own bucket in the path, and this is reported by
    /// `/healthz` so an operator can see which bucket the process was configured with rather than
    /// having to infer it from a successful write.
    pub fn with_bucket_hint(mut self, bucket: impl Into<String>) -> Self {
        self.bucket_hint = Some(bucket.into());
        self
    }

    /// Answer one request.
    pub fn handle(&self, request: &Request) -> Response {
        match request.path.as_str() {
            "/healthz" | "/health" => return self.health(request),
            "/" => {
                return Response::text(
                    404,
                    "no bucket in the path; address an object as /{bucket}/{key}\n",
                )
            }
            _ => {}
        }

        let trimmed = request.path.trim_start_matches('/');
        let (bucket, key) = match trimmed.split_once('/') {
            Some((bucket, key)) => (bucket, key),
            None => (trimmed, ""),
        };
        if bucket.is_empty() {
            return Response::text(400, "empty bucket in path\n");
        }

        if key.is_empty() {
            self.bucket_request(request, bucket)
        } else {
            self.object_request(request, bucket, key)
        }
    }

    fn health(&self, request: &Request) -> Response {
        if !matches!(request.method.as_str(), "GET" | "HEAD") {
            return method_not_allowed("GET, HEAD");
        }
        // THE LOCK IS TAKEN, AND THAT IS DELIBERATE. A readiness answer that never touches the
        // store reports the listener, not the store -- and the listener is the half that does not
        // fail. Taking the lock also reports a poisoned one, which is the state an operator most
        // needs to hear about and the one a bare "ok" would hide.
        match self.store.lock() {
            Ok(_) => {
                let bucket = self.bucket_hint.as_deref().unwrap_or("-");
                Response::text(200, format!("ok bucket={bucket}\n"))
            }
            Err(_) => Response::text(
                503,
                "the store lock is poisoned: a request panicked while holding it, so the store's \
                 state is not known to be consistent and this process should be restarted\n",
            ),
        }
    }

    fn bucket_request(&self, request: &Request, bucket: &str) -> Response {
        match request.method.as_str() {
            "PUT" => {
                let mut guard = match self.store.lock() {
                    Ok(guard) => guard,
                    Err(_) => return poisoned(),
                };
                // IDEMPOTENT, because a client that cannot tell whether its earlier PUT landed
                // will send it again, and a second create answering 409 would make a retry look
                // like a failure.
                match guard.create_bucket_idempotent(bucket, bucket) {
                    Ok(_) => Response::text(200, format!("bucket {bucket}\n")),
                    Err(err) => object_error_response(err),
                }
            }
            "GET" | "HEAD" => self.list_bucket(request, bucket),
            _ => method_not_allowed("PUT, GET, HEAD"),
        }
    }

    fn list_bucket(&self, request: &Request, bucket: &str) -> Response {
        let prefix = request.query_param("prefix").unwrap_or_default();
        let marker = request.query_param("marker");
        let limit = match request.query_param("max-keys") {
            Some(raw) => match raw.parse::<usize>() {
                Ok(parsed) => parsed.clamp(1, MAX_LIST_LIMIT),
                // REFUSED, NOT DEFAULTED. A caller that asked for a specific page size and got
                // the default instead would page through the listing wrongly and never know.
                Err(_) => return Response::text(400, "max-keys is not a number\n"),
            },
            None => DEFAULT_LIST_LIMIT,
        };

        let guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(),
        };
        match guard.list_objects(bucket, &prefix, marker.as_deref(), limit) {
            Ok(objects) => {
                let truncated = objects.len() >= limit;
                let mut body = String::new();
                let mut last = String::new();
                for metadata in &objects {
                    last = metadata.object_id.key.to_string();
                    body.push_str(&last);
                    body.push('\n');
                }
                // THE CONTINUATION IS REPORTED RATHER THAN LEFT TO BE COUNTED. A caller comparing
                // the number of lines against its own `max-keys` would have to guess whether a
                // full page means "exactly this many" or "there is more", and the two need
                // different next steps.
                let mut response = Response {
                    status: 200,
                    headers: vec![(
                        "content-type".to_string(),
                        "text/plain; charset=utf-8".to_string(),
                    )],
                    body: body.into_bytes(),
                }
                .with_header("x-matrixobject-truncated", truncated.to_string());
                if truncated && !last.is_empty() {
                    response = response.with_header("x-matrixobject-next-marker", last);
                }
                response
            }
            Err(err) => object_error_response(err),
        }
    }

    fn object_request(&self, request: &Request, bucket: &str, key: &str) -> Response {
        match request.method.as_str() {
            "PUT" => self.put_object(request, bucket, key),
            "GET" | "HEAD" => self.get_object(request, bucket, key),
            "DELETE" => self.delete_object(bucket, key),
            _ => method_not_allowed("PUT, GET, HEAD, DELETE"),
        }
    }

    fn put_object(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let content_type = request
            .header("content-type")
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_CONTENT_TYPE)
            .to_string();

        let mut user_metadata: BTreeMap<String, String> = BTreeMap::new();
        for (suffix, value) in request.headers_with_prefix(USER_METADATA_PREFIX) {
            user_metadata.insert(suffix.to_string(), value.to_string());
        }

        // BOTH CONDITIONS GO TO THE STORE RATHER THAN BEING CHECKED HERE. A check out here would
        // read the object, decide, and then write -- with the lock released in between on any
        // future refactor, and with two callers able to interleave. The store applies them inside
        // the same mutation it guards.
        let if_none_match = request.header("if-none-match").is_some_and(|value| {
            // `*` is the only form that means "only if absent"; an etag list here is an
            // `If-Match` in disguise and is not treated as one.
            value.trim() == "*"
        });
        let if_match_etag = request
            .header("if-match")
            .map(|value| unquote_etag(value).to_string());

        let options = PutOptions {
            if_none_match,
            if_match_etag,
            user_metadata,
            ..PutOptions::default()
        };

        let mut guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(),
        };
        match guard.put_object_with_options(bucket, key, request.body.clone(), content_type, options)
        {
            Ok(metadata) => {
                // FLUSHED HERE, AND THE RESULT IS NOT SWALLOWED. The store rewrites its durable
                // snapshot on commit only for some operations; a plain put mutates memory and
                // leaves the snapshot behind. A server that answered 200 and lost the object on
                // restart would be the worst kind of wrong, so a failed flush is reported as a
                // failure of the write -- the bytes are in memory either way, but the client is
                // not told they are durable when they are not.
                if let Err(err) = guard.flush_persistent_snapshot() {
                    return Response::text(
                        500,
                        format!(
                            "the object was written to memory but its durable snapshot could not \
                             be rewritten, so this write is not durable: {err}\n"
                        ),
                    );
                }
                let mut response = Response::status(200);
                apply_metadata_headers(&mut response, &metadata);
                response
            }
            Err(err) => object_error_response(err),
        }
    }

    fn get_object(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(),
        };

        // HEAD FIRST, FOR THE LENGTH A RANGE IS RESOLVED AGAINST. A range cannot be turned into an
        // offset and a length without knowing the total, and resolving it against a guess is how
        // a 206 ends up describing bytes the client did not ask for.
        let metadata = match guard.head_object(bucket, key) {
            Ok(metadata) => metadata,
            Err(err) => return object_error_response(err),
        };

        // A CONDITIONAL GET IS ANSWERED BEFORE ANY PAYLOAD IS READ, which is the only reason it
        // is worth answering: the saving is the read, not the transfer.
        if let Some(candidates) = request.header("if-none-match") {
            if etag_list_matches(candidates, &metadata.etag) {
                let mut response = Response::status(304);
                apply_metadata_headers(&mut response, &metadata);
                return response;
            }
        }

        match http::resolve_range(request.header("range"), metadata.length) {
            ResolvedRange::Whole => match guard.get_object(bucket, key) {
                Ok(object) => {
                    let mut response = Response {
                        status: 200,
                        headers: Vec::new(),
                        body: object.data,
                    };
                    apply_metadata_headers(&mut response, &object.metadata);
                    response
                }
                Err(err) => object_error_response(err),
            },
            ResolvedRange::Partial { offset, length } => {
                match guard.get_object_range(bucket, key, offset, length) {
                    Ok(object) => {
                        let last = offset + length - 1;
                        let total = metadata.length;
                        let mut response = Response {
                            status: 206,
                            headers: Vec::new(),
                            body: object.data,
                        };
                        apply_metadata_headers(&mut response, &metadata);
                        response.headers.push((
                            "content-range".to_string(),
                            format!("bytes {offset}-{last}/{total}"),
                        ));
                        response
                    }
                    Err(err) => object_error_response(err),
                }
            }
            ResolvedRange::Unsatisfiable => Response::text(
                416,
                format!(
                    "the requested range lies outside the object, which is {} bytes\n",
                    metadata.length
                ),
            )
            .with_header("content-range", format!("bytes */{}", metadata.length)),
        }
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Response {
        let mut guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(),
        };
        match guard.delete_object(bucket, key) {
            Ok(_) => {
                if let Err(err) = guard.flush_persistent_snapshot() {
                    return Response::text(
                        500,
                        format!(
                            "the object was removed from memory but its durable snapshot could \
                             not be rewritten, so it may come back on restart: {err}\n"
                        ),
                    );
                }
                Response::status(204)
            }
            // IDEMPOTENT, and this is a choice worth naming rather than a shrug. A client
            // retrying a delete it already completed wants to learn that the object is gone, not
            // that its retry failed; 404 here would make a successful outcome look like an error.
            // A caller that needs to know whether the object existed asks with HEAD.
            Err(ObjectError::NotFound(_)) => Response::status(204),
            Err(err) => object_error_response(err),
        }
    }
}

/// Put this object's identity and shape on a response.
fn apply_metadata_headers(response: &mut Response, metadata: &ObjectMetadata) {
    response
        .headers
        .push(("etag".to_string(), format!("\"{}\"", metadata.etag)));
    response.headers.push((
        "content-type".to_string(),
        metadata.content_type.to_string(),
    ));
    response.headers.push((
        "last-modified".to_string(),
        http::http_date(metadata.update_time_ms),
    ));
    response
        .headers
        .push(("accept-ranges".to_string(), "bytes".to_string()));
    response.headers.push((
        "x-matrixobject-version".to_string(),
        metadata.version.to_string(),
    ));
    response.headers.push((
        "x-matrixobject-length".to_string(),
        metadata.length.to_string(),
    ));
    for (name, value) in metadata.user_metadata.iter() {
        response
            .headers
            .push((format!("{USER_METADATA_PREFIX}{name}"), value.to_string()));
    }
}

/// Strip one layer of quoting and any weak-validator marker from an etag.
fn unquote_etag(value: &str) -> &str {
    value
        .trim()
        .trim_start_matches("W/")
        .trim_matches('"')
}

/// Whether an `If-None-Match` list names this etag.
fn etag_list_matches(candidates: &str, etag: &str) -> bool {
    if candidates.trim() == "*" {
        return true;
    }
    candidates
        .split(',')
        .any(|candidate| unquote_etag(candidate) == etag)
}

/// The store's error vocabulary, mapped to statuses.
///
/// `PreconditionFailed` is 412 and not 409: it is what a conditional write answers when its
/// condition did not hold, which is exactly what 412 means, and a client that sent `If-Match`
/// decides what to do next from that code.
fn object_error_response(err: ObjectError) -> Response {
    let (status, text) = match &err {
        ObjectError::NotFound(_) => (404, err.to_string()),
        ObjectError::InvalidArgument(_) => (400, err.to_string()),
        ObjectError::PreconditionFailed(_) => (412, err.to_string()),
        // REPORTED AS THE SERVER'S FAULT, because it is: corruption is a statement about stored
        // state, and nothing the client can change about its request will fix it.
        ObjectError::Corruption(_) => (500, err.to_string()),
    };
    Response::text(status, format!("{text}\n"))
}

fn method_not_allowed(allowed: &str) -> Response {
    Response::text(405, format!("allowed: {allowed}\n")).with_header("allow", allowed.to_string())
}

fn poisoned() -> Response {
    Response::text(
        503,
        "the store lock is poisoned: a request panicked while holding it, so the store's state is \
         not known to be consistent and this process should be restarted\n",
    )
}
