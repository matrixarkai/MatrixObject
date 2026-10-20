// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Routing one HTTP request onto the store's own object API, in the S3 wire format.
//!
//! # WHAT THIS SURFACE IS
//!
//! An S3-compatible endpoint for the verbs it implements. The bucket and key come from the path,
//! the conditional headers are the ones the store enforces internally (`If-None-Match`,
//! `If-Match`), user metadata travels as `x-amz-meta-*`, and every answer a client has to branch
//! on is the document S3 answers: an `<Error>` body carrying a `<Code>` on a failure, a
//! `<ListBucketResult>` on a bucket listing, a `<ListAllMyBucketsResult>` on `GET /`.
//!
//! Routing lives here and the wire format lives in [`super::s3`]. The split is so the two can
//! fail separately: a routing mistake is a wrong status, a format mistake is a body no client can
//! read, and a test can say which happened.
//!
//! Implemented: `GET /` (ListBuckets), `PUT` / `HEAD` / `GET` / `DELETE` on `/{bucket}`
//! (CreateBucket, HeadBucket, ListObjectsV2, DeleteBucket), and `PUT` / `GET` / `HEAD` / `DELETE`
//! on `/{bucket}/{key}`, with ranged reads.
//!
//! # WHAT IS DELIBERATELY NOT IMPLEMENTED
//!
//! Request signing, multipart upload, versioning verbs, tagging verbs, object ACLs, bucket
//! policies, lifecycle verbs. Anything a caller might reasonably expect and not get is answered
//! with an S3 error code rather than a plausible-looking wrong answer -- a client that reads
//! `NotImplemented` can fall back, and a client handed an empty success cannot.
//!
//! `/healthz` and `/health` are the exceptions and answer plain text. They are not S3 routes, they
//! are for an operator and for a container probe, and dressing a readiness answer in XML would
//! help neither.
//!
//! # IT IS UNAUTHENTICATED
//!
//! There is no signing, no token check and no transport security here. An `Authorization` header
//! is accepted and IGNORED rather than refused, so a signing client works; it is not verified.
//! Every request that reaches this surface can read, overwrite and delete any object in the
//! bucket. That is why the binary that serves it binds to loopback unless told otherwise, and why
//! this is stated at the top of the module rather than in a footnote: a store with an open HTTP
//! port is the whole store, open.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::http::{self, Request, Response, ResolvedRange};
use super::s3;
use crate::{MatrixObjectStore, ObjectError, ObjectMetadata, PutOptions};

/// Default `Content-Type` recorded for a body that arrives without one.
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Header prefix carrying caller-supplied object metadata.
pub const USER_METADATA_PREFIX: &str = "x-amz-meta-";

/// Most keys one bucket listing will answer with, when the caller names no limit.
pub const DEFAULT_LIST_LIMIT: usize = 1_000;

/// Most keys one bucket listing will answer with, whatever the caller asks for.
pub const MAX_LIST_LIMIT: usize = 10_000;

/// Most buckets `GET /` will answer with.
pub const MAX_BUCKET_PAGE: usize = 10_000;

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
            "/" => return self.service_request(request),
            _ => {}
        }

        let trimmed = request.path.trim_start_matches('/');
        let (bucket, key) = match trimmed.split_once('/') {
            Some((bucket, key)) => (bucket, key),
            None => (trimmed, ""),
        };
        if bucket.is_empty() {
            return s3::error_response(
                s3::S3ErrorCode::InvalidArgument,
                "the path names no bucket; address a bucket as /{bucket} and an object as \
                 /{bucket}/{key}",
                &request.path,
            );
        }

        if key.is_empty() {
            self.bucket_request(request, bucket)
        } else {
            self.object_request(request, bucket, key)
        }
    }

    fn health(&self, request: &Request) -> Response {
        if !matches!(request.method.as_str(), "GET" | "HEAD") {
            // PLAIN TEXT, not XML, and not via the S3 helper. `/healthz` is not an S3 route, so a
            // probe that gets an `<Error>` document here would be the one place this surface
            // answered a format nothing on the path asked for.
            return Response::text(405, "allowed: GET, HEAD\n")
                .with_header("allow", "GET, HEAD".to_string());
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
            Err(_) => Response::text(503, format!("{POISONED_TEXT}\n")),
        }
    }

    /// `GET /`, which in S3 is `ListBuckets`.
    fn service_request(&self, request: &Request) -> Response {
        if !matches!(request.method.as_str(), "GET" | "HEAD") {
            return method_not_allowed("GET, HEAD", "/");
        }
        let guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned("/"),
        };
        match guard.list_buckets(None, MAX_BUCKET_PAGE) {
            Ok(page) => s3::xml_response(200, s3::list_all_my_buckets_xml(&page.buckets)),
            Err(err) => s3_error(err, "/", false),
        }
    }

    fn bucket_request(&self, request: &Request, bucket: &str) -> Response {
        match request.method.as_str() {
            "PUT" => self.create_bucket(bucket),
            "GET" => self.list_bucket(request, bucket),
            "HEAD" => self.head_bucket(bucket),
            "DELETE" => self.delete_bucket(bucket),
            _ => method_not_allowed("PUT, GET, HEAD, DELETE", &format!("/{bucket}")),
        }
    }

    /// `CreateBucket`.
    fn create_bucket(&self, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let mut guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(&resource),
        };
        // IDEMPOTENT, because a client that cannot tell whether its earlier PUT landed will send
        // it again, and a second create answering 409 would make a retry look like a failure.
        // S3 agrees: creating a bucket you already own is a success, not a conflict.
        match guard.create_bucket_idempotent(bucket, bucket) {
            // NO BODY, and a `Location`. That is what S3 answers, and a client that parses the
            // body of a successful CreateBucket finds nothing there to parse.
            Ok(_) => Response::status(200).with_header("location", resource.clone()),
            Err(err) => s3_error(err, &resource, true),
        }
    }

    /// `HeadBucket`, which is an existence check and nothing else.
    ///
    /// It deliberately does NOT build the listing. The transport drops a body on HEAD, so routing
    /// this through the listing would answer the right status after doing all of the work and
    /// would report a `Content-Length` for bytes the client never receives.
    fn head_bucket(&self, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(&resource),
        };
        match guard.bucket_exists(bucket) {
            Ok(true) => Response::status(200),
            Ok(false) => s3::error_response(
                s3::S3ErrorCode::NoSuchBucket,
                "bucket not found",
                &resource,
            ),
            Err(err) => s3_error(err, &resource, true),
        }
    }

    /// `DeleteBucket`.
    fn delete_bucket(&self, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let mut guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(&resource),
        };
        // EMPTINESS IS CHECKED HERE, AHEAD OF THE DELETE, FOR THE CODE. The store refuses a
        // non-empty bucket as `InvalidArgument`, which is the same vocabulary it uses for a
        // malformed bucket name -- so by the time the error arrives the two are indistinguishable
        // and both would be reported as 400. S3 answers `BucketNotEmpty` with 409 for the first,
        // and a client branches on it to go and empty the bucket. Asking the question first is
        // what makes the two answerable apart.
        match guard.list_objects(bucket, "", None, 1) {
            Ok(objects) if !objects.is_empty() => {
                return s3::error_response(
                    s3::S3ErrorCode::BucketNotEmpty,
                    "the bucket still holds objects, so it cannot be deleted",
                    &resource,
                )
            }
            Ok(_) => {}
            Err(err) => return s3_error(err, &resource, true),
        }
        match guard.delete_bucket(bucket) {
            Ok(()) => {
                if let Err(err) = guard.flush_persistent_snapshot() {
                    return flush_failed(
                        &resource,
                        format!(
                            "the bucket was removed from memory but its durable snapshot could \
                             not be rewritten, so it may come back on restart: {err}"
                        ),
                    );
                }
                Response::status(204)
            }
            Err(err) => s3_error(err, &resource, true),
        }
    }

    /// `ListObjectsV2`, which is the listing an S3 client parses.
    ///
    /// The earlier answer here was newline-delimited keys as `text/plain`. It was honest about
    /// being unparseable by an S3 client and it was: a client reads `<IsTruncated>` and
    /// `<NextContinuationToken>` to page, and `<CommonPrefixes>` to walk a prefix tree, and gets
    /// none of that from a list of lines.
    ///
    /// `list-type=2` is accepted and not required. A V1 client sends no `list-type` and reads the
    /// same element names for everything it uses, so refusing it would turn a working client away
    /// for naming a version rather than for asking something this cannot answer.
    fn list_bucket(&self, request: &Request, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let prefix = request.query_param("prefix").unwrap_or_default();
        let delimiter = request.query_param("delimiter").filter(|d| !d.is_empty());
        // `continuation-token` is V2 and `marker` is V1; both mean "resume after this key", and
        // `start-after` means the same thing on a first page. Taking whichever arrived keeps a
        // client of either version paging correctly.
        let continuation = request
            .query_param("continuation-token")
            .or_else(|| request.query_param("marker"));
        let start_after = request.query_param("start-after");
        let resume = continuation.clone().or_else(|| start_after.clone());
        let max_keys = match request.query_param("max-keys") {
            Some(raw) => match raw.parse::<usize>() {
                Ok(parsed) => parsed.clamp(1, MAX_LIST_LIMIT),
                // REFUSED, NOT DEFAULTED. A caller that asked for a specific page size and got
                // the default instead would page through the listing wrongly and never know.
                Err(_) => {
                    return s3::error_response(
                        s3::S3ErrorCode::InvalidArgument,
                        "max-keys is not a number",
                        &resource,
                    )
                }
            },
            None => DEFAULT_LIST_LIMIT,
        };

        let guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(&resource),
        };
        // ASKED SEPARATELY, BECAUSE THE LISTING CANNOT ANSWER IT. `list_objects` ranges over the
        // object map, so a bucket that was never created answers an empty page -- and a client
        // told "this bucket is empty" about a bucket that does not exist keeps writing into a
        // name that will never hold anything. S3 answers `NoSuchBucket` and a client acts on it.
        match guard.bucket_exists(bucket) {
            Ok(true) => {}
            Ok(false) => {
                return s3::error_response(
                    s3::S3ErrorCode::NoSuchBucket,
                    "bucket not found",
                    &resource,
                )
            }
            Err(err) => return s3_error(err, &resource, true),
        }
        let objects = match guard.list_objects(bucket, &prefix, resume.as_deref(), max_keys) {
            Ok(objects) => objects,
            Err(err) => return s3_error(err, &resource, true),
        };
        let truncated = objects.len() >= max_keys;
        let mut last_key = String::new();
        let mut entries: Vec<s3::ListEntry> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        for metadata in &objects {
            let key = metadata.object_id.key.to_string();
            last_key = key.clone();
            // THE DELIMITER ROLLUP. A key whose remainder after the prefix still contains the
            // delimiter is not a result, it is evidence of a prefix -- and the prefix is reported
            // once however many keys sit under it, which is what lets a client walk a tree
            // without listing every leaf.
            if let Some(delimiter) = &delimiter {
                let remainder = key.strip_prefix(prefix.as_str()).unwrap_or(key.as_str());
                if let Some(at) = remainder.find(delimiter.as_str()) {
                    let rolled = format!("{prefix}{}", &remainder[..at + delimiter.len()]);
                    if !common_prefixes.contains(&rolled) {
                        common_prefixes.push(rolled);
                    }
                    continue;
                }
            }
            let storage_class = metadata.storage_class.to_string();
            entries.push(s3::ListEntry {
                key,
                last_modified_ms: metadata.update_time_ms,
                etag: metadata.etag(),
                size: metadata.length,
                // NEVER EMPTY. A client reads this to decide whether a restore is needed, and an
                // empty element reads as neither of the answers it knows.
                storage_class: if storage_class.is_empty() {
                    s3::DEFAULT_STORAGE_CLASS.to_string()
                } else {
                    storage_class
                },
            });
        }

        let result = s3::ListResult {
            bucket: bucket.to_string(),
            prefix,
            delimiter,
            max_keys,
            is_truncated: truncated,
            continuation_token: continuation,
            // ONLY WHEN THERE IS MORE. A token on a final page tells a client to ask again for
            // nothing, and a client that trusts the token over `IsTruncated` would loop.
            next_continuation_token: if truncated && !last_key.is_empty() {
                Some(last_key)
            } else {
                None
            },
            start_after,
            entries,
            common_prefixes,
        };
        s3::xml_response(200, s3::list_objects_v2_xml(&result))
    }

    fn object_request(&self, request: &Request, bucket: &str, key: &str) -> Response {
        match request.method.as_str() {
            "PUT" => self.put_object(request, bucket, key),
            "GET" | "HEAD" => self.get_object(request, bucket, key),
            "DELETE" => self.delete_object(bucket, key),
            _ => method_not_allowed(
                "PUT, GET, HEAD, DELETE",
                &format!("/{bucket}/{key}"),
            ),
        }
    }

    fn put_object(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
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
            Err(_) => return poisoned(&resource),
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
                    return flush_failed(
                        &resource,
                        format!(
                            "the object was written to memory but its durable snapshot could not \
                             be rewritten, so this write is not durable: {err}"
                        ),
                    );
                }
                let mut response = Response::status(200);
                apply_metadata_headers(&mut response, &metadata);
                response
            }
            Err(err) => s3_error(err, &resource, false),
        }
    }

    fn get_object(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        let guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(&resource),
        };

        // HEAD FIRST, FOR THE LENGTH A RANGE IS RESOLVED AGAINST. A range cannot be turned into an
        // offset and a length without knowing the total, and resolving it against a guess is how
        // a 206 ends up describing bytes the client did not ask for.
        let metadata = match guard.head_object(bucket, key) {
            Ok(metadata) => metadata,
            Err(err) => return s3_error(err, &resource, false),
        };

        // A CONDITIONAL GET IS ANSWERED BEFORE ANY PAYLOAD IS READ, which is the only reason it
        // is worth answering: the saving is the read, not the transfer.
        if let Some(candidates) = request.header("if-none-match") {
            if etag_list_matches(candidates, &metadata.etag()) {
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
                Err(err) => s3_error(err, &resource, false),
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
                    Err(err) => s3_error(err, &resource, false),
                }
            }
            // `InvalidRange` IS THE CODE, AND THE HEADER STAYS. The code is what an S3 client
            // branches on; `Content-Range: bytes */len` is what an HTTP client reads to learn the
            // size it should have asked within. Answering one and not the other leaves whichever
            // client it is guessing.
            ResolvedRange::Unsatisfiable => s3::error_response(
                s3::S3ErrorCode::InvalidRange,
                &format!(
                    "the requested range lies outside the object, which is {} bytes",
                    metadata.length
                ),
                &resource,
            )
            .with_header("content-range", format!("bytes */{}", metadata.length)),
        }
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        let mut guard = match self.store.lock() {
            Ok(guard) => guard,
            Err(_) => return poisoned(&resource),
        };
        match guard.delete_object(bucket, key) {
            Ok(_) => {
                if let Err(err) = guard.flush_persistent_snapshot() {
                    return flush_failed(
                        &resource,
                        format!(
                            "the object was removed from memory but its durable snapshot could \
                             not be rewritten, so it may come back on restart: {err}"
                        ),
                    );
                }
                Response::status(204)
            }
            // IDEMPOTENT, and this is a choice worth naming rather than a shrug. A client
            // retrying a delete it already completed wants to learn that the object is gone, not
            // that its retry failed; 404 here would make a successful outcome look like an error.
            // A caller that needs to know whether the object existed asks with HEAD. S3 answers
            // a delete of an absent key with 204 for the same reason.
            Err(ObjectError::NotFound(_)) => Response::status(204),
            Err(err) => s3_error(err, &resource, false),
        }
    }
}

/// Put this object's identity and shape on a response.
fn apply_metadata_headers(response: &mut Response, metadata: &ObjectMetadata) {
    response
        .headers
        .push(("etag".to_string(), format!("\"{}\"", metadata.etag())));
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

/// What a poisoned lock means, in one sentence, used by both the S3 and the plain-text answer.
const POISONED_TEXT: &str =
    "the store lock is poisoned: a request panicked while holding it, so the store's state is not \
     known to be consistent and this process should be restarted";

/// One of this store's errors, as the XML document an S3 client branches on.
///
/// `missing_is_bucket` decides whether a `NotFound` is `NoSuchBucket` or `NoSuchKey`. It is a
/// parameter rather than a guess because the store reports both the same way, and the two send a
/// client to different places: one creates the bucket and retries, the other stops.
fn s3_error(err: ObjectError, resource: &str, missing_is_bucket: bool) -> Response {
    let code = s3::code_for(&err, missing_is_bucket);
    s3::error_response(code, &err.to_string(), resource)
}

fn method_not_allowed(allowed: &str, resource: &str) -> Response {
    // The `Allow` header is kept alongside the XML: it is what an HTTP client reads, the body is
    // what an S3 client reads, and answering only one leaves the other guessing.
    s3::error_response(
        s3::S3ErrorCode::MethodNotAllowed,
        "the method is not allowed on this resource",
        resource,
    )
    .with_header("allow", allowed.to_string())
}

fn poisoned(resource: &str) -> Response {
    s3::error_response(s3::S3ErrorCode::ServiceUnavailable, POISONED_TEXT, resource)
}

/// A mutation that reached memory and did not reach the snapshot.
///
/// `InternalError` and not `ServiceUnavailable`: the request will not succeed by being retried
/// against this process, because the bytes are already applied in memory and the snapshot will
/// fail to rewrite for the same reason next time.
fn flush_failed(resource: &str, message: String) -> Response {
    s3::error_response(s3::S3ErrorCode::InternalError, &message, resource)
}
