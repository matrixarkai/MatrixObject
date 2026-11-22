// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The S3 verbs the cluster gateway serves beyond put, get, append and delete: `CopyObject`,
//! `DeleteObjects`, `DeleteBucket`, `ListBuckets`, the multipart family, conditional reads and
//! writes, and the answers for the sub-resources a client probes.
//!
//! Everything here is built from what the gateway already has: the plane's read, write, delete and
//! list, and (for `CopyObject`) the metadata plane's atomic copy, which moves no data. No metadata
//! or chunk change is needed.
//!
//! # MULTIPART UPLOADS
//!
//! An upload in progress is a set of ordinary objects under the reserved prefix
//! [`RESERVED_PREFIX`]: a record `.multipart/<id>/meta` that names the target key and the time it
//! was begun, and one object `.multipart/<id>/<part number>` per uploaded part. Completing reads
//! the claimed parts, publishes ONE object under the target key (so the target appears whole or
//! not at all, as every other write does) and then removes the parts and the record. Aborting
//! removes them. The assembled object is bounded by the gateway's whole-object limit
//! (`max_object_bytes`), because it is assembled in memory. Keys under the reserved prefix are not
//! served as objects and are not listed.
use super::*;
use crate::net::{s3_batch as batch, s3_multipart as mp};
use std::sync::atomic::{AtomicU64, Ordering};

/// Keys under this prefix hold the records and parts of multipart uploads in progress.
pub(super) const RESERVED_PREFIX: &str = ".multipart/";

const MAX_PART_NUMBER: u32 = 10_000;
const DEFAULT_LISTING: usize = 1000;

pub(super) fn is_reserved(key: &str) -> bool {
    key.starts_with(RESERVED_PREFIX) || key.starts_with(super::versions::VERSIONS_PREFIX) || key.starts_with(super::feed::FEED_PREFIX)
}

/// The key to resume a listing from so that every key starting with `common_prefix` is skipped.
pub(super) fn past_prefix(common_prefix: &str) -> String {
    format!("{common_prefix}\u{10ffff}")
}

/// Whether an `If-Match` / `If-None-Match` header names `etag` (or is `*`).
pub(super) fn etag_matches(header: &str, etag: &str) -> bool {
    header.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || candidate.trim_start_matches("W/").trim_matches('"') == etag
    })
}

/// The answer to a read whose `If-Match` / `If-None-Match` does not let it through, if any.
pub(super) fn read_condition(
    request: &Request,
    manifest: &ObjectManifest,
    resource: &str,
) -> Option<Response> {
    let etag = etag_of(manifest.generation);
    if let Some(value) = request.header("if-match") {
        if !etag_matches(value, &etag) {
            return Some(s3::error_response(
                s3::S3ErrorCode::PreconditionFailed,
                "If-Match does not name the current version of the object",
                resource,
            ));
        }
    }
    if let Some(value) = request.header("if-none-match") {
        if etag_matches(value, &etag) {
            return Some(Response::status(304).with_header("etag", format!("\"{etag}\"")));
        }
    }
    None
}

fn new_upload_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    let word = |salt: u64| {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u64(now_ms());
        hasher.write_u64(sequence);
        hasher.write_u64(salt);
        hasher.finish()
    };
    format!("{:016x}{:016x}", word(1), word(2))
}

fn valid_upload_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// The attributes of an upload as the lines that follow its begin time in its record: `ct`, `meta`, `tag`, `ret`, `hold`,
/// fields separated by tabs. A header value cannot hold a line break, so a line is one value.
fn encode_upload_attributes(attributes: &ChunkAttributes) -> String {
    let mut out = String::new();
    if !attributes.content_type.is_empty() {
        out.push_str(&format!("\nct\t{}", attributes.content_type));
    }
    for (key, value) in &attributes.user_metadata {
        out.push_str(&format!("\nmeta\t{key}\t{value}"));
    }
    for (key, value) in &attributes.tags {
        out.push_str(&format!("\ntag\t{key}\t{value}"));
    }
    if attributes.retention_mode != 0 {
        out.push_str(&format!("\nret\t{}\t{}", attributes.retention_mode, attributes.retain_until_ms));
    }
    if attributes.legal_hold {
        out.push_str("\nhold");
    }
    out
}

fn decode_upload_attributes(record: &str) -> ChunkAttributes {
    let mut attributes = ChunkAttributes::default();
    // the first line is the key and the second the time
    for line in record.lines().skip(2) {
        let mut fields = line.splitn(3, '\t');
        match (fields.next(), fields.next(), fields.next()) {
            (Some("ct"), Some(value), _) => attributes.content_type = value.to_string(),
            (Some("meta"), Some(key), Some(value)) => {
                attributes.user_metadata.insert(key.to_string(), value.to_string());
            }
            (Some("tag"), Some(key), Some(value)) => {
                attributes.tags.insert(key.to_string(), value.to_string());
            }
            (Some("ret"), Some(mode), Some(until)) => {
                attributes.retention_mode = mode.parse().unwrap_or(0);
                attributes.retain_until_ms = until.parse().unwrap_or(0);
            }
            (Some("hold"), _, _) => attributes.legal_hold = true,
            _ => {}
        }
    }
    attributes
}

fn record_key(id: &str) -> String {
    format!("{RESERVED_PREFIX}{id}/meta")
}

fn part_key(id: &str, number: u32) -> String {
    format!("{RESERVED_PREFIX}{id}/{number:05}")
}

/// The key to list after so that every key starting with `prefix` (which ends in `/`) follows.
fn just_before(prefix: &str) -> String {
    let mut before = prefix.to_string();
    before.pop();
    before.push('.');
    before
}

fn acl_xml() -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<AccessControlPolicy xmlns=\"{ns}\"><Owner><ID>matrixobject</ID><DisplayName>matrixobject</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>matrixobject</ID><DisplayName>matrixobject</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>\n",
        ns = s3::S3_XML_NAMESPACE
    )
}

impl ClusterGateway {
    /// `ListBuckets`: buckets are implicit in the replica plane (one exists as soon as an object is
    /// published under it) and the plane cannot enumerate them, so this names the bucket the
    /// gateway was started for.
    pub(super) fn list_buckets(&self, request: &Request) -> Response {
        if !matches!(request.method.as_str(), "GET" | "HEAD") {
            return method_not_allowed("GET, HEAD", "/");
        }
        s3::xml_response(200, s3::list_all_my_buckets_xml(&[self.bucket_hint.clone()]))
    }

    /// `DeleteBucket`: refused while any live object remains, otherwise a no-op (a bucket has no
    /// existence apart from its objects).
    pub(super) fn delete_bucket(&self, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let held = self.with_plane(|plane| {
            let mut after: Option<String> = None;
            loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if !summary.deleted && (!is_reserved(&summary.key) || summary.key.starts_with(super::versions::VERSIONS_PREFIX)) {
                        return Ok(true);
                    }
                }
                if !page.has_more {
                    return Ok(false);
                }
            }
        });
        match held {
            Err(error) => error_response(error, &resource, true),
            Ok(true) => s3::error_response(
                s3::S3ErrorCode::BucketNotEmpty,
                "the bucket still holds objects",
                &resource,
            ),
            Ok(false) if !self.options.require_buckets => Response::status(204),
            Ok(false) => match self.drop_bucket_record(bucket) {
                Ok(()) => Response::status(204),
                Err(error) => error_response(error, &resource, true),
            },
        }
    }

    /// `DeleteObjects`: `POST /{bucket}?delete` with a list of keys.
    pub(super) fn delete_objects(&self, request: &Request, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let asked = match batch::parse_delete_objects(&request.body) {
            Ok(asked) => asked,
            Err(reason) => {
                return s3::error_response(s3::S3ErrorCode::MalformedXml, &reason, &resource)
            }
        };
        let lock = match self.lock_check(request, bucket, &resource) {
            Ok(lock) => lock,
            Err(refusal) => return refusal,
        };
        let versioned = match self.versioning_on(bucket) {
            Ok(on) => on,
            Err(error) => return error_response(error, &resource, true),
        };
        let mut outcomes = Vec::with_capacity(asked.keys.len());
        for key in &asked.keys {
            if is_reserved(key) {
                outcomes.push(batch::DeleteOutcome::Failed {
                    key: key.clone(),
                    code: s3::S3ErrorCode::InvalidArgument,
                    message: format!("keys under {RESERVED_PREFIX} are reserved"),
                });
                continue;
            }
            self.forget(bucket, key);
            let removed = if versioned {
                self.versioned_delete(bucket, key, lock).and_then(|marker| marker.ok_or_else(|| ObjectError::NotFound("already deleted".into())))
            } else {
                self.delete_checked(bucket, key, lock)
            };
            match removed {
                Ok(manifest) => {
                    self.remember(&manifest);
                    self.note_change(bucket, "delete", key, manifest.generation, 0);
                    outcomes.push(batch::DeleteOutcome::Deleted(key.clone()));
                }
                // Deleting what is not there is a success, so a retried batch sees its keys gone.
                Err(ObjectError::NotFound(_)) => {
                    outcomes.push(batch::DeleteOutcome::Deleted(key.clone()))
                }
                Err(error) => outcomes.push(batch::DeleteOutcome::Failed {
                    key: key.clone(),
                    code: match &error {
                        ObjectError::PreconditionFailed(message) if message.starts_with(super::attributes::LOCKED) => s3::S3ErrorCode::AccessDenied,
                        _ => s3::code_for(&error, false),
                    },
                    message: error.to_string(),
                }),
            }
        }
        s3::xml_response(200, batch::delete_result_xml(&outcomes, asked.quiet))
    }

    /// `GetBucketVersioning` / `GetBucketAcl` and the object ACL and tagging reads: fixed answers
    /// a client probes for. Objects are not versioned, are private to the owner and carry no tags.
    pub(super) fn probe_answer(&self, what: &str) -> Response {
        let body = match what {
            "versioning" => batch::versioning_configuration_xml(false),
            "acl" => acl_xml(),
            _ => format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging xmlns=\"{}\"><TagSet></TagSet></Tagging>\n",
                s3::S3_XML_NAMESPACE
            ),
        };
        s3::xml_response(200, body)
    }

    /// `CopyObject`: `PUT` with `x-amz-copy-source`. On the SDK plane the copy is a metadata
    /// operation (the destination names the source's extents) and no data moves.
    pub(super) fn copy_object(
        &self,
        request: &Request,
        bucket: &str,
        key: &str,
        source: &str,
    ) -> Response {
        let resource = format!("/{bucket}/{key}");
        let (source_bucket, source_key, source_version) = match batch::parse_copy_source_with_version(source) {
            Ok(parsed) => parsed,
            Err(reason) => {
                return s3::error_response(s3::S3ErrorCode::InvalidArgument, &reason, &resource)
            }
        };
        if let Err(error) = crate::validate_bucket(&source_bucket)
            .and_then(|()| crate::validate_key(&source_key))
        {
            return error_response(error, &resource, false);
        }
        if is_reserved(&source_key) {
            return s3::error_response(
                s3::S3ErrorCode::InvalidArgument,
                &format!("keys under {RESERVED_PREFIX} are reserved"),
                &resource,
            );
        }
        // copying a version onto its own key is how an old version is brought back, so only the plain form is refused
        if source_bucket == bucket && source_key == key && source_version.is_none() {
            return s3::error_response(
                s3::S3ErrorCode::InvalidRequest,
                "the copy source and destination are the same object, so this request would change nothing",
                &resource,
            );
        }
        let source_resource = format!("/{source_bucket}/{source_key}");
        // the key the named version is stored at: the key itself when it is the current version
        let read_key = match &source_version {
            None => source_key.clone(),
            Some(version) => match self.version_key(&source_bucket, &source_key, version, &source_resource) {
                Ok(read_key) => read_key,
                Err(response) => return response,
            },
        };
        let versioned = match self.versioning_on(bucket) {
            Ok(on) => on,
            Err(error) => return error_response(error, &resource, true),
        };
        // the destination carries what the request says (its lock, and its default retention), and the source's content
        // type, metadata and tags unless the request replaces them
        let lock = match self.lock_check(request, bucket, &resource).and_then(|lock| self.with_quota(lock, bucket, &resource)) {
            Ok(lock) => lock,
            Err(refusal) => return refusal,
        };
        let named = match self.request_attributes(request, bucket, &resource) {
            Ok(named) => named,
            Err(refusal) => return refusal,
        };
        let replace_metadata = request
            .header("x-amz-metadata-directive")
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("REPLACE"));
        let replace_tags = request
            .header("x-amz-tagging-directive")
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("REPLACE"));
        const ATTEMPTS: usize = 4;
        for _ in 0..ATTEMPTS {
            let outcome = self.with_plane(|plane| {
                let (source, source_attributes) = match plane.read_manifest_with_attributes(&source_bucket, &read_key) {
                    Ok((manifest, carried)) if !manifest.deleted => (manifest, carried),
                    Ok(_) | Err(ObjectError::NotFound(_)) => {
                        return Ok(Err(s3::error_response(
                            s3::S3ErrorCode::NoSuchKey,
                            "the copy source does not exist",
                            &source_resource,
                        )))
                    }
                    Err(error) => return Err(error),
                };
                let etag = etag_of(source.generation);
                if let Some(value) = request.header("x-amz-copy-source-if-match") {
                    if !etag_matches(value, &etag) {
                        return Ok(Err(s3::error_response(
                            s3::S3ErrorCode::PreconditionFailed,
                            "x-amz-copy-source-if-match does not name the current version of the source",
                            &resource,
                        )));
                    }
                }
                if let Some(value) = request.header("x-amz-copy-source-if-none-match") {
                    if etag_matches(value, &etag) {
                        return Ok(Err(s3::error_response(
                            s3::S3ErrorCode::PreconditionFailed,
                            "x-amz-copy-source-if-none-match names the current version of the source",
                            &resource,
                        )));
                    }
                }
                let (current, current_attributes) = match plane.read_manifest_with_attributes(bucket, key) {
                    Ok((manifest, carried)) => (Some(manifest), carried),
                    Err(ObjectError::NotFound(_)) => (None, ChunkAttributes::default()),
                    Err(error) => return Err(error),
                };
                if current.as_ref().is_some_and(|manifest| !manifest.deleted) {
                    if let Some(refusal) = lock.refuse(&current_attributes) {
                        return Ok(Ok(Err(refusal)));
                    }
                }
                let replaced = current.as_ref().filter(|manifest| !manifest.deleted).map_or(0, |manifest| manifest.length);
                if let Some(refusal) = lock.refuse_quota(source.length, replaced) {
                    return Ok(Ok(Err(refusal)));
                }
                // a copy over a key keeps the version it replaces, like any other write
                if versioned {
                    if let Some(kept) = current.as_ref() {
                        if let Err(failure) = ClusterGateway::retain(plane, kept) {
                            return Ok(Ok(Err(failure)));
                        }
                    }
                }
                let destination = ChunkAttributes {
                    content_type: if replace_metadata { named.content_type.clone() } else { source_attributes.content_type.clone() },
                    user_metadata: if replace_metadata { named.user_metadata.clone() } else { source_attributes.user_metadata.clone() },
                    tags: if replace_tags { named.tags.clone() } else { source_attributes.tags.clone() },
                    retention_mode: named.retention_mode,
                    retain_until_ms: named.retain_until_ms,
                    legal_hold: named.legal_hold,
                    // the copy is a new write: the client that publishes stamps its own clock
                    modified_ms: 0,
                    // and it is in the class the request names, not the source's, and is not restored
                    storage_class: named.storage_class.clone(),
                    restore_until_ms: 0,
                };
                Ok(Ok(plane.copy_with(&source_bucket, &read_key, bucket, key, current, &destination)))
            });
            match outcome {
                Err(error) => return error_response(error, &resource, false),
                Ok(Err(refusal)) => return refusal,
                Ok(Ok(Ok(manifest))) => {
                    self.remember(&manifest);
                    self.note_change(bucket, "copy", key, manifest.generation, manifest.length);
                    return s3::xml_response(
                        200,
                        batch::copy_object_result_xml(now_ms(), &etag_of(manifest.generation)),
                    );
                }
                Ok(Ok(Err(failure))) => {
                    self.forget(bucket, key);
                    if failure.unconfirmed {
                        return unconfirmed(&failure.error, &resource);
                    }
                    // The destination changed between the read and the copy: look again.
                    if lost_race(&failure.error) {
                        continue;
                    }
                    return error_response(failure.error, &resource, false);
                }
            }
        }
        s3::error_response(
            s3::S3ErrorCode::PreconditionFailed,
            "the destination kept changing under this copy; gave up after several attempts",
            &resource,
        )
    }

    // ----- multipart -----

    /// The key and begin time an upload was created for, or the S3 answer for an unknown upload.
    fn upload_record(&self, bucket: &str, id: &str, resource: &str) -> Result<(String, u64), Response> {
        let unknown = || {
            s3::error_response(
                s3::S3ErrorCode::NoSuchUpload,
                "the upload does not exist, or it was completed or aborted",
                resource,
            )
        };
        if !valid_upload_id(id) {
            return Err(unknown());
        }
        let record = self.with_plane(|plane| plane.read(bucket, &record_key(id)));
        let (_, bytes) = match record {
            Ok(value) => value,
            Err(ObjectError::NotFound(_)) => return Err(unknown()),
            Err(error) => return Err(error_response(error, resource, false)),
        };
        let text = String::from_utf8_lossy(&bytes).to_string();
        match text.split_once('\n') {
            Some((key, rest)) => Ok((key.to_string(), rest.lines().next().unwrap_or("").trim().parse().unwrap_or(0))),
            None => Err(unknown()),
        }
    }

    /// The attributes named when the upload began.
    fn upload_attributes(&self, bucket: &str, id: &str, resource: &str) -> Result<ChunkAttributes, Response> {
        let record = self.with_plane(|plane| plane.read(bucket, &record_key(id)));
        match record {
            Ok((_, bytes)) => Ok(decode_upload_attributes(&String::from_utf8_lossy(&bytes))),
            Err(ObjectError::NotFound(_)) => Ok(ChunkAttributes::default()),
            Err(error) => Err(error_response(error, resource, false)),
        }
    }

    /// The parts an upload holds, by number: (number, generation, length).
    fn upload_parts(&self, bucket: &str, id: &str) -> Result<Vec<(u32, u64, u64)>, ObjectError> {
        let prefix = format!("{RESERVED_PREFIX}{id}/");
        self.with_plane(|plane| {
            let mut parts = Vec::new();
            let mut after = Some(just_before(&prefix));
            'pages: loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    let Some(rest) = summary.key.strip_prefix(prefix.as_str()) else {
                        break 'pages;
                    };
                    if summary.deleted || rest == "meta" {
                        continue;
                    }
                    if let Ok(number) = rest.parse::<u32>() {
                        parts.push((number, summary.generation, summary.length));
                    }
                }
                if !page.has_more {
                    break;
                }
            }
            parts.sort_unstable();
            Ok(parts)
        })
    }

    /// Remove an upload's parts and its record. Best effort: what cannot be removed now is
    /// invisible (reserved keys are not served) and is removed by a later abort.
    fn remove_upload(&self, bucket: &str, id: &str) {
        if let Ok(parts) = self.upload_parts(bucket, id) {
            for (number, _, _) in parts {
                let _ = self.with_plane(|plane| plane.delete(bucket, &part_key(id, number)));
            }
        }
        let _ = self.with_plane(|plane| plane.delete(bucket, &record_key(id)));
    }

    /// `CreateMultipartUpload`: `POST /{bucket}/{key}?uploads`.
    pub(super) fn create_multipart_upload(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        let named = match self.request_attributes(request, bucket, &resource) {
            Ok(named) => named,
            Err(refusal) => return refusal,
        };
        let id = new_upload_id();
        let record = format!("{key}\n{}{}", now_ms(), encode_upload_attributes(&named));
        let written = self.with_plane(|plane| {
            plane
                .write(bucket, &record_key(&id), None, record.as_bytes())
                .map(|_| ())
                .map_err(|failure| failure.error)
        });
        match written {
            Ok(()) => s3::xml_response(200, mp::initiate_multipart_upload_xml(bucket, key, &id)),
            Err(error) => error_response(error, &resource, false),
        }
    }

    /// `UploadPart`: `PUT /{bucket}/{key}?partNumber=N&uploadId=ID`.
    pub(super) fn upload_part(
        &self,
        request: &Request,
        bucket: &str,
        key: &str,
        id: &str,
        number: &str,
    ) -> Response {
        let resource = format!("/{bucket}/{key}");
        let number = match number.parse::<u32>() {
            Ok(number) if (1..=MAX_PART_NUMBER).contains(&number) => number,
            _ => {
                return s3::error_response(
                    s3::S3ErrorCode::InvalidArgument,
                    &format!("the part number must be a whole number from 1 to {MAX_PART_NUMBER}"),
                    &resource,
                )
            }
        };
        match self.upload_record(bucket, id, &resource) {
            Ok((target, _)) if target == key => {}
            Ok(_) => {
                return s3::error_response(
                    s3::S3ErrorCode::NoSuchUpload,
                    "the upload was begun for another key",
                    &resource,
                )
            }
            Err(response) => return response,
        }
        match self.replace_manifest(bucket, &part_key(id, number), &request.body, false, None) {
            Ok(manifest) => object_written(&manifest),
            Err(response) => response,
        }
    }

    /// `CompleteMultipartUpload`: `POST /{bucket}/{key}?uploadId=ID` with the part list.
    pub(super) fn complete_multipart_upload(
        &self,
        request: &Request,
        bucket: &str,
        key: &str,
        id: &str,
    ) -> Response {
        let resource = format!("/{bucket}/{key}");
        match self.upload_record(bucket, id, &resource) {
            Ok((target, _)) if target == key => {}
            Ok(_) => {
                return s3::error_response(
                    s3::S3ErrorCode::NoSuchUpload,
                    "the upload was begun for another key",
                    &resource,
                )
            }
            Err(response) => return response,
        }
        let claimed = match mp::parse_complete_multipart_upload(&request.body) {
            Ok(claimed) => claimed,
            Err(reason) => {
                return s3::error_response(s3::S3ErrorCode::MalformedXml, &reason, &resource)
            }
        };
        let held = match self.upload_parts(bucket, id) {
            Ok(held) => held,
            Err(error) => return error_response(error, &resource, false),
        };
        let stored: Vec<mp::StoredPart> = held
            .iter()
            .map(|(number, generation, _)| mp::StoredPart {
                part_number: *number,
                etag: etag_of(*generation),
            })
            .collect();
        if let Some((code, message)) = mp::validate_claimed_parts(&claimed, &stored) {
            return s3::error_response(code, &message, &resource);
        }
        let total: u64 = claimed
            .iter()
            .filter_map(|part| held.iter().find(|(number, _, _)| *number == part.part_number))
            .map(|(_, _, length)| *length)
            .sum();
        if total > self.options.max_object_bytes as u64 {
            return s3::error_response(
                s3::S3ErrorCode::InvalidRequest,
                &format!(
                    "the assembled object would be {total} bytes and this gateway assembles at most {}",
                    self.options.max_object_bytes
                ),
                &resource,
            );
        }
        let mut assembled = Vec::with_capacity(total as usize);
        for part in &claimed {
            let read = self.with_plane(|plane| plane.read(bucket, &part_key(id, part.part_number)));
            match read {
                // The part must still be the one the client named: a re-upload of the number
                // between the listing and now is a different part.
                Ok((manifest, bytes)) if etag_of(manifest.generation) == part.etag => {
                    assembled.extend_from_slice(&bytes)
                }
                Ok(_) | Err(ObjectError::NotFound(_)) => {
                    return s3::error_response(
                        s3::S3ErrorCode::InvalidPart,
                        &format!("part {} changed or vanished while the upload was completed", part.part_number),
                        &resource,
                    )
                }
                Err(error) => return error_response(error, &resource, false),
            }
        }
        let attributes = match self.upload_attributes(bucket, id, &resource) {
            Ok(attributes) => attributes,
            Err(response) => return response,
        };
        let lock = match self.lock_check(request, bucket, &resource).and_then(|lock| self.with_quota(lock, bucket, &resource)) {
            Ok(lock) => lock,
            Err(refusal) => return refusal,
        };
        match self.replace_manifest_with(bucket, key, &assembled, false, None, &attributes, lock) {
            Ok(manifest) => {
                self.remove_upload(bucket, id);
                s3::xml_response(
                    200,
                    mp::complete_multipart_upload_xml(bucket, key, &etag_of(manifest.generation)),
                )
            }
            Err(response) => response,
        }
    }

    /// `AbortMultipartUpload`: `DELETE /{bucket}/{key}?uploadId=ID`.
    pub(super) fn abort_multipart_upload(&self, bucket: &str, key: &str, id: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        match self.upload_record(bucket, id, &resource) {
            Ok(_) => {}
            Err(response) => return response,
        }
        self.remove_upload(bucket, id);
        Response::status(204)
    }

    /// `ListParts`: `GET /{bucket}/{key}?uploadId=ID`.
    pub(super) fn list_parts(&self, request: &Request, bucket: &str, key: &str, id: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        match self.upload_record(bucket, id, &resource) {
            Ok((target, _)) if target == key => {}
            Ok(_) => {
                return s3::error_response(
                    s3::S3ErrorCode::NoSuchUpload,
                    "the upload was begun for another key",
                    &resource,
                )
            }
            Err(response) => return response,
        }
        let marker = request
            .query_param("part-number-marker")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let max_parts = request
            .query_param("max-parts")
            .and_then(|value| value.parse::<usize>().ok())
            .map_or(DEFAULT_LISTING, |value| value.clamp(1, DEFAULT_LISTING));
        let held = match self.upload_parts(bucket, id) {
            Ok(held) => held,
            Err(error) => return error_response(error, &resource, false),
        };
        let mut later: Vec<_> = held.into_iter().filter(|(number, _, _)| *number > marker).collect();
        let truncated = later.len() > max_parts;
        later.truncate(max_parts);
        let next = if truncated { later.last().map(|(number, _, _)| *number) } else { None };
        let result = mp::ListPartsResult {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: id.to_string(),
            part_number_marker: marker,
            next_part_number_marker: next,
            max_parts,
            is_truncated: truncated,
            storage_class: s3::DEFAULT_STORAGE_CLASS.to_string(),
            parts: later
                .into_iter()
                .map(|(number, generation, length)| mp::PartEntry {
                    part_number: number,
                    last_modified_ms: 0,
                    etag: etag_of(generation),
                    size: length,
                })
                .collect(),
        };
        s3::xml_response(200, mp::list_parts_xml(&result))
    }

    /// `ListMultipartUploads`: `GET /{bucket}?uploads`.
    pub(super) fn list_multipart_uploads(&self, request: &Request, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let prefix = request.query_param("prefix").unwrap_or_default();
        let max_uploads = request
            .query_param("max-uploads")
            .and_then(|value| value.parse::<usize>().ok())
            .map_or(DEFAULT_LISTING, |value| value.clamp(1, DEFAULT_LISTING));
        let ids = self.with_plane(|plane| {
            let mut ids = Vec::new();
            let mut after = Some(just_before(RESERVED_PREFIX));
            'pages: loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    let Some(rest) = summary.key.strip_prefix(RESERVED_PREFIX) else {
                        break 'pages;
                    };
                    if let Some(id) = rest.strip_suffix("/meta") {
                        if !summary.deleted && valid_upload_id(id) {
                            ids.push(id.to_string());
                        }
                    }
                }
                if !page.has_more {
                    break;
                }
            }
            Ok(ids)
        });
        let ids = match ids {
            Ok(ids) => ids,
            Err(error) => return error_response(error, &resource, true),
        };
        let mut uploads = Vec::new();
        for id in ids {
            if let Ok((key, begun)) = self.upload_record(bucket, &id, &resource) {
                if key.starts_with(prefix.as_str()) {
                    uploads.push(mp::UploadEntry {
                        key,
                        upload_id: id,
                        initiated_ms: begun,
                        storage_class: s3::DEFAULT_STORAGE_CLASS.to_string(),
                    });
                }
            }
        }
        uploads.sort_by(|a, b| (&a.key, &a.upload_id).cmp(&(&b.key, &b.upload_id)));
        let truncated = uploads.len() > max_uploads;
        uploads.truncate(max_uploads);
        let next = if truncated { uploads.last().map(|upload| (upload.key.clone(), upload.upload_id.clone())) } else { None };
        let result = mp::ListUploadsResult {
            bucket: bucket.to_string(),
            prefix,
            delimiter: None,
            key_marker: String::new(),
            upload_id_marker: String::new(),
            next_key_marker: next.as_ref().map(|(key, _)| key.clone()),
            next_upload_id_marker: next.map(|(_, id)| id),
            max_uploads,
            is_truncated: truncated,
            uploads,
            common_prefixes: Vec::new(),
        };
        s3::xml_response(200, mp::list_multipart_uploads_xml(&result))
    }
}
