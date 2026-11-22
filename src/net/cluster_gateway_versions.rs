// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Object versioning on the chunk path.
//!
//! # WHERE A VERSION LIVES
//!
//! The current version of an object is the manifest at its key. When the bucket's versioning is enabled, the
//! manifest an overwrite or a delete is about to replace is first kept as a second manifest at the hidden key
//! `.versions/<key>/<generation>`: the same extents, the same attributes, a new name. No data is copied, because a
//! manifest names pages and two manifests may name the same ones, and the reclaim sweep frees a page only when no
//! manifest names it. A version's id is the generation it had when it was current. A delete publishes a tombstone,
//! which is the delete marker and is the current version until something is written; the marker is kept like any
//! other version when it is replaced.
//!
//! The keep and the replacement are two publications. A crash between them leaves a retained copy of what is still
//! current, and the retry keeps the same generation under the same hidden key, which already exists and is
//! accepted: the retry is idempotent. An append extends the current version in place and keeps no copy.
//!
//! Deleting a specific version removes its manifest (an object lock on it applies); deleting the current version
//! brings the latest retained one back as current.
use super::attributes::LockCheck;
use super::*;
use crate::bucket_config as bc;

/// Where retained versions live, under the bucket they belong to.
pub(super) const VERSIONS_PREFIX: &str = ".versions/";

/// The hidden key of `key`'s version `generation`. Zero padded, so the versions of a key sort oldest first.
pub(super) fn retained_key(key: &str, generation: u64) -> String {
    format!("{VERSIONS_PREFIX}{key}/{generation:020}")
}

/// Whether the manifest at a hidden key is a kept version. A kept version is live; a kept delete marker is a tombstone
/// at the first generation of its hidden key; a tombstone at a later one is a version that was removed.
fn is_kept(deleted: bool, generation: u64) -> bool {
    !deleted || generation == 1
}

/// `(key, generation)` of a hidden key, or `None` for any other.
fn parse_retained(hidden: &str) -> Option<(String, u64)> {
    let rest = hidden.strip_prefix(VERSIONS_PREFIX)?;
    let (key, generation) = rest.rsplit_once('/')?;
    Some((key.to_string(), generation.parse().ok()?))
}

impl ClusterGateway {
    /// Whether writes to `bucket` keep the version they replace.
    pub(super) fn versioning_on(&self, bucket: &str) -> Result<bool, ObjectError> {
        if !self.options.require_buckets {
            return Ok(false);
        }
        Ok(self.bucket_rules(bucket)?.is_some_and(|rules| rules.versioning == bc::VERSIONING_ENABLED))
    }

    /// Keep `current` (live or a delete marker) as a version. Keeping what is already kept succeeds.
    pub(super) fn retain(plane: &mut dyn ObjectPlane, current: &ObjectManifest) -> Result<(), PlaneFailure> {
        match plane.alias(&current.bucket, &current.key, &retained_key(&current.key, current.generation), None) {
            Ok(_) => Ok(()),
            Err(failure) if matches!(failure.error, ObjectError::PreconditionFailed(_)) && !failure.unconfirmed => Ok(()),
            Err(failure) => Err(failure),
        }
    }

    /// The generations of `key` that are kept, oldest first.
    fn retained_generations(&self, bucket: &str, key: &str) -> Result<Vec<u64>, ObjectError> {
        let prefix = format!("{VERSIONS_PREFIX}{key}/");
        self.with_plane(|plane| {
            let mut found = Vec::new();
            // keys are compared as bytes; the hidden keys of one object are contiguous
            let mut after: Option<String> = Some(format!("{VERSIONS_PREFIX}{key}."));
            loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if !summary.key.starts_with(&prefix) {
                        if summary.key.as_str() > prefix.as_str() {
                            return Ok(found);
                        }
                        continue;
                    }
                    if let Some((_, generation)) = parse_retained(&summary.key) {
                        if is_kept(summary.deleted, summary.generation) {
                            found.push(generation);
                        }
                    }
                }
                if !page.has_more || page.entries.is_empty() {
                    return Ok(found);
                }
            }
        })
    }

    /// `DELETE` of a key in a bucket with versioning: keep the current version and publish a delete marker.
    pub(super) fn versioned_delete(&self, bucket: &str, key: &str, lock: LockCheck) -> Result<Option<ObjectManifest>, ObjectError> {
        self.with_plane(|plane| {
            let (current, carried) = match plane.read_manifest_with_attributes(bucket, key) {
                Ok(found) => found,
                Err(ObjectError::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            if current.deleted {
                return Ok(None);
            }
            // a lock protects the bytes, and a delete marker hides them without touching them: it is not refused
            let _ = (&carried, lock);
            Self::retain(plane, &current).map_err(|failure| failure.error)?;
            plane.delete_at(&current).map(Some)
        })
    }

    /// The key the version `version` of `key` is stored at: the key itself when it is the current version, else its
    /// hidden key.
    pub(super) fn version_key(&self, bucket: &str, key: &str, version: &str, resource: &str) -> Result<String, Response> {
        let Ok(generation) = version.trim().parse::<u64>() else {
            return Err(s3::error_response(s3::S3ErrorCode::InvalidArgument, "a version id is a whole number", resource));
        };
        match self.with_plane(|plane| plane.read_manifest(bucket, key)) {
            Ok(current) if current.generation == generation => Ok(key.to_string()),
            Ok(_) | Err(ObjectError::NotFound(_)) => Ok(retained_key(key, generation)),
            Err(error) => Err(error_response(error, resource, false)),
        }
    }

    /// `GET`, `HEAD` or `DELETE` of one version: `?versionId=N`.
    pub(super) fn version_request(&self, request: &Request, bucket: &str, key: &str, version: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        let Ok(generation) = version.trim().parse::<u64>() else {
            return s3::error_response(s3::S3ErrorCode::InvalidArgument, "a version id is a whole number", &resource);
        };
        let method = request.method.as_str();
        if !matches!(method, "GET" | "HEAD" | "DELETE") {
            return method_not_allowed("GET, HEAD, DELETE", &resource);
        }
        let no_version = || s3::error_response(s3::S3ErrorCode::NoSuchVersion, "the version does not exist", &resource);
        let current = match self.with_plane(|plane| plane.read_manifest_with_attributes(bucket, key)) {
            Ok(found) => Some(found),
            Err(ObjectError::NotFound(_)) => None,
            Err(error) => return error_response(error, &resource, false),
        };
        // the current version is the key itself
        if let Some((manifest, carried)) = &current {
            if manifest.generation == generation {
                return match method {
                    "DELETE" => self.delete_current_version(request, bucket, key, manifest, carried),
                    _ if manifest.deleted => marker_response(generation, &resource),
                    _ => self.get_object(request, bucket, key, method == "HEAD"),
                };
            }
        }
        let hidden = retained_key(key, generation);
        let kept = match self.with_plane(|plane| plane.read_manifest_with_attributes(bucket, &hidden)) {
            Ok(found) => found,
            Err(ObjectError::NotFound(_)) => return no_version(),
            Err(error) => return error_response(error, &resource, false),
        };
        let (kept_manifest, kept_attributes) = kept;
        // a version that was removed leaves a later tombstone at its hidden key
        if !is_kept(kept_manifest.deleted, kept_manifest.generation) {
            return no_version();
        }
        if method == "DELETE" {
            let lock = match self.lock_check(request, bucket, &resource) {
                Ok(lock) => lock,
                Err(response) => return response,
            };
            if !kept_manifest.deleted {
                if let Some(refusal) = lock.refuse(&kept_attributes) {
                    return error_response(refusal.error, &resource, false);
                }
            }
            return match self.with_plane(|plane| plane.delete_at(&kept_manifest)) {
                Ok(_) => {
                    self.note_change(bucket, "delete-version", key, generation, 0);
                    Response::status(204).with_header("x-amz-version-id", generation.to_string())
                }
                Err(ObjectError::NotFound(_)) => no_version(),
                Err(error) => error_response(error, &resource, false),
            };
        }
        if kept_manifest.deleted {
            return marker_response(generation, &resource);
        }
        let length = kept_manifest.length;
        let mut response = if method == "HEAD" {
            Response::status(200)
        } else {
            match http::resolve_range(request.header("range"), length) {
                ResolvedRange::Whole => match self.with_plane(|plane| plane.read(bucket, &hidden)) {
                    Ok((_, data)) => Response { status: 200, headers: Vec::new(), body: data },
                    Err(error) => return error_response(error, &resource, false),
                },
                ResolvedRange::Partial { offset, length: span } => {
                    match self.with_plane(|plane| plane.read_range(bucket, &hidden, offset, span)) {
                        Ok((_, data)) => {
                            let mut partial = Response { status: 206, headers: Vec::new(), body: data };
                            partial.headers.push(("content-range".into(), format!("bytes {offset}-{}/{length}", offset + span - 1)));
                            partial
                        }
                        Err(error) => return error_response(error, &resource, false),
                    }
                }
                ResolvedRange::Unsatisfiable => {
                    return s3::error_response(
                        s3::S3ErrorCode::InvalidRange,
                        &format!("the requested range lies outside the version, which is {length} bytes"),
                        &resource,
                    )
                    .with_header("content-range", format!("bytes */{length}"))
                }
            }
        };
        let shown = ObjectManifest { generation, ..kept_manifest };
        object_headers(&mut response, &shown);
        attributes::apply_attribute_headers(&mut response, &kept_attributes);
        response
    }

    /// Delete the current version permanently: the latest kept version, if any, becomes current again.
    fn delete_current_version(&self, request: &Request, bucket: &str, key: &str, current: &ObjectManifest, carried: &ChunkAttributes) -> Response {
        let resource = format!("/{bucket}/{key}");
        let lock = match self.lock_check(request, bucket, &resource) {
            Ok(lock) => lock,
            Err(response) => return response,
        };
        if !current.deleted {
            if let Some(refusal) = lock.refuse(carried) {
                return error_response(refusal.error, &resource, false);
            }
        }
        let latest = match self.retained_generations(bucket, key) {
            Ok(generations) => generations.last().copied(),
            Err(error) => return error_response(error, &resource, false),
        };
        self.forget(bucket, key);
        let outcome = self.with_plane(|plane| match latest {
            // the latest kept version comes back on top of the current manifest, and its copy is dropped
            Some(generation) => {
                let hidden = retained_key(key, generation);
                let restored = plane.alias(bucket, &hidden, key, Some(current)).map_err(|failure| failure.error)?;
                if let Ok(kept) = plane.read_manifest(bucket, &hidden) {
                    let _ = plane.delete_at(&kept);
                }
                Ok(Some(restored))
            }
            None => plane.delete_at(current).map(|_| None),
        });
        match outcome {
            Ok(Some(restored)) => {
                self.remember(&restored);
                Response::status(204).with_header("x-amz-version-id", current.generation.to_string())
            }
            Ok(None) => Response::status(204).with_header("x-amz-version-id", current.generation.to_string()),
            Err(error) => error_response(error, &resource, false),
        }
    }

    /// `GET /{bucket}?versions`: every version and delete marker, newest first within a key.
    pub(super) fn list_versions(&self, request: &Request, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let prefix = request.query_param("prefix").unwrap_or_default();
        let marker = request.query_param("key-marker").unwrap_or_default();
        let max_keys = request.query_param("max-keys").and_then(|raw| raw.parse::<usize>().ok()).unwrap_or(1000).clamp(1, 1000);
        let collected = self.with_plane(|plane| {
            // (key, generation, is_latest, deleted, length)
            let mut rows: Vec<(String, u64, bool, bool, u64)> = Vec::new();
            let mut after: Option<String> = None;
            loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if let Some((key, generation)) = parse_retained(&summary.key) {
                        if key.starts_with(prefix.as_str()) && is_kept(summary.deleted, summary.generation) {
                            rows.push((key, generation, false, summary.deleted, summary.length));
                        }
                    } else if summary.key.starts_with(prefix.as_str()) && !s3_verbs::is_reserved(&summary.key) {
                        rows.push((summary.key.clone(), summary.generation, true, summary.deleted, summary.length));
                    }
                }
                if !page.has_more || page.entries.is_empty() {
                    break;
                }
            }
            Ok(rows)
        });
        let mut rows = match collected {
            Ok(rows) => rows,
            Err(error) => return error_response(error, &resource, true),
        };
        rows.retain(|row| row.0.as_str() > marker.as_str() || (marker.is_empty()));
        // by key, newest version first
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        let truncated = rows.len() > max_keys;
        rows.truncate(max_keys);
        let mut body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListVersionsResult xmlns=\"{}\"><Name>{bucket}</Name><Prefix>{}</Prefix><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",
            s3::S3_XML_NAMESPACE,
            bc::xml_escape(&prefix)
        );
        if truncated {
            if let Some(last) = rows.last() {
                body.push_str(&format!("<NextKeyMarker>{}</NextKeyMarker>", bc::xml_escape(&last.0)));
            }
        }
        for (key, generation, latest, deleted, length) in &rows {
            let tag = if *deleted { "DeleteMarker" } else { "Version" };
            body.push_str(&format!("<{tag}><Key>{}</Key><VersionId>{generation}</VersionId><IsLatest>{latest}</IsLatest>", bc::xml_escape(key)));
            if !*deleted {
                body.push_str(&format!("<ETag>\"{}\"</ETag><Size>{length}</Size>", etag_of(*generation)));
            }
            body.push_str(&format!("</{tag}>"));
        }
        body.push_str("</ListVersionsResult>\n");
        s3::xml_response(200, body)
    }

    /// The keys under `prefix` that have a kept version, in order.
    fn versioned_keys(&self, bucket: &str, prefix: &str) -> Result<Vec<String>, ObjectError> {
        self.with_plane(|plane| {
            let mut found: Vec<String> = Vec::new();
            let mut after: Option<String> = Some(".versions.".to_string());
            loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if !summary.key.starts_with(VERSIONS_PREFIX) {
                        return Ok(found);
                    }
                    if let Some((key, _)) = parse_retained(&summary.key) {
                        if key.starts_with(prefix) && found.last() != Some(&key) {
                            found.push(key);
                        }
                    }
                }
                if !page.has_more || page.entries.is_empty() {
                    return Ok(found);
                }
            }
        })
    }

    /// Expire kept versions that stopped being current `age_ms` ago, under `prefix`, keeping the `keep` newest whatever
    /// their age. A version's age is counted from when its successor was written; with `age_ms` zero every version past
    /// the `keep` newest goes.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn expire_noncurrent(
        &self,
        bucket: &str,
        prefix: &str,
        tag: Option<(&str, &str)>,
        age_ms: u64,
        keep: usize,
        now_ms: u64,
        lock: LockCheck,
        report: &mut super::lifecycle::LifecycleReport,
    ) -> Result<(), ObjectError> {
        for key in self.versioned_keys(bucket, prefix)? {
            let generations = self.retained_generations(bucket, &key)?;
            let current = match self.with_plane(|plane| plane.read_manifest_with_attributes(bucket, &key)) {
                Ok((_, carried)) => Some(carried.modified_ms),
                Err(ObjectError::NotFound(_)) => None,
                Err(error) => return Err(error),
            };
            let candidates = generations.len().saturating_sub(keep);
            for (index, generation) in generations.iter().enumerate().take(candidates) {
                let hidden = retained_key(&key, *generation);
                if age_ms > 0 {
                    // when the next version, kept or current, was written
                    let successor = match generations.get(index + 1) {
                        Some(next) => match self.with_plane(|plane| plane.read_manifest_with_attributes(bucket, &retained_key(&key, *next))) {
                            Ok((_, carried)) => carried.modified_ms,
                            Err(ObjectError::NotFound(_)) => continue,
                            Err(error) => return Err(error),
                        },
                        None => current.unwrap_or(0),
                    };
                    if successor == 0 || now_ms.saturating_sub(successor) < age_ms {
                        continue;
                    }
                }
                let verdict = self.with_plane(|plane| {
                    let (manifest, carried) = plane.read_manifest_with_attributes(bucket, &hidden)?;
                    if !manifest.deleted {
                        if !attributes::tag_matches(&carried, tag) {
                            return Ok(false);
                        }
                        if let Some(refusal) = lock.refuse(&carried) {
                            return Err(refusal.error);
                        }
                    }
                    plane.delete_at(&manifest).map(|_| true)
                });
                match verdict {
                    Ok(true) => report.expired_versions += 1,
                    Ok(false) => {}
                    Err(ObjectError::PreconditionFailed(message)) if message.starts_with(attributes::LOCKED) => report.kept_locked += 1,
                    Err(ObjectError::NotFound(_)) | Err(ObjectError::PreconditionFailed(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    /// Remove the kept delete markers that no kept version older than them is left under: a marker over nothing hides
    /// nothing.
    pub(super) fn expire_markers(&self, bucket: &str, prefix: &str, report: &mut super::lifecycle::LifecycleReport) -> Result<(), ObjectError> {
        for key in self.versioned_keys(bucket, prefix)? {
            loop {
                let Some(oldest) = self.retained_generations(bucket, &key)?.first().copied() else { break };
                let hidden = retained_key(&key, oldest);
                let verdict = self.with_plane(|plane| {
                    let (manifest, _) = plane.read_manifest_with_attributes(bucket, &hidden)?;
                    if !manifest.deleted {
                        return Ok(false);
                    }
                    plane.delete_at(&manifest).map(|_| true)
                });
                match verdict {
                    Ok(true) => report.expired_markers += 1,
                    Ok(false) | Err(ObjectError::NotFound(_)) | Err(ObjectError::PreconditionFailed(_)) => break,
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}

/// The answer for a version that is a delete marker.
fn marker_response(generation: u64, resource: &str) -> Response {
    s3::error_response(s3::S3ErrorCode::MethodNotAllowed, "the version is a delete marker", resource)
        .with_header("x-amz-delete-marker", "true")
        .with_header("x-amz-version-id", generation.to_string())
}
