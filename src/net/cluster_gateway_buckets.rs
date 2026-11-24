// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Buckets and their configuration: `CreateBucket`, `HeadBucket`, `DeleteBucket`, `ListBuckets`, and the
//! get, put and delete of the versioning, tagging, CORS, policy, lifecycle, quota and object-lock
//! configurations.
//!
//! A bucket is the record [`crate::bucket_config::BucketConfig`], stored as an ordinary chunk object in the
//! reserved system bucket. It exists exactly when its record does. A change to it is a compare-and-swap on the
//! record's generation, retried when another writer got in first, so two clients configuring one bucket never
//! overwrite each other. A request to a bucket that has no record is refused with `NoSuchBucket`.
use super::*;
use crate::bucket_config::{self as bc, BucketConfig, SYSTEM_BUCKET};

/// How long a gateway remembers that a bucket exists, so an object request costs no extra read.
const EXISTS_TTL: Duration = Duration::from_secs(2);
/// Attempts at a compare-and-swap update before the caller is told to retry.
const UPDATE_ROUNDS: usize = 5;

impl ClusterGateway {
    /// The record of `bucket` with the manifest that carries its generation, or `None` when it has none.
    pub(super) fn load_bucket(&self, bucket: &str) -> Result<Option<(ObjectManifest, BucketConfig)>, ObjectError> {
        match self.with_plane(|plane| plane.read(SYSTEM_BUCKET, &bc::config_key(bucket))) {
            Ok((manifest, bytes)) if !manifest.deleted => Ok(Some((manifest, BucketConfig::from_bytes(&bytes)?))),
            Ok(_) | Err(ObjectError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn store_bucket(&self, bucket: &str, expected: Option<ObjectManifest>, config: &BucketConfig) -> Result<(), ObjectError> {
        let bytes = config.to_bytes()?;
        let stored = self.with_plane(|plane| {
            plane
                .write(SYSTEM_BUCKET, &bc::config_key(bucket), expected, &bytes)
                .map(|_| ())
                .map_err(|failure| failure.error)
        });
        self.forget_bucket_rules(bucket);
        stored
    }

    fn create_bucket_record(&self, bucket: &str, config: &BucketConfig) -> Result<bool, ObjectError> {
        let expected = self.with_plane(|plane| {
            match plane.read_manifest(SYSTEM_BUCKET, &bc::config_key(bucket)) {
                Ok(manifest) => Ok(Some(manifest)),
                Err(ObjectError::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })?;
        if expected.as_ref().is_some_and(|manifest| !manifest.deleted) {
            return self.load_bucket(bucket)?.map(|_| false).ok_or_else(|| {
                ObjectError::PreconditionFailed("the bucket changed during creation; retry".into())
            });
        }
        match self.store_bucket(bucket, expected, config) {
            Ok(()) => Ok(true),
            Err(error @ ObjectError::PreconditionFailed(_)) => {
                if self.load_bucket(bucket)?.is_some() {
                    Ok(false)
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Create `bucket` if it has no record. Returns whether it was created.
    pub fn ensure_bucket(&self, bucket: &str) -> Result<bool, ObjectError> {
        crate::validate_bucket(bucket)?;
        if self.load_bucket(bucket)?.is_some() {
            return Ok(false);
        }
        let created = self.create_bucket_record(bucket, &BucketConfig::new(now_ms()))?;
        self.note_bucket(bucket, true);
        Ok(created)
    }

    /// Change the record of `bucket` with `change`, retried when the record moved under it.
    pub(super) fn update_bucket(
        &self,
        bucket: &str,
        change: impl Fn(&mut BucketConfig) -> Result<(), ObjectError>,
    ) -> Result<BucketConfig, ObjectError> {
        for _ in 0..UPDATE_ROUNDS {
            let Some((manifest, mut config)) = self.load_bucket(bucket)? else {
                return Err(ObjectError::NotFound(format!("bucket {bucket} does not exist")));
            };
            change(&mut config)?;
            match self.store_bucket(bucket, Some(manifest), &config) {
                Ok(()) => return Ok(config),
                Err(ObjectError::PreconditionFailed(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ObjectError::PreconditionFailed("the bucket configuration kept changing; retry".into()))
    }

    /// The configuration of `bucket` as of at most [`EXISTS_TTL`] ago, or `None` when it has no record. A change made
    /// through this gateway is seen at once; one made through another gateway is seen within the TTL.
    pub(super) fn bucket_rules(&self, bucket: &str) -> Result<Option<Arc<BucketConfig>>, ObjectError> {
        if let Ok(cache) = self.bucket_configs.lock() {
            if let Some((at, rules)) = cache.get(bucket) {
                if at.elapsed() < EXISTS_TTL {
                    return Ok(rules.clone());
                }
            }
        }
        let rules = self.load_bucket(bucket)?.map(|(_, config)| Arc::new(config));
        if let Ok(mut cache) = self.bucket_configs.lock() {
            if cache.len() > 4096 {
                cache.clear();
            }
            cache.insert(bucket.to_string(), (Instant::now(), rules.clone()));
        }
        Ok(rules)
    }

    pub(super) fn forget_bucket_rules(&self, bucket: &str) {
        if let Ok(mut cache) = self.bucket_configs.lock() {
            cache.remove(bucket);
        }
    }

    fn note_bucket(&self, bucket: &str, exists: bool) {
        if let Ok(mut known) = self.buckets_known.lock() {
            if known.len() > 4096 {
                known.clear();
            }
            known.insert(bucket.to_string(), (Instant::now(), exists));
        }
    }

    /// The refusal for a request to a bucket that has no record.
    pub(super) fn bucket_gate(&self, bucket: &str) -> Option<Response> {
        if bucket == SYSTEM_BUCKET {
            return Some(s3::error_response(s3::S3ErrorCode::NoSuchBucket, "the bucket does not exist", &format!("/{bucket}")));
        }
        if let Ok(known) = self.buckets_known.lock() {
            if let Some((at, exists)) = known.get(bucket) {
                if at.elapsed() < EXISTS_TTL {
                    return (!*exists).then(|| s3::error_response(s3::S3ErrorCode::NoSuchBucket, "the bucket does not exist", &format!("/{bucket}")));
                }
            }
        }
        match self.load_bucket(bucket) {
            Ok(Some(_)) => {
                self.note_bucket(bucket, true);
                None
            }
            Ok(None) => {
                self.note_bucket(bucket, false);
                Some(s3::error_response(s3::S3ErrorCode::NoSuchBucket, "the bucket does not exist", &format!("/{bucket}")))
            }
            Err(error) => Some(error_response(error, &format!("/{bucket}"), true)),
        }
    }

    /// `PUT /{bucket}`: create it. Creating a bucket that exists is a success.
    pub(super) fn create_bucket(&self, request: &Request, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        if let Err(error) = crate::validate_bucket(bucket) {
            return error_response(error, &resource, true);
        }
        if bucket == SYSTEM_BUCKET {
            return s3::error_response(s3::S3ErrorCode::InvalidArgument, "that name is reserved", &resource);
        }
        let lock = request.header("x-amz-bucket-object-lock-enabled").is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let mut config = BucketConfig::new(now_ms());
        if lock {
            config.versioning = bc::VERSIONING_ENABLED;
            config.object_lock_enabled = true;
        }
        match self.create_bucket_record(bucket, &config) {
            Ok(_) => {
                self.note_bucket(bucket, true);
                Response::status(200).with_header("location", resource)
            }
            Err(error) => error_response(error, &resource, true),
        }
    }

    /// `HEAD /{bucket}`.
    pub(super) fn head_bucket(&self, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        match self.load_bucket(bucket) {
            Ok(Some(_)) => Response::status(200),
            Ok(None) => s3::error_response(s3::S3ErrorCode::NoSuchBucket, "the bucket does not exist", &resource),
            Err(error) => error_response(error, &resource, true),
        }
    }

    /// The names of every bucket that has a record, in order.
    pub(super) fn bucket_names(&self) -> Result<Vec<String>, ObjectError> {
        self.with_plane(|plane| {
            let mut names = Vec::new();
            // keys are compared as bytes, and '.' sorts just before '/': this starts at the first record
            let mut after: Option<String> = Some("bucket.".to_string());
            loop {
                let page = plane.list(SYSTEM_BUCKET, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    match bc::bucket_of_key(&summary.key) {
                        Some(name) if !summary.deleted => names.push(name.to_string()),
                        Some(_) => {}
                        None if summary.key.starts_with(bc::CONFIG_PREFIX) => {}
                        // past the records
                        None => return Ok(names),
                    }
                }
                if !page.has_more {
                    return Ok(names);
                }
            }
        })
    }

    /// `GET /` : `ListBuckets`.
    pub(super) fn list_buckets_from_records(&self, request: &Request) -> Response {
        if !matches!(request.method.as_str(), "GET" | "HEAD") {
            return method_not_allowed("GET, HEAD", "/");
        }
        match self.bucket_names() {
            Ok(names) => s3::xml_response(200, s3::list_all_my_buckets_xml(&names)),
            Err(error) => error_response(error, "/", true),
        }
    }

    /// Remove the record of `bucket` after `delete_bucket` found it empty.
    pub(super) fn drop_bucket_record(&self, bucket: &str) -> Result<(), ObjectError> {
        match self.with_plane(|plane| plane.delete(SYSTEM_BUCKET, &bc::config_key(bucket))) {
            Ok(_) | Err(ObjectError::NotFound(_)) => {
                self.forget_bucket_rules(bucket);
                self.note_bucket(bucket, false);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// The bucket sub-resources that carry configuration. Returns `None` for one this does not serve.
    pub(super) fn bucket_configuration(&self, request: &Request, bucket: &str, sub: &str) -> Option<Response> {
        let resource = format!("/{bucket}");
        let method = request.method.as_str();
        let missing = |code: s3::S3ErrorCode, message: &str| s3::error_response(code, message, &resource);
        let body = || String::from_utf8_lossy(&request.body).to_string();
        let malformed = |error: ObjectError| s3::error_response(s3::S3ErrorCode::MalformedXml, &error.to_string(), &resource);
        Some(match (method, sub) {
            ("GET", _) => {
                let config = match self.load_bucket(bucket) {
                    Ok(Some((_, config))) => config,
                    Ok(None) => return Some(missing(s3::S3ErrorCode::NoSuchBucket, "the bucket does not exist")),
                    Err(error) => return Some(error_response(error, &resource, true)),
                };
                match sub {
                    "versioning" => s3::xml_response(200, bc::versioning_xml(config.versioning)),
                    "tagging" => s3::xml_response(200, bc::tagging_xml(&config.tags)),
                    "cors" if config.cors.is_empty() => missing(s3::S3ErrorCode::NoSuchCorsConfiguration, "the bucket has no CORS configuration"),
                    "cors" => s3::xml_response(200, bc::cors_xml(&config.cors)),
                    "policy" if config.policy.is_empty() => missing(s3::S3ErrorCode::NoSuchBucketPolicy, "the bucket has no policy"),
                    "policy" => {
                        let mut reply = Response::status(200).with_header("content-type", "application/json");
                        reply.body = config.policy.into_bytes();
                        reply
                    }
                    "lifecycle" if config.lifecycle.is_empty() => missing(s3::S3ErrorCode::NoSuchLifecycleConfiguration, "the bucket has no lifecycle configuration"),
                    "lifecycle" => s3::xml_response(200, bc::lifecycle_xml(&config.lifecycle)),
                    "object-lock" if !config.object_lock_enabled => missing(s3::S3ErrorCode::ObjectLockConfigurationNotFound, "object lock is not enabled on the bucket"),
                    "object-lock" => s3::xml_response(200, bc::object_lock_xml(&config)),
                    "quota" => s3::xml_response(200, format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Quota><Bytes>{}</Bytes></Quota>\n", config.quota_bytes)),
                    "changefeed" => s3::xml_response(200, bc::change_feed_xml(config.change_feed)),
                    _ => return None,
                }
            }
            ("PUT", _) => {
                let text = body();
                let result = match sub {
                    "versioning" => bc::parse_versioning(&text).and_then(|state| {
                        self.update_bucket(bucket, |config| {
                            if config.object_lock_enabled && state != bc::VERSIONING_ENABLED {
                                return Err(ObjectError::InvalidArgument("versioning cannot be suspended on a bucket with object lock".into()));
                            }
                            config.versioning = state;
                            Ok(())
                        })
                    }),
                    "tagging" => match bc::parse_tagging(&text) {
                        Ok(tags) => self.update_bucket(bucket, move |config| {
                            config.tags = tags.clone();
                            Ok(())
                        }),
                        Err(error) => return Some(malformed(error)),
                    },
                    "cors" => match bc::parse_cors(&text) {
                        Ok(rules) => self.update_bucket(bucket, move |config| {
                            config.cors = rules.clone();
                            Ok(())
                        }),
                        Err(error) => return Some(malformed(error)),
                    },
                    "policy" => {
                        let policy = text.clone();
                        self.update_bucket(bucket, move |config| {
                            config.policy = policy.clone();
                            Ok(())
                        })
                    }
                    "lifecycle" => match bc::parse_lifecycle(&text) {
                        Ok(rules) => self.update_bucket(bucket, move |config| {
                            config.lifecycle = rules.clone();
                            Ok(())
                        }),
                        Err(error) => return Some(malformed(error)),
                    },
                    "object-lock" => match bc::parse_object_lock(&text) {
                        Ok((enabled, retention)) => self.update_bucket(bucket, move |config| {
                            // lock can be turned on, never turned off again; it works with versioning or without
                            config.object_lock_enabled = config.object_lock_enabled || enabled;
                            config.default_retention = retention.clone();
                            Ok(())
                        }),
                        Err(error) => return Some(malformed(error)),
                    },
                    "changefeed" => match bc::parse_change_feed(&text) {
                        Ok(enabled) => self.update_bucket(bucket, move |config| {
                            config.change_feed = enabled;
                            Ok(())
                        }),
                        Err(error) => return Some(malformed(error)),
                    },
                    "quota" => match bc::xml_first(&text, "Bytes").and_then(|b| b.trim().parse::<u64>().ok()) {
                        Some(bytes) => self.update_bucket(bucket, move |config| {
                            config.quota_bytes = bytes;
                            Ok(())
                        }),
                        None => return Some(malformed(ObjectError::InvalidArgument("a quota names Bytes".into()))),
                    },
                    _ => return None,
                };
                match result {
                    Ok(_) => Response::status(200),
                    Err(ObjectError::NotFound(_)) => missing(s3::S3ErrorCode::NoSuchBucket, "the bucket does not exist"),
                    Err(error) => error_response(error, &resource, true),
                }
            }
            ("DELETE", "tagging" | "cors" | "policy" | "lifecycle" | "quota") => {
                let result = self.update_bucket(bucket, |config| {
                    match sub {
                        "tagging" => config.tags.clear(),
                        "cors" => config.cors.clear(),
                        "policy" => config.policy.clear(),
                        "lifecycle" => config.lifecycle.clear(),
                        _ => config.quota_bytes = 0,
                    }
                    Ok(())
                });
                match result {
                    Ok(_) => Response::status(204),
                    Err(ObjectError::NotFound(_)) => missing(s3::S3ErrorCode::NoSuchBucket, "the bucket does not exist"),
                    Err(error) => error_response(error, &resource, true),
                }
            }
            _ => return None,
        })
    }
}
