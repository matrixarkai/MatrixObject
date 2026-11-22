// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! What an object carries besides its bytes, on the chunk path: its content type, its `x-amz-meta-*` metadata, its
//! tags, and the object-lock retention and legal hold, with the rules that make a lock a lock.
//!
//! The attributes are fields of the chunk manifest ([`crate::chunk_manifest::ChunkAttributes`]), so they are part of
//! the generation the catalog publishes: changing them publishes the same extents again one generation later, and
//! two writers changing one object's tags never overwrite each other (the second is refused and retried on what the
//! first left).
//!
//! # HOW A LOCK IS ENFORCED
//!
//! The catalog's decision is clock-free, so the gateway enforces retention. The check and the write meet at the
//! generation: a gateway reads the manifest with its attributes, refuses the overwrite or delete when the object is
//! locked at the time of the request, and otherwise publishes on top of exactly the generation it read. Any change
//! to the attributes in between is a new generation, so the publication is refused and the request starts again. A
//! GOVERNANCE retention may be bypassed with `x-amz-bypass-governance-retention: true`; a COMPLIANCE retention and a
//! legal hold may not. The check applies only in a bucket created with object lock: elsewhere the retention and legal
//! hold headers are refused, so no object in it can be locked, and a plain write costs no extra read.
use super::*;
use crate::bucket_config::{self as bc, parse_iso8601_ms};
use crate::chunk_manifest::{MAX_OBJECT_TAGS, RETENTION_COMPLIANCE, RETENTION_GOVERNANCE};

const BYPASS_HEADER: &str = "x-amz-bypass-governance-retention";
const DAY_MS: u64 = 86_400_000;
/// The start of the refusal a locked object produces; [`error_response`] answers it with `AccessDenied`.
pub(super) const LOCKED: &str = "the object is locked";

/// Whether a request must respect object lock, and whether it asks to bypass a governance retention.
#[derive(Clone, Copy, Debug)]
pub(super) struct LockCheck {
    pub enabled: bool,
    pub bypass: bool,
    /// The bucket's quota and its usage, when the request is a write into a bucket that has one.
    pub quota: Option<(u64, u64)>,
}

impl LockCheck {
    pub const OFF: Self = Self { enabled: false, bypass: false, quota: None };

    /// The refusal for an object that carries `attributes`, when it is locked now.
    pub fn refuse(&self, attributes: &ChunkAttributes) -> Option<PlaneFailure> {
        (self.enabled && attributes.locked_at(now_ms(), self.bypass)).then(|| {
            PlaneFailure::from(ObjectError::PreconditionFailed(format!(
                "{LOCKED}: its retention period or legal hold has not ended"
            )))
        })
    }
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                match std::str::from_utf8(&bytes[i + 1..i + 3]).ok().and_then(|hex| u8::from_str_radix(hex, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            byte => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The tags of an `x-amz-tagging` header: `key=value` pairs joined by `&`, percent-encoded.
fn parse_tagging_header(value: &str) -> Result<std::collections::BTreeMap<String, String>, ObjectError> {
    let mut tags = std::collections::BTreeMap::new();
    for pair in value.split('&').filter(|pair| !pair.is_empty()) {
        let (key, tag) = pair.split_once('=').unwrap_or((pair, ""));
        if tags.insert(percent_decode(key), percent_decode(tag)).is_some() {
            return Err(ObjectError::InvalidArgument("a tag key appears twice".into()));
        }
    }
    if tags.len() > MAX_OBJECT_TAGS {
        return Err(ObjectError::InvalidArgument(format!("an object carries at most {MAX_OBJECT_TAGS} tags")));
    }
    Ok(tags)
}

fn mode_name(mode: u32) -> &'static str {
    match mode {
        RETENTION_GOVERNANCE => "GOVERNANCE",
        RETENTION_COMPLIANCE => "COMPLIANCE",
        _ => "",
    }
}

fn parse_mode(text: &str) -> Option<u32> {
    match text.trim() {
        "GOVERNANCE" => Some(RETENTION_GOVERNANCE),
        "COMPLIANCE" => Some(RETENTION_COMPLIANCE),
        _ => None,
    }
}

/// Whether an object carrying `carried` passes a rule's tag filter (`None`: the rule has none).
pub(super) fn tag_matches(carried: &ChunkAttributes, tag: Option<(&str, &str)>) -> bool {
    match tag {
        None => true,
        Some((key, value)) => carried.tags.get(key).is_some_and(|found| found == value),
    }
}

/// The headers an object answers with for what it carries. The content type replaces the default one.
pub(super) fn apply_attribute_headers(response: &mut Response, attributes: &ChunkAttributes) {
    if !attributes.content_type.is_empty() {
        response.headers.retain(|(name, _)| name != "content-type");
        response.headers.push(("content-type".into(), attributes.content_type.clone()));
    }
    for (key, value) in &attributes.user_metadata {
        response.headers.push((format!("x-amz-meta-{key}"), value.clone()));
    }
    if !attributes.tags.is_empty() {
        response.headers.push(("x-amz-tagging-count".into(), attributes.tags.len().to_string()));
    }
    if attributes.retention_mode != 0 {
        response.headers.push(("x-amz-object-lock-mode".into(), mode_name(attributes.retention_mode).into()));
        response
            .headers
            .push(("x-amz-object-lock-retain-until-date".into(), s3::iso8601(attributes.retain_until_ms)));
    }
    if attributes.legal_hold {
        response.headers.push(("x-amz-object-lock-legal-hold".into(), "ON".into()));
    }
    if attributes.modified_ms != 0 {
        response.headers.push(("last-modified".into(), http::http_date(attributes.modified_ms)));
    }
    if !attributes.storage_class.is_empty() {
        response.headers.push(("x-amz-storage-class".into(), attributes.storage_class.clone()));
    }
    if attributes.restore_until_ms > now_ms() {
        response.headers.push((
            "x-amz-restore".into(),
            format!("ongoing-request=\"false\", expiry-date=\"{}\"", http::http_date(attributes.restore_until_ms)),
        ));
    }
}

/// One change to an object's attributes, applied to whatever the object carries when the request is decided.
enum Change {
    Tags(std::collections::BTreeMap<String, String>),
    Retention(u32, u64),
    Hold(bool),
    /// Keep the object readable from its colder class until this time.
    Restore(u64),
}

impl Change {
    /// Apply the change, or say why a lock forbids it.
    fn apply(&self, attributes: &mut ChunkAttributes, lock: LockCheck) -> Result<(), &'static str> {
        match self {
            Change::Tags(tags) => attributes.tags = tags.clone(),
            Change::Hold(on) => attributes.legal_hold = *on,
            Change::Restore(until) => attributes.restore_until_ms = *until,
            Change::Retention(mode, until) => {
                // a period in force is only lengthened, unless it is GOVERNANCE and the caller bypasses it
                let in_force = attributes.retention_mode != 0 && attributes.retain_until_ms > now_ms();
                let weaker = (*mode != attributes.retention_mode && attributes.retention_mode == RETENTION_COMPLIANCE)
                    || *until < attributes.retain_until_ms;
                if in_force && weaker && !(attributes.retention_mode == RETENTION_GOVERNANCE && lock.bypass) {
                    return Err("a retention in force is not shortened or relaxed");
                }
                attributes.retention_mode = *mode;
                attributes.retain_until_ms = *until;
            }
        }
        Ok(())
    }
}

impl ClusterGateway {
    /// Whether `bucket` was created with object lock (always false where buckets are implicit).
    fn object_lock_enabled(&self, bucket: &str) -> Result<bool, ObjectError> {
        if !self.options.require_buckets {
            return Ok(false);
        }
        Ok(self.bucket_rules(bucket)?.is_some_and(|rules| rules.object_lock_enabled))
    }

    /// How this request must treat a locked object.
    pub(super) fn lock_check(&self, request: &Request, bucket: &str, resource: &str) -> Result<LockCheck, Response> {
        let enabled = self.object_lock_enabled(bucket).map_err(|error| error_response(error, resource, true))?;
        let bypass = request.header(BYPASS_HEADER).is_some_and(|value| value.trim().eq_ignore_ascii_case("true"));
        Ok(LockCheck { enabled, bypass, quota: None })
    }

    /// The attributes a write names with its headers, and the bucket's default retention when it names none.
    pub(super) fn request_attributes(&self, request: &Request, bucket: &str, resource: &str) -> Result<ChunkAttributes, Response> {
        let invalid = |message: String| s3::error_response(s3::S3ErrorCode::InvalidArgument, &message, resource);
        let mut attributes = ChunkAttributes::default();
        if let Some(content_type) = request.header("content-type") {
            attributes.content_type = content_type.trim().to_string();
        }
        for (name, value) in &request.headers {
            if let Some(key) = name.to_ascii_lowercase().strip_prefix("x-amz-meta-") {
                attributes.user_metadata.insert(key.to_string(), value.clone());
            }
        }
        if let Some(tagging) = request.header("x-amz-tagging") {
            attributes.tags = parse_tagging_header(tagging).map_err(|error| invalid(error.to_string()))?;
        }
        if let Some(class) = request.header("x-amz-storage-class").map(str::trim).filter(|class| !class.is_empty() && *class != "STANDARD") {
            if class.len() > 32 || !class.bytes().all(|byte| byte.is_ascii_uppercase() || byte == b'_') {
                return Err(invalid("x-amz-storage-class is a name in capitals, such as STANDARD_IA or GLACIER".into()));
            }
            attributes.storage_class = class.to_string();
        }
        let mode = request.header("x-amz-object-lock-mode");
        let until = request.header("x-amz-object-lock-retain-until-date");
        let hold = request.header("x-amz-object-lock-legal-hold");
        let lock_enabled = self.object_lock_enabled(bucket).map_err(|error| error_response(error, resource, true))?;
        if (mode.is_some() || until.is_some() || hold.is_some()) && !lock_enabled {
            return Err(s3::error_response(
                s3::S3ErrorCode::InvalidRequest,
                "the bucket was not created with object lock, so an object in it cannot carry a retention or a legal hold",
                resource,
            ));
        }
        match (mode, until) {
            (Some(mode), Some(until)) => {
                attributes.retention_mode =
                    parse_mode(mode).ok_or_else(|| invalid("x-amz-object-lock-mode is GOVERNANCE or COMPLIANCE".into()))?;
                attributes.retain_until_ms = parse_iso8601_ms(until)
                    .filter(|until| *until > now_ms())
                    .ok_or_else(|| invalid("x-amz-object-lock-retain-until-date is an ISO 8601 time in the future".into()))?;
            }
            (None, None) => {}
            _ => {
                return Err(invalid(
                    "x-amz-object-lock-mode and x-amz-object-lock-retain-until-date are given together".into(),
                ))
            }
        }
        match hold.map(str::trim) {
            None => {}
            Some("ON") => attributes.legal_hold = true,
            Some("OFF") => {}
            Some(other) => return Err(invalid(format!("x-amz-object-lock-legal-hold is ON or OFF, and {other:?} is neither"))),
        }
        if attributes.retention_mode == 0 && lock_enabled {
            if let Some(rules) = self.bucket_rules(bucket).map_err(|error| error_response(error, resource, true))? {
                if let Some(default) = &rules.default_retention {
                    attributes.retention_mode = default.mode;
                    attributes.retain_until_ms = now_ms() + u64::from(default.days) * DAY_MS;
                }
            }
        }
        Ok(attributes)
    }

    /// The object sub-resources `?tagging`, `?retention` and `?legal-hold`. `None` for a method they do not take.
    pub(super) fn object_attribute_request(&self, request: &Request, bucket: &str, key: &str, sub: &str) -> Option<Response> {
        let resource = format!("/{bucket}/{key}");
        let method = request.method.as_str();
        let text = String::from_utf8_lossy(&request.body).to_string();
        let malformed = |message: String| s3::error_response(s3::S3ErrorCode::MalformedXml, &message, &resource);
        // `?versionId=N` names the version the request is about: the current one, or one kept under a hidden key
        let target = match request.query_param("versionId") {
            None => key.to_string(),
            Some(version) => match self.version_key(bucket, key, &version, &resource) {
                Ok(target) => target,
                Err(response) => return Some(response),
            },
        };
        let read = || self.with_plane(|plane| plane.read_manifest_with_attributes(bucket, &target));
        let live = |read: Result<(ObjectManifest, ChunkAttributes), ObjectError>| match read {
            Ok((manifest, attributes)) if !manifest.deleted => Ok((manifest, attributes)),
            Ok(_) | Err(ObjectError::NotFound(_)) => Err(no_such_key(&resource)),
            Err(error) => Err(error_response(error, &resource, false)),
        };
        let lock = match self.lock_check(request, bucket, &resource) {
            Ok(lock) => lock,
            Err(response) => return Some(response),
        };
        let change = match (method, sub) {
            ("GET", _) => {
                let (_, attributes) = match live(read()) {
                    Ok(found) => found,
                    Err(response) => return Some(response),
                };
                return match sub {
                    "tagging" => Some(s3::xml_response(200, bc::tagging_xml(&attributes.tags))),
                    "retention" if attributes.retention_mode == 0 => Some(s3::error_response(
                        s3::S3ErrorCode::ObjectLockConfigurationNotFound,
                        "the object has no retention",
                        &resource,
                    )),
                    "retention" => Some(s3::xml_response(
                        200,
                        format!(
                            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Retention><Mode>{}</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>\n",
                            mode_name(attributes.retention_mode),
                            s3::iso8601(attributes.retain_until_ms)
                        ),
                    )),
                    "legal-hold" => Some(s3::xml_response(
                        200,
                        format!(
                            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LegalHold><Status>{}</Status></LegalHold>\n",
                            if attributes.legal_hold { "ON" } else { "OFF" }
                        ),
                    )),
                    _ => None,
                };
            }
            ("PUT", "tagging") => match bc::parse_tagging(&text) {
                Ok(tags) if tags.len() <= MAX_OBJECT_TAGS => Change::Tags(tags),
                Ok(_) => {
                    return Some(s3::error_response(
                        s3::S3ErrorCode::InvalidArgument,
                        &format!("an object carries at most {MAX_OBJECT_TAGS} tags"),
                        &resource,
                    ))
                }
                Err(error) => return Some(malformed(error.to_string())),
            },
            ("DELETE", "tagging") => Change::Tags(Default::default()),
            ("PUT", "retention") | ("PUT", "legal-hold") if !lock.enabled => {
                return Some(s3::error_response(
                    s3::S3ErrorCode::InvalidRequest,
                    "the bucket was not created with object lock, so an object in it cannot carry a retention or a legal hold",
                    &resource,
                ))
            }
            ("PUT", "retention") => {
                let Some(mode) = bc::xml_first(&text, "Mode").and_then(|mode| parse_mode(&mode)) else {
                    return Some(malformed("a retention names a Mode, GOVERNANCE or COMPLIANCE".into()));
                };
                let Some(until) = bc::xml_first(&text, "RetainUntilDate").and_then(|date| parse_iso8601_ms(&date)) else {
                    return Some(malformed("a retention names a RetainUntilDate, an ISO 8601 time".into()));
                };
                if until <= now_ms() {
                    return Some(s3::error_response(
                        s3::S3ErrorCode::InvalidArgument,
                        "the retain-until date must be in the future",
                        &resource,
                    ));
                }
                Change::Retention(mode, until)
            }
            ("POST", "restore") => match bc::xml_first(&text, "Days").and_then(|days| days.trim().parse::<u64>().ok()).filter(|days| (1..=3650).contains(days)) {
                Some(days) => Change::Restore(now_ms() + days * DAY_MS),
                None => return Some(malformed("a restore request names Days, from 1 to 3650".into())),
            },
            ("PUT", "legal-hold") => match bc::xml_first(&text, "Status").as_deref().map(str::trim) {
                Some("ON") => Change::Hold(true),
                Some("OFF") => Change::Hold(false),
                _ => return Some(malformed("a legal hold names a Status, ON or OFF".into())),
            },
            _ => return None,
        };
        let done = || Response::status(match method {
            "DELETE" => 204,
            "POST" => 202,
            _ => 200,
        });
        for _ in 0..4 {
            let (manifest, attributes) = match live(read()) {
                Ok(found) => found,
                Err(response) => return Some(response),
            };
            if matches!(change, Change::Restore(_)) && attributes.storage_class.is_empty() {
                return Some(s3::error_response(
                    s3::S3ErrorCode::InvalidObjectState,
                    "a restore is for an object moved to another storage class; this one is in the standard class",
                    &resource,
                ));
            }
            let mut changed = attributes.clone();
            if let Err(message) = change.apply(&mut changed, lock) {
                return Some(s3::error_response(s3::S3ErrorCode::AccessDenied, &format!("{LOCKED}: {message}"), &resource));
            }
            if changed == attributes {
                return Some(done());
            }
            match self.with_plane(|plane| Ok(plane.update_attributes(&manifest, &changed))) {
                Ok(Ok(published)) => {
                    self.remember(&published);
                    return Some(done());
                }
                // another writer moved the object: apply the same change on what it left
                Ok(Err(failure)) if lost_race(&failure.error) => continue,
                Ok(Err(failure)) if failure.unconfirmed => return Some(unconfirmed(&failure.error, &resource)),
                Ok(Err(failure)) => return Some(error_response(failure.error, &resource, false)),
                Err(error) => return Some(error_response(error, &resource, false)),
            }
        }
        Some(s3::error_response(
            s3::S3ErrorCode::PreconditionFailed,
            "the object kept changing under this request; gave up after several attempts",
            &resource,
        ))
    }
}
