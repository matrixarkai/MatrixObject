// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A bucket and the configuration it carries: versioning, tags, CORS rules, a policy document, lifecycle
//! rules, a quota and the default object-lock retention.
//!
//! A bucket is one small record, stored as an ordinary chunk object under the reserved system bucket
//! ([`SYSTEM_BUCKET`], key [`config_key`]). Its generation is the compare-and-swap of every change to it, so
//! two writers configuring one bucket never overwrite each other, and a bucket exists exactly when its record
//! does. The record is replicated and durable like any object, and survives the loss of a replica the way any
//! object does.
//!
//! This module is pure: the record, its validation, its framed encoding and the XML the S3 configuration
//! requests speak. The gateway that serves the requests is `net::cluster_gateway_buckets`.
use crate::object_manifest::{frame, unframe};
use crate::ObjectError;
use prost::Message;

/// The only record version.
pub const BUCKET_CONFIG_FORMAT: u32 = 1;
const MAGIC: &[u8; 4] = b"MOBC";
const MAX_RECORD_BYTES: usize = 256 * 1024;
/// The bucket that holds the records of every other bucket.
pub const SYSTEM_BUCKET: &str = "matrixobject-system";
/// The prefix, inside [`SYSTEM_BUCKET`], of the records.
pub const CONFIG_PREFIX: &str = "bucket/";
/// The most CORS rules, lifecycle rules and tags one bucket may carry.
pub const MAX_RULES: usize = 100;
pub const MAX_TAGS: usize = 50;
const MAX_POLICY_BYTES: usize = 20 * 1024;
const MAX_FIELD_BYTES: usize = 1024;

/// The key of `bucket`'s record in [`SYSTEM_BUCKET`].
pub fn config_key(bucket: &str) -> String {
    format!("{CONFIG_PREFIX}{bucket}")
}

/// The bucket a record key names, if it is one.
pub fn bucket_of_key(key: &str) -> Option<&str> {
    key.strip_prefix(CONFIG_PREFIX).filter(|name| !name.is_empty() && !name.contains('/'))
}

/// Versioning has never been configured.
pub const VERSIONING_NEVER: u32 = 0;
pub const VERSIONING_ENABLED: u32 = 1;
pub const VERSIONING_SUSPENDED: u32 = 2;
/// Object-lock default retention modes.
pub const RETENTION_NONE: u32 = 0;
pub const RETENTION_GOVERNANCE: u32 = 1;
pub const RETENTION_COMPLIANCE: u32 = 2;

fn invalid(message: &str) -> ObjectError {
    ObjectError::InvalidArgument(message.to_string())
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct CorsRule {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, repeated, tag = "2")]
    pub allowed_origins: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    pub allowed_methods: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    pub allowed_headers: Vec<String>,
    #[prost(string, repeated, tag = "5")]
    pub expose_headers: Vec<String>,
    #[prost(uint32, tag = "6")]
    pub max_age_seconds: u32,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct LifecycleRule {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub prefix: String,
    #[prost(bool, tag = "3")]
    pub enabled: bool,
    /// Delete an object this many days after it was last written; zero for no expiration.
    #[prost(uint32, tag = "4")]
    pub expiration_days: u32,
    /// Delete the parts of a multipart upload begun this many days ago and never completed; zero for none.
    #[prost(uint32, tag = "5")]
    pub abort_multipart_days: u32,
    /// Delete a non-current version this many days after it stopped being current; zero for none.
    #[prost(uint32, tag = "6")]
    pub noncurrent_expiration_days: u32,
    /// The rule applies only to objects carrying this tag (key and value); empty for no tag filter.
    #[prost(string, tag = "7")]
    pub tag_key: String,
    #[prost(string, tag = "8")]
    pub tag_value: String,
    /// Keep this many of the newest non-current versions whatever their age; the older ones are deleted. Zero keeps no
    /// count: every non-current version is subject to `noncurrent_expiration_days` alone.
    #[prost(uint32, tag = "9")]
    pub noncurrent_versions_to_keep: u32,
    /// Remove a delete marker kept as a version once no version older than it is kept.
    #[prost(bool, tag = "10")]
    pub expire_delete_markers: bool,
    /// Move an object to `transition_storage_class` this many days after it was last written; zero for none.
    #[prost(uint32, tag = "11")]
    pub transition_days: u32,
    #[prost(string, tag = "12")]
    pub transition_storage_class: String,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct DefaultRetention {
    /// [`RETENTION_GOVERNANCE`] or [`RETENTION_COMPLIANCE`].
    #[prost(uint32, tag = "1")]
    pub mode: u32,
    #[prost(uint32, tag = "2")]
    pub days: u32,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct BucketConfig {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    #[prost(uint64, tag = "2")]
    pub created_ms: u64,
    #[prost(uint32, tag = "3")]
    pub versioning: u32,
    #[prost(btree_map = "string, string", tag = "4")]
    pub tags: std::collections::BTreeMap<String, String>,
    #[prost(message, repeated, tag = "5")]
    pub cors: Vec<CorsRule>,
    /// The policy document, as the client sent it.
    #[prost(string, tag = "6")]
    pub policy: String,
    #[prost(message, repeated, tag = "7")]
    pub lifecycle: Vec<LifecycleRule>,
    /// The most bytes the live objects of the bucket may hold; zero for no quota.
    #[prost(uint64, tag = "8")]
    pub quota_bytes: u64,
    #[prost(bool, tag = "9")]
    pub object_lock_enabled: bool,
    #[prost(message, optional, tag = "10")]
    pub default_retention: Option<DefaultRetention>,
    /// Whether the bucket keeps a change feed.
    #[prost(bool, tag = "11")]
    pub change_feed: bool,
}

impl BucketConfig {
    /// A new bucket with nothing configured.
    pub fn new(created_ms: u64) -> Self {
        Self { format_version: BUCKET_CONFIG_FORMAT, created_ms, ..Default::default() }
    }

    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.format_version != BUCKET_CONFIG_FORMAT {
            return Err(invalid("unsupported bucket record version"));
        }
        if !matches!(self.versioning, VERSIONING_NEVER | VERSIONING_ENABLED | VERSIONING_SUSPENDED) {
            return Err(invalid("unknown versioning state"));
        }
        // Object lock works with versioning or without it, but a bucket with object lock never suspends versioning.
        if self.object_lock_enabled && self.versioning == VERSIONING_SUSPENDED {
            return Err(invalid("object lock does not allow versioning to be suspended"));
        }
        if self.tags.len() > MAX_TAGS || self.tags.iter().any(|(k, v)| k.is_empty() || k.len() > 128 || v.len() > 256) {
            return Err(invalid("a bucket carries at most 50 tags, a key of 1 to 128 bytes and a value of at most 256"));
        }
        if self.cors.len() > MAX_RULES || self.lifecycle.len() > MAX_RULES {
            return Err(invalid("too many rules"));
        }
        for rule in &self.cors {
            let bad_method = rule.allowed_methods.iter().any(|m| !matches!(m.as_str(), "GET" | "PUT" | "POST" | "DELETE" | "HEAD"));
            if rule.allowed_origins.is_empty() || rule.allowed_methods.is_empty() || bad_method {
                return Err(invalid("a CORS rule needs an allowed origin and GET, PUT, POST, DELETE or HEAD methods"));
            }
            let too_long = rule.id.len() > MAX_FIELD_BYTES
                || rule.allowed_origins.iter().chain(&rule.allowed_headers).chain(&rule.expose_headers).any(|s| s.len() > MAX_FIELD_BYTES);
            if too_long {
                return Err(invalid("a CORS field is too long"));
            }
        }
        for rule in &self.lifecycle {
            let acts = rule.expiration_days > 0
                || rule.abort_multipart_days > 0
                || rule.noncurrent_expiration_days > 0
                || rule.noncurrent_versions_to_keep > 0
                || rule.expire_delete_markers
                || (rule.transition_days > 0 && !rule.transition_storage_class.is_empty());
            if !acts {
                return Err(invalid("a lifecycle rule must expire, transition or clean up something"));
            }
            if rule.tag_key.is_empty() != rule.tag_value.is_empty() && rule.tag_key.is_empty() {
                return Err(invalid("a lifecycle tag filter names a key"));
            }
            if rule.transition_storage_class.len() > 32 {
                return Err(invalid("a storage class is at most 32 bytes"));
            }
            if rule.id.len() > MAX_FIELD_BYTES || rule.prefix.len() > MAX_FIELD_BYTES {
                return Err(invalid("a lifecycle rule field is too long"));
            }
        }
        if self.policy.len() > MAX_POLICY_BYTES {
            return Err(invalid("the policy document is larger than 20 KiB"));
        }
        if let Some(retention) = &self.default_retention {
            if !matches!(retention.mode, RETENTION_GOVERNANCE | RETENTION_COMPLIANCE) || retention.days == 0 {
                return Err(invalid("a default retention needs GOVERNANCE or COMPLIANCE and a number of days"));
            }
            if !self.object_lock_enabled {
                return Err(invalid("a default retention needs object lock to be enabled"));
            }
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        frame(MAGIC, self, MAX_RECORD_BYTES)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = unframe(MAGIC, bytes, MAX_RECORD_BYTES)?;
        value.validate()?;
        Ok(value)
    }
}

// ---------------------------------------------------------------------------------------------
// The XML of the configuration requests. Small on purpose: the elements S3 clients send, read by
// name, with the five predefined entities. Anything else a client sends is ignored, and a body
// with none of the expected elements is refused by the caller.
// ---------------------------------------------------------------------------------------------

pub fn xml_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

pub fn xml_unescape(value: &str) -> String {
    value.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

/// The inner text of every `<tag>...</tag>` in `body`, in order, unescaped. A tag may carry attributes.
pub fn xml_all(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        // `<TagSet` must not match `<Tag`: the next byte has to end the name
        match after.bytes().next() {
            Some(b'>') | Some(b' ') | Some(b'/') => {}
            _ => {
                rest = after;
                continue;
            }
        }
        let Some(gt) = after.find('>') else { break };
        if after[..gt].ends_with('/') {
            out.push(String::new());
            rest = &after[gt + 1..];
            continue;
        }
        let inner = &after[gt + 1..];
        let Some(end) = inner.find(&close) else { break };
        out.push(xml_unescape(&inner[..end]));
        rest = &inner[end + close.len()..];
    }
    out
}

/// The inner text of the first `<tag>` in `body`, or `None`.
pub fn xml_first(body: &str, tag: &str) -> Option<String> {
    xml_all(body, tag).into_iter().next()
}

fn raw_blocks(body: &str, tag: &str) -> Vec<String> {
    // the raw inner XML of each `<tag>`, not unescaped: the blocks are parsed again for their own children
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        match after.bytes().next() {
            Some(b'>') | Some(b' ') => {}
            _ => {
                rest = after;
                continue;
            }
        }
        let Some(gt) = after.find('>') else { break };
        let inner = &after[gt + 1..];
        let Some(end) = inner.find(&close) else { break };
        out.push(inner[..end].to_string());
        rest = &inner[end + close.len()..];
    }
    out
}

const NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

fn document(root: &str, inner: &str) -> String {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root} xmlns=\"{NS}\">{inner}</{root}>\n")
}

// ---- versioning
pub fn versioning_xml(state: u32) -> String {
    let status = match state {
        VERSIONING_ENABLED => "<Status>Enabled</Status>",
        VERSIONING_SUSPENDED => "<Status>Suspended</Status>",
        _ => "",
    };
    document("VersioningConfiguration", status)
}

/// The state a `PutBucketVersioning` body asks for.
pub fn parse_versioning(body: &str) -> Result<u32, ObjectError> {
    match xml_first(body, "Status").as_deref() {
        Some("Enabled") => Ok(VERSIONING_ENABLED),
        Some("Suspended") => Ok(VERSIONING_SUSPENDED),
        _ => Err(invalid("a versioning configuration names Enabled or Suspended")),
    }
}

// ---- tagging
pub fn tagging_xml(tags: &std::collections::BTreeMap<String, String>) -> String {
    let mut set = String::new();
    for (key, value) in tags {
        set.push_str(&format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", xml_escape(key), xml_escape(value)));
    }
    document("Tagging", &format!("<TagSet>{set}</TagSet>"))
}

pub fn parse_tagging(body: &str) -> Result<std::collections::BTreeMap<String, String>, ObjectError> {
    let mut tags = std::collections::BTreeMap::new();
    for block in raw_blocks(body, "Tag") {
        let key = xml_first(&block, "Key").ok_or_else(|| invalid("a tag has a Key"))?;
        let value = xml_first(&block, "Value").unwrap_or_default();
        if tags.insert(key, value).is_some() {
            return Err(invalid("a tag key is named twice"));
        }
    }
    if body.find("<Tagging").is_none() {
        return Err(invalid("not a Tagging document"));
    }
    Ok(tags)
}

// ---- CORS
pub fn cors_xml(rules: &[CorsRule]) -> String {
    let mut out = String::new();
    for rule in rules {
        out.push_str("<CORSRule>");
        if !rule.id.is_empty() {
            out.push_str(&format!("<ID>{}</ID>", xml_escape(&rule.id)));
        }
        for origin in &rule.allowed_origins {
            out.push_str(&format!("<AllowedOrigin>{}</AllowedOrigin>", xml_escape(origin)));
        }
        for method in &rule.allowed_methods {
            out.push_str(&format!("<AllowedMethod>{}</AllowedMethod>", xml_escape(method)));
        }
        for header in &rule.allowed_headers {
            out.push_str(&format!("<AllowedHeader>{}</AllowedHeader>", xml_escape(header)));
        }
        for header in &rule.expose_headers {
            out.push_str(&format!("<ExposeHeader>{}</ExposeHeader>", xml_escape(header)));
        }
        if rule.max_age_seconds > 0 {
            out.push_str(&format!("<MaxAgeSeconds>{}</MaxAgeSeconds>", rule.max_age_seconds));
        }
        out.push_str("</CORSRule>");
    }
    document("CORSConfiguration", &out)
}

pub fn parse_cors(body: &str) -> Result<Vec<CorsRule>, ObjectError> {
    let mut rules = Vec::new();
    for block in raw_blocks(body, "CORSRule") {
        rules.push(CorsRule {
            id: xml_first(&block, "ID").unwrap_or_default(),
            allowed_origins: xml_all(&block, "AllowedOrigin"),
            allowed_methods: xml_all(&block, "AllowedMethod"),
            allowed_headers: xml_all(&block, "AllowedHeader"),
            expose_headers: xml_all(&block, "ExposeHeader"),
            max_age_seconds: match xml_first(&block, "MaxAgeSeconds") {
                None => 0,
                Some(value) => value.trim().parse().map_err(|_| invalid("MaxAgeSeconds is a number"))?,
            },
        });
    }
    if rules.is_empty() {
        return Err(invalid("a CORS configuration holds at least one CORSRule"));
    }
    Ok(rules)
}

/// Whether `origin` matches an allowed-origin pattern (`*` anywhere, at most once, as S3 allows).
pub fn origin_matches(pattern: &str, origin: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == origin,
        Some((head, tail)) => origin.len() >= head.len() + tail.len() && origin.starts_with(head) && origin.ends_with(tail),
    }
}

// ---- lifecycle
pub fn lifecycle_xml(rules: &[LifecycleRule]) -> String {
    let mut out = String::new();
    for rule in rules {
        out.push_str("<Rule>");
        if !rule.id.is_empty() {
            out.push_str(&format!("<ID>{}</ID>", xml_escape(&rule.id)));
        }
        let tag = if rule.tag_key.is_empty() {
            String::new()
        } else {
            format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", xml_escape(&rule.tag_key), xml_escape(&rule.tag_value))
        };
        out.push_str(&match (rule.prefix.is_empty(), tag.is_empty()) {
            (_, true) => format!("<Filter><Prefix>{}</Prefix></Filter>", xml_escape(&rule.prefix)),
            (true, false) => format!("<Filter>{tag}</Filter>"),
            (false, false) => format!("<Filter><And><Prefix>{}</Prefix>{tag}</And></Filter>", xml_escape(&rule.prefix)),
        });
        out.push_str(if rule.enabled { "<Status>Enabled</Status>" } else { "<Status>Disabled</Status>" });
        if rule.expiration_days > 0 || rule.expire_delete_markers {
            let mut inner = String::new();
            if rule.expiration_days > 0 {
                inner.push_str(&format!("<Days>{}</Days>", rule.expiration_days));
            }
            if rule.expire_delete_markers {
                inner.push_str("<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>");
            }
            out.push_str(&format!("<Expiration>{inner}</Expiration>"));
        }
        if rule.transition_days > 0 && !rule.transition_storage_class.is_empty() {
            out.push_str(&format!(
                "<Transition><Days>{}</Days><StorageClass>{}</StorageClass></Transition>",
                rule.transition_days,
                xml_escape(&rule.transition_storage_class)
            ));
        }
        if rule.noncurrent_expiration_days > 0 || rule.noncurrent_versions_to_keep > 0 {
            let mut inner = String::new();
            if rule.noncurrent_expiration_days > 0 {
                inner.push_str(&format!("<NoncurrentDays>{}</NoncurrentDays>", rule.noncurrent_expiration_days));
            }
            if rule.noncurrent_versions_to_keep > 0 {
                inner.push_str(&format!("<NewerNoncurrentVersions>{}</NewerNoncurrentVersions>", rule.noncurrent_versions_to_keep));
            }
            out.push_str(&format!("<NoncurrentVersionExpiration>{inner}</NoncurrentVersionExpiration>"));
        }
        if rule.abort_multipart_days > 0 {
            out.push_str(&format!("<AbortIncompleteMultipartUpload><DaysAfterInitiation>{}</DaysAfterInitiation></AbortIncompleteMultipartUpload>", rule.abort_multipart_days));
        }
        out.push_str("</Rule>");
    }
    document("LifecycleConfiguration", &out)
}

pub fn parse_lifecycle(body: &str) -> Result<Vec<LifecycleRule>, ObjectError> {
    let number = |block: &str, tag: &str| -> Result<u32, ObjectError> {
        match xml_first(block, tag) {
            None => Ok(0),
            Some(value) => value.trim().parse().map_err(|_| invalid("a lifecycle day count is a number")),
        }
    };
    let mut rules = Vec::new();
    for block in raw_blocks(body, "Rule") {
        let prefix = xml_first(&block, "Prefix").unwrap_or_default();
        let enabled = match xml_first(&block, "Status").as_deref() {
            Some("Enabled") => true,
            Some("Disabled") => false,
            _ => return Err(invalid("a lifecycle rule is Enabled or Disabled")),
        };
        // each action's numbers are read inside its own element, so `Days` of an expiration and of a transition differ
        let scoped = |element: &str| raw_blocks(&block, element).into_iter().next().unwrap_or_default();
        let expiration = scoped("Expiration");
        let transition = scoped("Transition");
        let noncurrent = scoped("NoncurrentVersionExpiration");
        let (tag_key, tag_value) = match raw_blocks(&block, "Tag").into_iter().next() {
            Some(tag) => (xml_first(&tag, "Key").unwrap_or_default(), xml_first(&tag, "Value").unwrap_or_default()),
            None => (String::new(), String::new()),
        };
        let transition_days = number(&transition, "Days")?;
        let transition_storage_class = xml_first(&transition, "StorageClass").unwrap_or_default();
        if (transition_days > 0) != !transition_storage_class.is_empty() && !transition.is_empty() {
            return Err(invalid("a lifecycle transition names Days and a StorageClass together"));
        }
        rules.push(LifecycleRule {
            id: xml_first(&block, "ID").unwrap_or_default(),
            prefix,
            enabled,
            expiration_days: number(&expiration, "Days")?,
            abort_multipart_days: number(&block, "DaysAfterInitiation")?,
            noncurrent_expiration_days: number(&noncurrent, "NoncurrentDays")?,
            tag_key,
            tag_value,
            noncurrent_versions_to_keep: number(&noncurrent, "NewerNoncurrentVersions")?,
            expire_delete_markers: xml_first(&expiration, "ExpiredObjectDeleteMarker").as_deref().map(str::trim) == Some("true"),
            transition_days,
            transition_storage_class,
        });
    }
    if rules.is_empty() {
        return Err(invalid("a lifecycle configuration holds at least one Rule"));
    }
    Ok(rules)
}

// ---- object lock
pub fn object_lock_xml(config: &BucketConfig) -> String {
    let mut inner = String::new();
    if config.object_lock_enabled {
        inner.push_str("<ObjectLockEnabled>Enabled</ObjectLockEnabled>");
    }
    if let Some(retention) = &config.default_retention {
        let mode = if retention.mode == RETENTION_COMPLIANCE { "COMPLIANCE" } else { "GOVERNANCE" };
        inner.push_str(&format!("<Rule><DefaultRetention><Mode>{mode}</Mode><Days>{}</Days></DefaultRetention></Rule>", retention.days));
    }
    document("ObjectLockConfiguration", &inner)
}

/// What a `PutObjectLockConfiguration` body asks for: whether lock is on and the default retention.
pub fn parse_object_lock(body: &str) -> Result<(bool, Option<DefaultRetention>), ObjectError> {
    let enabled = match xml_first(body, "ObjectLockEnabled").as_deref() {
        Some("Enabled") => true,
        _ => return Err(invalid("an object lock configuration names ObjectLockEnabled Enabled")),
    };
    let retention = match raw_blocks(body, "DefaultRetention").into_iter().next() {
        None => None,
        Some(block) => {
            let mode = match xml_first(&block, "Mode").as_deref() {
                Some("GOVERNANCE") => RETENTION_GOVERNANCE,
                Some("COMPLIANCE") => RETENTION_COMPLIANCE,
                _ => return Err(invalid("a default retention mode is GOVERNANCE or COMPLIANCE")),
            };
            let days = xml_first(&block, "Days").and_then(|d| d.trim().parse().ok()).ok_or_else(|| invalid("a default retention names Days"))?;
            Some(DefaultRetention { mode, days })
        }
    };
    Ok((enabled, retention))
}

#[cfg(test)]
#[path = "bucket_config_tests.rs"]
mod tests;

/// Milliseconds since the epoch of an ISO 8601 UTC time such as `2026-12-31T23:59:59Z` or `2026-12-31T23:59:59.500Z`.
/// `None` for anything else.
pub fn parse_iso8601_ms(text: &str) -> Option<u64> {
    let text = text.trim();
    let body = text.strip_suffix('Z')?;
    let (date, time) = body.split_once('T')?;
    let mut d = date.split('-');
    let (year, month, day): (i64, i64, i64) = (d.next()?.parse().ok()?, d.next()?.parse().ok()?, d.next()?.parse().ok()?);
    if d.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) || year < 1970 {
        return None;
    }
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, fraction),
        None => (time, ""),
    };
    let mut c = clock.split(':');
    let (hour, minute, second): (i64, i64, i64) = (c.next()?.parse().ok()?, c.next()?.parse().ok()?, c.next()?.parse().ok()?);
    if c.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let millis: i64 = if fraction.is_empty() {
        0
    } else if fraction.len() <= 9 && fraction.bytes().all(|b| b.is_ascii_digit()) {
        format!("{:0<3}", &fraction[..fraction.len().min(3)]).parse().ok()?
    } else {
        return None;
    };
    // days from the civil date (the proleptic Gregorian calendar)
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    u64::try_from(seconds * 1_000 + millis).ok()
}

pub fn change_feed_xml(enabled: bool) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ChangeFeedConfiguration><Status>{}</Status></ChangeFeedConfiguration>\n",
        if enabled { "Enabled" } else { "Disabled" }
    )
}

pub fn parse_change_feed(body: &str) -> Result<bool, ObjectError> {
    match xml_first(body, "Status").as_deref().map(str::trim) {
        Some("Enabled") => Ok(true),
        Some("Disabled") => Ok(false),
        _ => Err(invalid("a change feed configuration names a Status, Enabled or Disabled")),
    }
}
