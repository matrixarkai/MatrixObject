// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Object tagging and bucket CORS: the two configurations this store already keeps and no S3
//! client could read or write.
//!
//! # WHY CORS IN PARTICULAR WAS AN INCOHERENCE
//!
//! This crate's default CORS policy is what [`super::service`]'s header list was built around --
//! the module's own doc comment names `ETag`, `Content-Type`, `Content-Length`, `Last-Modified`,
//! `Accept-Ranges` and `Content-Range` as the exposed headers. The store holds a real, validated
//! [`crate::BucketCorsPolicy`] per bucket. A browser client, which is the only kind of client CORS exists
//! for, had no way to see it or set it.
//!
//! # ONE RULE IN, ONE RULE OUT -- AND A SECOND RULE IS REFUSED
//!
//! S3's `CORSConfiguration` holds a LIST of `CORSRule`s, each pairing its own origins with its own
//! methods and headers. This store holds ONE flat policy: a set of origins, a set of methods, a set
//! of headers, one max-age.
//!
//! Those are not the same shape, and the difference is not cosmetic. Flattening two rules into one
//! policy UNIONS them -- so an origin that rule A allowed only `GET` would come back allowed
//! whatever rule B permitted, which is to say the store would grant access the request never asked
//! for. A configuration that would be silently widened is REFUSED, and the refusal says why.
//!
//! One rule is accepted and round-trips exactly. Closing the rest needs the store to carry a list,
//! which is a store change and not something this module can paper over.

use std::collections::BTreeMap;

use super::s3::{xml_escape, S3_XML_NAMESPACE};
use super::s3_multipart::xml_unescape;

/// A `Tagging` body.
pub fn tagging_xml(tags: &BTreeMap<String, String>) -> String {
    let mut body = String::with_capacity(128 + tags.len() * 64);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<Tagging xmlns=\"");
    body.push_str(S3_XML_NAMESPACE);
    body.push_str("\"><TagSet>");
    for (key, value) in tags {
        body.push_str(&format!(
            "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
            xml_escape(key),
            xml_escape(value)
        ));
    }
    body.push_str("</TagSet></Tagging>\n");
    body
}

/// Read a `Tagging` body.
///
/// An EMPTY `<TagSet/>` is accepted and means "no tags", which is what a client sends to clear
/// them through `PUT` rather than `DELETE`. That is different from the empty part list a
/// completion refuses: there, nothing was asked for; here, clearing is the ask.
pub fn parse_tagging(body: &[u8]) -> Result<BTreeMap<String, String>, String> {
    let text = std::str::from_utf8(body)
        .map_err(|_| "the request body is not UTF-8 text".to_string())?;
    if text.contains("<!DOCTYPE") || text.contains("<!ENTITY") {
        return Err("the request body carries a document type declaration, which this endpoint \
                    does not accept"
            .to_string());
    }
    if !text.contains("<Tagging") {
        return Err("the request body is not a Tagging document".to_string());
    }
    if !text.contains("<TagSet") {
        return Err("the Tagging document carries no <TagSet>".to_string());
    }
    let mut tags = BTreeMap::new();
    let mut rest = text;
    while let Some(open) = rest.find("<Tag>") {
        let after_open = &rest[open + "<Tag>".len()..];
        let close = after_open
            .find("</Tag>")
            .ok_or_else(|| "a <Tag> element is not closed".to_string())?;
        let block = &after_open[..close];
        let key = element(block, "Key")
            .ok_or_else(|| "a <Tag> element carries no <Key>".to_string())?;
        let value = element(block, "Value")
            .ok_or_else(|| "a <Tag> element carries no <Value>".to_string())?;
        let key = xml_unescape(key);
        if key.is_empty() {
            return Err("a <Tag> names an empty key".to_string());
        }
        // A REPEATED KEY IS REFUSED, NOT OVERWRITTEN. A tag set is a map, so the last value would
        // win silently -- and a client that sent the same key twice has a bug whose effect it
        // cannot see from a success.
        if tags.insert(key.clone(), xml_unescape(value)).is_some() {
            return Err(format!("the tag key {key:?} appears more than once"));
        }
        rest = &after_open[close + "</Tag>".len()..];
    }
    Ok(tags)
}

fn element<'a>(block: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = block.find(&open)? + open.len();
    let end = block[start..].find(&close)? + start;
    Some(&block[start..end])
}

/// Every occurrence of `<name>…</name>` in order.
fn elements(block: &str, name: &str) -> Vec<String> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let mut out = Vec::new();
    let mut rest = block;
    while let Some(at) = rest.find(&open) {
        let after = &rest[at + open.len()..];
        let Some(end) = after.find(&close) else {
            break;
        };
        out.push(xml_unescape(&after[..end]));
        rest = &after[end + close.len()..];
    }
    out
}

/// A `CORSConfiguration` body, from this store's one flat policy.
pub fn cors_configuration_xml(
    allowed_origins: &[String],
    allowed_methods: &[String],
    allowed_headers: &[String],
    expose_headers: &[String],
    max_age_seconds: u64,
) -> String {
    let mut body = String::with_capacity(256);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<CORSConfiguration xmlns=\"");
    body.push_str(S3_XML_NAMESPACE);
    body.push_str("\"><CORSRule>");
    for origin in allowed_origins {
        body.push_str(&format!(
            "<AllowedOrigin>{}</AllowedOrigin>",
            xml_escape(origin)
        ));
    }
    for method in allowed_methods {
        body.push_str(&format!(
            "<AllowedMethod>{}</AllowedMethod>",
            xml_escape(method)
        ));
    }
    for header in allowed_headers {
        body.push_str(&format!(
            "<AllowedHeader>{}</AllowedHeader>",
            xml_escape(header)
        ));
    }
    for header in expose_headers {
        body.push_str(&format!(
            "<ExposeHeader>{}</ExposeHeader>",
            xml_escape(header)
        ));
    }
    if max_age_seconds > 0 {
        body.push_str(&format!(
            "<MaxAgeSeconds>{max_age_seconds}</MaxAgeSeconds>"
        ));
    }
    body.push_str("</CORSRule></CORSConfiguration>\n");
    body
}

/// What one `CORSRule` asked for.
pub struct CorsRule {
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: u64,
}

/// Read a `CORSConfiguration` body holding exactly one rule.
///
/// A second rule is refused. See the module comment: this store keeps one flat policy, and
/// flattening two rules would UNION their origins with their methods, granting an origin access
/// that the rule naming it never gave. A refusal is the only answer that cannot be wrong.
pub fn parse_cors_configuration(body: &[u8]) -> Result<CorsRule, String> {
    let text = std::str::from_utf8(body)
        .map_err(|_| "the request body is not UTF-8 text".to_string())?;
    if text.contains("<!DOCTYPE") || text.contains("<!ENTITY") {
        return Err("the request body carries a document type declaration, which this endpoint \
                    does not accept"
            .to_string());
    }
    if !text.contains("<CORSConfiguration") {
        return Err("the request body is not a CORSConfiguration document".to_string());
    }
    let rules = text.matches("<CORSRule>").count();
    if rules == 0 {
        return Err("the configuration carries no <CORSRule>".to_string());
    }
    if rules > 1 {
        return Err(format!(
            "the configuration carries {rules} rules and this store keeps one set of origins, \
             methods and headers for a bucket. Accepting more would union them, which would \
             allow an origin the methods a different rule granted -- so it is refused rather \
             than widened"
        ));
    }
    let start = text.find("<CORSRule>").expect("counted above") + "<CORSRule>".len();
    let end = text[start..]
        .find("</CORSRule>")
        .ok_or_else(|| "the <CORSRule> element is not closed".to_string())?
        + start;
    let block = &text[start..end];

    let allowed_origins = elements(block, "AllowedOrigin");
    let allowed_methods = elements(block, "AllowedMethod");
    if allowed_origins.is_empty() {
        return Err("a CORS rule needs at least one <AllowedOrigin>".to_string());
    }
    if allowed_methods.is_empty() {
        return Err("a CORS rule needs at least one <AllowedMethod>".to_string());
    }
    let max_age_seconds = match element(block, "MaxAgeSeconds") {
        None => 0,
        Some(raw) => raw
            .trim()
            .parse()
            .map_err(|_| format!("MaxAgeSeconds {:?} is not a number", raw.trim()))?,
    };
    Ok(CorsRule {
        allowed_origins,
        allowed_methods,
        allowed_headers: elements(block, "AllowedHeader"),
        expose_headers: elements(block, "ExposeHeader"),
        max_age_seconds,
    })
}
