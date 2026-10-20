// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `ListObjectVersions`, and the version id a client carries between requests.
//!
//! # WHY THIS EXISTS AT ALL
//!
//! `PutBucketVersioning` landed without it, and that left an incoherent surface: a client could
//! turn versioning ON and then had no S3 way to see a version, read one, or remove one. The switch
//! was there and the thing it switches on was not.
//!
//! # THE VERSION ID IS A NUMBER HERE AND A STRING IN S3
//!
//! S3's version id is an opaque string and a client is required to treat it as one -- it reads the
//! id out of a response and sends it back untouched. This store numbers its versions, so the id on
//! the wire is that number spelled out.
//!
//! A client that honours the opacity cannot tell the difference. A client that tries to ORDER ids
//! by comparing them as strings would get "10" before "9", but such a client is already broken
//! against S3, where the ids are not ordered at all. What a correct client orders by is the
//! listing, which is returned newest-first, and `<IsLatest>`.
//!
//! # `null` IS NOT ACCEPTED, AND THAT IS A DECISION
//!
//! S3 uses the literal `null` as the version id of an object written before versioning was turned
//! on. This store gives every object a version number from the start, so there is no such object
//! and no `null` to report. A request naming `versionId=null` is REFUSED rather than quietly served
//! the current object: a client asking for a specific version and handed a different one is the
//! failure the whole verb exists to prevent.

use super::s3::{xml_escape, S3_XML_NAMESPACE};

/// One version in a `ListObjectVersions` answer.
pub struct VersionEntry {
    pub key: String,
    pub version_id: u64,
    pub is_latest: bool,
    /// A delete marker rather than a version holding bytes.
    ///
    /// S3 reports these as `<DeleteMarker>` elements rather than `<Version>`, and the distinction
    /// is what lets a client tell "this key was deleted" from "this key never existed" -- which is
    /// precisely the object someone lists versions in order to recover.
    pub delete_marker: bool,
    pub last_modified_ms: u64,
    pub etag: String,
    pub size: u64,
    pub storage_class: String,
}

/// What a `ListObjectVersions` answered.
pub struct ListVersionsResult {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub key_marker: String,
    pub version_id_marker: String,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<u64>,
    pub max_keys: usize,
    pub is_truncated: bool,
    pub entries: Vec<VersionEntry>,
    pub common_prefixes: Vec<String>,
}

/// A `ListVersionsResult` body.
pub fn list_object_versions_xml(result: &ListVersionsResult) -> String {
    let mut body = String::with_capacity(512 + result.entries.len() * 220);
    body.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    body.push_str("<ListVersionsResult xmlns=\"");
    body.push_str(S3_XML_NAMESPACE);
    body.push_str("\">");
    body.push_str(&format!("<Name>{}</Name>", xml_escape(&result.bucket)));
    body.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(&result.prefix)));
    if let Some(delimiter) = &result.delimiter {
        body.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(delimiter)));
    }
    body.push_str(&format!(
        "<KeyMarker>{}</KeyMarker>",
        xml_escape(&result.key_marker)
    ));
    body.push_str(&format!(
        "<VersionIdMarker>{}</VersionIdMarker>",
        xml_escape(&result.version_id_marker)
    ));
    if let Some(next) = &result.next_key_marker {
        body.push_str(&format!(
            "<NextKeyMarker>{}</NextKeyMarker>",
            xml_escape(next)
        ));
    }
    if let Some(next) = result.next_version_id_marker {
        body.push_str(&format!(
            "<NextVersionIdMarker>{next}</NextVersionIdMarker>"
        ));
    }
    body.push_str(&format!("<MaxKeys>{}</MaxKeys>", result.max_keys));
    body.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        result.is_truncated
    ));
    for entry in &result.entries {
        // TWO DIFFERENT ELEMENT NAMES, and a client branches on which it got. A delete marker
        // reported as a `<Version>` would be read as an object holding zero bytes, which is a
        // different thing from an object that is not there.
        let element = if entry.delete_marker {
            "DeleteMarker"
        } else {
            "Version"
        };
        body.push_str(&format!("<{element}>"));
        body.push_str(&format!("<Key>{}</Key>", xml_escape(&entry.key)));
        body.push_str(&format!("<VersionId>{}</VersionId>", entry.version_id));
        body.push_str(&format!("<IsLatest>{}</IsLatest>", entry.is_latest));
        body.push_str(&format!(
            "<LastModified>{}</LastModified>",
            super::s3::iso8601(entry.last_modified_ms)
        ));
        // A DELETE MARKER CARRIES NEITHER, which is what S3 sends. An etag and a size on something
        // that holds no bytes are two invented values, and a client comparing etags would match a
        // delete marker against a real object.
        if !entry.delete_marker {
            let quoted = format!("\"{}\"", entry.etag);
            body.push_str(&format!("<ETag>{}</ETag>", xml_escape(&quoted)));
            body.push_str(&format!("<Size>{}</Size>", entry.size));
            body.push_str(&format!(
                "<StorageClass>{}</StorageClass>",
                xml_escape(&entry.storage_class)
            ));
        }
        body.push_str(&format!("</{element}>"));
    }
    for prefix in &result.common_prefixes {
        body.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(prefix)
        ));
    }
    body.push_str("</ListVersionsResult>\n");
    body
}

/// Read a `versionId` query parameter.
///
/// `Ok(None)` means none was given. An unreadable one is an error rather than a fallback to the
/// current object: a client that asked for a specific version and was handed a different one is
/// the failure this verb exists to prevent, and it would be silent.
pub fn parse_version_id(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        return Err("versionId=null names an object written before versioning was turned on, and \
                    every object in this store carries a version from the start, so there is no \
                    such version"
            .to_string());
    }
    match trimmed.parse::<u64>() {
        // DEFENCE IN DEPTH for the zero, and recorded as such: `head_object_version` refuses
        // version 0 itself (`versioned_head.rs:9`), so removing this changes the message and not
        // the outcome. What this function uniquely catches is `null` and anything that is not a
        // number, neither of which ever reaches the store to be refused.
        Ok(0) => Err("version ids start at 1".to_string()),
        Ok(version) => Ok(version),
        Err(_) => Err(format!("versionId {trimmed:?} is not a version this store issues")),
    }
}
