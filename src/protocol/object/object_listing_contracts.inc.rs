// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectListPage {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub objects: Vec<ObjectMetadata>,
    pub common_prefixes: Vec<String>,
    pub scanned_objects: u64,
    pub emitted_objects: u64,
    pub emitted_common_prefixes: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub has_more: bool,
    pub read_proof: ObjectReadProof,
    pub object_list_page_proof_format: String,
    pub object_list_page_proof_checksum: String,
    pub expected_object_list_page_proof_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectVersionPage {
    pub bucket: String,
    pub key: String,
    pub marker: Option<u64>,
    pub next_version_marker: Option<u64>,
    pub versions: Vec<ObjectMetadata>,
    pub requested_versions: u64,
    pub emitted_versions: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub has_more: bool,
    pub read_proof: ObjectReadProof,
    pub object_list_page_proof_format: String,
    pub object_list_page_proof_checksum: String,
    pub expected_object_list_page_proof_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketObjectVersionPage {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub version_marker: Option<u64>,
    pub next_key_marker: Option<String>,
    pub next_version_marker: Option<u64>,
    pub versions: Vec<ObjectMetadata>,
    pub common_prefixes: Vec<String>,
    pub scanned_keys: u64,
    pub emitted_versions: u64,
    pub emitted_common_prefixes: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub has_more: bool,
    pub read_proof: ObjectReadProof,
    pub object_list_page_proof_format: String,
    pub object_list_page_proof_checksum: String,
    pub expected_object_list_page_proof_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectTagPage {
    pub bucket: String,
    pub tag_key: String,
    pub tag_value: String,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub objects: Vec<ObjectMetadata>,
    pub scanned_objects: u64,
    pub emitted_objects: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub has_more: bool,
    pub read_proof: ObjectReadProof,
    pub object_list_page_proof_format: String,
    pub object_list_page_proof_checksum: String,
    pub expected_object_list_page_proof_checksum: String,
}

