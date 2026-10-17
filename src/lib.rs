use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_CHUNK_READ_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Granularity at which a chunk payload is integrity-checked on read.
const CHUNK_VALIDATION_PAGE_BYTES: usize = 64 * 1024;
/// Fewest data shards a chunk payload is split into for its XOR sidecar.
const CHUNK_EC_MIN_DATA_FRAGMENTS: usize = 2;
/// Most data shards, bounding how many recovery hypotheses a repair tries.
///
/// 16 rather than 8: the sidecar costs one shard, so doubling the cap halves it
/// again -- 6.25% of a chunk instead of 12.5%. Recovery still reconstructs a
/// single damaged shard, it just tries up to sixteen hypotheses instead of
/// eight, each one pass over the payload, and recovery is a rare path. The
/// shard-size floor below still applies, so this only takes effect on chunks
/// large enough to keep shards above it: a ~1 MiB chunk reaches sixteen shards
/// of 64 KiB, and a ~225 KiB chunk sixteen of ~14 KiB, both well above a page.
const CHUNK_EC_MAX_DATA_FRAGMENTS: usize = 16;
/// A shard is never smaller than this, so splitting further never shrinks the
/// recoverable damage extent below a page.
///
/// 8 KiB rather than 64: the floor caps how many shards a chunk can be split
/// into, and the sidecar costs one shard, so too high a floor makes small-object
/// workloads pay a far larger sidecar than large ones. A workload of 1 KiB
/// objects packs into one ~225 KiB chunk per replica, which a 64 KiB floor limits
/// to three shards -- a sidecar a third of the chunk, against an eighth for a
/// ~1 MiB chunk. At 8 KiB the same chunk reaches eight shards of ~28 KiB each,
/// still seven times a 4 KiB page, so single-page and single-sector damage stays
/// inside one shard exactly as before.
const CHUNK_EC_MIN_FRAGMENT_BYTES: usize = 8 * 1024;
const DEFAULT_CHUNK_READ_CACHE_MAX_CACHEABLE_READ_BYTES: u64 = 4 * 1024 * 1024;
const PIPELINE_OFFSET_MAX: u64 = i64::MAX as u64;
const DEFAULT_METADATA_ROUTE_CACHE_ENTRIES: usize = 100_000;
const DEFAULT_METADATA_ROUTE_CACHE_TTL_MS: u64 = 1000;
const MAX_METADATA_ROUTE_CACHE_ENTRIES: usize = 10_000_000;
const MAX_METADATA_ROUTE_CACHE_TTL_MS: u64 = 60 * 60 * 1000;
const MAX_METADATA_ROUTE_BATCH_KEYS: usize = 4096;
const DEFAULT_PAGE_READ_PARALLELISM: usize = 64;
const DEFAULT_PAGE_WRITE_PARALLELISM: usize = 64;
const DEFAULT_PAGE_BATCH_READ_PARALLELISM: usize = 64;
const DEFAULT_PAGE_BATCH_WRITE_PARALLELISM: usize = 64;
const DEFAULT_PAGE_BATCH_DISCARD_PARALLELISM: usize = 64;
const DEFAULT_PAGE_REPAIR_PARALLELISM: usize = 64;
const DEFAULT_PAGE_CHUNK_FANOUT_PARALLELISM: usize = 64;
const DEFAULT_PRIMARY_BATCH_READ_REPAIR_PROBE_LIMIT: usize = 8;
const STORAGE_ADMISSION_PROOF_FORMAT: &str = "MXOS-STORAGE-ADMISSION-PROOF2";
const CACHE_BATCH_PROOF_FORMAT: &str = "MXOS-CACHE-BATCH-PROOF1";
const METADATA_BATCH_READ_PROOF_FORMAT: &str = "MXOS-METADATA-BATCH-READ-PROOF1";
const OBJECT_BATCH_READ_PROOF_FORMAT: &str = "MXOS-OBJECT-BATCH-READ-PROOF1";
const DELETE_BATCH_PROOF_FORMAT: &str = "MXOS-DELETE-BATCH-PROOF1";
const PUT_BATCH_PROOF_FORMAT: &str = "MXOS-PUT-BATCH-PROOF1";
const COPY_BATCH_PROOF_FORMAT: &str = "MXOS-COPY-BATCH-PROOF1";
const MULTIPART_CREATE_PROOF_FORMAT: &str = "MXOS-MULTIPART-CREATE-PROOF1";
const MULTIPART_PART_PROOF_FORMAT: &str = "MXOS-MULTIPART-PART-PROOF1";
const MULTIPART_COMPLETE_PROOF_FORMAT: &str = "MXOS-MULTIPART-COMPLETE-PROOF1";
const MULTIPART_ABORT_PROOF_FORMAT: &str = "MXOS-MULTIPART-ABORT-PROOF1";
const OBJECT_LIST_PAGE_PROOF_FORMAT: &str = "MXOS-OBJECT-LIST-PAGE-PROOF1";
const OBJECT_CHANGE_PAGE_PROOF_FORMAT: &str = "MXOS-OBJECT-CHANGE-PAGE-PROOF1";
const CHUNK_HEAD_BATCH_PROOF_FORMAT: &str = "MXOS-CHUNK-HEAD-BATCH-PROOF1";
const CHUNK_READ_BATCH_PROOF_FORMAT: &str = "MXOS-CHUNK-READ-BATCH-PROOF1";
const PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET: usize = usize::MAX;
const PAGE_CHUNK_SERVICE_MODE_ALL_ELIGIBLE_TARGET: usize = usize::MAX;
const DEFAULT_LOCAL_REPLICA_STORE_PARALLELISM: usize = 8;
const MAX_STORAGE_PARALLELISM: usize = 4096;
const DEFAULT_STORAGE_RECEIPT_RETENTION: usize = 100_000;
const MAX_STORAGE_RECEIPT_RETENTION: usize = 1_000_000;
const DEFAULT_META_RETENTION_LIMIT: usize = 1_000_000;
const MAX_META_RETENTION_LIMIT: usize = 10_000_000;
const DEFAULT_META_MAX_BATCH_MUTATIONS: usize = 10_000;
const DEFAULT_META_MAX_BATCH_VALUE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_META_MAX_OPEN_TRANSACTIONS: usize = 1_000_000;
const DEFAULT_META_MAX_NAMESPACE_LEASES: usize = 1_000_000;
const DEFAULT_META_MAX_COORDINATOR_SESSIONS: usize = 1_000_000;
const DEFAULT_META_MAX_SHARDS: usize = 1_000_000;
const MAX_META_ADMISSION_LIMIT: usize = 10_000_000;
const MAX_META_BATCH_VALUE_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_META_QOS_WINDOW_MS: u64 = 1000;
const MAX_META_QOS_WINDOW_MS: u64 = 60 * 60 * 1000;
const DEFAULT_PAGE_ROUTE_QOS_WINDOW_MS: u64 = 1000;
const MAX_PAGE_ROUTE_QOS_WINDOW_MS: u64 = 60 * 60 * 1000;
const DEFAULT_CHUNK_MUTATION_QOS_WINDOW_MS: u64 = 1000;
const MAX_CHUNK_MUTATION_QOS_WINDOW_MS: u64 = 60 * 60 * 1000;
const MAX_BUCKET_LIST_LIMIT: usize = 100_000;
const DEFAULT_CORS_ALLOWED_METHODS: [&str; 6] = ["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS"];
const MAX_BUCKET_POLICY_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_OBJECT_METADATA_ENTRIES: usize = 128;
const MAX_OBJECT_METADATA_KEY_BYTES: usize = 256;
const MAX_OBJECT_METADATA_VALUE_BYTES: usize = 4096;
const MAX_OBJECT_METADATA_TOTAL_BYTES: usize = 64 * 1024;
const DEFAULT_CORS_ALLOWED_HEADERS: [&str; 25] = [
    "authorization",
    "content-type",
    "content-md5",
    "content-length",
    "etag",
    "if-match",
    "if-none-match",
    "range",
    "x-amz-acl",
    "x-amz-content-sha256",
    "x-amz-copy-source",
    "x-amz-copy-source-if-match",
    "x-amz-copy-source-if-none-match",
    "x-amz-copy-source-if-modified-since",
    "x-amz-copy-source-if-unmodified-since",
    "x-amz-copy-source-range",
    "x-amz-date",
    "x-amz-decoded-content-length",
    "x-amz-expected-bucket-owner",
    "x-amz-meta-*",
    "x-amz-metadata-directive",
    "x-amz-security-token",
    "x-amz-storage-class",
    "x-amz-tagging",
    "x-matrixobjectstore-*",
];
const DEFAULT_CORS_EXPOSE_HEADERS: [&str; 22] = [
    "ETag",
    "Accept-Ranges",
    "Content-Range",
    "Content-Length",
    "Content-Type",
    "Last-Modified",
    "x-amz-bucket-region",
    "x-amz-delete-marker",
    "x-amz-version-id",
    "x-amz-restore",
    "x-matrixobjectstore-cache",
    "x-matrixobjectstore-root-blockserver-route-control-proof-count",
    "x-matrixobjectstore-root-blockserver-route-control-proof-checksum",
    "x-matrixobjectstore-root-blockserver-route-control-min-commit-index",
    "x-matrixobjectstore-root-blockserver-route-control-max-commit-index",
    "x-matrixobjectstore-storage-admission-proof-format",
    "x-matrixobjectstore-storage-admission-proof-checksum",
    "x-matrixobjectstore-storage-admission-reason",
    "x-matrixobjectstore-requested-read-index",
    "x-matrixobjectstore-requested-commit-index",
    "x-matrixobjectstore-consensus-read-index",
    "x-matrixobjectstore-metadata-commit-index",
];

#[cfg(feature = "raft-rs")]
pub mod raft_backend {
    pub fn raft_rs_linked() -> bool {
        let _ = raft::prelude::ConfChange::default();
        true
    }
}

#[cfg(not(feature = "raft-rs"))]
pub mod raft_backend {
    pub fn raft_rs_linked() -> bool {
        false
    }
}

#[path = "protocol/object.rs"]
mod object_protocol;
pub use object_protocol::*;
use object_protocol::{
    append_optional_string_field, append_route_control_fields, control_fields_checksum,
    string_map_to_indexed_fields, validate_pipeline_data_range, validate_pipeline_range,
};

mod lifecycle;
pub use lifecycle::*;

#[path = "protocol/metadata.rs"]
mod metadata_protocol;
pub use metadata_protocol::*;

#[path = "services/meta.rs"]
mod meta_service;
pub use meta_service::*;
#[path = "protocol/root.rs"]
mod root_protocol;
pub use root_protocol::*;

#[path = "services/root.rs"]
mod root_service;
pub use root_service::*;

#[path = "services/chunk.rs"]
mod chunk_service;
pub use chunk_service::*;
use chunk_service::{
    select_bounded_chunk_discard_ranges_from, select_bounded_chunk_write_ranges_from,
    trim_btree_map_to_retention, trim_vec_front_to_retention, validate_page_route_qos_config,
    validate_chunk_mutation_qos_config, validate_meta_retention_limit,
    validate_metadata_admission_policy, validate_metadata_qos_config,
    validate_metadata_route_cache_config, validate_storage_receipt_retention,
    verify_storage_receipt_page_invariants,
};

#[path = "services/block.rs"]
mod page_service;
pub use page_service::*;
use page_service::{
    default_chunk_endpoints, select_admitted_page_discard_requests,
    validate_page_chunk_endpoint_topology,
};

#[path = "protocol/storage.rs"]
mod storage_protocol;
pub use storage_protocol::*;

mod raft;
pub use raft::*;

#[path = "store/core.rs"]
mod core_store;
pub use core_store::*;

#[path = "store/raft_backed.rs"]
mod raft_backed_store;
pub use raft_backed_store::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectError {
    NotFound(String),
    InvalidArgument(String),
    PreconditionFailed(String),
    Corruption(String),
}

impl fmt::Display for ObjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectError::NotFound(message) => write!(f, "not found: {}", message),
            ObjectError::InvalidArgument(message) => write!(f, "invalid argument: {}", message),
            ObjectError::PreconditionFailed(message) => {
                write!(f, "precondition failed: {}", message)
            }
            ObjectError::Corruption(message) => write!(f, "corruption: {}", message),
        }
    }
}

impl std::error::Error for ObjectError {}

#[path = "internal/codec.rs"]
mod internal_codec;
pub use internal_codec::*;

#[cfg(test)]
mod tests;
