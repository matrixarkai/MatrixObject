use super::*;

pub struct RaftBackedObjectStore<R: RustRaftLog> {
    pub(crate) raft: R,
    pub(crate) store: MatrixObjectStore,
}

include!("raft_backed/bootstrap_catalog.inc.rs");
include!("raft_backed/object_bucket_mutations.inc.rs");
include!("raft_backed/object_lifecycle_reads.inc.rs");
include!("raft_backed/chunk_block_control.inc.rs");
include!("raft_backed/snapshot_repair.inc.rs");
include!("raft_backed/storage_metadata_maintenance.inc.rs");
include!("raft_backed/metadata_raft_control.inc.rs");
include!("raft_backed/receipt_query.inc.rs");
include!("raft_backed/object_query_snapshot_replay.inc.rs");
