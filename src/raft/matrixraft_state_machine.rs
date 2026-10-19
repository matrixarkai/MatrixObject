// Running the metadata service as a MatrixRaft state machine.
//
// `matrixraft::StateMachine` is three methods -- apply, snapshot and
// install_snapshot -- and the metadata service already has all three shapes:
// `apply_command` for mutations, `export_metadata_snapshot_chunks` for a
// snapshot and `install_raft_metadata_snapshot_bytes` to take one back.
//
// Two of the three are wired here. The snapshot path introduces **no new
// format**: MatrixRaft carries opaque bytes in `SnapshotChunk::data`, and what
// travels in it is the metadata snapshot encoding this store already writes and
// already validates on install. So a snapshot that crosses a raft group is the
// same bytes a local install would accept, and the checksum and
// `last_applied_index` checks inside `install_raft_metadata_snapshot_bytes` run
// unchanged.
//
// `apply` is deliberately not wired, and that is a decision rather than an
// omission -- see the comment on it.

// The facade files are included directly into matrixraft's lib.rs, so these
// types are declared at its crate root rather than under a module path.
use matrixraft::{
    ApplyRequest, ApplyResponse, LogId, RaftError, SnapshotChunk, SnapshotMetadata, StateMachine,
};

/// The payload size a snapshot is exported in, matching the 64 KiB chunk the
/// transport is built around.
const MATRIXRAFT_SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;

/// A metadata service a MatrixRaft node can drive.
pub struct MetadataStateMachine {
    store: MatrixObjectStore,
    /// Bytes accumulated across the pages of one incoming snapshot.
    installing: Vec<u8>,
    /// The commit index an installed snapshot must reach, as
    /// `install_raft_metadata_snapshot_bytes` requires.
    min_commit_index: u64,
}

impl MetadataStateMachine {
    pub fn new(store: MatrixObjectStore) -> Self {
        Self {
            store,
            installing: Vec::new(),
            min_commit_index: 0,
        }
    }

    /// The commit index an installed snapshot is required to reach. A snapshot
    /// older than this is refused by the install itself.
    pub fn set_min_commit_index(&mut self, min_commit_index: u64) {
        self.min_commit_index = min_commit_index;
    }

    pub fn store(&self) -> &MatrixObjectStore {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut MatrixObjectStore {
        &mut self.store
    }

    pub fn into_store(self) -> MatrixObjectStore {
        self.store
    }
}

impl StateMachine for MetadataStateMachine {
    /// Not wired, and the reason is worth stating where someone will read it.
    ///
    /// A MatrixRaft payload is `Vec<u8>`, so applying an entry means decoding
    /// one into an `ObjectCommand`. `ObjectCommand` has **212 variants** and no
    /// byte codec, so writing one means choosing the encoding of the raft log
    /// itself -- a durable format, in a crate that already pins 160 format
    /// constants hashed into its checksums. That is a decision to take
    /// deliberately rather than a gap to fill while wiring a trait, so this
    /// reports it rather than inventing one.
    ///
    /// The snapshot half below needs no such choice, which is why it is here.
    fn apply(&mut self, request: ApplyRequest) -> Result<ApplyResponse, RaftError> {
        Err(RaftError::InvalidRequest(format!(
            "metadata apply needs an ObjectCommand byte codec, which would fix \
             the raft log format for 212 command variants: {} bytes at group {} \
             index {}",
            request.payload.len(),
            request.group_id,
            request.log_id.index
        )))
    }

    fn snapshot(&self) -> Result<SnapshotChunk, RaftError> {
        let pages = self
            .store
            .export_metadata_snapshot_chunks(MATRIXRAFT_SNAPSHOT_CHUNK_BYTES)
            .map_err(|err| RaftError::Storage(format!("metadata snapshot export failed: {err}")))?;
        let last_applied_index = pages.last().map_or(0, |chunk| chunk.last_applied_index);
        let total_bytes = pages.last().map_or(0, |chunk| chunk.total_bytes);
        let mut data = Vec::with_capacity(total_bytes as usize);
        for chunk in &pages {
            data.extend_from_slice(&chunk.payload);
        }
        Ok(SnapshotChunk {
            meta: SnapshotMetadata {
                // The index the snapshot is complete to. A term is not carried
                // by a metadata snapshot, so it is left at zero rather than
                // invented: what a receiver checks is the index.
                snapshot_id: format!("metadata-{last_applied_index}"),
                last_log_id: LogId {
                    term: 0,
                    index: last_applied_index,
                },
                membership: Vec::new(),
                members: Vec::new(),
            },
            offset: 0,
            data,
            done: true,
        })
    }

    fn install_snapshot(&mut self, chunk: SnapshotChunk) -> Result<(), RaftError> {
        // A sender may restart a snapshot, so an offset of zero begins a fresh
        // one rather than appending to whatever a previous attempt left.
        if chunk.offset == 0 {
            self.installing.clear();
        } else if chunk.offset as usize != self.installing.len() {
            return Err(RaftError::InvalidRequest(format!(
                "metadata snapshot chunk starts at {} but {} bytes have arrived",
                chunk.offset,
                self.installing.len()
            )));
        }
        self.installing.extend_from_slice(&chunk.data);
        if !chunk.done {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.installing);
        self.store
            .install_raft_metadata_snapshot_bytes(&bytes, self.min_commit_index)
            .map_err(|err| {
                RaftError::Storage(format!("metadata snapshot install failed: {err}"))
            })?;
        Ok(())
    }
}
