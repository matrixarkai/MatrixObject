// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementSnapshot {
    pub nodes: BTreeMap<String, PlacementNode>,
    pub placements: BTreeMap<ObjectId, ObjectPlacement>,
    pub leader_lease: Option<PlacementLeaderLease>,
    pub leader_lease_receipts: BTreeMap<String, PlacementLeaderLeaseReceipt>,
    pub coordinator_config: MetadataCoordinatorConfig,
    pub next_commit_index: u64,
    pub checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementSnapshotChunk {
    pub offset: u64,
    pub total_bytes: u64,
    pub page_checksum: String,
    pub full_checksum: String,
    pub eof: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct PlacementSnapshotChunkAssembler {
    expected_total_bytes: Option<u64>,
    expected_full_checksum: Option<String>,
    buffer: Vec<u8>,
}
