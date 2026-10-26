// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn placement_nodes(&self) -> Vec<PlacementNode> {
        self.meta_service.list_nodes()
    }

    pub fn placement_raft_status(&self, min_capacity_free_bytes: u64) -> PlacementRaftStatus {
        self.meta_service.raft_status(
            raft_quorum(self.raft_voter_node_ids.len() as u64),
            min_capacity_free_bytes,
        )
    }

}
