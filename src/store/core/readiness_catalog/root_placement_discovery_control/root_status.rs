impl MatrixObjectStore {
    pub fn root_nodes(&self) -> Vec<RootNode> {
        self.root_service.list_nodes()
    }

    pub fn root_raft_status(&self, min_capacity_free_bytes: u64) -> RootRaftStatus {
        self.root_service.raft_status(
            raft_quorum(self.raft_voter_node_ids.len() as u64),
            min_capacity_free_bytes,
        )
    }

}
