impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_expire_root_node_heartbeats(
        &mut self,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ExpireRootNodeHeartbeats { ttl_ms, now_ms })
    }

    pub fn propose_remove_root_node(
        &mut self,
        node_id: &str,
        force: bool,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RemoveRootNode {
            node_id: node_id.to_string(),
            force,
            now_ms,
        })
    }

    pub fn propose_configure_root_coordinator_config(
        &mut self,
        config: MetadataCoordinatorConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureRootCoordinator { config })
    }

    pub fn propose_campaign_root_leader(
        &mut self,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CampaignRootLeader {
            node_id: node_id.to_string(),
            ttl_ms,
            now_ms,
        })
    }
}
