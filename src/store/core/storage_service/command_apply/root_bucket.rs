impl MatrixObjectStore {
    fn apply_root_bucket_command(
        &mut self,
        command: ObjectCommand,
        raft_entry_index: Option<u64>,
    ) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::CreateBucket { bucket } => {
                self.apply_create_bucket(&bucket, now_millis())
            }
            ObjectCommand::CreateBucketIdempotent {
                bucket,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_create_bucket_idempotent(
                    bucket,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.metadata),
            ObjectCommand::DeleteBucket {
                bucket,
                update_time_ms,
            } => {
                self.apply_delete_bucket(&bucket, update_time_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new(bucket, ""),
                    version: 0,
                    length: 0,
                    etag: String::new(),
                    content_type: "bucket/delete".to_string(),
                    user_metadata: BTreeMap::new(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms,
                    extents: Vec::new(),
                    deleted: true,
                })
            }
            ObjectCommand::DeleteBucketIdempotent {
                bucket,
                update_time_ms,
                idempotency_key,
            } => self
                .apply_delete_bucket_idempotent(
                    bucket,
                    update_time_ms,
                    idempotency_key,
                    raft_entry_index.unwrap_or(0),
                )
                .map(|receipt| receipt.metadata),
            ObjectCommand::RegisterRootNode { node } => {
                let node_id = node.node_id.clone();
                self.register_root_node(node)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_rootnode", node_id),
                    version: 0,
                    length: 0,
                    etag: String::new(),
                    content_type: "root/node".to_string(),
                    user_metadata: BTreeMap::new(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_millis(),
                    extents: Vec::new(),
                    deleted: false,
                })
            }
            ObjectCommand::HeartbeatRootNode {
                node_id,
                used_bytes,
                capacity_bytes,
                draining,
                online,
                now_ms,
            } => {
                self.heartbeat_root_node(
                    &node_id,
                    used_bytes,
                    capacity_bytes,
                    draining,
                    online,
                    now_ms,
                )?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_rootheartbeat", node_id),
                    version: now_ms,
                    length: capacity_bytes,
                    etag: String::new(),
                    content_type: "root/node-heartbeat".to_string(),
                    user_metadata: BTreeMap::new(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_ms,
                    extents: Vec::new(),
                    deleted: !online,
                })
            }
            ObjectCommand::HeartbeatRootPageHealth {
                node_id,
                health,
                now_ms,
            } => {
                self.heartbeat_root_page_health(&node_id, health, now_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_rootblockhealth", node_id),
                    version: now_ms,
                    length: 0,
                    etag: String::new(),
                    content_type: "root/block-health-heartbeat".to_string(),
                    user_metadata: BTreeMap::new(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_ms,
                    extents: Vec::new(),
                    deleted: false,
                })
            }
            ObjectCommand::HeartbeatRootChunkHealth {
                node_id,
                health,
                now_ms,
            } => {
                self.heartbeat_root_chunk_health(&node_id, health, now_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_rootchunkhealth", node_id),
                    version: now_ms,
                    length: 0,
                    etag: String::new(),
                    content_type: "root/chunk-health-heartbeat".to_string(),
                    user_metadata: BTreeMap::new(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_ms,
                    extents: Vec::new(),
                    deleted: false,
                })
            }
            ObjectCommand::ExpireRootNodeHeartbeats { ttl_ms, now_ms } => {
                let expired_count = self.expire_root_node_heartbeats(ttl_ms, now_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_rootheartbeatexpiry", "nodes"),
                    version: now_ms,
                    length: expired_count,
                    etag: String::new(),
                    content_type: "root/node-heartbeat-expiry".to_string(),
                    user_metadata: BTreeMap::new(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_ms,
                    extents: Vec::new(),
                    deleted: expired_count == 0,
                })
            }
            ObjectCommand::RemoveRootNode {
                node_id,
                force,
                now_ms,
            } => {
                let report = self.remove_root_node(&node_id, force)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_rootnoderemove", node_id),
                    version: now_ms,
                    length: report.referencing_bytes,
                    etag: String::new(),
                    content_type: "root/node-remove".to_string(),
                    user_metadata: BTreeMap::new(),
                    storage_class: k_standard_storage_class().to_string(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_ms,
                    extents: Vec::new(),
                    deleted: report.removed,
                })
            }
            ObjectCommand::ConfigureRootCoordinator { config } => {
                let config = self.configure_root_coordinator_config(config)?;
                Ok(root_coordinator_config_metadata(&config))
            }
            ObjectCommand::CampaignRootLeader {
                node_id,
                ttl_ms,
                now_ms,
            } => {
                let lease = self.campaign_root_leader(&node_id, ttl_ms, now_ms)?;
                Ok(root_leader_lease_metadata(
                    &lease,
                    "rootserver/leader-campaign",
                ))
            }
            ObjectCommand::CampaignRootLeaderIdempotent {
                node_id,
                ttl_ms,
                now_ms,
                idempotency_key,
            } => {
                let (lease, receipt) = self.campaign_root_leader_idempotent(
                    &node_id,
                    ttl_ms,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(root_leader_lease_metadata(
                    &lease,
                    if receipt.idempotency_key == idempotency_key {
                        "rootserver/leader-campaign-idempotent"
                    } else {
                        "rootserver/leader-campaign"
                    },
                ))
            }
            ObjectCommand::RenewRootLeader {
                node_id,
                epoch,
                ttl_ms,
                now_ms,
            } => {
                let lease = self.renew_root_leader(&node_id, epoch, ttl_ms, now_ms)?;
                Ok(root_leader_lease_metadata(
                    &lease,
                    "rootserver/leader-renew",
                ))
            }
            _ => unreachable!("command classified outside RootBucket"),
        }
    }
}
