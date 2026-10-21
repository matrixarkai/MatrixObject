// SPDX-License-Identifier: Apache-2.0
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
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "bucket/delete".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms,
                    extents: Vec::new().into(),
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
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "root/node".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_millis(),
                    extents: Vec::new().into(),
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
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "root/node-heartbeat".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
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
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "root/block-health-heartbeat".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
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
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "root/chunk-health-heartbeat".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::ExpireRootNodeHeartbeats { ttl_ms, now_ms } => {
                let expired_count = self.expire_root_node_heartbeats(ttl_ms, now_ms)?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_rootheartbeatexpiry", "nodes"),
                    version: now_ms,
                    length: expired_count,
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "root/node-heartbeat-expiry".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
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
                    etag_digest_bits: 0,
                    has_etag: false,
                    content_type: "root/node-remove".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: StorageClass::Standard,
                    lock: ObjectLock::default(),
                    tags: BTreeMap::new().into(),
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
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
