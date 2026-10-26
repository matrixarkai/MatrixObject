// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl RootSnapshot {
    pub fn from_bytes(data: &[u8]) -> Result<Self, ObjectError> {
        let mut cursor = SnapshotByteCursor::new(data);
        let magic = cursor.read_string()?;
        let version = magic
            .strip_prefix("MOSRSROOT")
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .filter(|version| (1..=32).contains(version))
            .ok_or_else(|| {
                ObjectError::Corruption("root snapshot byte magic mismatch".to_string())
            })?;
        if magic != format!("MOSRSROOT{version}") {
            return Err(ObjectError::Corruption(
                "root snapshot byte magic mismatch".to_string(),
            ));
        }
        let features = RootSnapshotDecodeFeatures::for_version(version);
        let next_commit_index = cursor.read_u64()?;
        let nodes = read_root_snapshot_nodes(&mut cursor, features)?;
        let mut placements = BTreeMap::new();
        for _ in 0..cursor.read_count("root snapshot placement count")? {
            let map_object_id = ObjectId {
                bucket: cursor.read_string()?.into(),
                key: cursor.read_string()?.into(),
            };
            let object_id = ObjectId {
                bucket: cursor.read_string()?.into(),
                key: cursor.read_string()?.into(),
            };
            let version = cursor.read_u64()?;
            let length = cursor.read_u64()?;
            let update_time_ms = cursor.read_u64()?;
            let mut node_ids = Vec::new();
            for _ in 0..cursor.read_count("root snapshot placement node count")? {
                node_ids.push(cursor.read_string()?);
            }
            placements.insert(
                map_object_id,
                ObjectPlacement {
                    object_id,
                    version,
                    length,
                    node_ids,
                    update_time_ms,
                },
            );
        }
        let leader_lease = match cursor.read_u64()? {
            0 => None,
            1 => Some(RootLeaderLease {
                leader_node_id: cursor.read_string()?,
                epoch: cursor.read_u64()?,
                acquire_time_ms: cursor.read_u64()?,
                expire_time_ms: cursor.read_u64()?,
                ttl_ms: cursor.read_u64()?,
            }),
            _ => {
                return Err(ObjectError::Corruption(
                    "root snapshot leader lease presence flag is invalid".to_string(),
                ))
            }
        };
        let mut leader_lease_receipts = BTreeMap::new();
        if features.has_leader_lease_receipts {
            for _ in 0..cursor.read_count("root snapshot leader lease receipt count")? {
                let map_key = cursor.read_string()?;
                let mut receipt = RootLeaderLeaseReceipt {
                    idempotency_key: cursor.read_string()?,
                    operation: cursor.read_string()?,
                    request_checksum: cursor.read_string()?,
                    lease: RootLeaderLease {
                        leader_node_id: cursor.read_string()?,
                        epoch: cursor.read_u64()?,
                        acquire_time_ms: cursor.read_u64()?,
                        expire_time_ms: cursor.read_u64()?,
                        ttl_ms: cursor.read_u64()?,
                    },
                    commit_index: if features.has_root_leader_lease_receipt_proof {
                        cursor.read_u64()?
                    } else {
                        next_commit_index.saturating_sub(1)
                    },
                    update_time_ms: if features.has_root_leader_lease_receipt_proof {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    receipt_proof_checksum: if features.has_root_leader_lease_receipt_proof {
                        cursor.read_string()?
                    } else {
                        String::new()
                    },
                };
                if receipt.receipt_proof_checksum.is_empty() {
                    receipt = receipt.with_proof_checksum();
                }
                leader_lease_receipts.insert(map_key, receipt);
            }
        }
        let coordinator_config = if features.has_root_coordinator_config {
            read_metadata_coordinator_config(&mut cursor)?
        } else {
            MetadataCoordinatorConfig {
                namespace_path: "/matrixobjectstore/rootserver".to_string(),
                ..MetadataCoordinatorConfig::default()
            }
        };
        let checksum = cursor.read_string()?;
        let checksum_end = cursor.position();
        let envelope_checksum = cursor.read_string()?;
        if cursor.position() != data.len() {
            return Err(ObjectError::Corruption(
                "root snapshot byte payload has trailing data".to_string(),
            ));
        }
        if !etag_matches(&data[..checksum_end], &envelope_checksum) {
            return Err(ObjectError::Corruption(
                "root snapshot byte envelope checksum mismatch".to_string(),
            ));
        }
        let snapshot = Self {
            nodes,
            placements,
            leader_lease,
            leader_lease_receipts,
            coordinator_config,
            next_commit_index,
            checksum,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub(crate) fn checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            true, true, true, true, true, true, true, true, true, true, true,
        )
    }
}
