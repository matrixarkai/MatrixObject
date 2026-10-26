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
        let CatalogRoster {
            nodes,
            placements,
            leader_lease,
            leader_lease_receipts,
            coordinator_config,
        } = read_catalog_roster(&mut cursor, features, next_commit_index)?;
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

/// The roster shape a snapshot other than the placement snapshot writes.
///
/// # WHY THIS IS A CONSTANT AND NOT "THE CURRENT VERSION"
///
/// Reading "whatever the newest placement snapshot writes" would mean a later change to that
/// encoder silently changed a different snapshot's bytes. Pinning the number makes such a
/// change a decision, and `the_metadata_snapshot_encoding_is_byte_for_byte_what_it_was` fails
/// if the bytes move underneath it either way.
pub(crate) const CATALOG_ROSTER_SHAPE_VERSION: u64 = 32;

/// A control-plane roster as it sits in a snapshot: the nodes, the placements they hold, the
/// leader lease, the lease receipts, and the coordinator configuration they were agreed under.
pub(crate) struct CatalogRoster {
    pub(crate) nodes: BTreeMap<String, RootNode>,
    pub(crate) placements: BTreeMap<ObjectId, ObjectPlacement>,
    pub(crate) leader_lease: Option<RootLeaderLease>,
    pub(crate) leader_lease_receipts: BTreeMap<String, RootLeaderLeaseReceipt>,
    pub(crate) coordinator_config: MetadataCoordinatorConfig,
}

/// Read a roster back out of the bytes `append_catalog_roster` wrote.
///
/// # WHY THIS IS SHARED
///
/// A second snapshot is about to carry the roster, which means reading it back in a second
/// place. A decoder written out twice is the same hazard as a rule written out twice, with a
/// worse failure: the two would disagree about what the same bytes mean, and whichever one
/// reads a given store decides what that store says.
///
/// `fallback_commit_index` is only consulted for snapshots old enough to predate the receipt
/// proof fields, where a receipt's commit index was not stored and had to be inferred from
/// the snapshot it was found in. A caller writing the current shape never reaches it.
pub(crate) fn read_catalog_roster(
    cursor: &mut SnapshotByteCursor,
    features: RootSnapshotDecodeFeatures,
    fallback_commit_index: u64,
) -> Result<CatalogRoster, ObjectError> {
    let nodes = read_root_snapshot_nodes(cursor, features)?;
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
                    fallback_commit_index.saturating_sub(1)
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
        read_metadata_coordinator_config(cursor)?
    } else {
        MetadataCoordinatorConfig {
            namespace_path: "/matrixobjectstore/rootserver".to_string(),
            ..MetadataCoordinatorConfig::default()
        }
    };
    Ok(CatalogRoster {
        nodes,
        placements,
        leader_lease,
        leader_lease_receipts,
        coordinator_config,
    })
}
