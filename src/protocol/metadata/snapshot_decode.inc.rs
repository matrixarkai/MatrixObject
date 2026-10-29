// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MetaSnapshot {
    pub fn from_bytes(data: &[u8]) -> Result<Self, ObjectError> {
        // Version 18 exists with both the original roster layout and the explicit
        // page-tier field emitted by the shared placement encoder. Both paths verify
        // the complete envelope and validate the restored snapshot before accepting it.
        Self::decode_with_roster_shape(data, PLACEMENT_ROSTER_SHAPE_VERSION)
            .or_else(|original_error| {
                Self::decode_with_roster_shape(data, 33).map_err(|_| original_error)
            })
    }

    fn decode_with_roster_shape(data: &[u8], roster_shape: u64) -> Result<Self, ObjectError> {
        Self::decode_cursor(SnapshotByteCursor::new(data), roster_shape)
    }

    #[cfg(any(feature = "metadata-consensus", test))]
    pub(crate) const STREAM_ROSTER_SHAPE: u64 = 33;

    #[cfg(any(feature = "metadata-consensus", test))]
    pub(crate) fn read_from(reader: &mut dyn std::io::Read, length: u64) -> Result<Self, ObjectError> {
        let length = usize::try_from(length).map_err(|_| ObjectError::Corruption("metadata snapshot exceeds platform bounds".into()))?;
        Self::decode_cursor(SnapshotByteCursor::from_reader(reader, length), Self::STREAM_ROSTER_SHAPE)
    }

    fn decode_cursor(mut cursor: SnapshotByteCursor<'_>, roster_shape: u64) -> Result<Self, ObjectError> {
        let magic = cursor.read_string()?;
        let version = magic
            .strip_prefix("MOSRSMETA")
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .filter(|version| (1..=18).contains(version))
            .ok_or_else(|| {
                ObjectError::Corruption("metadata snapshot byte magic mismatch".to_string())
            })?;
        if magic != format!("MOSRSMETA{version}") {
            return Err(ObjectError::Corruption(
                "metadata snapshot byte magic mismatch".to_string(),
            ));
        }
        if cursor.stream.is_some() && version != 18 {
            return Err(ObjectError::Corruption("streamed metadata snapshot needs version 18".into()));
        }
        let has_transaction_read_versions = version >= 2;
        let has_tombstone_compaction_counters = version >= 3;
        let has_batch_request_receipts = version >= 4;
        let has_raft_snapshot_compaction_counters = version >= 5;
        let has_lease_receipts = version >= 6;
        let has_coordinator_session_receipts = version >= 7;
        let has_shard_receipts = version >= 8;
        let has_lease_receipt_release_flag = version >= 9;
        let has_transaction_receipts = version >= 10;
        let has_retention_limits = version >= 11;
        let has_admission_policy = version >= 12;
        let has_qos = version >= 13;
        let has_coordinator_config = version >= 14;
        let has_coordinator_config_receipts = version >= 15;
        // A RECORD'S VALUE STATES ITS SHAPE from 17 on. Before that every value was text --
        // an object record carried the whole rendering -- so an older snapshot reads as text
        // and is EQUAL to the digest a live write produces, which is what makes this
        // readable rather than a migration a deployment has to perform.
        let has_record_value_kind = version >= 17;
        let has_placement_roster = version >= 18;
        let next_commit_index = cursor.read_u64()?;
        let next_change_sequence = cursor.read_u64()?;
        let compacted_tombstone_count = if has_tombstone_compaction_counters {
            cursor.read_u64()?
        } else {
            0
        };
        let last_tombstone_compaction_commit_index = if has_tombstone_compaction_counters {
            cursor.read_u64()?
        } else {
            0
        };
        let raft_snapshot_floor_commit_index = if has_raft_snapshot_compaction_counters {
            cursor.read_u64()?
        } else {
            0
        };
        let raft_snapshot_compacted_change_count = if has_raft_snapshot_compaction_counters {
            cursor.read_u64()?
        } else {
            0
        };
        let raft_snapshot_compacted_batch_receipt_count = if has_raft_snapshot_compaction_counters {
            cursor.read_u64()?
        } else {
            0
        };
        let change_log_retention_limit = if has_retention_limits {
            cursor.read_count("metadata snapshot change log retention limit")?
        } else {
            DEFAULT_META_RETENTION_LIMIT
        };
        let mutation_receipt_retention_limit = if has_retention_limits {
            cursor.read_count("metadata snapshot mutation receipt retention limit")?
        } else {
            DEFAULT_META_RETENTION_LIMIT
        };
        let admission_policy = if has_admission_policy {
            read_metadata_admission_policy(&mut cursor)?
        } else {
            MetadataAdmissionPolicy::default()
        };
        let (qos_config, qos_state) = if has_qos {
            (
                read_metadata_qos_config(&mut cursor)?,
                read_metadata_qos_state(&mut cursor)?,
            )
        } else {
            (MetadataQosConfig::default(), MetadataQosState::default())
        };
        let coordinator_config = if has_coordinator_config {
            read_metadata_coordinator_config(&mut cursor)?
        } else {
            MetadataCoordinatorConfig::default()
        };
        let mut coordinator_config_receipts = BTreeMap::new();
        if has_coordinator_config_receipts {
            for _ in 0..cursor.read_count("metadata coordinator config receipt count")? {
                let idempotency_key = cursor.read_string()?;
                coordinator_config_receipts.insert(
                    idempotency_key,
                    read_metadata_coordinator_config_receipt(&mut cursor)?,
                );
            }
        }
        let mut records = BTreeMap::new();
        for _ in 0..cursor.read_count("metadata snapshot record count")? {
            // A snapshot states the key once; the map files the record under
            // it and the record names it again, and both now do so through one
            // allocation. Building each separately would rebuild the copy.
            let key: SharedText = cursor.read_string()?.into();
            let value_kind = if has_record_value_kind {
                cursor.read_u64()?
            } else {
                0
            };
            let value_text = cursor.read_string()?;
            // THE ORDER IS THE FORMAT -- see `read_metadata_record`. These three read from
            // the cursor and must stay in this order.
            let record = MetadataRecord::new(
                key.clone(),
                RecordValue::from_kind_tag(value_kind, &value_text)?,
                cursor.read_u64()?,
                cursor.read_u64()?,
                cursor.read_bool()?,
            );
            records.insert(key, record);
        }
        let mut leases = BTreeMap::new();
        for _ in 0..cursor.read_count("metadata snapshot lease count")? {
            let namespace = cursor.read_string()?;
            let lease = MetadataLease {
                namespace: namespace.clone(),
                owner: cursor.read_string()?,
                epoch: cursor.read_u64()?,
                expire_time_ms: cursor.read_u64()?,
                ttl_ms: cursor.read_u64()?,
            };
            leases.insert(namespace, lease);
        }
        let mut lease_receipts = BTreeMap::new();
        if has_lease_receipts {
            for _ in 0..cursor.read_count("metadata snapshot lease receipt count")? {
                let idempotency_key = cursor.read_string()?;
                lease_receipts.insert(
                    idempotency_key,
                    read_metadata_lease_receipt(&mut cursor, has_lease_receipt_release_flag)?,
                );
            }
        }
        let mut coordinator_sessions = BTreeMap::new();
        for _ in 0..cursor.read_count("metadata snapshot coordinator session count")? {
            let session_id = cursor.read_string()?;
            let session = CoordinatorSession {
                session_id: session_id.clone(),
                owner: cursor.read_string()?,
                endpoint: cursor.read_string()?,
                epoch: cursor.read_u64()?,
                acquire_time_ms: cursor.read_u64()?,
                last_renew_ms: cursor.read_u64()?,
                expire_time_ms: cursor.read_u64()?,
                ttl_ms: cursor.read_u64()?,
            };
            coordinator_sessions.insert(session_id, session);
        }
        let mut coordinator_session_receipts = BTreeMap::new();
        if has_coordinator_session_receipts {
            for _ in 0..cursor.read_count("metadata snapshot coordinator session receipt count")? {
                let idempotency_key = cursor.read_string()?;
                coordinator_session_receipts.insert(
                    idempotency_key,
                    read_coordinator_session_receipt(&mut cursor)?,
                );
            }
        }
        let mut shards = BTreeMap::new();
        for _ in 0..cursor.read_count("metadata snapshot shard count")? {
            let shard_id = cursor.read_string()?;
            let shard = MetadataShard {
                shard_id: shard_id.clone(),
                start_key: cursor.read_string()?,
                end_key: cursor.read_string()?,
                owner_node_id: cursor.read_string()?,
                epoch: cursor.read_u64()?,
                draining: cursor.read_bool()?,
            };
            shards.insert(shard_id, shard);
        }
        let mut shard_receipts = BTreeMap::new();
        if has_shard_receipts {
            for _ in 0..cursor.read_count("metadata snapshot shard receipt count")? {
                let idempotency_key = cursor.read_string()?;
                shard_receipts.insert(idempotency_key, read_metadata_shard_receipt(&mut cursor)?);
            }
        }
        let mut batch_request_receipts = BTreeMap::new();
        if has_batch_request_receipts {
            for _ in 0..cursor.read_count("metadata snapshot batch receipt count")? {
                let idempotency_key = cursor.read_string()?;
                batch_request_receipts
                    .insert(
                        idempotency_key,
                        read_metadata_batch_receipt(&mut cursor, has_record_value_kind)?,
                    );
            }
        }
        let mut transactions = BTreeMap::new();
        for _ in 0..cursor.read_count("metadata snapshot transaction count")? {
            let transaction_id = cursor.read_string()?;
            let owner = cursor.read_string()?;
            let expire_time_ms = cursor.read_u64()?;
            let state = cursor.read_string()?;
            let begin_time_ms = cursor.read_u64()?;
            let update_time_ms = cursor.read_u64()?;
            let mut mutations = Vec::new();
            for _ in 0..cursor.read_count("metadata snapshot transaction mutation count")? {
                mutations.push(MetadataMutation {
                    key: cursor.read_string()?.into(),
                    record_value: cursor.read_string()?.into(),
                    expected_version: cursor.read_u64()?,
                    delete: cursor.read_bool()?,
                });
            }
            let mut read_versions = BTreeMap::new();
            if has_transaction_read_versions {
                for _ in 0..cursor.read_count("metadata snapshot transaction read version count")? {
                    read_versions.insert(cursor.read_string()?, cursor.read_u64()?);
                }
            }
            transactions.insert(
                transaction_id.clone(),
                MetadataTransaction {
                    transaction_id,
                    owner,
                    expire_time_ms,
                    state,
                    mutations,
                    read_versions,
                    begin_time_ms,
                    update_time_ms,
                },
            );
        }
        let mut transaction_receipts = BTreeMap::new();
        if has_transaction_receipts {
            for _ in 0..cursor.read_count("metadata snapshot transaction receipt count")? {
                let idempotency_key = cursor.read_string()?;
                transaction_receipts.insert(
                    idempotency_key,
                    read_metadata_transaction_receipt(&mut cursor, has_record_value_kind)?,
                );
            }
        }
        let mut change_checkpoints = BTreeMap::new();
        for _ in 0..cursor.read_count("metadata snapshot checkpoint count")? {
            let checkpoint_key = cursor.read_string()?;
            let checkpoint = MetadataChangeCheckpoint {
                consumer_id: cursor.read_string()?,
                prefix: cursor.read_string()?,
                after_change_sequence: cursor.read_u64()?,
                after_commit_index: cursor.read_u64()?,
                update_time_ms: cursor.read_u64()?,
                owner: cursor.read_string()?,
                note: cursor.read_string()?,
            };
            change_checkpoints.insert(checkpoint_key, checkpoint);
        }
        let mut changes = Vec::new();
        for _ in 0..cursor.read_count("metadata snapshot change count")? {
            // Read in wire order, so the key can be taken from the record it
            // names before the rest of the fields are read.
            let sequence = cursor.read_u64()?;
            let commit_index = cursor.read_u64()?;
            let key_text = cursor.read_string()?;
            let key = records
                .get_key_value(key_text.as_str())
                .map(|(shared, _)| shared.clone())
                .unwrap_or_else(|| key_text.into());
            changes.push(MetadataChange {
                sequence,
                commit_index,
                key,
                version: cursor.read_u64()?,
                deleted: cursor.read_bool()?,
                update_time_ms: cursor.read_u64()?,
            });
        }
        // THE CONTROL-PLANE ROSTER, absent before version 18. An older snapshot decodes to
        // an empty roster that still satisfies every roster rule, so it loads rather than
        // being refused -- and because its stored checksum was computed without these bytes,
        // the re-stamp below covers it.
        let (placement_commit_index, placement_roster) = if has_placement_roster {
            let commit_index = cursor.read_u64()?;
            let roster = read_placement_roster(
                &mut cursor,
                PlacementSnapshotDecodeFeatures::for_version(roster_shape),
                commit_index,
            )?;
            (commit_index, roster)
        } else {
            (
                1,
                PlacementRoster {
                    nodes: BTreeMap::new(),
                    placements: BTreeMap::new(),
                    leader_lease: None,
                    leader_lease_receipts: BTreeMap::new(),
                    coordinator_config: MetadataCoordinatorConfig::default(),
                    rewrote_retired_spellings: false,
                },
            )
        };
        let checksum = cursor.read_string()?;
        let envelope_digest = cursor.prefix_digest();
        let envelope_checksum = cursor.read_string()?;
        if cursor.position() != cursor.length {
            return Err(ObjectError::Corruption(
                "metadata snapshot byte payload has trailing data".to_string(),
            ));
        }
        if parse_etag_digest(&envelope_checksum) != Some(envelope_digest) {
            return Err(ObjectError::Corruption(
                "metadata snapshot byte envelope checksum mismatch".to_string(),
            ));
        }
        let mut snapshot = Self {
            records,
            leases,
            lease_receipts,
            coordinator_config,
            coordinator_config_receipts,
            coordinator_sessions,
            coordinator_session_receipts,
            shards,
            shard_receipts,
            batch_request_receipts,
            transactions,
            transaction_receipts,
            change_checkpoints,
            changes,
            next_commit_index,
            next_change_sequence,
            compacted_tombstone_count,
            last_tombstone_compaction_commit_index,
            raft_snapshot_floor_commit_index,
            raft_snapshot_compacted_change_count,
            raft_snapshot_compacted_batch_receipt_count,
            change_log_retention_limit,
            mutation_receipt_retention_limit,
            admission_policy,
            qos_config,
            qos_state,
            placement_nodes: placement_roster.nodes,
            placements: placement_roster.placements,
            placement_leader_lease: placement_roster.leader_lease,
            placement_leader_lease_receipts: placement_roster.leader_lease_receipts,
            placement_coordinator_config: placement_roster.coordinator_config,
            placement_commit_index,
            checksum,
        };
        if !has_batch_request_receipts
            || !has_lease_receipts
            || !has_lease_receipt_release_flag
            || !has_coordinator_session_receipts
            || !has_shard_receipts
            || !has_transaction_receipts
            || !has_retention_limits
            || !has_admission_policy
            || !has_qos
            || !has_coordinator_config
            || !has_coordinator_config_receipts
            || !has_placement_roster
            // A RETIRED NODE-KIND SPELLING REWRITTEN ON THE WAY IN changes the material the
            // checksum is taken over, whatever the version. This is the one clause here that
            // is not about a version at all.
            || placement_roster.rewrote_retired_spellings
        {
            snapshot.checksum = snapshot.checksum_material_hash();
        }
        snapshot.validate()?;
        Ok(snapshot)
    }
}

#[cfg(test)]
mod metadata_stream_decode_tests {
    use super::*;
    #[test]
    fn metadata_reader_matches_bytes_and_checks_truncation() {
        let snapshot = crate::MatrixMetaService::new().export_snapshot();
        let bytes = snapshot.to_bytes().unwrap();
        struct Short<'a>(&'a [u8]);
        impl std::io::Read for Short<'_> {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let n = out.len().min(3).min(self.0.len());
                out[..n].copy_from_slice(&self.0[..n]); self.0 = &self.0[n..]; Ok(n)
            }
        }
        assert_eq!(MetaSnapshot::read_from(&mut Short(&bytes), bytes.len() as u64).unwrap(), snapshot);
        for cut in [0, 1, bytes.len()/2, bytes.len()-1] {
            assert!(MetaSnapshot::read_from(&mut Short(&bytes[..cut]), bytes.len() as u64).is_err());
        }
        let mut bad = bytes.clone(); let last = bad.len()-1; bad[last] ^= 1;
        assert!(MetaSnapshot::read_from(&mut Short(&bad), bad.len() as u64).is_err());
        assert!(MetaSnapshot::read_from(&mut Short(&bytes), bytes.len() as u64 + 1).is_err());
    }
}
