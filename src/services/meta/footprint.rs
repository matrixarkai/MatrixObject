// What the metadata service holds, in bytes.
//
// The footprint audit read one of this service's twenty-odd maps and reported
// four others as entry counts, so everything here was missing from every
// metadata total it produced. The split is by what drives growth: the record
// map grows with the stored objects, the change log with mutations, the receipt
// maps with requests, the entity maps with the cluster, and the export caches
// hold whole encoded snapshots.
//
// The legs are written as loops rather than folds because each one charges key
// text through the audit's ledger, and the ledger has to be borrowed mutably
// one holder at a time. That is also what makes the figures honest: the record
// map, the records a receipt retained and the change log can all name one key
// through one allocation, and charging it at each of them would report copies
// that do not exist.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MetaServiceFootprint {
    pub(crate) records: u64,
    pub(crate) change_log: u64,
    /// Counts, not bytes, so deliberately not part of `total()`. The difference
    /// between the two is the log's slack, which is the point of reporting them.
    pub(crate) change_log_len: u64,
    pub(crate) change_log_capacity: u64,
    pub(crate) receipts: u64,
    pub(crate) entities: u64,
    pub(crate) export_caches: u64,
}

impl MetaServiceFootprint {
    pub(crate) fn total(&self) -> u64 {
        self.records
            .saturating_add(self.change_log)
            .saturating_add(self.receipts)
            .saturating_add(self.entities)
            .saturating_add(self.export_caches)
    }
}

/// Inline plus heap for a `BTreeMap<String, V>`: one key handle and one value
/// per entry, the key's own characters, and whatever the value owns beyond its
/// inline size. B-tree node slack is excluded, as it is everywhere here.
fn meta_map_bytes<V, F>(map: &BTreeMap<String, V>, value_heap: F) -> u64
where
    F: Fn(&V) -> u64,
{
    let inline = (map.len() as u64)
        .saturating_mul((std::mem::size_of::<String>() + std::mem::size_of::<V>()) as u64);
    map.iter().fold(inline, |total, (key, value)| {
        total
            .saturating_add(audit_string_heap(key))
            .saturating_add(value_heap(value))
    })
}

/// A `Vec<V>` at its capacity, plus whatever each element owns.
fn meta_vec_bytes<V, F>(items: &[V], capacity: usize, value_heap: F) -> u64
where
    F: Fn(&V) -> u64,
{
    items.iter().fold(
        (capacity as u64).saturating_mul(std::mem::size_of::<V>() as u64),
        |total, item| total.saturating_add(value_heap(item)),
    )
}

fn meta_string_vec_bytes(values: &[String]) -> u64 {
    meta_vec_bytes(values, values.len(), audit_string_heap)
}

fn meta_lease_heap(lease: &MetadataLease) -> u64 {
    audit_string_heap(&lease.namespace).saturating_add(audit_string_heap(&lease.owner))
}

fn meta_shard_heap(shard: &MetadataShard) -> u64 {
    audit_string_heap(&shard.shard_id)
        .saturating_add(audit_string_heap(&shard.start_key))
        .saturating_add(audit_string_heap(&shard.end_key))
        .saturating_add(audit_string_heap(&shard.owner_node_id))
}

fn meta_session_heap(session: &CoordinatorSession) -> u64 {
    audit_string_heap(&session.session_id)
        .saturating_add(audit_string_heap(&session.owner))
        .saturating_add(audit_string_heap(&session.endpoint))
}

fn meta_config_heap(config: &MetadataCoordinatorConfig) -> u64 {
    audit_string_heap(&config.backend)
        .saturating_add(audit_string_heap(&config.namespace_path))
        .saturating_add(meta_string_vec_bytes(&config.endpoints))
}

fn meta_optional_string_heap(value: Option<&String>) -> u64 {
    value.map(audit_string_heap).unwrap_or(0)
}

/// What one record owns: its own naming of the key, charged through the ledger
/// because the map it is filed under names it through the same allocation, and
/// its value.
fn meta_record_heap(ledger: &mut SharedTextLedger, record: &MetadataRecord) -> u64 {
    ledger
        .charge(&record.key)
        .saturating_add(audit_boxed_str_heap(&record.value))
}

fn meta_records_vec_bytes(ledger: &mut SharedTextLedger, records: &[MetadataRecord]) -> u64 {
    let mut total =
        (records.len() as u64).saturating_mul(std::mem::size_of::<MetadataRecord>() as u64);
    for record in records {
        total = total.saturating_add(meta_record_heap(ledger, record));
    }
    total
}

fn meta_transaction_heap(ledger: &mut SharedTextLedger, transaction: &MetadataTransaction) -> u64 {
    let mut total = audit_string_heap(&transaction.transaction_id)
        .saturating_add(audit_string_heap(&transaction.owner))
        .saturating_add(audit_string_heap(&transaction.state))
        .saturating_add(
            (transaction.mutations.capacity() as u64)
                .saturating_mul(std::mem::size_of::<MetadataMutation>() as u64),
        );
    for mutation in &transaction.mutations {
        total = total
            .saturating_add(ledger.charge(&mutation.key))
            .saturating_add(audit_string_heap(&mutation.value));
    }
    total = total.saturating_add(
        (transaction.read_versions.len() as u64)
            .saturating_mul((std::mem::size_of::<String>() + std::mem::size_of::<u64>()) as u64),
    );
    for key in transaction.read_versions.keys() {
        total = total.saturating_add(audit_string_heap(key));
    }
    total
}

impl MatrixMetaService {
    pub(crate) fn footprint_bytes(&self, ledger: &mut SharedTextLedger) -> MetaServiceFootprint {
        let mut records = (self.records.len() as u64).saturating_mul(
            (std::mem::size_of::<SharedText>() + std::mem::size_of::<MetadataRecord>()) as u64,
        );
        for (key, record) in self.records.iter() {
            records = records
                .saturating_add(ledger.charge(key))
                .saturating_add(meta_record_heap(ledger, record));
        }

        // One record per record mutation, trimmed only when an operator
        // reconfigures retention.
        let change_log_len = self.changes.len() as u64;
        let change_log_capacity = self.changes.capacity() as u64;
        let mut change_log = (self.changes.capacity() as u64)
            .saturating_mul(std::mem::size_of::<MetadataChange>() as u64);
        for change in &self.changes {
            change_log = change_log.saturating_add(ledger.charge(&change.key));
        }

        // Each receipt retains the records, shards or transaction it answered
        // for, so a receipt map holds whole copies of the data it describes --
        // except for the key text, which it can name through the live record's
        // allocation.
        let mut receipts = 0u64;
        receipts = receipts.saturating_add(
            (self.batch_request_receipts.len() as u64).saturating_mul(
                (std::mem::size_of::<String>() + std::mem::size_of::<MetadataBatchReceipt>())
                    as u64,
            ),
        );
        for (key, receipt) in self.batch_request_receipts.iter() {
            receipts = receipts
                .saturating_add(audit_string_heap(key))
                .saturating_add(audit_string_heap(&receipt.idempotency_key))
                .saturating_add(audit_string_heap(&receipt.request_checksum))
                .saturating_add(audit_string_heap(&receipt.expected_owner))
                .saturating_add(meta_optional_string_heap(receipt.lease_owner.as_ref()))
                .saturating_add(meta_records_vec_bytes(ledger, &receipt.records));
        }
        receipts = receipts.saturating_add(
            (self.transaction_receipts.len() as u64).saturating_mul(
                (std::mem::size_of::<String>() + std::mem::size_of::<MetadataTransactionReceipt>())
                    as u64,
            ),
        );
        for (key, receipt) in self.transaction_receipts.iter() {
            receipts = receipts
                .saturating_add(audit_string_heap(key))
                .saturating_add(audit_string_heap(&receipt.idempotency_key))
                .saturating_add(audit_string_heap(&receipt.operation))
                .saturating_add(audit_string_heap(&receipt.request_checksum))
                .saturating_add(meta_transaction_heap(ledger, &receipt.transaction))
                .saturating_add(meta_records_vec_bytes(ledger, &receipt.records));
        }
        receipts = receipts
            .saturating_add(meta_map_bytes(&self.lease_receipts, |receipt| {
                audit_string_heap(&receipt.idempotency_key)
                    .saturating_add(audit_string_heap(&receipt.operation))
                    .saturating_add(audit_string_heap(&receipt.request_checksum))
                    .saturating_add(meta_lease_heap(&receipt.lease))
            }))
            .saturating_add(meta_map_bytes(&self.shard_receipts, |receipt| {
                audit_string_heap(&receipt.idempotency_key)
                    .saturating_add(audit_string_heap(&receipt.operation))
                    .saturating_add(audit_string_heap(&receipt.request_checksum))
                    .saturating_add(meta_vec_bytes(
                        &receipt.shards,
                        receipt.shards.len(),
                        meta_shard_heap,
                    ))
            }))
            .saturating_add(meta_map_bytes(
                &self.coordinator_session_receipts,
                |receipt| {
                    audit_string_heap(&receipt.idempotency_key)
                        .saturating_add(audit_string_heap(&receipt.operation))
                        .saturating_add(audit_string_heap(&receipt.request_checksum))
                        .saturating_add(meta_session_heap(&receipt.session))
                },
            ))
            .saturating_add(meta_map_bytes(
                &self.coordinator_config_receipts,
                |receipt| {
                    audit_string_heap(&receipt.idempotency_key)
                        .saturating_add(audit_string_heap(&receipt.request_checksum))
                        .saturating_add(meta_config_heap(&receipt.config))
                },
            ))
            .saturating_add(meta_map_bytes(&self.leader_lease_receipts, |receipt| {
                audit_string_heap(&receipt.idempotency_key)
                    .saturating_add(audit_string_heap(&receipt.operation))
                    .saturating_add(audit_string_heap(&receipt.request_checksum))
                    .saturating_add(audit_string_heap(&receipt.receipt_proof_checksum))
                    .saturating_add(audit_string_heap(&receipt.lease.leader_node_id))
            }));

        let mut entities = meta_map_bytes(&self.nodes, |node| {
            audit_string_heap(&node.node_id)
                .saturating_add(audit_string_heap(&node.endpoint))
                .saturating_add(audit_string_heap(&node.service_kind))
                .saturating_add(audit_string_heap(&node.failure_domain))
        });
        entities = entities.saturating_add(
            (self.placements.len() as u64).saturating_mul(
                (std::mem::size_of::<ObjectId>() + std::mem::size_of::<ObjectPlacement>()) as u64,
            ),
        );
        for (object_id, placement) in self.placements.iter() {
            entities = entities
                .saturating_add(audit_object_id_heap(ledger, object_id))
                .saturating_add(audit_object_id_heap(ledger, &placement.object_id))
                .saturating_add(meta_string_vec_bytes(&placement.node_ids));
        }
        let mut transactions = (self.transactions.len() as u64).saturating_mul(
            (std::mem::size_of::<String>() + std::mem::size_of::<MetadataTransaction>()) as u64,
        );
        for (key, transaction) in self.transactions.iter() {
            transactions = transactions
                .saturating_add(audit_string_heap(key))
                .saturating_add(meta_transaction_heap(ledger, transaction));
        }
        entities = entities
            .saturating_add(transactions)
            .saturating_add(meta_map_bytes(&self.leases, meta_lease_heap))
            .saturating_add(meta_map_bytes(&self.shards, meta_shard_heap))
            .saturating_add(meta_map_bytes(
                &self.coordinator_sessions,
                meta_session_heap,
            ))
            .saturating_add(meta_map_bytes(&self.change_checkpoints, |checkpoint| {
                audit_string_heap(&checkpoint.consumer_id)
                    .saturating_add(audit_string_heap(&checkpoint.prefix))
                    .saturating_add(audit_string_heap(&checkpoint.owner))
                    .saturating_add(audit_string_heap(&checkpoint.note))
            }))
            .saturating_add(meta_config_heap(&self.coordinator_config))
            .saturating_add(meta_config_heap(&self.placement_coordinator_config))
            .saturating_add(
                self.leader_lease
                    .as_ref()
                    .map(|lease| audit_string_heap(&lease.leader_node_id))
                    .unwrap_or(0),
            );

        // A whole encoded snapshot, cached. This is the largest single thing
        // the service can hold and none of it was in any total.
        let export_caches = self
            .snapshot_export_cache
            .borrow()
            .as_ref()
            .map(|cache| cache.footprint_bytes())
            .unwrap_or(0)
            .saturating_add(
                self.placement_snapshot_export_cache
                    .borrow()
                    .as_ref()
                    .map(|cache| cache.footprint_bytes())
                    .unwrap_or(0),
            )
            .saturating_add(meta_optional_string_heap(
                self.root_state_fingerprint_cache.borrow().as_ref(),
            ))
            .saturating_add(self.metadata_snapshot_assembler.footprint_bytes());

        MetaServiceFootprint {
            records,
            change_log,
            change_log_len,
            change_log_capacity,
            receipts,
            entities,
            export_caches,
        }
    }
}

impl MetaSnapshotExportCache {
    fn footprint_bytes(&self) -> u64 {
        (self.bytes.len() as u64)
            .saturating_add(audit_string_heap(&self.stamp))
            .saturating_add(audit_string_heap(&self.full_checksum))
    }
}

impl RootSnapshotExportCache {
    fn footprint_bytes(&self) -> u64 {
        (self.bytes.len() as u64)
            .saturating_add(audit_string_heap(&self.stamp))
            .saturating_add(audit_string_heap(&self.checksum))
    }
}
