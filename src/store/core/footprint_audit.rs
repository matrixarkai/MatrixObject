/// Heap bytes a `String` owns -- its capacity, not its length, because the
/// slack is resident too and making slack visible is what this audit is for.
/// The 24 bytes of pointer, length and capacity live inside whatever holds the
/// string and are counted there.
pub(crate) fn audit_string_heap(value: &String) -> u64 {
    value.capacity() as u64
}

/// Inline plus heap for a `BTreeMap<String, String>`, excluding B-tree node
/// slack: a node holds up to eleven pairs, so the entries are counted but the
/// unused capacity of a partly filled node is not.
fn audit_string_map_bytes(map: &BTreeMap<String, String>) -> u64 {
    let inline = (map.len() as u64)
        .saturating_mul(2u64.saturating_mul(std::mem::size_of::<String>() as u64));
    map.iter().fold(inline, |total, (key, value)| {
        total
            .saturating_add(audit_string_heap(key))
            .saturating_add(audit_string_heap(value))
    })
}

/// What one in-flight multipart upload holds: its own strings, its part table,
/// and the part bytes themselves. The payload is returned separately because it
/// is data awaiting commit rather than metadata describing data.
fn audit_multipart_bytes(upload: &MultipartUpload) -> (u64, u64) {
    let metadata = (std::mem::size_of::<MultipartUpload>() as u64)
        .saturating_add(audit_string_heap(&upload.upload_id))
        .saturating_add(audit_string_heap(&upload.bucket))
        .saturating_add(audit_string_heap(&upload.key))
        .saturating_add(audit_string_heap(&upload.content_type))
        .saturating_add(
            (upload.parts.len() as u64).saturating_mul(
                (std::mem::size_of::<u32>() + std::mem::size_of::<MultipartPart>()) as u64,
            ),
        )
        .saturating_add((upload.part_payloads.len() as u64).saturating_mul(
            (std::mem::size_of::<u32>() + std::mem::size_of::<Vec<u8>>()) as u64,
        ));
    let payload = upload
        .part_payloads
        .values()
        .fold(0u64, |total, part| total.saturating_add(part.capacity() as u64));
    (metadata, payload)
}

pub(crate) fn audit_object_id_heap(object_id: &ObjectId) -> u64 {
    audit_string_heap(&object_id.bucket).saturating_add(audit_string_heap(&object_id.key))
}

/// Everything one `ObjectMetadata` owns beyond its own inline size.
fn audit_object_metadata_heap(metadata: &ObjectMetadata) -> u64 {
    audit_object_id_heap(&metadata.object_id)
        .saturating_add(audit_string_heap(&metadata.etag))
        .saturating_add(audit_string_heap(&metadata.content_type))
        .saturating_add(audit_string_heap(&metadata.storage_class))
        .saturating_add(audit_string_map_bytes(&metadata.user_metadata))
        .saturating_add(audit_string_map_bytes(&metadata.tags))
        .saturating_add(
            (metadata.extents.capacity() as u64)
                .saturating_mul(std::mem::size_of::<ObjectExtent>() as u64),
        )
}

impl MatrixObjectStore {
    /// A line-item resident-byte budget for the store.
    ///
    /// Reports `len` and `capacity` separately for every byte-carrying buffer, so
    /// allocation slack shows up instead of hiding inside a total, and entry counts
    /// for the maps whose cost is per-entry overhead rather than payload.
    ///
    /// Two totals. `ACCOUNTED.byte_buffers` is payload buffers, replica data,
    /// shards and checksums -- an amplification derived from it alone is
    /// data-only, because everything else used to be reported as an entry count.
    /// `ACCOUNTED.total_bytes` adds the metadata the store holds to describe
    /// that data, and is the one to divide by `logical.written`. Neither
    /// includes B-tree node slack or allocator overhead, so the remainder
    /// against process RSS is those.
    pub fn footprint_audit(&self) -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> = Vec::new();
        let mut push = |k: &str, v: u64| out.push((k.to_string(), v));

        // ---- the store's own copy of every written byte ----------------------
        // The store no longer holds payload bytes, only a length and a hash per
        // write. `indexed_bytes` is what those lengths add up to - the bytes the
        // page service holds - and `index_capacity` is what this map itself costs.
        let mut payload_len = 0u64;
        let mut payload_cap = 0u64;
        for value in self.payloads.values() {
            payload_len = payload_len.saturating_add(value.len);
            payload_cap = payload_cap
                .saturating_add(std::mem::size_of::<StoredPayload>() as u64);
        }
        push("payloads.entries", self.payloads.len() as u64);
        push("payloads.indexed_bytes", payload_len);
        push("payloads.index_capacity", payload_cap);

        // ---- metadata maps (cost is per-entry, not payload) -----------------
        push("objects.entries", self.objects.len() as u64);
        push("versions.entries", self.versions.len() as u64);
        let version_rows: u64 = self
            .versions
            .values()
            .map(|rows| rows.len() as u64)
            .fold(0u64, |a, b| a.saturating_add(b));
        push("versions.rows", version_rows);
        push("object_changes.len", self.object_changes.len() as u64);
        push("object_changes.capacity", self.object_changes.capacity() as u64);
        push("segments.entries", self.slabs.len() as u64);
        push("applied_raft_receipts.entries", self.applied_raft_receipts.len() as u64);
        push("multipart_uploads.entries", self.multipart_uploads.len() as u64);
        push("metadata_route_cache.entries", self.metadata_route_cache.len() as u64);

        // ---- meta service ---------------------------------------------------
        push("meta.records", self.meta_service.records.len() as u64);
        push("meta.shards", self.meta_service.shards.len() as u64);
        push("meta.transactions", self.meta_service.transactions.len() as u64);
        push("meta.batch_receipts", self.meta_service.batch_request_receipts.len() as u64);

        // ---- page service: receipt maps ------------------------------------
        let b = &self.page_service;
        push("block.pending_allocations", b.pending_allocations.len() as u64);
        push("block.alloc_receipts", b.allocation_request_receipts.len() as u64);
        push("block.batch_alloc_receipts", b.batch_allocation_request_receipts.len() as u64);
        push("block.release_receipts", b.release_allocation_request_receipts.len() as u64);
        push("block.write_receipts", b.write_request_receipts.len() as u64);
        push("block.batch_write_receipts", b.batch_write_request_receipts.len() as u64);
        push("block.discard_receipts", b.discard_request_receipts.len() as u64);
        push("block.batch_discard_receipts", b.batch_discard_request_receipts.len() as u64);
        push("block.replicas", b.replicas.len() as u64);

        // ---- the replicas: where the bytes actually live ---------------------
        let mut chunks = 0u64;
        let mut data_len = 0u64;
        let mut data_cap = 0u64;
        let mut shard_len = 0u64;
        let mut shard_cap = 0u64;
        let mut shard_count = 0u64;
        let mut checksum_bytes = 0u64;
        let mut discard_rows = 0u64;
        let mut chunk_receipts = 0u64;
        for replica in &b.replicas {
            chunks = chunks.saturating_add(replica.chunks.len() as u64);
            for state in replica.chunks.values() {
                data_len = data_len.saturating_add(state.data.len() as u64);
                data_cap = data_cap.saturating_add(state.data.capacity() as u64);
                checksum_bytes = checksum_bytes.saturating_add(state.checksum.capacity() as u64);
                discard_rows = discard_rows.saturating_add(state.discarded_ranges.len() as u64);
                shard_count = shard_count.saturating_add(1);
                shard_len = shard_len.saturating_add(state.ec_parity_fragment.len() as u64);
                shard_cap = shard_cap.saturating_add(state.ec_parity_fragment.capacity() as u64);
            }
            chunk_receipts = chunk_receipts
                .saturating_add(replica.write_request_receipts.len() as u64)
                .saturating_add(replica.discard_request_receipts.len() as u64)
                .saturating_add(replica.batch_write_request_receipts.len() as u64)
                .saturating_add(replica.batch_discard_request_receipts.len() as u64);
        }
        push("replica.chunks", chunks);
        push("replica.data.len", data_len);
        push("replica.data.capacity", data_cap);
        push("replica.ec_shards.count", shard_count);
        push("replica.ec_shards.len", shard_len);
        push("replica.ec_shards.capacity", shard_cap);
        push("replica.checksum.capacity", checksum_bytes);
        push("replica.discard_rows", discard_rows);
        push("replica.receipts", chunk_receipts);

        // ---- the metadata, which the byte-buffer headline excludes -----------
        let objects_bytes = self.objects.iter().fold(0u64, |total, (object_id, metadata)| {
            total
                .saturating_add(std::mem::size_of::<ObjectId>() as u64)
                .saturating_add(std::mem::size_of::<ObjectMetadata>() as u64)
                .saturating_add(audit_object_id_heap(object_id))
                .saturating_add(audit_object_metadata_heap(metadata))
        });
        let versions_bytes = self.versions.iter().fold(0u64, |total, (object_id, rows)| {
            let row_bytes = rows.iter().fold(
                (rows.capacity() as u64)
                    .saturating_mul(std::mem::size_of::<ObjectMetadata>() as u64),
                |inner, metadata| inner.saturating_add(audit_object_metadata_heap(metadata)),
            );
            total
                .saturating_add(std::mem::size_of::<ObjectId>() as u64)
                .saturating_add(audit_object_id_heap(object_id))
                .saturating_add(row_bytes)
        });
        let changes_bytes = self.object_changes.iter().fold(
            (self.object_changes.capacity() as u64)
                .saturating_mul(std::mem::size_of::<ObjectChangeRecord>() as u64),
            |total, change| {
                total
                    .saturating_add(audit_string_heap(&change.operation))
                    .saturating_add(audit_object_id_heap(&change.object_id))
                    .saturating_add(audit_string_heap(&change.etag))
            },
        );
        // The service accounts for itself: its change log, receipt maps,
        // entity maps and snapshot export caches are private to its module,
        // and until this was asked for, only the record map was in any total.
        let meta = self.meta_service.footprint_bytes();
        let meta_record_bytes = meta.records;
        // The payload headline counts only the value struct; the key is a
        // `(SlabId, u64)` and was never in the figure.
        let payload_key_bytes = (self.payloads.len() as u64)
            .saturating_mul(std::mem::size_of::<(SlabId, u64)>() as u64);
        push("objects.bytes", objects_bytes);
        // The two largest metadata line items, broken into their parts. Both
        // sums are asserted against the totals above by
        // `the_metadata_breakdown_accounts_for_every_byte_of_its_total`: a new
        // field counted in one place and not the other fails that test.
        //
        // `objects` holds the identity twice -- once as the map key and once
        // inside the value -- so `objects.id_text_duplicated` says how many of
        // those two texts are the same, rather than leaving it to be inferred.
        {
            let n = self.objects.len() as u64;
            push(
                "objects.mapkey_inline",
                n.saturating_mul(std::mem::size_of::<ObjectId>() as u64),
            );
            push(
                "objects.metadata_inline",
                n.saturating_mul(std::mem::size_of::<ObjectMetadata>() as u64),
            );
            push("objects.metadata_size", std::mem::size_of::<ObjectMetadata>() as u64);
            let mut mapkey_heap = 0u64;
            let mut inner_id_heap = 0u64;
            let mut etag_heap = 0u64;
            let mut content_type_heap = 0u64;
            let mut storage_class_heap = 0u64;
            let mut user_metadata_bytes = 0u64;
            let mut tags_bytes = 0u64;
            let mut extents_bytes = 0u64;
            let mut extents_len = 0u64;
            let mut extents_capacity = 0u64;
            let mut id_text_duplicated = 0u64;
            for (object_id, metadata) in self.objects.iter() {
                mapkey_heap = mapkey_heap.saturating_add(audit_object_id_heap(object_id));
                inner_id_heap =
                    inner_id_heap.saturating_add(audit_object_id_heap(&metadata.object_id));
                etag_heap = etag_heap.saturating_add(audit_string_heap(&metadata.etag));
                content_type_heap =
                    content_type_heap.saturating_add(audit_string_heap(&metadata.content_type));
                storage_class_heap =
                    storage_class_heap.saturating_add(audit_string_heap(&metadata.storage_class));
                user_metadata_bytes = user_metadata_bytes
                    .saturating_add(audit_string_map_bytes(&metadata.user_metadata));
                tags_bytes = tags_bytes.saturating_add(audit_string_map_bytes(&metadata.tags));
                extents_len = extents_len.saturating_add(metadata.extents.len() as u64);
                extents_capacity =
                    extents_capacity.saturating_add(metadata.extents.capacity() as u64);
                extents_bytes = extents_bytes.saturating_add(
                    (metadata.extents.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<ObjectExtent>() as u64),
                );
                if object_id.bucket == metadata.object_id.bucket
                    && object_id.key == metadata.object_id.key
                {
                    id_text_duplicated = id_text_duplicated.saturating_add(1);
                }
            }
            push("objects.mapkey_heap", mapkey_heap);
            push("objects.inner_id_heap", inner_id_heap);
            push("objects.id_text_duplicated", id_text_duplicated);
            push("objects.etag_heap", etag_heap);
            push("objects.content_type_heap", content_type_heap);
            push("objects.storage_class_heap", storage_class_heap);
            push("objects.user_metadata_bytes", user_metadata_bytes);
            push("objects.tags_bytes", tags_bytes);
            push("objects.extents_bytes", extents_bytes);
            push("objects.extents_len", extents_len);
            push("objects.extents_capacity", extents_capacity);
        }

        push("versions.bytes", versions_bytes);
        push("object_changes.bytes", changes_bytes);
        push("meta.records.bytes", meta_record_bytes);
        // `meta.records` holds the key twice as well -- once as the map key
        // and once in the record -- and the value is a formatted restatement of
        // fields the `objects` map already holds structured. The `_len` counters
        // sit beside the `_capacity` ones because the difference is slack, and
        // slack is what this audit exists to show.
        {
            let records = &self.meta_service.records;
            let n = records.len() as u64;
            push(
                "meta.records.struct_inline",
                n.saturating_mul(
                    (std::mem::size_of::<String>() + std::mem::size_of::<MetadataRecord>()) as u64,
                ),
            );
            push("meta.records.record_size", std::mem::size_of::<MetadataRecord>() as u64);
            let mut mapkey_len = 0u64;
            let mut mapkey_capacity = 0u64;
            let mut record_key_capacity = 0u64;
            let mut value_len = 0u64;
            let mut value_capacity = 0u64;
            let mut key_text_duplicated = 0u64;
            for (key, record) in records.iter() {
                mapkey_len = mapkey_len.saturating_add(key.len() as u64);
                mapkey_capacity = mapkey_capacity.saturating_add(key.capacity() as u64);
                record_key_capacity =
                    record_key_capacity.saturating_add(record.key.capacity() as u64);
                value_len = value_len.saturating_add(record.value.len() as u64);
                value_capacity = value_capacity.saturating_add(record.value.capacity() as u64);
                if key.as_str() == record.key.as_str() {
                    key_text_duplicated = key_text_duplicated.saturating_add(1);
                }
            }
            push("meta.records.mapkey_len", mapkey_len);
            push("meta.records.mapkey_capacity", mapkey_capacity);
            push("meta.records.record_key_capacity", record_key_capacity);
            push("meta.records.key_text_duplicated", key_text_duplicated);
            push("meta.records.value_len", value_len);
            push("meta.records.value_capacity", value_capacity);
        }

        push("meta.change_log.bytes", meta.change_log);
        push("meta.receipts.bytes", meta.receipts);
        push("meta.entities.bytes", meta.entities);
        push("meta.export_caches.bytes", meta.export_caches);
        push("payloads.key_bytes", payload_key_bytes);
        let (multipart_metadata_bytes, multipart_payload_bytes) = self
            .multipart_uploads
            .iter()
            .fold((0u64, 0u64), |(meta, payload), (upload_id, upload)| {
                let (one_meta, one_payload) = audit_multipart_bytes(upload);
                (
                    meta.saturating_add(std::mem::size_of::<String>() as u64)
                        .saturating_add(audit_string_heap(upload_id))
                        .saturating_add(one_meta),
                    payload.saturating_add(one_payload),
                )
            });
        push("multipart.metadata_bytes", multipart_metadata_bytes);
        // The part bytes of uploads that have not completed. Held in memory
        // until commit or abort, and previously invisible: the audit reported
        // only how many uploads existed.
        push("multipart.payload_bytes", multipart_payload_bytes);
        let metadata_bytes = objects_bytes
            .saturating_add(versions_bytes)
            .saturating_add(changes_bytes)
            .saturating_add(meta.total())
            .saturating_add(payload_key_bytes)
            .saturating_add(multipart_metadata_bytes);
        push("ACCOUNTED.metadata_bytes", metadata_bytes);

        // ---- the headline: bytes we can actually name ------------------------
        let accounted = payload_cap
            .saturating_add(data_cap)
            .saturating_add(shard_cap)
            .saturating_add(checksum_bytes);
        push("ACCOUNTED.byte_buffers", accounted);
        push(
            "ACCOUNTED.total_bytes",
            accounted
                .saturating_add(metadata_bytes)
                .saturating_add(multipart_payload_bytes),
        );
        push("logical.written", self.logical_written_bytes);
        push("physical.written", self.physical_written_bytes);
        out
    }
}
