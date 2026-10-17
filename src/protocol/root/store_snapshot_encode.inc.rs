impl StoreSnapshot {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        let mut out = Vec::with_capacity(self.encoded_size_hint(
            self.meta_snapshot.encoded_size_hint(),
            self.root_snapshot.encoded_size_hint(),
        ));
        append_string(&mut out, "MOSRSSTORE27");
        append_u64(&mut out, self.options.slab_size);
        append_u64(&mut out, self.options.max_extent_bytes);
        append_u64(&mut out, self.options.chunk_size);
        append_u64(&mut out, self.options.volume_count);
        append_u64(&mut out, self.options.replica_count as u64);
        append_u64(&mut out, self.options.write_quorum as u64);
        append_u64(&mut out, self.options.compaction_fragment_threshold as u64);
        append_u64(&mut out, self.options.metadata_route_cache_entries as u64);
        append_u64(&mut out, self.options.metadata_route_cache_ttl_ms);
        append_u64(&mut out, self.options.page_read_parallelism as u64);
        append_u64(&mut out, self.options.page_write_parallelism as u64);
        append_u64(&mut out, self.options.page_batch_read_parallelism as u64);
        append_u64(&mut out, self.options.page_batch_write_parallelism as u64);
        append_u64(
            &mut out,
            self.options.page_batch_discard_parallelism as u64,
        );
        append_u64(&mut out, self.options.page_repair_parallelism as u64);
        append_u64(&mut out, self.options.page_chunk_fanout_parallelism as u64);
        append_u64(
            &mut out,
            self.options.local_replica_store_parallelism as u64,
        );
        append_u64(&mut out, self.options.control_plane_mode.snapshot_code());
        append_u64(&mut out, self.options.object_manifest_checkpoint_interval);
        append_u64(&mut out, self.options.object_metadata_checkpoint_interval);
        append_string(&mut out, &self.options.metaserver_catalog_endpoints);
        append_string(&mut out, &self.options.rootserver_catalog_endpoint);
        append_u64(&mut out, self.next_slab_id);
        append_u64(&mut out, self.next_upload_sequence);
        append_u64(&mut out, self.next_object_change_sequence);
        append_u64(&mut out, self.logical_written_bytes);
        append_u64(&mut out, self.physical_written_bytes);
        append_u64(&mut out, self.last_applied_index);
        append_u64(&mut out, self.last_applied_term);
        append_string_set(&mut out, &self.buckets);
        append_string_set(&mut out, &self.bucket_versioning);
        append_u64(&mut out, self.bucket_retention.len() as u64);
        for (bucket, retain_for_ms) in &self.bucket_retention {
            append_string(&mut out, bucket);
            append_u64(&mut out, *retain_for_ms);
        }
        append_u64(&mut out, self.bucket_quotas.len() as u64);
        for (bucket, quota) in &self.bucket_quotas {
            append_string(&mut out, bucket);
            append_u64(&mut out, quota.max_live_bytes);
            append_u64(&mut out, quota.max_live_objects);
        }
        append_u64(&mut out, self.bucket_lifecycle.len() as u64);
        for (bucket, policy) in &self.bucket_lifecycle {
            append_string(&mut out, bucket);
            append_lifecycle_policy(&mut out, policy);
        }
        append_u64(&mut out, self.bucket_policy_documents.len() as u64);
        for (bucket, policy) in &self.bucket_policy_documents {
            append_string(&mut out, bucket);
            append_bucket_policy_document(&mut out, policy);
        }
        append_u64(&mut out, self.bucket_tags.len() as u64);
        for (bucket, tag_set) in &self.bucket_tags {
            append_string(&mut out, bucket);
            append_bucket_tag_set(&mut out, tag_set);
        }
        append_u64(&mut out, self.bucket_cors.len() as u64);
        for (bucket, policy) in &self.bucket_cors {
            append_string(&mut out, bucket);
            append_cors_policy(&mut out, policy);
        }
        append_u64(&mut out, self.bucket_policy_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.bucket_policy_receipts {
            append_string(&mut out, idempotency_key);
            append_bucket_policy_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.bucket_cors_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.bucket_cors_receipts {
            append_string(&mut out, idempotency_key);
            append_bucket_cors_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.objects.len() as u64);
        for (object_id, metadata) in &self.objects {
            append_object_id(&mut out, object_id);
            append_metadata(&mut out, metadata);
        }
        append_u64(&mut out, self.versions.len() as u64);
        for (object_id, versions) in &self.versions {
            append_object_id(&mut out, object_id);
            append_u64(&mut out, versions.len() as u64);
            for metadata in versions {
                append_metadata(&mut out, metadata);
            }
        }
        append_u64(&mut out, self.multipart_uploads.len() as u64);
        for (upload_id, upload) in &self.multipart_uploads {
            append_string(&mut out, upload_id);
            append_multipart_upload(&mut out, upload);
        }
        append_u64(&mut out, self.multipart_complete_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.multipart_complete_receipts {
            append_string(&mut out, idempotency_key);
            append_multipart_complete_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.multipart_abort_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.multipart_abort_receipts {
            append_string(&mut out, idempotency_key);
            append_multipart_abort_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.object_changes.len() as u64);
        for change in &self.object_changes {
            append_object_change_record(&mut out, change);
        }
        append_u64(&mut out, self.object_change_cursors.len() as u64);
        for (consumer_id, change_cursor) in &self.object_change_cursors {
            append_string(&mut out, consumer_id);
            append_object_change_cursor(&mut out, change_cursor);
        }
        append_u64(&mut out, self.slabs.len() as u64);
        for (slab_id, slab) in &self.slabs {
            append_slab_id(&mut out, slab_id);
            append_slab_info(&mut out, slab);
        }
        append_u64(&mut out, self.payloads.len() as u64);
        for ((slab_id, offset), payload) in &self.payloads {
            append_slab_id(&mut out, slab_id);
            append_u64(&mut out, *offset);
            append_u64(&mut out, payload.len() as u64);
            out.extend_from_slice(payload);
        }
        let meta_length_offset = out.len();
        append_u64(&mut out, 0);
        let meta_start = out.len();
        self.meta_snapshot.append_to_bytes(&mut out);
        let meta_len = (out.len() - meta_start) as u64;
        out[meta_length_offset..meta_length_offset + std::mem::size_of::<u64>()]
            .copy_from_slice(&meta_len.to_le_bytes());

        let root_length_offset = out.len();
        append_u64(&mut out, 0);
        let root_start = out.len();
        self.root_snapshot.append_to_bytes(&mut out);
        let root_len = (out.len() - root_start) as u64;
        out[root_length_offset..root_length_offset + std::mem::size_of::<u64>()]
            .copy_from_slice(&root_len.to_le_bytes());
        append_u64(&mut out, self.applied_raft_receipts.len() as u64);
        for (index, receipt) in &self.applied_raft_receipts {
            append_u64(&mut out, *index);
            append_raft_apply_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.raft_voter_node_ids.len() as u64);
        for voter in &self.raft_voter_node_ids {
            append_string(&mut out, voter);
        }
        append_u64(&mut out, self.raft_healthy_peer_count);
        append_u64(&mut out, self.raft_membership_epoch);
        append_u64(&mut out, self.raft_membership_receipts.len() as u64);
        for (mutation_id, receipt) in &self.raft_membership_receipts {
            append_string(&mut out, mutation_id);
            append_raft_membership_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.chunk_read_cache_max_bytes_per_replica);
        append_u64(
            &mut out,
            self.chunk_read_cache_max_cacheable_read_bytes_per_replica,
        );
        append_u64(&mut out, self.page_write_quorum as u64);
        append_u64(&mut out, self.page_read_quorum as u64);
        append_u64(&mut out, self.page_chunk_endpoints.len() as u64);
        for endpoint in &self.page_chunk_endpoints {
            append_string(&mut out, endpoint);
        }
        append_u64(&mut out, self.page_disabled_chunk_endpoints.len() as u64);
        for index in &self.page_disabled_chunk_endpoints {
            append_u64(&mut out, *index as u64);
        }
        append_string(&mut out, &self.snapshot_checksum);
        let checksum_end = out.len();
        let envelope_checksum = etag(&out[..checksum_end]);
        append_string(&mut out, &envelope_checksum);
        Ok(out)
    }

    fn encoded_size_hint(&self, meta_snapshot_bytes: usize, root_snapshot_bytes: usize) -> usize {
        const FIXED_HEADER_BYTES: usize = 512;
        const U64: usize = std::mem::size_of::<u64>();
        fn strings_size<'a, I>(values: I) -> usize
        where
            I: IntoIterator<Item = &'a String>,
        {
            values
                .into_iter()
                .fold(0usize, |total, value| total.saturating_add(8 + value.len()))
        }
        fn string_map_size(values: &BTreeMap<String, String>) -> usize {
            U64.saturating_add(values.iter().fold(0usize, |total, (key, value)| {
                total
                    .saturating_add(encoded_string_len(key))
                    .saturating_add(encoded_string_len(value))
            }))
        }
        fn object_id_size(object_id: &ObjectId) -> usize {
            encoded_string_len(&object_id.bucket).saturating_add(encoded_string_len(&object_id.key))
        }
        fn metadata_size(metadata: &ObjectMetadata) -> usize {
            object_id_size(&metadata.object_id)
                .saturating_add(U64 * 9)
                .saturating_add(encoded_string_len(&metadata.etag))
                .saturating_add(encoded_string_len(&metadata.content_type))
                .saturating_add(encoded_string_len(&metadata.storage_class))
                .saturating_add(string_map_size(&metadata.user_metadata))
                .saturating_add(string_map_size(&metadata.tags))
                .saturating_add(metadata.extents.len().saturating_mul(U64 * 4))
        }
        fn slab_size(slab: &SlabInfo) -> usize {
            U64 * 7 + slab
                .stale_ranges
                .len()
                .saturating_add(slab.discarded_ranges.len())
                .saturating_mul(U64 * 2)
        }
        fn string_list_size(values: &[String]) -> usize {
            U64.saturating_add(strings_size(values.iter()))
        }
        fn optional_string_size(value: Option<&String>) -> usize {
            encoded_string_len(value.map(String::as_str).unwrap_or(""))
        }
        fn put_options_size(options: &PutOptions) -> usize {
            U64.saturating_mul(2)
                .saturating_add(optional_string_size(options.if_match_etag.as_ref()))
                .saturating_add(optional_string_size(options.expected_checksum.as_ref()))
                .saturating_add(optional_string_size(options.lease_owner.as_ref()))
                .saturating_add(string_map_size(&options.user_metadata))
                .saturating_add(string_map_size(&options.tags))
                .saturating_add(optional_string_size(options.storage_class.as_ref()))
                .saturating_add(string_list_size(&options.if_match_etags))
        }
        fn lifecycle_policy_size(policy: &BucketLifecyclePolicy) -> usize {
            U64.saturating_mul(6)
                .saturating_add(optional_string_size(policy.transition_storage_class.as_ref()))
                .saturating_add(optional_string_size(policy.filter_tag_key.as_ref()))
                .saturating_add(optional_string_size(policy.filter_tag_value.as_ref()))
                .saturating_add(optional_string_size(policy.filter_prefix.as_ref()))
        }
        fn cors_policy_size(policy: &BucketCorsPolicy) -> usize {
            string_list_size(&policy.allowed_origins)
                .saturating_add(string_list_size(&policy.allowed_methods))
                .saturating_add(string_list_size(&policy.allowed_headers))
                .saturating_add(string_list_size(&policy.expose_headers))
                .saturating_add(U64)
        }
        fn bucket_policy_receipt_size(receipt: &BucketPolicyMutationReceipt) -> usize {
            encoded_string_len(&receipt.idempotency_key)
                .saturating_add(encoded_string_len(&receipt.method))
                .saturating_add(encoded_string_len(&receipt.bucket))
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(metadata_size(&receipt.metadata))
                .saturating_add(U64 * 2)
        }
        fn bucket_cors_receipt_size(receipt: &BucketCorsMutationReceipt) -> usize {
            encoded_string_len(&receipt.idempotency_key)
                .saturating_add(encoded_string_len(&receipt.bucket))
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(metadata_size(&receipt.report.metadata))
                .saturating_add(cors_policy_size(&receipt.report.policy))
                .saturating_add(U64 * 2)
        }
        fn multipart_upload_size(upload: &MultipartUpload) -> usize {
            let part_bytes = upload.parts.iter().fold(0usize, |total, (part_number, part)| {
                let payload_bytes = upload
                    .part_payloads
                    .get(part_number)
                    .map(Vec::len)
                    .unwrap_or(0);
                total
                    .saturating_add(U64 * 5)
                    .saturating_add(encoded_string_len(&part.etag))
                    .saturating_add(optional_string_size(part.idempotency_key.as_ref()))
                    .saturating_add(payload_bytes)
            });
            encoded_string_len(&upload.upload_id)
                .saturating_add(encoded_string_len(&upload.bucket))
                .saturating_add(encoded_string_len(&upload.key))
                .saturating_add(encoded_string_len(&upload.content_type))
                .saturating_add(U64)
                .saturating_add(optional_string_size(upload.create_idempotency_key.as_ref()))
                .saturating_add(put_options_size(&upload.options))
                .saturating_add(U64 * 5)
                .saturating_add(part_bytes)
        }
        fn multipart_complete_receipt_size(receipt: &MultipartCompleteReceipt) -> usize {
            encoded_string_len(&receipt.idempotency_key)
                .saturating_add(encoded_string_len(&receipt.upload_id))
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(metadata_size(&receipt.metadata))
                .saturating_add(U64 * 2)
        }
        fn multipart_abort_receipt_size(receipt: &MultipartAbortReceipt) -> usize {
            encoded_string_len(&receipt.idempotency_key)
                .saturating_add(encoded_string_len(&receipt.upload_id))
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(multipart_upload_size(&receipt.upload))
                .saturating_add(U64 * 2)
        }
        fn object_change_record_size(change: &ObjectChangeRecord) -> usize {
            U64.saturating_mul(6)
                .saturating_add(encoded_string_len(&change.operation))
                .saturating_add(object_id_size(&change.object_id))
                .saturating_add(encoded_string_len(&change.etag))
        }
        fn object_change_cursor_size(cursor: &ObjectChangeCursor) -> usize {
            encoded_string_len(&cursor.consumer_id).saturating_add(U64 * 2)
        }

        let payload_bytes = self
            .payloads
            .values()
            .fold(0usize, |total, payload| total.saturating_add(payload.len()));
        let object_metadata_bytes = self.objects.iter().fold(0usize, |total, (object_id, metadata)| {
            total
                .saturating_add(object_id_size(object_id))
                .saturating_add(metadata_size(metadata))
        });
        let version_metadata_bytes = self.versions.iter().fold(0usize, |total, (object_id, versions)| {
            total
                .saturating_add(object_id_size(object_id))
                .saturating_add(U64)
                .saturating_add(versions.iter().fold(0usize, |version_total, metadata| {
                    version_total.saturating_add(metadata_size(metadata))
                }))
        });
        let slab_bytes = self.slabs.values().fold(0usize, |total, slab| {
            total.saturating_add(U64 * 2).saturating_add(slab_size(slab))
        });
        let applied_receipt_bytes = self
            .applied_raft_receipts
            .values()
            .fold(0usize, |total, receipt| {
                total
                    .saturating_add(U64 * 3)
                    .saturating_add(encoded_string_len(&receipt.command_checksum))
                    .saturating_add(metadata_size(&receipt.result))
            });
        let multipart_upload_bytes = self
            .multipart_uploads
            .values()
            .fold(0usize, |total, upload| total.saturating_add(multipart_upload_size(upload)));
        let multipart_complete_receipt_bytes =
            self.multipart_complete_receipts
                .values()
                .fold(0usize, |total, receipt| {
                    total.saturating_add(multipart_complete_receipt_size(receipt))
                });
        let multipart_abort_receipt_bytes =
            self.multipart_abort_receipts
                .values()
                .fold(0usize, |total, receipt| {
                    total.saturating_add(multipart_abort_receipt_size(receipt))
                });
        let bucket_lifecycle_bytes = self
            .bucket_lifecycle
            .values()
            .fold(0usize, |total, policy| total.saturating_add(lifecycle_policy_size(policy)));
        let bucket_policy_document_bytes =
            self.bucket_policy_documents
                .values()
                .fold(0usize, |total, policy| {
                    total.saturating_add(encoded_string_len(&policy.document))
                });
        let bucket_tag_bytes = self
            .bucket_tags
            .values()
            .fold(0usize, |total, tag_set| total.saturating_add(string_map_size(&tag_set.tags)));
        let bucket_cors_bytes = self
            .bucket_cors
            .values()
            .fold(0usize, |total, policy| total.saturating_add(cors_policy_size(policy)));
        let bucket_policy_receipt_bytes =
            self.bucket_policy_receipts
                .values()
                .fold(0usize, |total, receipt| {
                    total.saturating_add(bucket_policy_receipt_size(receipt))
                });
        let bucket_cors_receipt_bytes =
            self.bucket_cors_receipts
                .values()
                .fold(0usize, |total, receipt| {
                    total.saturating_add(bucket_cors_receipt_size(receipt))
                });
        let object_change_bytes = self
            .object_changes
            .iter()
            .fold(0usize, |total, change| total.saturating_add(object_change_record_size(change)));
        let object_change_cursor_bytes =
            self.object_change_cursors
                .values()
                .fold(0usize, |total, cursor| {
                    total.saturating_add(object_change_cursor_size(cursor))
                });
        let metadata_bytes = strings_size(self.buckets.iter())
            .saturating_add(strings_size(self.bucket_versioning.iter()))
            .saturating_add(strings_size(self.bucket_retention.keys()))
            .saturating_add(strings_size(self.bucket_quotas.keys()))
            .saturating_add(strings_size(self.bucket_lifecycle.keys()))
            .saturating_add(strings_size(self.bucket_policy_documents.keys()))
            .saturating_add(strings_size(self.bucket_tags.keys()))
            .saturating_add(strings_size(self.bucket_cors.keys()))
            .saturating_add(strings_size(self.bucket_policy_receipts.keys()))
            .saturating_add(strings_size(self.bucket_cors_receipts.keys()))
            .saturating_add(strings_size(self.multipart_uploads.keys()))
            .saturating_add(strings_size(self.multipart_complete_receipts.keys()))
            .saturating_add(strings_size(self.multipart_abort_receipts.keys()))
            .saturating_add(strings_size(self.object_change_cursors.keys()))
            .saturating_add(strings_size(self.raft_voter_node_ids.iter()))
            .saturating_add(strings_size(self.raft_membership_receipts.keys()))
            .saturating_add(strings_size(self.page_chunk_endpoints.iter()));
        FIXED_HEADER_BYTES
            .saturating_add(meta_snapshot_bytes)
            .saturating_add(root_snapshot_bytes)
            .saturating_add(payload_bytes)
            .saturating_add(metadata_bytes)
            .saturating_add(self.bucket_retention.len().saturating_mul(16))
            .saturating_add(self.bucket_quotas.len().saturating_mul(32))
            .saturating_add(bucket_lifecycle_bytes)
            .saturating_add(bucket_policy_document_bytes)
            .saturating_add(bucket_tag_bytes)
            .saturating_add(bucket_cors_bytes)
            .saturating_add(bucket_policy_receipt_bytes)
            .saturating_add(bucket_cors_receipt_bytes)
            .saturating_add(object_metadata_bytes)
            .saturating_add(version_metadata_bytes)
            .saturating_add(multipart_upload_bytes)
            .saturating_add(multipart_complete_receipt_bytes)
            .saturating_add(multipart_abort_receipt_bytes)
            .saturating_add(object_change_bytes)
            .saturating_add(object_change_cursor_bytes)
            .saturating_add(slab_bytes)
            .saturating_add(applied_receipt_bytes)
            .saturating_add(self.raft_membership_receipts.len().saturating_mul(128))
            .saturating_add(self.page_disabled_chunk_endpoints.len().saturating_mul(8))
    }
}
