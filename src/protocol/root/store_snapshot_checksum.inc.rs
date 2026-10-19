impl StoreSnapshot {
    pub(crate) fn checksum_material_hash(&self) -> String {
        let mut material = Vec::with_capacity(self.checksum_material_size_hint());
        append_u64(&mut material, self.options.slab_size);
        append_u64(&mut material, self.options.max_extent_bytes);
        append_u64(&mut material, self.options.page_size);
        append_u64(&mut material, self.options.volume_count);
        append_u64(&mut material, self.options.replica_count as u64);
        append_u64(&mut material, self.options.write_quorum as u64);
        append_u64(
            &mut material,
            self.options.compaction_fragment_threshold as u64,
        );
        append_u64(
            &mut material,
            self.options.metadata_route_cache_entries as u64,
        );
        append_u64(&mut material, self.options.metadata_route_cache_ttl_ms);
        append_u64(&mut material, self.options.page_read_parallelism as u64);
        append_u64(&mut material, self.options.page_write_parallelism as u64);
        append_u64(
            &mut material,
            self.options.page_batch_read_parallelism as u64,
        );
        append_u64(
            &mut material,
            self.options.page_batch_write_parallelism as u64,
        );
        append_u64(
            &mut material,
            self.options.page_batch_discard_parallelism as u64,
        );
        append_u64(&mut material, self.options.page_repair_parallelism as u64);
        append_u64(
            &mut material,
            self.options.page_chunk_fanout_parallelism as u64,
        );
        append_u64(
            &mut material,
            self.options.local_replica_store_parallelism as u64,
        );
        append_u64(
            &mut material,
            self.options.control_plane_mode.snapshot_code(),
        );
        append_u64(
            &mut material,
            self.options.object_manifest_checkpoint_interval,
        );
        append_u64(
            &mut material,
            self.options.object_metadata_checkpoint_interval,
        );
        append_string(&mut material, &self.options.metaserver_catalog_endpoints);
        append_string(&mut material, &self.options.rootserver_catalog_endpoint);
        append_u64(&mut material, self.next_slab_id);
        append_u64(&mut material, self.logical_written_bytes);
        append_u64(&mut material, self.physical_written_bytes);
        append_u64(&mut material, self.last_applied_index);
        append_u64(&mut material, self.last_applied_term);
        append_u64(&mut material, self.applied_raft_receipts.len() as u64);
        for (index, receipt) in &self.applied_raft_receipts {
            append_u64(&mut material, *index);
            append_raft_apply_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.raft_voter_node_ids.len() as u64);
        for voter in &self.raft_voter_node_ids {
            append_string(&mut material, voter);
        }
        append_u64(&mut material, self.raft_healthy_peer_count);
        append_u64(&mut material, self.raft_membership_epoch);
        append_u64(&mut material, self.raft_membership_receipts.len() as u64);
        for (mutation_id, receipt) in &self.raft_membership_receipts {
            append_string(&mut material, mutation_id);
            append_raft_membership_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.chunk_read_cache_max_bytes_per_replica);
        append_u64(
            &mut material,
            self.chunk_read_cache_max_cacheable_read_bytes_per_replica,
        );
        append_u64(&mut material, self.page_write_quorum as u64);
        append_u64(&mut material, self.page_read_quorum as u64);
        append_u64(&mut material, self.page_chunk_endpoints.len() as u64);
        for endpoint in &self.page_chunk_endpoints {
            append_string(&mut material, endpoint);
        }
        append_u64(
            &mut material,
            self.page_disabled_chunk_endpoints.len() as u64,
        );
        for index in &self.page_disabled_chunk_endpoints {
            append_u64(&mut material, *index as u64);
        }
        append_string_set(&mut material, &self.buckets);
        append_string_set(&mut material, &self.bucket_versioning);
        append_u64(&mut material, self.bucket_retention.len() as u64);
        for (bucket, retain_for_ms) in &self.bucket_retention {
            append_string(&mut material, bucket);
            append_u64(&mut material, *retain_for_ms);
        }
        append_u64(&mut material, self.bucket_quotas.len() as u64);
        for (bucket, quota) in &self.bucket_quotas {
            append_string(&mut material, bucket);
            append_u64(&mut material, quota.max_live_bytes);
            append_u64(&mut material, quota.max_live_objects);
        }
        append_u64(&mut material, self.bucket_lifecycle.len() as u64);
        for (bucket, policy) in &self.bucket_lifecycle {
            append_string(&mut material, bucket);
            append_lifecycle_policy(&mut material, policy);
        }
        append_u64(&mut material, self.bucket_policy_documents.len() as u64);
        for (bucket, policy) in &self.bucket_policy_documents {
            append_string(&mut material, bucket);
            append_bucket_policy_document(&mut material, policy);
        }
        append_u64(&mut material, self.bucket_tags.len() as u64);
        for (bucket, tag_set) in &self.bucket_tags {
            append_string(&mut material, bucket);
            append_bucket_tag_set(&mut material, tag_set);
        }
        append_u64(&mut material, self.bucket_cors.len() as u64);
        for (bucket, policy) in &self.bucket_cors {
            append_string(&mut material, bucket);
            append_cors_policy(&mut material, policy);
        }
        append_u64(&mut material, self.bucket_policy_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.bucket_policy_receipts {
            append_string(&mut material, idempotency_key);
            append_bucket_policy_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.bucket_cors_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.bucket_cors_receipts {
            append_string(&mut material, idempotency_key);
            append_bucket_cors_receipt(&mut material, receipt);
        }
        append_string(&mut material, &self.meta_snapshot.checksum);
        append_string(&mut material, &self.root_snapshot.checksum);
        append_u64(&mut material, self.next_upload_sequence);
        append_u64(&mut material, self.next_object_change_sequence);
        for (object_id, metadata) in &self.objects {
            append_object_id(&mut material, object_id);
            append_metadata(&mut material, metadata);
        }
        for (object_id, versions) in &self.versions {
            append_object_id(&mut material, object_id);
            append_u64(&mut material, versions.len() as u64);
            for metadata in versions {
                append_metadata(&mut material, metadata);
            }
        }
        for (slab_id, slab) in &self.slabs {
            append_slab_id(&mut material, slab_id);
            append_slab_info(&mut material, slab);
        }
        for ((slab_id, offset), payload) in &self.payloads {
            append_slab_id(&mut material, slab_id);
            append_u64(&mut material, *offset);
            append_u64(&mut material, payload.len() as u64);
            material.extend_from_slice(payload);
        }
        for (upload_id, upload) in &self.multipart_uploads {
            append_string(&mut material, upload_id);
            append_multipart_upload(&mut material, upload);
        }
        append_u64(&mut material, self.multipart_complete_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.multipart_complete_receipts {
            append_string(&mut material, idempotency_key);
            append_multipart_complete_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.multipart_abort_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.multipart_abort_receipts {
            append_string(&mut material, idempotency_key);
            append_multipart_abort_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.object_changes.len() as u64);
        for change in &self.object_changes {
            append_object_change_record(&mut material, change);
        }
        append_u64(&mut material, self.object_change_cursors.len() as u64);
        for (consumer_id, cursor) in &self.object_change_cursors {
            append_string(&mut material, consumer_id);
            append_object_change_cursor(&mut material, cursor);
        }
        etag(&material)
    }

    fn checksum_material_size_hint(&self) -> usize {
        self.encoded_size_hint(64, 64)
    }
}
