// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn encoded_catalog_identity_len(node_id: &str, endpoint: &str, failure_domain: &str) -> usize {
        encoded_string_len(node_id) + encoded_string_len(endpoint) + encoded_string_len(failure_domain)
    }

    fn append_objectstore_gateway_registration_journal_record(
        &self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 5
                + Self::encoded_catalog_identity_len(node_id, endpoint, failure_domain)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 25);
        append_string(&mut payload, node_id);
        append_string(&mut payload, endpoint);
        append_string(&mut payload, failure_domain);
        append_u64(&mut payload, capacity_bytes);
        append_u64(&mut payload, used_bytes);
        append_u64(&mut payload, draining as u64);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_objectstore_gateway_heartbeat_journal_record(
        &self,
        node_id: &str,
        capacity_bytes: u64,
        used_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 6
                + encoded_string_len(node_id)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 26);
        append_string(&mut payload, node_id);
        append_u64(&mut payload, capacity_bytes);
        append_u64(&mut payload, used_bytes);
        append_u64(&mut payload, draining as u64);
        append_u64(&mut payload, online as u64);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_objectstore_gateway_offline_journal_record(
        &self,
        node_id: &str,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(node_id)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 27);
        append_string(&mut payload, node_id);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_pageserver_catalog_registration_journal_record(
        &self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 6
                + Self::encoded_catalog_identity_len(node_id, endpoint, failure_domain)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 28);
        append_string(&mut payload, node_id);
        append_string(&mut payload, endpoint);
        append_string(&mut payload, failure_domain);
        append_u64(&mut payload, allocator_shard_index);
        append_u64(&mut payload, allocator_shard_count);
        append_u64(&mut payload, min_capacity_free_bytes);
        append_u64(&mut payload, min_reclaimable_bytes);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_pageserver_catalog_heartbeat_journal_record(
        &self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 7
                + encoded_string_len(node_id)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 29);
        append_string(&mut payload, node_id);
        append_u64(&mut payload, allocator_shard_index);
        append_u64(&mut payload, allocator_shard_count);
        append_u64(&mut payload, min_capacity_free_bytes);
        append_u64(&mut payload, min_reclaimable_bytes);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_u64(&mut payload, online as u64);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_pageserver_catalog_offline_journal_record(
        &self,
        node_id: &str,
        allocator_shard_index: u64,
        allocator_shard_count: u64,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 6
                + encoded_string_len(node_id)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 30);
        append_string(&mut payload, node_id);
        append_u64(&mut payload, allocator_shard_index);
        append_u64(&mut payload, allocator_shard_count);
        append_u64(&mut payload, min_capacity_free_bytes);
        append_u64(&mut payload, min_reclaimable_bytes);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_replicaserver_catalog_registration_journal_record(
        &self,
        node_id: &str,
        endpoint: &str,
        failure_domain: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 5
                + Self::encoded_catalog_identity_len(node_id, endpoint, failure_domain)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 31);
        append_string(&mut payload, node_id);
        append_string(&mut payload, endpoint);
        append_string(&mut payload, failure_domain);
        append_u64(&mut payload, replica_index as u64);
        append_u64(&mut payload, min_capacity_free_bytes);
        append_u64(&mut payload, min_reclaimable_bytes);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_replicaserver_catalog_heartbeat_journal_record(
        &self,
        node_id: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
        online: bool,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 6
                + encoded_string_len(node_id)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 32);
        append_string(&mut payload, node_id);
        append_u64(&mut payload, replica_index as u64);
        append_u64(&mut payload, min_capacity_free_bytes);
        append_u64(&mut payload, min_reclaimable_bytes);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_u64(&mut payload, online as u64);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_replicaserver_catalog_offline_journal_record(
        &self,
        node_id: &str,
        replica_index: usize,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
        catalog_source: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 5
                + encoded_string_len(node_id)
                + encoded_string_len(catalog_source),
        );
        append_u64(&mut payload, 33);
        append_string(&mut payload, node_id);
        append_u64(&mut payload, replica_index as u64);
        append_u64(&mut payload, min_capacity_free_bytes);
        append_u64(&mut payload, min_reclaimable_bytes);
        append_u64(&mut payload, now_ms);
        append_string(&mut payload, catalog_source);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

}
