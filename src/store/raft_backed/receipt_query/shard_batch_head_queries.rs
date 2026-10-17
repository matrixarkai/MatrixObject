impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn head_objects_batch_from_meta_shard(
        &self,
        objects: Vec<ObjectId>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ObjectMetadataBatchRead, MetadataRaftReadiness), ObjectError> {
        self.store.head_objects_batch_with_metadata_read(
            objects,
            max_items,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn head_objects_batch_from_meta_shard_route_fenced(
        &self,
        objects: Vec<ObjectId>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_shard_topology_checksum: &str,
        now_ms: u64,
    ) -> Result<(ObjectMetadataBatchRead, MetadataRaftReadiness), ObjectError> {
        self.store
            .head_objects_batch_with_metadata_read_route_fenced(
                objects,
                max_items,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
                Some(expected_shard_epoch),
                Some(expected_shard_topology_checksum),
            )
    }

    pub fn head_objects_batch_with_read_proof_from_meta_shard(
        &self,
        objects: Vec<ObjectId>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectMetadataBatchRead, ObjectError> {
        let (batch, readiness) = self.head_objects_batch_from_meta_shard(
            objects,
            max_items,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenObjectMetadataBatchRead {
            batch,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_objects_batch_with_read_proof_from_meta_shard_fenced(
        &self,
        objects: Vec<ObjectId>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_metadata_batch_read_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenObjectMetadataBatchRead, ObjectError> {
        let expected = expected_metadata_batch_read_proof_checksum.as_ref();
        let mut proven = self.head_objects_batch_with_read_proof_from_meta_shard(
            objects,
            max_items,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        check_expected_metadata_batch_read_proof_checksum(
            "HeadObjectsBatch",
            &proven.batch.metadata_batch_read_proof_checksum,
            Some(expected),
        )?;
        proven.batch.expected_metadata_batch_read_proof_checksum = expected.to_string();
        Ok(proven)
    }

    pub fn head_objects_conditional_batch_from_meta_shard(
        &self,
        requests: Vec<ConditionalObjectMetadataRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ConditionalObjectMetadataBatchRead, MetadataRaftReadiness), ObjectError> {
        self.store
            .head_objects_conditional_batch_with_metadata_read(
                requests,
                max_items,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
    }

    pub fn head_objects_conditional_batch_with_read_proof_from_meta_shard(
        &self,
        requests: Vec<ConditionalObjectMetadataRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectMetadataBatchRead, ObjectError> {
        let (batch, readiness) = self.head_objects_conditional_batch_from_meta_shard(
            requests,
            max_items,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenConditionalObjectMetadataBatchRead {
            batch,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_objects_conditional_batch_with_read_proof_from_meta_shard_fenced(
        &self,
        requests: Vec<ConditionalObjectMetadataRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_metadata_batch_read_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenConditionalObjectMetadataBatchRead, ObjectError> {
        let expected = expected_metadata_batch_read_proof_checksum.as_ref();
        let mut proven = self.head_objects_conditional_batch_with_read_proof_from_meta_shard(
            requests,
            max_items,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        check_expected_metadata_batch_read_proof_checksum(
            "HeadObjectsConditionalBatch",
            &proven.batch.metadata_batch_read_proof_checksum,
            Some(expected),
        )?;
        proven.batch.expected_metadata_batch_read_proof_checksum = expected.to_string();
        Ok(proven)
    }
}
