// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn list_object_versions_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        options: MetadataReadOptions,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        let object_id = ObjectId::new(bucket, key);
        let readiness = self.prepare_metadata_read(&meta_object_key(&object_id), &options)?;
        let versions = self.list_object_versions(bucket, key)?;
        Ok((versions, readiness))
    }

    pub fn list_object_versions_page_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.list_object_versions_page_with_metadata_read_list_page_proof_fenced(
            bucket,
            key,
            marker,
            limit,
            max_payload_bytes,
            options,
            None,
        )
    }

    pub fn list_object_versions_page_with_metadata_read_list_page_proof_fenced(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
        expected_object_list_page_proof_checksum: Option<&str>,
    ) -> Result<(ObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let object_id = ObjectId::new(bucket, key);
        let readiness = self.prepare_metadata_read(&meta_object_key(&object_id), &options)?;
        let mut page =
            self.list_object_versions_page(bucket, key, marker, limit, max_payload_bytes)?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_version_page_proof(
            "ListObjectVersions",
            &mut page,
            expected_object_list_page_proof_checksum,
        )?;
        Ok((page, readiness))
    }

    pub fn list_bucket_object_versions_page_with_metadata_read(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(BucketObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.list_bucket_object_versions_page_with_metadata_read_list_page_proof_fenced(
            bucket,
            prefix,
            delimiter,
            key_marker,
            version_marker,
            limit,
            max_payload_bytes,
            options,
            None,
        )
    }

    pub fn list_bucket_object_versions_page_with_metadata_read_list_page_proof_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
        expected_object_list_page_proof_checksum: Option<&str>,
    ) -> Result<(BucketObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
        }
        if let Some(key_marker) = key_marker {
            validate_key(key_marker)?;
        }
        let meta_prefix = if prefix.is_empty() {
            format!("object/{bucket}/")
        } else {
            meta_object_key(&ObjectId::new(bucket, prefix))
        };
        let readiness = self.prepare_metadata_read(&meta_prefix, &options)?;
        let mut page = self.list_bucket_object_versions_page(
            bucket,
            prefix,
            delimiter,
            key_marker,
            version_marker,
            limit,
            max_payload_bytes,
        )?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_bucket_object_version_page_proof(
            "ListBucketObjectVersions",
            &mut page,
            expected_object_list_page_proof_checksum,
        )?;
        Ok((page, readiness))
    }
}
