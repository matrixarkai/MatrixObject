// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn delete_object(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.delete_object_with_match(bucket, key, None)
    }

    pub fn delete_object_with_match(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::DeleteObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            update_time_ms,
            if_match_etag: if_match_etag.clone(),
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_delete_object_journal_record(
            bucket,
            key,
            update_time_ms,
            if_match_etag.as_deref(),
            false,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn delete_object_with_match_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectDeleteReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let request = ObjectDeleteRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            if_match_etag: if_match_etag.clone(),
            version: None,
        };
        let metadata = self.delete_object_with_match(bucket, key, if_match_etag)?;
        Ok(ObjectDeleteReport {
            request,
            deleted_bytes: metadata.length,
            metadata,
            read_proof: ObjectReadProof::default(),
            route_control_proof,
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
        })
    }

    pub fn delete_object_with_match_and_metadata_read(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        read_options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectDeleteReport, ObjectError> {
        let (_, readiness) = self.head_object_with_metadata_read(bucket, key, read_options)?;
        let mut report = self.delete_object_with_match_fenced(
            bucket,
            key,
            if_match_etag,
            expected_route_control_checksum,
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        Ok(report)
    }

    pub fn delete_object_with_root_release(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let metadata = self.apply_delete_with_root_release(
            bucket.to_string(),
            key.to_string(),
            update_time_ms,
            if_match_etag.clone(),
        )?;
        if let Err(err) = self.append_delete_object_journal_record(
            bucket,
            key,
            update_time_ms,
            if_match_etag.as_deref(),
            true,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn delete_object_with_root_release_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectDeleteReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let request = ObjectDeleteRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            if_match_etag: if_match_etag.clone(),
            version: None,
        };
        let metadata = self.delete_object_with_root_release(bucket, key, if_match_etag)?;
        Ok(ObjectDeleteReport {
            request,
            deleted_bytes: metadata.length,
            metadata,
            read_proof: ObjectReadProof::default(),
            route_control_proof,
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
        })
    }

    pub fn delete_object_with_root_release_and_metadata_read(
        &mut self,
        bucket: &str,
        key: &str,
        if_match_etag: Option<String>,
        read_options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectDeleteReport, ObjectError> {
        let (_, readiness) = self.head_object_with_metadata_read(bucket, key, read_options)?;
        let mut report = self.delete_object_with_root_release_fenced(
            bucket,
            key,
            if_match_etag,
            expected_route_control_checksum,
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        Ok(report)
    }

}
