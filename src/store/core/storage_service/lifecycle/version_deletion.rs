// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn delete_object_version(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::DeleteObjectVersion {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) =
            self.append_delete_object_version_journal_record(bucket, key, version, update_time_ms)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn delete_object_version_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectDeleteReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let request = ObjectDeleteRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            if_match_etag: None,
            version: Some(version),
        };
        let metadata = self.delete_object_version(bucket, key, version)?;
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

    pub fn delete_object_version_with_metadata_read(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        read_options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectDeleteReport, ObjectError> {
        let (_, readiness) =
            self.head_object_version_with_metadata_read(bucket, key, version, read_options)?;
        let mut report = self.delete_object_version_fenced(
            bucket,
            key,
            version,
            expected_route_control_checksum,
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        Ok(report)
    }

}
