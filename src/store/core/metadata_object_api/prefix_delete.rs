// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn delete_objects_by_prefix(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
    ) -> Result<ObjectPrefixDeleteReport, ObjectError> {
        self.delete_objects_by_prefix_fenced(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            None,
        )
    }

    pub fn delete_objects_by_prefix_fenced(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectPrefixDeleteReport, ObjectError> {
        self.delete_objects_by_prefix_fenced_with_delete_proof(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_route_control_checksum,
            None,
        )
    }

    pub fn delete_objects_by_prefix_fenced_with_delete_proof(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_route_control_checksum: Option<&str>,
        expected_delete_batch_proof_checksum: Option<&str>,
    ) -> Result<ObjectPrefixDeleteReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if max_objects == 0 {
            return Err(ObjectError::InvalidArgument(
                "prefix delete max_objects must be positive".to_string(),
            ));
        }
        let listed = self.list_objects(bucket, prefix, marker, max_objects)?;
        let next_marker = listed
            .last()
            .filter(|_| listed.len() >= max_objects)
            .map(|metadata| metadata.object_id.key.clone());
        let requests: Vec<ObjectDeleteRequest> = listed
            .iter()
            .map(|metadata| ObjectDeleteRequest {
                bucket: metadata.object_id.bucket.to_string(),
                key: metadata.object_id.key.to_string(),
                if_match_etag: Some(metadata.etag().to_string()),
                version: None,
            })
            .collect();
        let batch = self.batch_delete_objects_fenced_with_delete_proof(
            requests,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_route_control_checksum,
            expected_delete_batch_proof_checksum,
        )?;
        Ok(ObjectPrefixDeleteReport {
            prefix: prefix.to_string(),
            marker: marker.map(ToString::to_string),
            next_marker: next_marker.map(|marker| marker.to_string()),
            listed_objects: listed.len() as u64,
            has_more: listed.len() >= max_objects,
            batch,
            read_proof: ObjectReadProof::default(),
        })
    }

    pub fn delete_objects_by_prefix_with_metadata_read(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        read_options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectPrefixDeleteReport, ObjectError> {
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
        }
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        let meta_prefix = if prefix.is_empty() {
            format!("object/{bucket}/")
        } else {
            meta_object_key(&ObjectId::new(bucket, prefix))
        };
        let readiness = self.prepare_metadata_read(&meta_prefix, &read_options)?;
        let proof = object_read_proof_from_readiness(&readiness);
        let mut report = self.delete_objects_by_prefix_fenced(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_route_control_checksum,
        )?;
        report.batch.read_proof = proof.clone();
        report.read_proof = proof;
        Ok(report)
    }

    pub fn delete_objects_by_prefix_with_metadata_read_delete_proof_fenced(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        read_options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
        expected_delete_batch_proof_checksum: Option<&str>,
    ) -> Result<ObjectPrefixDeleteReport, ObjectError> {
        validate_bucket(bucket)?;
        if !prefix.is_empty() {
            validate_key(prefix)?;
        }
        if let Some(marker) = marker {
            validate_key(marker)?;
        }
        let meta_prefix = if prefix.is_empty() {
            format!("object/{bucket}/")
        } else {
            meta_object_key(&ObjectId::new(bucket, prefix))
        };
        let readiness = self.prepare_metadata_read(&meta_prefix, &read_options)?;
        let proof = object_read_proof_from_readiness(&readiness);
        let mut report = self.delete_objects_by_prefix_fenced_with_delete_proof(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_route_control_checksum,
            expected_delete_batch_proof_checksum,
        )?;
        report.batch.read_proof = proof.clone();
        report.read_proof = proof;
        Ok(report)
    }

}
