impl MatrixObjectStore {
    pub fn list_multipart_uploads_with_metadata_read(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        options: MetadataReadOptions,
    ) -> Result<(Vec<MultipartUpload>, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_by_key_with_metadata_read(
            bucket, None, None, marker, limit, options,
        )
    }

    pub fn list_multipart_uploads_by_key_with_metadata_read(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        options: MetadataReadOptions,
    ) -> Result<(Vec<MultipartUpload>, MetadataRaftReadiness), ObjectError> {
        if let Some(bucket) = bucket {
            validate_bucket(bucket)?;
        }
        if let Some(prefix) = prefix {
            if prefix.len() > 4096 {
                return Err(ObjectError::InvalidArgument(
                    "multipart upload prefix must be at most 4096 bytes".to_string(),
                ));
            }
        }
        if let Some(key_marker) = key_marker {
            validate_key(key_marker)?;
        }
        if let Some(upload_id_marker) = upload_id_marker {
            validate_upload_id(upload_id_marker)?;
        }
        let readiness = self.prepare_metadata_read("object/_multipart/", &options)?;
        let uploads = self.list_multipart_uploads_by_key(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            limit,
        )?;
        Ok((uploads, readiness))
    }

    pub fn list_multipart_uploads_page_with_metadata_read(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_page_by_key_with_metadata_read(
            bucket,
            None,
            None,
            marker,
            limit,
            max_payload_bytes,
            options,
        )
    }

    pub fn list_multipart_uploads_page_by_key_with_metadata_read(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_hierarchy_page_by_key_with_metadata_read(
            bucket,
            prefix,
            None,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
            options,
        )
    }

    pub fn list_multipart_uploads_hierarchy_page_by_key_with_metadata_read(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        if let Some(bucket) = bucket {
            validate_bucket(bucket)?;
        }
        if let Some(prefix) = prefix {
            if prefix.len() > 4096 {
                return Err(ObjectError::InvalidArgument(
                    "multipart upload prefix must be at most 4096 bytes".to_string(),
                ));
            }
        }
        if let Some(delimiter) = delimiter {
            if delimiter.is_empty()
                || delimiter.len() > 1024
                || delimiter.contains('\0')
                || delimiter.contains('\n')
            {
                return Err(ObjectError::InvalidArgument(
                    "multipart upload delimiter is invalid".to_string(),
                ));
            }
        }
        if let Some(key_marker) = key_marker {
            validate_key(key_marker)?;
        }
        if let Some(upload_id_marker) = upload_id_marker {
            validate_upload_id(upload_id_marker)?;
        }
        let readiness = self.prepare_metadata_read("object/_multipart/", &options)?;
        let mut page = self.list_multipart_uploads_hierarchy_page_by_key(
            bucket,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
        )?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        Ok((page, readiness))
    }

}
