impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_multipart_uploads_with_read_proof_linearizable(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<MultipartUpload>>, ObjectError> {
        let (uploads, readiness) =
            self.list_multipart_uploads_linearizable(bucket, marker, limit, now_ms)?;
        Ok(proven_metadata_read(uploads, &readiness))
    }

    pub fn list_multipart_uploads_by_key_with_read_proof_linearizable(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<Vec<MultipartUpload>>, ObjectError> {
        let (uploads, readiness) = self.list_multipart_uploads_by_key_linearizable(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            limit,
            now_ms,
        )?;
        Ok(proven_metadata_read(uploads, &readiness))
    }

    pub fn list_multipart_uploads_page_with_read_proof_linearizable(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMultipartUploadPage, ObjectError> {
        let (page, readiness) = self.list_multipart_uploads_page_linearizable(
            bucket,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(ProvenMultipartUploadPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_multipart_uploads_page_by_key_with_read_proof_linearizable(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMultipartUploadPage, ObjectError> {
        self.list_multipart_uploads_hierarchy_page_by_key_with_read_proof_linearizable(
            bucket,
            prefix,
            None,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
            now_ms,
        )
    }

    pub fn list_multipart_uploads_hierarchy_page_by_key_with_read_proof_linearizable(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMultipartUploadPage, ObjectError> {
        let (page, readiness) = self.list_multipart_uploads_hierarchy_page_by_key_linearizable(
            bucket,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(ProvenMultipartUploadPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }
}
