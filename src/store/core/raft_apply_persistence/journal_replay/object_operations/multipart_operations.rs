impl MatrixObjectStore {
    fn apply_persistent_journal_multipart_operation(
        &mut self,
        op: u64,
        mut cursor: &mut SnapshotByteCursor<'_>,
    ) -> Result<bool, ObjectError> {
        match op {
            12 => {
                let upload_id = cursor.read_string()?;
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let content_type = cursor.read_string()?;
                let create_time_ms = cursor.read_u64()?;
                let options = read_journal_put_options(&mut cursor)?;
                self.apply_create_multipart_upload(
                    upload_id,
                    bucket,
                    key,
                    content_type,
                    options,
                    create_time_ms,
                )?;
            }
            13 => {
                let upload_id = cursor.read_string()?;
                let part_number_u64 = cursor.read_u64()?;
                if part_number_u64 == 0 || part_number_u64 > u32::MAX as u64 {
                    return Err(ObjectError::Corruption(
                        "persistent journal multipart part number is invalid".to_string(),
                    ));
                }
                let update_time_ms = cursor.read_u64()?;
                let data_len = cursor.read_u64()? as usize;
                let data = cursor.read_bytes(data_len)?.to_vec();
                self.apply_upload_part(upload_id, part_number_u64 as u32, data, update_time_ms)?;
            }
            14 => {
                let upload_id = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let expected_size = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_u64()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal multipart size presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                let expected_etag = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_string()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal multipart etag presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                let expected_checksum = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_string()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal multipart checksum presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                let replica_count = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_count("persistent journal multipart replica count")?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal multipart replica presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                if let Some(replica_count) = replica_count {
                    self.apply_complete_multipart_upload_with_root_placement(
                        upload_id,
                        expected_size,
                        expected_etag,
                        expected_checksum,
                        replica_count,
                        update_time_ms,
                    )?;
                } else {
                    self.apply_complete_multipart_upload(
                        upload_id,
                        expected_size,
                        expected_etag,
                        expected_checksum,
                        update_time_ms,
                    )?;
                }
            }
            15 => {
                let upload_id = cursor.read_string()?;
                let _update_time_ms = cursor.read_u64()?;
                self.apply_abort_multipart_upload(upload_id)?;
            }
            16 => {
                let bucket = cursor.read_string()?;
                let enabled = cursor.read_bool()?;
                self.apply_command(ObjectCommand::SetBucketVersioning { bucket, enabled })?;
            }
            17 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let version = cursor.read_u64()?;
                let update_time_ms = cursor.read_u64()?;
                self.apply_delete_object_version(bucket, key, version, update_time_ms)?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
