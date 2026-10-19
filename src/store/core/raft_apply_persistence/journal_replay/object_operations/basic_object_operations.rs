// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_persistent_journal_basic_object_operation(
        &mut self,
        op: u64,
        mut cursor: &mut SnapshotByteCursor<'_>,
        payload_len: usize,
    ) -> Result<bool, ObjectError> {
        match op {
            1 => {
                let bucket = cursor.read_string()?;
                self.create_bucket(&bucket)?;
            }
            2 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let content_type = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let data_len = cursor.read_u64()? as usize;
                let data = cursor.read_bytes(data_len)?.to_vec();
                let options = if cursor.position() < payload_len {
                    read_journal_put_options(&mut cursor)?
                } else {
                    PutOptions::default()
                };
                let root_replica_count = if cursor.position() < payload_len {
                    read_journal_optional_replica_count(
                        &mut cursor,
                        "persistent journal append root replica count",
                    )?
                } else {
                    None
                };
                if let Some(replica_count) = root_replica_count {
                    self.apply_append_with_root_placement(
                        bucket,
                        key,
                        data,
                        content_type,
                        update_time_ms,
                        options,
                        replica_count,
                    )?;
                } else {
                    self.apply_append(bucket, key, data, content_type, update_time_ms, options)?;
                }
            }
            3 => {
                let bucket = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                self.apply_delete_bucket(&bucket, update_time_ms)?;
            }
            4 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let content_type = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let data_len = cursor.read_u64()? as usize;
                let data = cursor.read_bytes(data_len)?.to_vec();
                let options = if cursor.position() < payload_len {
                    read_journal_put_options(&mut cursor)?
                } else {
                    PutOptions::default()
                };
                let root_replica_count = if cursor.position() < payload_len {
                    read_journal_optional_replica_count(
                        &mut cursor,
                        "persistent journal put root replica count",
                    )?
                } else {
                    None
                };
                if let Some(replica_count) = root_replica_count {
                    self.apply_put_with_root_placement(
                        bucket,
                        key,
                        data,
                        content_type,
                        update_time_ms,
                        options,
                        replica_count,
                    )?;
                } else {
                    self.apply_put_storage_batched(
                        bucket,
                        key,
                        data,
                        content_type,
                        update_time_ms,
                        options,
                        "put_object",
                    )?;
                }
            }
            5 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let if_match_etag = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_string()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal delete if-match presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                let release_root_placement = cursor.read_bool()?;
                if release_root_placement {
                    self.apply_delete_with_root_release(
                        bucket,
                        key,
                        update_time_ms,
                        if_match_etag,
                    )?;
                } else {
                    self.apply_delete(bucket, key, update_time_ms, if_match_etag)?;
                }
            }
            6 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let tags = read_string_map(&mut cursor, "persistent journal object tag count")?;
                self.apply_set_object_tags(bucket, key, tags, update_time_ms)?;
            }
            7 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let legal_hold = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_bool()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal legal-hold presence flag is invalid".to_string(),
                        ))
                    }
                };
                let retention_until_ms = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_u64()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal retention presence flag is invalid".to_string(),
                        ))
                    }
                };
                if let Some(enabled) = legal_hold {
                    self.apply_set_object_legal_hold(
                        bucket.clone(),
                        key.clone(),
                        enabled,
                        update_time_ms,
                    )?;
                }
                if let Some(retention_until_ms) = retention_until_ms {
                    self.apply_set_object_retention(
                        bucket,
                        key,
                        retention_until_ms,
                        update_time_ms,
                    )?;
                }
            }
            8 => {
                let bucket = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let retain_for_ms = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_u64()?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal bucket retention presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                let quota = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(BucketQuota {
                        max_live_bytes: cursor.read_u64()?,
                        max_live_objects: cursor.read_u64()?,
                    }),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal bucket quota presence flag is invalid".to_string(),
                        ))
                    }
                };
                let lifecycle = if cursor.position() < payload_len {
                    match cursor.read_u64()? {
                        0 => None,
                        1 => Some(read_lifecycle_policy(&mut cursor, 11)?),
                        _ => {
                            return Err(ObjectError::Corruption(
                                "persistent journal bucket lifecycle presence flag is invalid"
                                    .to_string(),
                            ))
                        }
                    }
                } else {
                    None
                };
                let cors = if cursor.position() < payload_len {
                    match cursor.read_u64()? {
                        0 => None,
                        1 => Some(read_cors_policy(&mut cursor)?),
                        _ => {
                            return Err(ObjectError::Corruption(
                                "persistent journal bucket cors presence flag is invalid"
                                    .to_string(),
                            ))
                        }
                    }
                } else {
                    None
                };
                if let Some(retain_for_ms) = retain_for_ms {
                    self.apply_configure_bucket_retention(
                        bucket.clone(),
                        retain_for_ms,
                        update_time_ms,
                    )?;
                }
                if let Some(quota) = quota {
                    self.apply_configure_bucket_quota(bucket.clone(), quota, update_time_ms)?;
                }
                if let Some(lifecycle) = lifecycle {
                    self.apply_configure_bucket_lifecycle(
                        bucket.clone(),
                        lifecycle,
                        update_time_ms,
                    )?;
                }
                if let Some(cors) = cors {
                    self.apply_configure_bucket_cors(bucket, cors, update_time_ms)?;
                }
            }
            9 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let storage_class = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let replica_count = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_count("persistent journal transition replica count")?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal transition replica presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                if let Some(replica_count) = replica_count {
                    self.apply_transition_object_with_root_placement(
                        bucket,
                        key,
                        storage_class,
                        update_time_ms,
                        replica_count,
                        None,
                    )?;
                } else {
                    self.apply_transition_object(bucket, key, storage_class, update_time_ms, None)?;
                }
            }
            10 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let restore_until_ms = cursor.read_u64()?;
                let update_time_ms = cursor.read_u64()?;
                self.apply_restore_object(bucket, key, restore_until_ms, update_time_ms)?;
            }
            24 => {
                let bucket = cursor.read_string()?;
                let update_time_ms = cursor.read_u64()?;
                let policy = read_bucket_policy_document(&mut cursor)?;
                self.apply_configure_bucket_policy(bucket, policy, update_time_ms)?;
            }
            11 => {
                let bucket = cursor.read_string()?;
                let key = cursor.read_string()?;
                let version = cursor.read_u64()?;
                let update_time_ms = cursor.read_u64()?;
                let replica_count = match cursor.read_u64()? {
                    0 => None,
                    1 => Some(cursor.read_count("persistent journal restore replica count")?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "persistent journal restore replica presence flag is invalid"
                                .to_string(),
                        ))
                    }
                };
                if let Some(replica_count) = replica_count {
                    self.apply_restore_object_version_with_root_placement(
                        bucket,
                        key,
                        version,
                        update_time_ms,
                        replica_count,
                    )?;
                } else {
                    self.apply_restore_object_version(bucket, key, version, update_time_ms)?;
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
