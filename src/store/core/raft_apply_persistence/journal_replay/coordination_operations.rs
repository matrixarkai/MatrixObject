// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn apply_persistent_journal_coordination_operation(
        &mut self,
        op: u64,
        cursor: &mut SnapshotByteCursor<'_>,
    ) -> Result<bool, ObjectError> {
        match op {
            40 => {
                let session_id = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let endpoint = cursor.read_string()?;
                let ttl_ms = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                self.register_meta_coordinator_session(
                    &session_id,
                    &owner,
                    &endpoint,
                    ttl_ms,
                    now_ms,
                )?;
            }
            41 => {
                let session_id = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let epoch = cursor.read_u64()?;
                let ttl_ms = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                self.renew_meta_coordinator_session(&session_id, &owner, epoch, ttl_ms, now_ms)?;
            }
            42 => {
                let session_id = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let epoch = cursor.read_u64()?;
                self.release_meta_coordinator_session(&session_id, &owner, epoch)?;
            }
            43 => {
                let now_ms = cursor.read_u64()?;
                let limit = cursor.read_u64()? as usize;
                let include_fenced = cursor.read_u64()? != 0;
                self.expire_meta_coordinator_sessions_including_fenced(
                    now_ms,
                    limit,
                    include_fenced,
                )?;
            }
            44 => {
                let namespace = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let ttl_ms = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                self.acquire_metadata_lease(&namespace, &owner, ttl_ms, now_ms)?;
            }
            45 => {
                let namespace = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let epoch = cursor.read_u64()?;
                let ttl_ms = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                self.renew_metadata_lease(&namespace, &owner, epoch, ttl_ms, now_ms)?;
            }
            46 => {
                let namespace = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let epoch = cursor.read_u64()?;
                self.release_metadata_lease(&namespace, &owner, epoch)?;
            }
            47 => {
                let prefix = cursor.read_string()?;
                let now_ms = cursor.read_u64()?;
                let limit = cursor.read_u64()? as usize;
                self.expire_metadata_leases(&prefix, now_ms, limit)?;
            }
            48 => {
                let transaction_id = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let ttl_ms = cursor.read_u64()?;
                let mut mutations = Vec::new();
                for _ in 0..cursor.read_count("persistent journal transaction mutation count")? {
                    mutations.push(MetadataMutation {
                        key: cursor.read_string()?.into(),
                        value: cursor.read_string()?,
                        expected_version: cursor.read_u64()?,
                        delete: cursor.read_bool()?,
                    });
                }
                let now_ms = cursor.read_u64()?;
                self.begin_metadata_transaction(
                    &transaction_id,
                    &owner,
                    ttl_ms,
                    mutations,
                    now_ms,
                )?;
            }
            49 => {
                let transaction_id = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let ttl_ms = cursor.read_u64()?;
                let now_ms = cursor.read_u64()?;
                self.renew_metadata_transaction(&transaction_id, &owner, ttl_ms, now_ms)?;
            }
            50 => {
                let transaction_id = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let now_ms = cursor.read_u64()?;
                self.commit_metadata_transaction(&transaction_id, &owner, now_ms)?;
            }
            51 => {
                let transaction_id = cursor.read_string()?;
                let owner = cursor.read_string()?;
                let now_ms = cursor.read_u64()?;
                self.abort_metadata_transaction(&transaction_id, &owner, now_ms)?;
            }
            52 => {
                let now_ms = cursor.read_u64()?;
                self.expire_metadata_transactions(now_ms)?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
