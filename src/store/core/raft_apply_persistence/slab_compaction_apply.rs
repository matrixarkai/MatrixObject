impl MatrixObjectStore {
    fn collect_chunk_compaction_live_objects(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<Vec<ObjectId>, ObjectError> {
        if !self.chunks.contains_key(chunk_id) {
            return Err(ObjectError::NotFound(format!("chunk {:?}", chunk_id)));
        }
        let mut live_objects = Vec::new();
        for metadata in self.objects.values() {
            if metadata.deleted {
                continue;
            }
            if metadata
                .extents
                .iter()
                .any(|extent| &extent.chunk_id == chunk_id)
            {
                live_objects.push(metadata.object_id.clone());
            }
        }
        Ok(live_objects)
    }

    pub fn compact_chunk_at(
        &mut self,
        chunk_id: &ChunkId,
        update_time_ms: u64,
    ) -> Result<(), ObjectError> {
        let live_objects = self.collect_chunk_compaction_live_objects(chunk_id)?;
        for object_id in live_objects {
            let object = self.get_object(&object_id.bucket, &object_id.key)?;
            self.apply_put_storage_batched(
                object_id.bucket.to_string(),
                object_id.key.to_string(),
                object.data,
                object.metadata.content_type.to_string(),
                update_time_ms,
                PutOptions::default(),
                "compact_segment_put",
            )?;
        }
        if let Some(chunk) = self.chunks.get_mut(chunk_id) {
            chunk.stale_ranges.push(DataRange {
                offset: 0,
                length: chunk.next_offset,
            });
            chunk.stale_ranges = merge_ranges(std::mem::take(&mut chunk.stale_ranges));
            chunk.stale_bytes = chunk.next_offset;
            chunk.live_bytes = 0;
        }
        Ok(())
    }

    pub fn compact_chunk(&mut self, chunk_id: &ChunkId) -> Result<(), ObjectError> {
        self.compact_chunk_at(chunk_id, now_millis())
    }

    pub fn compact_all_at(
        &mut self,
        min_stale_bytes: u64,
        update_time_ms: u64,
    ) -> Result<u64, ObjectError> {
        let mut candidates = self
            .chunks
            .iter()
            .filter_map(|(chunk_id, chunk)| {
                if chunk.stale_bytes >= min_stale_bytes && chunk.next_offset > 0 {
                    Some(chunk_id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|chunk_id| (chunk_id.volume_id, chunk_id.id));
        let mut compacted = 0u64;
        for chunk_id in candidates {
            self.compact_chunk_at(&chunk_id, update_time_ms)?;
            compacted = compacted.saturating_add(1);
        }
        Ok(compacted)
    }

    pub fn compact_all(&mut self, min_stale_bytes: u64) -> Result<u64, ObjectError> {
        self.compact_all_at(min_stale_bytes, now_millis())
    }

    pub fn plan_full_compaction(&self, min_stale_bytes: u64) -> FullCompactionPlan {
        self.plan_full_compaction_fenced(min_stale_bytes, None, "")
            .expect("unfenced full compaction planning cannot fail")
    }

    pub fn plan_full_compaction_fenced(
        &self,
        min_stale_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<FullCompactionPlan, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let gc_report = self.gc_report_with_discard_policy_proof_fenced(
            min_stale_bytes,
            0,
            expected_route_control_checksum,
            expected_object_gc_report_proof_checksum.as_ref(),
        )?;
        let mut candidate_chunk_ids = Vec::new();
        let mut candidate_stale_bytes = 0u64;
        let mut candidate_live_bytes = 0u64;
        let mut candidate_logical_bytes = 0u64;
        for (chunk_id, chunk) in &self.chunks {
            if chunk.stale_bytes >= min_stale_bytes && chunk.next_offset > 0 {
                candidate_chunk_ids.push(chunk_id.clone());
                candidate_stale_bytes = saturating_add(candidate_stale_bytes, chunk.stale_bytes);
                candidate_live_bytes = saturating_add(candidate_live_bytes, chunk.live_bytes);
                candidate_logical_bytes =
                    saturating_add(candidate_logical_bytes, chunk.logical_bytes);
            }
        }
        candidate_chunk_ids.sort_by_key(|chunk_id| (chunk_id.volume_id, chunk_id.id));
        Ok(FullCompactionPlan {
            min_stale_bytes,
            candidate_count: candidate_chunk_ids.len() as u64,
            candidate_stale_bytes,
            candidate_live_bytes,
            candidate_logical_bytes,
            candidate_chunk_ids,
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or("")
                .to_string(),
            object_gc_report_proof_checksum: gc_report.proof_checksum(),
        })
    }

    pub fn prepare_chunk_compaction_raft_entry(
        &self,
        chunk_id: &ChunkId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkCompaction, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_chunk_compaction_raft_entry_fenced(
            chunk_id,
            min_stale_bytes,
            update_time_ms,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
            None,
        )
    }

    pub fn prepare_chunk_compaction_raft_entry_fenced(
        &self,
        chunk_id: &ChunkId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
        expected_object_gc_report_proof_checksum: Option<&str>,
    ) -> Result<PreparedChunkCompaction, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let expected_object_gc_report_proof_checksum =
            expected_object_gc_report_proof_checksum.unwrap_or("");
        if !expected_object_gc_report_proof_checksum.is_empty() {
            self.gc_report_with_discard_policy_proof_fenced(
                min_stale_bytes,
                0,
                expected_route_control_checksum,
                expected_object_gc_report_proof_checksum,
            )?;
        }
        if expected_previous_commit_index != self.last_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "segment compaction expected previous commit {} does not match applied {}",
                expected_previous_commit_index, self.last_applied_index
            )));
        }
        let chunk = self
            .chunks
            .get(chunk_id)
            .ok_or_else(|| ObjectError::NotFound(format!("chunk {:?}", chunk_id)))?;
        if chunk.stale_bytes < min_stale_bytes {
            return Err(ObjectError::PreconditionFailed(format!(
                "segment stale bytes {} below compaction threshold {}",
                chunk.stale_bytes, min_stale_bytes
            )));
        }
        let live_objects = self.collect_chunk_compaction_live_objects(chunk_id)?;
        let before_state_fingerprint = self.state_fingerprint_without_payload_clones();
        let mut projected = self.clone();
        projected.compact_chunk_at(chunk_id, update_time_ms)?;
        let after_state_fingerprint = projected.state_fingerprint_without_payload_clones();
        let request_checksum =
            chunk_compaction_request_checksum(chunk_id, min_stale_bytes, update_time_ms);
        let route_control_proof = self.chunk_service.route_control_proof();
        let expected_route_control_checksum =
            expected_route_control_checksum.unwrap_or("").to_string();
        let mut prepared = PreparedChunkCompaction {
            chunk_id: chunk_id.clone(),
            min_stale_bytes,
            update_time_ms,
            live_objects,
            source_stale_ranges: merge_range_slice(&chunk.stale_ranges),
            source_discarded_ranges: merge_range_slice(&chunk.discarded_ranges),
            before_state_fingerprint,
            after_state_fingerprint,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            route_control_proof,
            expected_route_control_checksum,
            expected_object_gc_report_proof_checksum: expected_object_gc_report_proof_checksum
                .to_string(),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_compaction_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_chunk_compaction(
        &mut self,
        prepared: PreparedChunkCompaction,
    ) -> Result<(), ObjectError> {
        let expected_checksum = prepared_chunk_compaction_checksum(&PreparedChunkCompaction {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared segment compaction checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != chunk_compaction_request_checksum(
                &prepared.chunk_id,
                prepared.min_stale_bytes,
                prepared.update_time_ms,
            )
        {
            return Err(ObjectError::Corruption(
                "prepared segment compaction request checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_previous_commit_index != self.last_applied_index {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment compaction previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.state_fingerprint_without_payload_clones() {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment compaction source state changed".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment compaction route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment compaction route proof checksum mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "prepared segment compaction",
            )?;
        let current = self.prepare_chunk_compaction_raft_entry_fenced(
            &prepared.chunk_id,
            prepared.min_stale_bytes,
            prepared.update_time_ms,
            prepared.expected_previous_commit_index,
            Some(prepared.route_control_proof.state_checksum.as_str()),
            if prepared.expected_object_gc_report_proof_checksum.is_empty() {
                None
            } else {
                Some(prepared.expected_object_gc_report_proof_checksum.as_str())
            },
        )?;
        if current.live_objects != prepared.live_objects
            || current.source_stale_ranges != prepared.source_stale_ranges
            || current.source_discarded_ranges != prepared.source_discarded_ranges
            || current.after_state_fingerprint != prepared.after_state_fingerprint
            || current.route_control_proof != prepared.route_control_proof
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment compaction no longer matches current state".to_string(),
            ));
        }
        self.compact_chunk_at(&prepared.chunk_id, prepared.update_time_ms)?;
        if self.state_fingerprint_without_payload_clones() != prepared.after_state_fingerprint {
            return Err(ObjectError::Corruption(
                "prepared segment compaction post-state checksum mismatch".to_string(),
            ));
        }
        Ok(())
    }

}
