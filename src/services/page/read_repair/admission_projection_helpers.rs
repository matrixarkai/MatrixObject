impl MatrixChunkService {
    fn projected_chunk_lengths_for_locations(
        locations: &[ChunkLocation],
    ) -> Result<BTreeMap<PageId, u64>, ObjectError> {
        let mut projected = BTreeMap::new();
        for loc in locations {
            let end = validate_pipeline_range("chunk write", loc.page_offset, loc.length)?;
            projected
                .entry(loc.page_id.clone())
                .and_modify(|length: &mut u64| *length = (*length).max(end))
                .or_insert(end);
        }
        Ok(projected)
    }

    fn projected_chunk_lengths_for_grouped_write_views(
        grouped: &BTreeMap<(ChunkId, PageId), Vec<(u64, &[u8])>>,
    ) -> Result<BTreeMap<PageId, u64>, ObjectError> {
        let mut projected = BTreeMap::new();
        for ((_, page_id), writes) in grouped {
            for (offset, data) in writes {
                let end = validate_pipeline_range("chunk write", *offset, data.len() as u64)?;
                projected
                    .entry(page_id.clone())
                    .and_modify(|length: &mut u64| *length = (*length).max(end))
                    .or_insert(end);
            }
        }
        Ok(projected)
    }

    fn replica_accepts_projected_chunk_lengths(
        replica: &MatrixReplicaService,
        projected_lengths: &BTreeMap<PageId, u64>,
    ) -> Result<(), ObjectError> {
        if let Some(err) = replica.writable_admission_error() {
            return Err(err);
        }
        let mut projected_physical = replica.physical_bytes();
        for (page_id, new_len) in projected_lengths {
            let current_len = replica
                .pages
                .get(page_id)
                .map_or(0u64, |state| state.data.len() as u64);
            projected_physical = projected_physical
                .checked_sub(current_len)
                .and_then(|base| base.checked_add((*new_len).max(current_len)))
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "chunk service capacity accounting overflows".to_string(),
                    )
                })?;
        }
        if replica.capacity_bytes > 0 && projected_physical > replica.capacity_bytes {
            return Err(ObjectError::PreconditionFailed(
                "chunk service capacity exceeded".to_string(),
            ));
        }
        Ok(())
    }

    fn record_projected_write_admission_rejection(
        replica: &mut MatrixReplicaService,
        err: &ObjectError,
        rejected_chunks: u64,
    ) {
        if rejected_chunks == 0 {
            return;
        }
        if let ObjectError::PreconditionFailed(message) = err {
            if message.contains("capacity") {
                replica.capacity_rejected_writes = replica
                    .capacity_rejected_writes
                    .saturating_add(rejected_chunks);
            } else {
                replica.service_mode_rejected_mutations = replica
                    .service_mode_rejected_mutations
                    .saturating_add(rejected_chunks);
            }
        }
    }

    fn admissible_write_replicas(
        &mut self,
        projected_lengths: &BTreeMap<PageId, u64>,
    ) -> Result<Vec<usize>, ObjectError> {
        let mut accepted = Vec::with_capacity(self.replicas.len());
        let mut first_error: Option<ObjectError> = None;
        let rejected_chunks = projected_lengths.len() as u64;
        for index in 0..self.replicas.len() {
            if !self.replica_writable(index) {
                continue;
            }
            match Self::replica_accepts_projected_chunk_lengths(
                &self.replicas[index],
                projected_lengths,
            ) {
                Ok(()) => accepted.push(index),
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err.clone());
                    }
                    Self::record_projected_write_admission_rejection(
                        &mut self.replicas[index],
                        &err,
                        rejected_chunks,
                    );
                }
            }
        }
        if accepted.len() < self.write_quorum {
            return Err(first_error.unwrap_or_else(|| {
                ObjectError::Corruption("page write quorum was not met".to_string())
            }));
        }
        Ok(accepted)
    }

    fn replica_accepts_discard_ranges(
        replica: &MatrixReplicaService,
        discard_ranges: &BTreeMap<PageId, Vec<DataRange>>,
    ) -> Result<(), ObjectError> {
        for (page_id, ranges) in discard_ranges {
            let admission = replica.can_accept_chunk_discard(page_id, ranges)?;
            if admission.can_accept {
                continue;
            }
            if admission.service_mode_limited {
                let err = replica.writable_admission_error().unwrap_or_else(|| {
                    ObjectError::PreconditionFailed(
                        "chunk service cannot accept discard".to_string(),
                    )
                });
                return Err(err);
            }
            if admission.missing_chunk {
                return Err(ObjectError::NotFound("chunk is missing".to_string()));
            }
            if admission.range_invalid {
                return Err(ObjectError::InvalidArgument(
                    "chunk discard exceeds logical length".to_string(),
                ));
            }
            return Err(ObjectError::PreconditionFailed(
                "chunk service cannot accept discard".to_string(),
            ));
        }
        Ok(())
    }

    fn admissible_discard_replicas(
        &mut self,
        discard_ranges: &BTreeMap<PageId, Vec<DataRange>>,
    ) -> Result<Vec<usize>, ObjectError> {
        let mut accepted = Vec::with_capacity(self.replicas.len());
        let mut first_error: Option<ObjectError> = None;
        let rejected_chunks = discard_ranges.len() as u64;
        for index in 0..self.replicas.len() {
            if !self.replica_writable(index) {
                continue;
            }
            match Self::replica_accepts_discard_ranges(&self.replicas[index], discard_ranges) {
                Ok(()) => accepted.push(index),
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err.clone());
                    }
                    if let ObjectError::PreconditionFailed(_) = err {
                        Self::record_projected_write_admission_rejection(
                            &mut self.replicas[index],
                            &err,
                            rejected_chunks,
                        );
                    }
                }
            }
        }
        if accepted.len() < self.write_quorum {
            return Err(first_error.unwrap_or_else(|| {
                ObjectError::Corruption("page discard quorum was not met".to_string())
            }));
        }
        Ok(accepted)
    }
}
