impl MatrixChunkService {
    pub fn discard_slab_range(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
    ) -> Result<(), ObjectError> {
        self.discard_slab_range_with_chunk_receipts(slab_id, offset, length, None)
    }

    pub fn discard_slab_range_storage_fenced(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.check_expected_chunk_control_proof_checksum(expected_chunk_control_proof_checksum)?;
        self.discard_slab_range(slab_id, offset, length)
    }

    pub fn discard_slab_range_storage_fenced_report(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> Result<ChunkStorageMutationReport, ObjectError> {
        self.discard_slab_range_storage_fenced(
            slab_id,
            offset,
            length,
            expected_route_control_checksum,
            expected_chunk_control_proof_checksum,
        )?;
        let stats = ChunkBatchStats {
            input_ranges: if length == 0 { 0 } else { 1 },
            mapped_chunk_ranges: self.mapped_chunk_count(offset, length)?,
            payload_bytes: length,
            ..ChunkBatchStats::default()
        };
        Ok(self.storage_mutation_report(
            stats,
            expected_route_control_checksum,
            expected_chunk_control_proof_checksum,
        ))
    }

    fn discard_slab_range_with_chunk_receipts(
        &mut self,
        slab_id: &SlabId,
        offset: u64,
        length: u64,
        idempotency_key: Option<&str>,
    ) -> Result<(), ObjectError> {
        self.admit_page_route_qos(0, 0, 0, 0, 1, length, now_millis())?;
        let locations = self.map_range(slab_id, offset, length)?;
        let mut discard_ranges: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        for loc in &locations {
            discard_ranges
                .entry(loc.page_id.clone())
                .or_default()
                .push(DataRange {
                    offset: loc.page_offset,
                    length: loc.length,
                });
        }
        let accepted_replicas = self.admissible_discard_replicas(&discard_ranges)?;
        for loc in locations {
            let mut ok = 0usize;
            let mut first_error: Option<ObjectError> = None;
            for (index, replica) in self.replicas.iter_mut().enumerate() {
                if !accepted_replicas.contains(&index) {
                    continue;
                }
                let discard_result = if let Some(base_key) = idempotency_key {
                    let page_key = replica_receipt_key(
                        "sd",
                        base_key,
                        &loc.page_id,
                        index,
                        &(loc.page_offset, loc.length),
                    );
                    replica
                        .discard_chunk_idempotent(
                            &loc.page_id,
                            loc.page_offset,
                            loc.length,
                            &page_key,
                        )
                        .map(|_| ())
                } else {
                    replica.discard_chunk(&loc.page_id, loc.page_offset, loc.length)
                };
                match discard_result {
                    Ok(()) => ok += 1,
                    Err(err) if first_error.is_none() => first_error = Some(err),
                    Err(_) => {}
                }
            }
            if ok < self.write_quorum {
                return Err(first_error.unwrap_or_else(|| {
                    ObjectError::Corruption("block discard quorum was not met".to_string())
                }));
            }
            if ok < accepted_replicas.len() {
                self.cumulative_quorum_discard_degraded_chunks = self
                    .cumulative_quorum_discard_degraded_chunks
                    .saturating_add(1);
            }
        }
        Ok(())
    }
}
