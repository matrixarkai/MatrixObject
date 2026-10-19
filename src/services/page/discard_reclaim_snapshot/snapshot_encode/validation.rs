// SPDX-License-Identifier: Apache-2.0
impl ChunkServiceSnapshot {
    pub fn validate(&self) -> Result<(), ObjectError> {
        validate_storage_receipt_retention(self.receipt_retention)?;
        if self.page_size == 0 {
            return Err(ObjectError::Corruption(
                "page service snapshot has zero chunk size".to_string(),
            ));
        }
        if self.replicas.is_empty() {
            return Err(ObjectError::Corruption(
                "page service snapshot has no replicas".to_string(),
            ));
        }
        if self.write_quorum == 0 || self.write_quorum > self.replicas.len() {
            return Err(ObjectError::Corruption(
                "page service snapshot write quorum is invalid".to_string(),
            ));
        }
        if self.read_quorum == 0 || self.read_quorum > self.replicas.len() {
            return Err(ObjectError::Corruption(
                "page service snapshot read quorum is invalid".to_string(),
            ));
        }
        validate_page_chunk_endpoint_topology(
            &self.chunk_endpoints,
            &self.disabled_chunk_endpoints,
            self.write_quorum,
            self.read_quorum,
            self.replicas.len(),
        )
        .map_err(|err| {
            ObjectError::Corruption(format!("page service endpoint topology is invalid: {err}"))
        })?;
        validate_page_route_qos_config(&self.route_qos_config).map_err(|err| {
            ObjectError::Corruption(format!("page service route qos is invalid: {err}"))
        })?;
        if self.chunk_route_control_next_log_index <= self.chunk_route_control_commit_index {
            return Err(ObjectError::Corruption(
                "page service route control log watermark is invalid".to_string(),
            ));
        }
        if self.allocator_chunk_size == 0
            || self.allocator_chunk_size < self.page_size
            || self.allocator_next_chunk_id == 0
            || self.allocator_next_offset > self.allocator_chunk_size
        {
            return Err(ObjectError::Corruption(
                "page service snapshot allocator cursor is invalid".to_string(),
            ));
        }
        for (allocation_id, allocation) in &self.pending_allocations {
            if allocation_id != &allocation.allocation_id
                || allocation.length == 0
                || allocation.chunk_id.volume_id != 0
                || allocation.chunk_id.id == 0
                || allocation.offset.saturating_add(allocation.length) > self.allocator_chunk_size
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot pending allocation is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, allocation) in &self.allocation_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if allocation.length == 0
                || allocation.chunk_id.volume_id != 0
                || allocation.chunk_id.id == 0
                || allocation.offset.saturating_add(allocation.length) > self.allocator_chunk_size
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot allocation receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, allocations) in &self.batch_allocation_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if allocations.is_empty() || allocations.len() > 100_000 {
                return Err(ObjectError::Corruption(
                    "page service snapshot batch allocation receipt count is invalid".to_string(),
                ));
            }
            for allocation in allocations {
                if allocation.length == 0
                    || allocation.chunk_id.volume_id != 0
                    || allocation.chunk_id.id == 0
                    || allocation.offset.saturating_add(allocation.length)
                        > self.allocator_chunk_size
                {
                    return Err(ObjectError::Corruption(
                        "page service snapshot batch allocation receipt is invalid".to_string(),
                    ));
                }
            }
        }
        for (idempotency_key, allocation) in &self.release_allocation_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if allocation.length == 0
                || allocation.chunk_id.volume_id != 0
                || allocation.chunk_id.id == 0
                || allocation.offset.saturating_add(allocation.length) > self.allocator_chunk_size
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot release allocation receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.write_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.length == 0
                || receipt.chunk_id.volume_id != 0
                || receipt.chunk_id.id == 0
                || receipt.request_checksum
                    != page_write_receipt_request_checksum(
                        &receipt.chunk_id,
                        receipt.offset,
                        receipt.length,
                        &receipt.payload_checksum,
                    )
                || receipt.payload_checksum.is_empty()
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot write receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.batch_write_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.payload_checksum.is_empty()
                || receipt.stats.input_ranges == 0
                || receipt.stats.payload_bytes == 0
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot batch write receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.discard_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.length == 0
                || receipt.chunk_id.volume_id != 0
                || receipt.chunk_id.id == 0
                || receipt.request_checksum
                    != page_discard_request_checksum(
                        &receipt.chunk_id,
                        receipt.offset,
                        receipt.length,
                    )
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot discard receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.batch_discard_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.stats.input_ranges == 0
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot batch discard receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.repair_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.request_checksum != chunk_repair_request_checksum(&receipt.page_id)
                || receipt.report.page_id.as_ref() != Some(&receipt.page_id)
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot repair receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.range_repair_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.request_checksum
                    != page_range_repair_request_checksum(
                        &receipt.chunk_id,
                        receipt.offset,
                        receipt.length,
                        receipt.max_repair_chunks,
                        receipt.start_chunk_index,
                    )
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot range repair receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.audit_repair_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.request_checksum
                    != page_audit_repair_request_checksum(
                        &receipt.after_chunk_id,
                        receipt.max_chunks,
                        receipt.max_payload_bytes,
                    )
            {
                return Err(ObjectError::Corruption(
                    "page service snapshot audit repair receipt is invalid".to_string(),
                ));
            }
        }
        for index in &self.quarantined_replicas {
            if *index >= self.replicas.len() {
                return Err(ObjectError::Corruption(
                    "page service snapshot quarantine index is invalid".to_string(),
                ));
            }
        }
        let active_replicas = self
            .replicas
            .len()
            .saturating_sub(self.quarantined_replicas.len());
        if active_replicas < self.write_quorum || active_replicas < self.read_quorum {
            return Err(ObjectError::Corruption(
                "page service snapshot quarantine violates quorum".to_string(),
            ));
        }
        let writable_replicas = self
            .replicas
            .iter()
            .enumerate()
            .filter(|(index, replica)| {
                !self.quarantined_replicas.contains(index)
                    && !self.disabled_chunk_endpoints.contains(index)
                    && !replica.frozen
            })
            .count();
        if writable_replicas < self.write_quorum {
            return Err(ObjectError::Corruption(
                "page service snapshot frozen replicas violate write quorum".to_string(),
            ));
        }
        for replica in &self.replicas {
            replica.validate()?;
        }
        for chunk in &self.sealed_chunks {
            if chunk.volume_id != 0 || chunk.id == 0 {
                return Err(ObjectError::Corruption(
                    "page service snapshot sealed chunk is invalid".to_string(),
                ));
            }
        }
        let active_chunk = ChunkId {
            volume_id: 0,
            id: self.allocator_next_chunk_id,
        };
        if self.sealed_chunks.contains(&active_chunk)
            && self.allocator_next_offset < self.allocator_chunk_size
        {
            return Err(ObjectError::Corruption(
                "page service snapshot allocator cursor points into sealed chunk".to_string(),
            ));
        }
        if self.checksum != self.checksum_material_hash()
            && self.checksum != self.pre_repair_counter_checksum_material_hash()
            && self.checksum != self.legacy_checksum_material_hash()
            && self.checksum != self.legacy_receipt_checksum_material_hash()
        {
            return Err(ObjectError::Corruption(
                "page service snapshot checksum mismatch".to_string(),
            ));
        }
        Ok(())
    }
}
