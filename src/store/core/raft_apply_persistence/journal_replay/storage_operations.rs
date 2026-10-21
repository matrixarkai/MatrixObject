// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn apply_persistent_journal_storage_operation(
        &mut self,
        op: u64,
        mut cursor: &mut SnapshotByteCursor<'_>,
    ) -> Result<bool, ObjectError> {
        match op {
            53 => {
                let mut lengths = Vec::new();
                for _ in 0..cursor.read_count("persistent journal block allocation length count")? {
                    lengths.push(cursor.read_u64()?);
                }
                let now_ms = cursor.read_u64()?;
                let idempotency_key = cursor.read_string()?;
                self.allocate_page_ranges_idempotent(lengths, now_ms, &idempotency_key)?;
            }
            54 => {
                let allocation_id = cursor.read_string()?;
                let idempotency_key = cursor.read_string()?;
                self.release_page_allocation_idempotent(&allocation_id, &idempotency_key)?;
            }
            55 => {
                let chunk_id = read_chunk_id(&mut cursor)?;
                let offset = cursor.read_u64()?;
                let data_len = cursor.read_u64()? as usize;
                let data = cursor.read_bytes(data_len)?.to_vec();
                let idempotency_key = cursor.read_string()?;
                self.write_page_range_idempotent(&chunk_id, offset, &data, &idempotency_key)?;
            }
            56 => {
                let count = cursor.read_count("persistent journal block write count")?;
                let mut writes = Vec::with_capacity(count);
                for _ in 0..count {
                    let chunk_id = read_chunk_id(&mut cursor)?;
                    let offset = cursor.read_u64()?;
                    let data_len = cursor.read_u64()? as usize;
                    let data = cursor.read_bytes(data_len)?.to_vec();
                    writes.push(ChunkRangeWrite {
                        chunk_id,
                        offset,
                        data,
                    });
                }
                let idempotency_key = cursor.read_string()?;
                self.write_page_ranges_idempotent(writes, &idempotency_key)?;
            }
            57 => {
                let chunk_id = read_chunk_id(&mut cursor)?;
                let offset = cursor.read_u64()?;
                let length = cursor.read_u64()?;
                let idempotency_key = cursor.read_string()?;
                self.discard_page_range_idempotent(&chunk_id, offset, length, &idempotency_key)?;
            }
            58 => {
                let count = cursor.read_count("persistent journal block discard count")?;
                let mut requests = Vec::with_capacity(count);
                for _ in 0..count {
                    requests.push(ChunkRangeRequest {
                        chunk_id: read_chunk_id(&mut cursor)?,
                        offset: cursor.read_u64()?,
                        length: cursor.read_u64()?,
                    });
                }
                let idempotency_key = cursor.read_string()?;
                self.discard_page_ranges_idempotent(requests, &idempotency_key)?;
            }
            59 => {
                let replica_index =
                    cursor.read_count("persistent journal chunk write replica index")?;
                let page_id = read_page_id(&mut cursor)?;
                let offset = cursor.read_u64()?;
                let data_len = cursor.read_u64()? as usize;
                let data = cursor.read_bytes(data_len)?.to_vec();
                let idempotency_key = cursor.read_string()?;
                self.write_direct_chunk_range_idempotent(
                    replica_index,
                    page_id,
                    offset,
                    &data,
                    &idempotency_key,
                )?;
            }
            60 => {
                let replica_index =
                    cursor.read_count("persistent journal chunk batch write replica index")?;
                let page_id = read_page_id(&mut cursor)?;
                let count = cursor.read_count("persistent journal chunk batch write count")?;
                let mut writes = Vec::with_capacity(count);
                for _ in 0..count {
                    let offset = cursor.read_u64()?;
                    let data_len = cursor.read_u64()? as usize;
                    let data = cursor.read_bytes(data_len)?.to_vec();
                    writes.push((offset, data));
                }
                let idempotency_key = cursor.read_string()?;
                self.write_direct_chunk_ranges_idempotent(
                    replica_index,
                    page_id,
                    writes,
                    &idempotency_key,
                )?;
            }
            61 => {
                let replica_index =
                    cursor.read_count("persistent journal chunk discard replica index")?;
                let page_id = read_page_id(&mut cursor)?;
                let offset = cursor.read_u64()?;
                let length = cursor.read_u64()?;
                let idempotency_key = cursor.read_string()?;
                self.discard_direct_chunk_range_idempotent(
                    replica_index,
                    &page_id,
                    offset,
                    length,
                    &idempotency_key,
                )?;
            }
            62 => {
                let replica_index =
                    cursor.read_count("persistent journal chunk batch discard replica index")?;
                let page_id = read_page_id(&mut cursor)?;
                let count = cursor.read_count("persistent journal chunk batch discard count")?;
                let mut ranges = Vec::with_capacity(count);
                for _ in 0..count {
                    ranges.push(read_range(&mut cursor)?);
                }
                let idempotency_key = cursor.read_string()?;
                self.discard_direct_chunk_ranges_idempotent(
                    replica_index,
                    &page_id,
                    ranges,
                    &idempotency_key,
                )?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
