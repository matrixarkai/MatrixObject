// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn replay_persistent_journal(&mut self, journal_path: &str) -> Result<u64, ObjectError> {
        let bytes = match std::fs::read(journal_path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(err) => {
                return Err(ObjectError::Corruption(format!(
                    "failed to read persistent journal {}: {}",
                    journal_path, err
                )));
            }
        };
        if bytes.is_empty() {
            return Ok(0);
        }
        let mut cursor = SnapshotByteCursor::new(&bytes);
        let magic = cursor.read_string()?;
        if magic != PERSISTENT_JOURNAL_MAGIC {
            return Err(ObjectError::Corruption(
                "persistent journal magic mismatch".to_string(),
            ));
        }
        let mut replayed = 0u64;
        while cursor.position() < bytes.len() {
            let payload_len = cursor.read_u64()? as usize;
            if payload_len > MAX_PERSISTENT_JOURNAL_RECORD_BYTES {
                return Err(ObjectError::Corruption(
                    "persistent journal record is too large".to_string(),
                ));
            }
            let payload = cursor.read_bytes(payload_len)?;
            let checksum = cursor.read_string()?;
            if !etag_matches(payload, &checksum) {
                return Err(ObjectError::Corruption(
                    "persistent journal record checksum mismatch".to_string(),
                ));
            }
            self.apply_persistent_journal_payload(payload)?;
            replayed = replayed.saturating_add(1);
        }
        Ok(replayed)
    }

    fn apply_persistent_journal_payload(&mut self, payload: &[u8]) -> Result<(), ObjectError> {
        let mut cursor = SnapshotByteCursor::new(payload);
        let op = cursor.read_u64()?;
        let handled =
            self.apply_persistent_journal_object_operation(op, &mut cursor, payload.len())?
                || self.apply_persistent_journal_catalog_operation(op, &mut cursor)?
                || self.apply_persistent_journal_coordination_operation(op, &mut cursor)?
                || self.apply_persistent_journal_storage_operation(op, &mut cursor)?;
        if !handled {
            return Err(ObjectError::Corruption(format!(
                "persistent journal record op {op} is unsupported"
            )));
        }
        if cursor.position() != payload.len() {
            return Err(ObjectError::Corruption(
                "persistent journal record has trailing bytes".to_string(),
            ));
        }
        Ok(())
    }
}

include!("journal_replay/object_operations.rs");
include!("journal_replay/catalog_operations.rs");
include!("journal_replay/coordination_operations.rs");
include!("journal_replay/storage_operations.rs");
