// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl StoreSnapshot {
    fn validate_snapshot_checksum(&self) -> Result<(), ObjectError> {
        let expected = self.checksum_material_hash();
        if self.snapshot_checksum != expected {
            return Err(ObjectError::Corruption(
                "snapshot checksum mismatch".to_string(),
            ));
        }
        Ok(())
    }
}
