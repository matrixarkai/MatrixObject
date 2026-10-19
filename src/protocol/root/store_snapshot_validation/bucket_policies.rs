// SPDX-License-Identifier: Apache-2.0
impl StoreSnapshot {
    fn validate_bucket_policies(&self) -> Result<(), ObjectError> {
        for (bucket, retain_for_ms) in &self.bucket_retention {
            validate_bucket(bucket)?;
            if *retain_for_ms == 0 {
                return Err(ObjectError::Corruption(
                    "snapshot bucket retention stores zero duration".to_string(),
                ));
            }
        }
        for (bucket, quota) in &self.bucket_quotas {
            validate_bucket(bucket)?;
            if quota.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket quota stores disabled policy".to_string(),
                ));
            }
        }
        for (bucket, policy) in &self.bucket_lifecycle {
            validate_bucket(bucket)?;
            validate_lifecycle_policy(policy)?;
            if policy.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket lifecycle stores disabled policy".to_string(),
                ));
            }
        }
        for (bucket, policy) in &self.bucket_policy_documents {
            validate_bucket(bucket)?;
            validate_bucket_policy_document(policy)?;
            if policy.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket policy document stores disabled policy".to_string(),
                ));
            }
        }
        for (bucket, tag_set) in &self.bucket_tags {
            validate_bucket(bucket)?;
            validate_tags(&tag_set.tags)?;
            if tag_set.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket tag set stores disabled tag set".to_string(),
                ));
            }
        }
        for (bucket, policy) in &self.bucket_cors {
            validate_bucket(bucket)?;
            validate_cors_policy(policy)?;
            if policy.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket cors stores disabled policy".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.bucket_policy_receipts {
            validate_bucket_policy_receipt(receipt)?;
            if idempotency_key != &receipt.idempotency_key {
                return Err(ObjectError::Corruption(
                    "snapshot bucket policy receipt key mismatch".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.bucket_cors_receipts {
            validate_bucket_cors_receipt(receipt)?;
            if idempotency_key != &receipt.idempotency_key {
                return Err(ObjectError::Corruption(
                    "snapshot bucket cors receipt key mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }
}
