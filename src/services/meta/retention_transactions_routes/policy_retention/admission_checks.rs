impl MatrixMetaService {
    fn check_mutation_admission(&self, mutations: &[MetadataMutation]) -> Result<(), ObjectError> {
        if mutations.len() > self.admission_policy.max_batch_mutations {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata batch mutation count {} exceeds admission limit {}",
                mutations.len(),
                self.admission_policy.max_batch_mutations
            )));
        }
        let value_bytes = mutations.iter().try_fold(0_u64, |total, mutation| {
            total
                .checked_add(mutation.value.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("metadata batch value bytes overflow".to_string())
                })
        })?;
        if value_bytes > self.admission_policy.max_batch_value_bytes {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata batch value bytes {} exceeds admission limit {}",
                value_bytes, self.admission_policy.max_batch_value_bytes
            )));
        }
        Ok(())
    }

    fn check_open_transaction_admission(&self, transaction_id: &str) -> Result<(), ObjectError> {
        if !self.transactions.contains_key(transaction_id)
            && self.transactions.len() >= self.admission_policy.max_open_transactions
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata open transaction admission limit reached".to_string(),
            ));
        }
        Ok(())
    }

    fn check_namespace_lease_admission(&self, namespace: &str) -> Result<(), ObjectError> {
        if !self.leases.contains_key(namespace)
            && self.leases.len() >= self.admission_policy.max_namespace_leases
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata namespace lease admission limit reached".to_string(),
            ));
        }
        Ok(())
    }

    fn check_coordinator_session_admission(&self, session_id: &str) -> Result<(), ObjectError> {
        if !self.coordinator_sessions.contains_key(session_id)
            && self.coordinator_sessions.len() >= self.admission_policy.max_coordinator_sessions
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata coordinator session admission limit reached".to_string(),
            ));
        }
        Ok(())
    }

    fn check_shard_admission(&self, additional_shards: usize) -> Result<(), ObjectError> {
        if self.shards.len().saturating_add(additional_shards) > self.admission_policy.max_shards {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard admission limit reached".to_string(),
            ));
        }
        Ok(())
    }

}
