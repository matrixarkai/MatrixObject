impl MatrixObjectStore {
    fn apply_transaction_metadata_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::BeginMetaTransaction {
                transaction_id,
                owner,
                ttl_ms,
                mutations,
                now_ms,
            } => {
                let transaction = self.begin_metadata_transaction(
                    &transaction_id,
                    &owner,
                    ttl_ms,
                    mutations,
                    now_ms,
                )?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &transaction.transaction_id,
                    0,
                    transaction.mutations.len() as u64,
                    "metaserver/transaction-begin",
                    transaction.update_time_ms,
                ))
            }
            ObjectCommand::BeginMetaTransactionIdempotent {
                transaction_id,
                owner,
                ttl_ms,
                mutations,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.begin_metadata_transaction_idempotent(
                    &transaction_id,
                    &owner,
                    ttl_ms,
                    mutations,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &receipt.transaction.transaction_id,
                    receipt.commit_index,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "metaserver/transaction-begin-idempotent-deduped"
                    } else {
                        "metaserver/transaction-begin-idempotent"
                    },
                    receipt.update_time_ms,
                ))
            }
            ObjectCommand::RenewMetaTransaction {
                transaction_id,
                owner,
                ttl_ms,
                now_ms,
            } => {
                let transaction =
                    self.renew_metadata_transaction(&transaction_id, &owner, ttl_ms, now_ms)?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &transaction.transaction_id,
                    0,
                    transaction.mutations.len() as u64,
                    "metaserver/transaction-renew",
                    transaction.update_time_ms,
                ))
            }
            ObjectCommand::RenewMetaTransactionIdempotent {
                transaction_id,
                owner,
                ttl_ms,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.renew_metadata_transaction_idempotent(
                    &transaction_id,
                    &owner,
                    ttl_ms,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(metadata_control_metadata(
                    "_metatxn",
                    &receipt.transaction.transaction_id,
                    receipt.commit_index,
                    receipt.deduped as u64,
                    if receipt.deduped {
                        "metaserver/transaction-renew-idempotent-deduped"
                    } else {
                        "metaserver/transaction-renew-idempotent"
                    },
                    receipt.update_time_ms,
                ))
            }
            _ => unreachable!("command classified outside MetadataTransactions"),
        }
    }
}
