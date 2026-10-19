// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    fn encoded_metadata_mutation_len(mutation: &MetadataMutation) -> usize {
        encoded_string_len(&mutation.key)
            + encoded_string_len(&mutation.value)
            + std::mem::size_of::<u64>() * 2
    }

    fn append_metadata_lease_acquire_journal_record(
        &self,
        namespace: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + encoded_string_len(namespace)
                + encoded_string_len(owner),
        );
        append_u64(&mut payload, 44);
        append_string(&mut payload, namespace);
        append_string(&mut payload, owner);
        append_u64(&mut payload, ttl_ms);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metadata_lease_renew_journal_record(
        &self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 4
                + encoded_string_len(namespace)
                + encoded_string_len(owner),
        );
        append_u64(&mut payload, 45);
        append_string(&mut payload, namespace);
        append_string(&mut payload, owner);
        append_u64(&mut payload, epoch);
        append_u64(&mut payload, ttl_ms);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metadata_lease_release_journal_record(
        &self,
        namespace: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(namespace)
                + encoded_string_len(owner),
        );
        append_u64(&mut payload, 46);
        append_string(&mut payload, namespace);
        append_string(&mut payload, owner);
        append_u64(&mut payload, epoch);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metadata_lease_expiry_journal_record(
        &self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload =
            Vec::with_capacity(std::mem::size_of::<u64>() * 3 + encoded_string_len(prefix));
        append_u64(&mut payload, 47);
        append_string(&mut payload, prefix);
        append_u64(&mut payload, now_ms);
        append_u64(&mut payload, limit as u64);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metadata_transaction_begin_journal_record(
        &self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: &[MetadataMutation],
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mutation_bytes = mutations
            .iter()
            .map(Self::encoded_metadata_mutation_len)
            .sum::<usize>();
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 4
                + encoded_string_len(transaction_id)
                + encoded_string_len(owner)
                + mutation_bytes,
        );
        append_u64(&mut payload, 48);
        append_string(&mut payload, transaction_id);
        append_string(&mut payload, owner);
        append_u64(&mut payload, ttl_ms);
        append_u64(&mut payload, mutations.len() as u64);
        for mutation in mutations {
            append_metadata_mutation(&mut payload, mutation);
        }
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metadata_transaction_renew_journal_record(
        &self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + encoded_string_len(transaction_id)
                + encoded_string_len(owner),
        );
        append_u64(&mut payload, 49);
        append_string(&mut payload, transaction_id);
        append_string(&mut payload, owner);
        append_u64(&mut payload, ttl_ms);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metadata_transaction_commit_journal_record(
        &self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(transaction_id)
                + encoded_string_len(owner),
        );
        append_u64(&mut payload, 50);
        append_string(&mut payload, transaction_id);
        append_string(&mut payload, owner);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metadata_transaction_abort_journal_record(
        &self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(transaction_id)
                + encoded_string_len(owner),
        );
        append_u64(&mut payload, 51);
        append_string(&mut payload, transaction_id);
        append_string(&mut payload, owner);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metadata_transaction_expiry_journal_record(
        &self,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(std::mem::size_of::<u64>() * 2);
        append_u64(&mut payload, 52);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

}
