// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn last_applied_index(&self) -> u64 {
        self.last_applied_index
    }

    fn ensure_put_options_read_fence(
        &self,
        operation: &str,
        options: &PutOptions,
    ) -> Result<(), ObjectError> {
        let required_read_index = options.min_read_index.max(options.min_commit_index);
        if self.last_applied_index < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} read index is not satisfied: applied={} required={}",
                operation, self.last_applied_index, required_read_index
            )));
        }
        Ok(())
    }

    pub(crate) fn ensure_metadata_mutation_fence(
        &self,
        operation: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(), ObjectError> {
        let required_read_index = min_read_index.max(min_commit_index);
        if self.last_applied_index < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata read index is not satisfied: applied={} required={}",
                operation, self.last_applied_index, required_read_index
            )));
        }
        let metadata_commit_index = self.last_metadata_commit_index();
        if metadata_commit_index < min_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} metadata commit index is not satisfied: committed={} required={}",
                operation, metadata_commit_index, min_commit_index
            )));
        }
        Ok(())
    }

    fn ensure_put_requests_read_fence(
        &self,
        operation: &str,
        requests: &[ObjectPutRequest],
    ) -> Result<(), ObjectError> {
        let required_read_index = requests
            .iter()
            .map(|request| {
                request
                    .options
                    .min_read_index
                    .max(request.options.min_commit_index)
            })
            .max()
            .unwrap_or(0);
        if self.last_applied_index < required_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} read index is not satisfied: applied={} required={}",
                operation, self.last_applied_index, required_read_index
            )));
        }
        Ok(())
    }

    pub fn last_metadata_commit_index(&self) -> u64 {
        self.meta_service.next_commit_index.saturating_sub(1)
    }

    pub fn object_manifest_checkpoint_index(&self) -> u64 {
        self.last_object_manifest_checkpoint_index
    }

    pub fn object_metadata_checkpoint_index(&self) -> u64 {
        self.last_object_metadata_checkpoint_index
    }

    pub fn object_manifest_checkpoint_lag(&self) -> u64 {
        self.last_metadata_commit_index()
            .saturating_sub(self.last_object_manifest_checkpoint_index)
    }

    pub fn object_metadata_checkpoint_lag(&self) -> u64 {
        self.last_metadata_commit_index()
            .saturating_sub(self.last_object_metadata_checkpoint_index)
    }

    pub fn flush_object_checkpoint_indexes(&mut self) {
        let commit_index = self.last_metadata_commit_index();
        self.last_object_manifest_checkpoint_index = commit_index;
        self.last_object_metadata_checkpoint_index = commit_index;
    }

}
