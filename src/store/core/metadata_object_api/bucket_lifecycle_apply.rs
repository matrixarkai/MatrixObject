// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn apply_bucket_lifecycle_policies(
        &mut self,
        max_actions: usize,
    ) -> Result<LifecycleApplyResult, ObjectError> {
        let now_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let result = self.apply_bucket_lifecycle_policies_at(max_actions, now_ms)?;
        if result.total_actions() > 0 {
            if let Err(err) = self.append_lifecycle_apply_journal_record(max_actions, now_ms, false)
            {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        Ok(result)
    }

    pub fn apply_bucket_lifecycle_policies_with_metadata_read(
        &mut self,
        max_actions: usize,
        options: MetadataReadOptions,
    ) -> Result<(LifecycleApplyResult, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(meta_lifecycle_apply_key(), &options)?;
        let result = self.apply_bucket_lifecycle_policies(max_actions)?;
        Ok((result, readiness))
    }

    pub fn apply_bucket_lifecycle_transitions(
        &mut self,
        max_actions: usize,
    ) -> Result<LifecycleApplyResult, ObjectError> {
        let now_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let result = self.apply_bucket_lifecycle_transitions_at(max_actions, now_ms)?;
        if result.total_actions() > 0 {
            if let Err(err) = self.append_lifecycle_apply_journal_record(max_actions, now_ms, true)
            {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        Ok(result)
    }

    pub fn apply_bucket_lifecycle_transitions_with_metadata_read(
        &mut self,
        max_actions: usize,
        options: MetadataReadOptions,
    ) -> Result<(LifecycleApplyResult, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(meta_lifecycle_transition_key(), &options)?;
        let result = self.apply_bucket_lifecycle_transitions(max_actions)?;
        Ok((result, readiness))
    }
}
