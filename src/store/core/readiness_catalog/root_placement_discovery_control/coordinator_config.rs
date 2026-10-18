impl MatrixObjectStore {
    pub fn root_coordinator_config(&self) -> MetadataCoordinatorConfig {
        self.meta_service.placement_coordinator_config()
    }


    pub fn root_coordinator_policy_control_fields(
        &self,
        field_prefix: &str,
    ) -> BTreeMap<String, String> {
        self.root_coordinator_config()
            .to_control_fields(field_prefix)
    }


    pub fn configure_root_coordinator_config(
        &mut self,
        config: MetadataCoordinatorConfig,
    ) -> Result<MetadataCoordinatorConfig, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let config = self.meta_service.configure_placement_coordinator_config(config)?;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_root_coordinator_config_journal_record(&config) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(config)
    }
}
