// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn bootstrap_root_catalog_from_env(
        &mut self,
    ) -> Result<Option<RootCatalogBootstrapReport>, ObjectError> {
        let require_bootstrap =
            first_non_empty_env_bool(&MATRIX_ROOTSERVER_REQUIRE_RAFT_BOOTSTRAP_ENV_NAMES)?
                .unwrap_or(false);
        let requested_commit_index =
            first_non_empty_env_u64(&MATRIX_ROOTSERVER_RAFT_COMMIT_INDEX_ENV_NAMES)?;
        let requested_read_index =
            first_non_empty_env_u64(&MATRIX_ROOTSERVER_RAFT_READ_INDEX_ENV_NAMES)?;
        if !require_bootstrap && requested_commit_index.is_none() && requested_read_index.is_none()
        {
            return Ok(None);
        }

        let before_status = self.store.root_raft_status(0);
        let target_commit_index = requested_commit_index.unwrap_or(before_status.commit_index);
        let target_read_index = requested_read_index.unwrap_or(before_status.read_index);
        let target_index = target_commit_index.max(target_read_index);
        if before_status.commit_index > target_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog bootstrap target is behind local state: target={} local={}",
                target_index, before_status.commit_index
            )));
        }
        let max_steps =
            first_non_empty_env_u64(&MATRIX_ROOTSERVER_RAFT_BOOTSTRAP_MAX_STEPS_ENV_NAMES)?
                .unwrap_or(65_536);
        let preferred_node_id = first_non_empty_env(&MATRIX_ROOTSERVER_NODE_ID_ENV_NAMES)?;
        if before_status.commit_index >= target_index {
            let after_status = self.store.root_raft_status(0);
            if after_status.read_index < target_read_index {
                return Err(ObjectError::PreconditionFailed(format!(
                    "root catalog bootstrap read index is below target: target={} local={}",
                    target_read_index, after_status.read_index
                )));
            }
            let report = RootCatalogBootstrapReport {
                target_commit_index,
                target_read_index,
                max_steps,
                applied_steps: 0,
                heartbeat_node_id: String::new(),
                before_status,
                after_status,
                last_metadata: None,
            };
            if let Some(expected) =
                first_non_empty_env(&MATRIX_ROOTSERVER_CATALOG_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
            {
                report.verify_expected_report_checksum(expected.as_str())?;
            }
            return Ok(Some(report));
        }
        let select_root_node = |store: &MatrixObjectStore| -> Result<RootNode, ObjectError> {
            let mut nodes: Vec<RootNode> = store
                .root_nodes()
                .into_iter()
                .filter(|node| {
                    node.service_kind == "rootserver"
                        && preferred_node_id
                            .as_ref()
                            .map_or(true, |node_id| &node.node_id == node_id)
                })
                .collect();
            nodes.sort_by(|lhs, rhs| lhs.node_id.cmp(&rhs.node_id));
            nodes
                .iter()
                .find(|node| node.online && !node.draining)
                .cloned()
                .or_else(|| nodes.first().cloned())
                .ok_or_else(|| {
                    ObjectError::PreconditionFailed(
                        "root catalog bootstrap requires a registered rootserver node".to_string(),
                    )
                })
        };

        let bootstrap_node = select_root_node(&self.store)?;
        let mut applied_steps = 0;
        let mut last_metadata = None;
        while self.store.root_raft_status(0).commit_index < target_index {
            if applied_steps >= max_steps {
                return Err(ObjectError::PreconditionFailed(
                    "root catalog bootstrap exceeded max steps before target commit/read index"
                        .to_string(),
                ));
            }
            if self.raft.commit_index() != self.store.last_applied_index() {
                return Err(ObjectError::PreconditionFailed(
                    "root catalog bootstrap requires raft/store commit alignment".to_string(),
                ));
            }
            let current = self
                .store
                .root_nodes()
                .into_iter()
                .find(|node| node.node_id == bootstrap_node.node_id)
                .ok_or_else(|| {
                    ObjectError::PreconditionFailed(format!(
                        "root catalog bootstrap node disappeared: {}",
                        bootstrap_node.node_id
                    ))
                })?;
            let now_ms = now_millis().max(current.last_heartbeat_ms.saturating_add(1));
            let metadata = self.propose_heartbeat_root_node(
                &current.node_id,
                current.used_bytes,
                current.capacity_bytes,
                current.draining,
                current.online,
                now_ms,
            )?;
            last_metadata = Some(metadata);
            applied_steps += 1;
        }

        let after_status = self.store.root_raft_status(0);
        if after_status.read_index < target_read_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog bootstrap read index is below target: target={} local={}",
                target_read_index, after_status.read_index
            )));
        }
        let report = RootCatalogBootstrapReport {
            target_commit_index,
            target_read_index,
            max_steps,
            applied_steps,
            heartbeat_node_id: bootstrap_node.node_id,
            before_status,
            after_status,
            last_metadata,
        };
        if let Some(expected) =
            first_non_empty_env(&MATRIX_ROOTSERVER_CATALOG_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
        {
            report.verify_expected_report_checksum(expected.as_str())?;
        }
        Ok(Some(report))
    }

    pub fn bootstrap_root_service_from_env_report(
        &mut self,
    ) -> Result<RootServiceBootstrapReport, ObjectError> {
        let before_status = self.store.root_raft_status(0);
        let catalog_report = self.bootstrap_root_catalog_from_env()?;
        let after_status = self.store.root_raft_status(0);
        let report = RootServiceBootstrapReport {
            catalog_report,
            before_status,
            after_status,
        };
        if let Some(expected) =
            first_non_empty_env(&MATRIX_ROOTSERVER_SERVICE_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
        {
            report.verify_expected_report_checksum(expected)?;
        }
        Ok(report)
    }
}
