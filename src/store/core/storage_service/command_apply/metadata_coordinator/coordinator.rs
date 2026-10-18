impl MatrixObjectStore {
    fn apply_coordinator_metadata_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::ConfigureMetaCoordinator { config } => {
                let config = self.configure_metadata_coordinator_config(config)?;
                Ok(metadata_coordinator_config_metadata(
                    &config,
                    "metaserver/coordinator-config",
                    self.last_applied_index(),
                ))
            }
            ObjectCommand::ApplyPreparedMetaCoordinator { prepared } => {
                let receipt = self.apply_prepared_metadata_coordinator_config(prepared)?;
                Ok(metadata_coordinator_config_metadata(
                    &receipt.config,
                    if receipt.deduped {
                        "metaserver/prepared-coordinator-config-deduped"
                    } else {
                        "metaserver/prepared-coordinator-config"
                    },
                    receipt.commit_index,
                ))
            }
            ObjectCommand::RegisterMetaCoordinatorSession {
                session_id,
                owner,
                endpoint,
                ttl_ms,
                now_ms,
            } => {
                let session = self.register_meta_coordinator_session(
                    &session_id,
                    &owner,
                    &endpoint,
                    ttl_ms,
                    now_ms,
                )?;
                Ok(coordinator_session_metadata(&session, "metaserver/session"))
            }
            ObjectCommand::RegisterMetaCoordinatorSessionIdempotent {
                session_id,
                owner,
                endpoint,
                ttl_ms,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.register_meta_coordinator_session_idempotent(
                    &session_id,
                    &owner,
                    &endpoint,
                    ttl_ms,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(coordinator_session_metadata(
                    &receipt.session,
                    "metaserver/session-idempotent",
                ))
            }
            ObjectCommand::RenewMetaCoordinatorSession {
                session_id,
                owner,
                epoch,
                ttl_ms,
                now_ms,
            } => {
                let session = self.renew_meta_coordinator_session(
                    &session_id,
                    &owner,
                    epoch,
                    ttl_ms,
                    now_ms,
                )?;
                Ok(coordinator_session_metadata(
                    &session,
                    "metaserver/session-renew",
                ))
            }
            ObjectCommand::RenewMetaCoordinatorSessionIdempotent {
                session_id,
                owner,
                epoch,
                ttl_ms,
                now_ms,
                idempotency_key,
            } => {
                let receipt = self.renew_meta_coordinator_session_idempotent(
                    &session_id,
                    &owner,
                    epoch,
                    ttl_ms,
                    now_ms,
                    &idempotency_key,
                )?;
                Ok(coordinator_session_metadata(
                    &receipt.session,
                    "metaserver/session-renew-idempotent",
                ))
            }
            ObjectCommand::ReleaseMetaCoordinatorSession {
                session_id,
                owner,
                epoch,
            } => {
                let session = self.release_meta_coordinator_session(&session_id, &owner, epoch)?;
                Ok(coordinator_session_metadata(
                    &session,
                    "metaserver/session-release",
                ))
            }
            ObjectCommand::ReleaseMetaCoordinatorSessionIdempotent {
                session_id,
                owner,
                epoch,
                idempotency_key,
            } => {
                let receipt = self.release_meta_coordinator_session_idempotent(
                    &session_id,
                    &owner,
                    epoch,
                    &idempotency_key,
                )?;
                Ok(coordinator_session_metadata(
                    &receipt.session,
                    "metaserver/session-release-idempotent",
                ))
            }
            ObjectCommand::ApplyPreparedMetaCoordinatorSession { prepared } => {
                let receipt = self.apply_prepared_meta_coordinator_session(prepared)?;
                Ok(coordinator_session_metadata(
                    &receipt.session,
                    if receipt.released {
                        "metaserver/prepared-session-release"
                    } else if receipt.operation == "renew" {
                        "metaserver/prepared-session-renew"
                    } else {
                        "metaserver/prepared-session-register"
                    },
                ))
            }
            ObjectCommand::ExpireMetaCoordinatorSessions {
                now_ms,
                limit,
                include_fenced,
            } => {
                let expired = self.expire_meta_coordinator_sessions_including_fenced(
                    now_ms,
                    limit,
                    include_fenced,
                )?;
                Ok(ObjectMetadata {
                    object_id: ObjectId::new("_metacoord", "expired"),
                    version: 0,
                    length: expired.len() as u64,
                    etag: String::new().into(),
                    content_type: "metaserver/session-expire".to_string().into(),
                    user_metadata: BTreeMap::new().into(),
                    storage_class: k_standard_storage_class().to_string().into(),
                    restore_until_ms: 0,
                    tags: BTreeMap::new().into(),
                    legal_hold: false,
                    legal_hold_update_time_ms: 0,
                    retention_until_ms: 0,
                    update_time_ms: now_ms,
                    extents: Vec::new().into(),
                    deleted: false,
                })
            }
            ObjectCommand::ApplyPreparedMetaCoordinatorSessionExpiry { prepared } => {
                let now_ms = prepared.now_ms;
                let expired = self
                    .meta_service
                    .apply_prepared_coordinator_session_expiry(prepared)?;
                if !expired.is_empty() {
                    self.invalidate_metadata_route_cache();
                }
                Ok(metadata_control_metadata(
                    "_metacoord",
                    "prepared-expired",
                    expired.len() as u64,
                    expired
                        .iter()
                        .map(|session| session.epoch)
                        .max()
                        .unwrap_or(0),
                    "metaserver/prepared-session-expire",
                    now_ms,
                ))
            }
            _ => unreachable!("command classified outside Coordinator"),
        }
    }
}
