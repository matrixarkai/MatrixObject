impl MatrixRootService {
    fn validate_leader_candidate(&self, node_id: &str, ttl_ms: u64) -> Result<(), ObjectError> {
        validate_node_id(node_id)?;
        if ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "root leader lease ttl must be positive".to_string(),
            ));
        }
        if ttl_ms > self.coordinator_config.session_ttl_ms {
            return Err(ObjectError::InvalidArgument(
                "root leader lease ttl exceeds configured coordinator session ttl".to_string(),
            ));
        }
        let node = self.nodes.get(node_id).ok_or_else(|| {
            ObjectError::NotFound("root leader candidate node missing".to_string())
        })?;
        if node.service_kind != "rootserver" || !node.online || node.draining {
            return Err(ObjectError::PreconditionFailed(
                "root leader candidate is not an active rootserver".to_string(),
            ));
        }
        if !is_embedded_coordinator_backend(&self.coordinator_config.backend)
            && !self
                .coordinator_config
                .endpoints
                .iter()
                .any(|endpoint| endpoint == &node.endpoint)
        {
            return Err(ObjectError::PreconditionFailed(
                "root leader candidate endpoint is outside configured coordinator endpoints"
                    .to_string(),
            ));
        }
        Ok(())
    }
}
