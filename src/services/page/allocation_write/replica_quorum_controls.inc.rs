impl MatrixChunkService {
    pub fn write_quorum(&self) -> usize {
        self.write_quorum
    }

    pub fn read_quorum(&self) -> usize {
        self.read_quorum
    }

    pub fn configure_write_quorum(&mut self, write_quorum: usize) -> Result<(), ObjectError> {
        self.validate_runtime_quorum(write_quorum, self.read_quorum)?;
        self.write_quorum = write_quorum;
        Ok(())
    }

    pub fn configure_read_quorum(&mut self, read_quorum: usize) -> Result<(), ObjectError> {
        self.validate_runtime_quorum(self.write_quorum, read_quorum)?;
        self.read_quorum = read_quorum;
        Ok(())
    }

    pub fn configure_quorums(
        &mut self,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Result<(), ObjectError> {
        self.validate_runtime_quorum(write_quorum, read_quorum)?;
        self.write_quorum = write_quorum;
        self.read_quorum = read_quorum;
        Ok(())
    }

    fn validate_runtime_quorum(
        &self,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Result<(), ObjectError> {
        if write_quorum == 0 || write_quorum > self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "write_quorum must be 1..replica_count".to_string(),
            ));
        }
        if read_quorum == 0 || read_quorum > self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "read_quorum must be 1..replica_count".to_string(),
            ));
        }
        let active_replicas = self
            .replicas
            .len()
            .saturating_sub(self.quarantined_replicas.len())
            .saturating_sub(self.disabled_chunk_endpoints.len());
        let writable_replicas = self
            .replicas
            .iter()
            .enumerate()
            .filter(|(index, _)| self.replica_writable(*index))
            .count();
        if write_quorum > writable_replicas {
            return Err(ObjectError::PreconditionFailed(
                "block write quorum exceeds writable replica count".to_string(),
            ));
        }
        if read_quorum > active_replicas {
            return Err(ObjectError::PreconditionFailed(
                "block quorum exceeds active replica count".to_string(),
            ));
        }
        Ok(())
    }

    pub fn quarantined_replica_indices(&self) -> Vec<usize> {
        self.quarantined_replicas.iter().copied().collect()
    }

    pub fn frozen_replica_indices(&self) -> Vec<usize> {
        self.replicas
            .iter()
            .enumerate()
            .filter_map(|(index, replica)| replica.is_frozen().then_some(index))
            .collect()
    }

    pub fn set_replica_frozen(
        &mut self,
        replica_index: usize,
        frozen: bool,
    ) -> Result<(), ObjectError> {
        if replica_index >= self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "replica index is out of range".to_string(),
            ));
        }
        if frozen && !self.replicas[replica_index].is_frozen() {
            let writable_after = self
                .replicas
                .iter()
                .enumerate()
                .filter(|(index, replica)| {
                    *index != replica_index && self.replica_active(*index) && !replica.is_frozen()
                })
                .count();
            if writable_after < self.write_quorum {
                return Err(ObjectError::PreconditionFailed(
                    "replica freeze would violate block write quorum".to_string(),
                ));
            }
        }
        self.replicas[replica_index].set_frozen(frozen);
        Ok(())
    }

    pub fn set_replica_quarantine(
        &mut self,
        replica_index: usize,
        quarantined: bool,
    ) -> Result<(), ObjectError> {
        if replica_index >= self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "replica index is out of range".to_string(),
            ));
        }
        if quarantined {
            let active_after = self
                .replicas
                .len()
                .saturating_sub(self.quarantined_replicas.len())
                .saturating_sub(self.disabled_chunk_endpoints.len())
                .saturating_sub(if self.quarantined_replicas.contains(&replica_index) {
                    0
                } else {
                    1
                });
            if active_after < self.write_quorum || active_after < self.read_quorum {
                return Err(ObjectError::PreconditionFailed(
                    "replica quarantine would violate block quorum".to_string(),
                ));
            }
            let writable_after = self
                .replicas
                .iter()
                .enumerate()
                .filter(|(index, replica)| {
                    *index != replica_index && self.replica_active(*index) && !replica.is_frozen()
                })
                .count();
            if writable_after < self.write_quorum {
                return Err(ObjectError::PreconditionFailed(
                    "replica quarantine would violate block write quorum".to_string(),
                ));
            }
            self.quarantined_replicas.insert(replica_index);
        } else {
            self.quarantined_replicas.remove(&replica_index);
        }
        Ok(())
    }
}
