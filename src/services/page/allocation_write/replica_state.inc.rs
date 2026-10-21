// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    fn replica_active(&self, replica_index: usize) -> bool {
        !self.quarantined_replicas.contains(&replica_index)
            && !self.replica_endpoint_disabled(replica_index)
    }

    fn replica_writable(&self, replica_index: usize) -> bool {
        self.replica_active(replica_index)
            && !self.replicas[replica_index].is_frozen()
            && !self.replicas[replica_index].is_draining()
    }
}
