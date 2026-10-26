// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_refresh_all_object_root_placements(
        &mut self,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_refresh_object_root_placements_page(
            None,
            max_objects,
            replica_count,
            update_time_ms,
        )
    }

    pub fn propose_refresh_object_root_placements_page(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RefreshAllObjectRootPlacements {
            marker,
            max_objects,
            replica_count,
            update_time_ms,
        })
    }

    pub fn propose_refresh_object_root_placements_page_report(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<PlacementRefreshRunReport, ObjectError> {
        let mut projected = self.store.clone();
        let report = projected.apply_refresh_object_root_placements_page(
            marker.clone(),
            max_objects,
            replica_count,
            update_time_ms,
        )?;
        let metadata = self.propose_refresh_object_root_placements_page(
            marker,
            max_objects,
            replica_count,
            update_time_ms,
        )?;
        Ok(PlacementRefreshRunReport { report, metadata })
    }

    pub fn propose_refresh_all_object_root_placements_linearizable(
        &mut self,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_refresh_object_root_placements_page_linearizable(
            None,
            max_objects,
            replica_count,
            update_time_ms,
        )
    }

    pub fn propose_refresh_all_object_root_placements_with_read_proof_linearizable(
        &mut self,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_refresh_object_root_placements_page_with_read_proof_linearizable_committed_at(
            None,
            max_objects,
            replica_count,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_refresh_object_root_placements_page_linearizable(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_root_read_index("refresh object root placements page")?;
        self.propose_refresh_object_root_placements_page(
            marker,
            max_objects,
            replica_count,
            update_time_ms,
        )
    }

    pub fn propose_refresh_object_root_placements_page_with_read_proof_linearizable(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_refresh_object_root_placements_page_with_read_proof_linearizable_committed_at(
            marker,
            max_objects,
            replica_count,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_refresh_object_root_placements_page_with_read_proof_linearizable_committed_at(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "refresh object root placements page",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_refresh_object_root_placements_page_linearizable(
            marker,
            max_objects,
            replica_count,
            update_time_ms,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "refresh object root placements page post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_refresh_object_root_placements_page_report_linearizable(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<PlacementRefreshRunReport, ObjectError> {
        self.ensure_root_read_index("refresh object root placements page report")?;
        self.propose_refresh_object_root_placements_page_report(
            marker,
            max_objects,
            replica_count,
            update_time_ms,
        )
    }

    pub fn propose_refresh_object_root_placements_page_report_with_read_proof_linearizable(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<PlacementRefreshRunReport>, ObjectError> {
        self.propose_refresh_object_root_placements_page_report_with_read_proof_linearizable_committed_at(
            marker,
            max_objects,
            replica_count,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_refresh_object_root_placements_page_report_with_route_proof_linearizable_fenced(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<PlacementRefreshRunReport>, ObjectError> {
        self.propose_refresh_object_root_placements_page_report_with_route_proof_linearizable_fenced_committed_at(
            marker,
            max_objects,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_refresh_object_root_placements_page_report_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<PlacementRefreshRunReport>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "refresh object root placements page report route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_refresh_object_root_placements_page_report_with_read_proof_linearizable_committed_at(
                marker,
                max_objects,
                replica_count,
                update_time_ms,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenRouteFencedMetadataRead {
            value: proven.value,
            proof: proven.proof,
            route_control_proof,
            expected_route_control_checksum: expected_checksum,
        })
    }

    pub fn propose_refresh_object_root_placements_page_report_with_read_proof_linearizable_committed_at(
        &mut self,
        marker: Option<ObjectId>,
        max_objects: usize,
        replica_count: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PlacementRefreshRunReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "refresh object root placements page report",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_refresh_object_root_placements_page_report_linearizable(
            marker,
            max_objects,
            replica_count,
            update_time_ms,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "refresh object root placements page report post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }
}
