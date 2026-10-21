// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn object_gc_report_linearizable(
        &self,
        now_ms: u64,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.object_gc_report_with_discard_budget_linearizable(0, now_ms)
    }

    pub fn object_gc_report_with_discard_budget_linearizable(
        &self,
        max_discard_bytes: u64,
        now_ms: u64,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.object_gc_report_with_discard_policy_linearizable(1, max_discard_bytes, now_ms)
    }

    pub fn object_gc_report_with_discard_policy_linearizable(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        now_ms: u64,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.object_gc_report_with_discard_policy_linearizable_fenced(
            min_discard_bytes,
            max_discard_bytes,
            now_ms,
            "",
        )
    }

    pub fn object_gc_report_linearizable_fenced(
        &self,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.object_gc_report_with_discard_budget_linearizable_fenced(
            0,
            now_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn object_gc_report_with_discard_budget_linearizable_fenced(
        &self,
        max_discard_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.object_gc_report_with_discard_policy_linearizable_fenced(
            1,
            max_discard_bytes,
            now_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn object_gc_report_with_discard_policy_linearizable_fenced(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "object gc report",
            expected_page_route_control_checksum,
        )?;
        self.store
            .gc_report_with_discard_policy_and_metadata_read_fenced(
                min_discard_bytes,
                max_discard_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
                (!expected_page_route_control_checksum.is_empty())
                    .then_some(expected_page_route_control_checksum),
            )
    }

    pub fn object_gc_report_linearizable_proof_fenced(
        &self,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.object_gc_report_with_discard_budget_linearizable_proof_fenced(
            0,
            now_ms,
            expected_page_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn object_gc_report_with_discard_budget_linearizable_proof_fenced(
        &self,
        max_discard_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.object_gc_report_with_discard_policy_linearizable_proof_fenced(
            1,
            max_discard_bytes,
            now_ms,
            expected_page_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn object_gc_report_with_discard_policy_linearizable_proof_fenced(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        let (report, readiness) = self.object_gc_report_with_discard_policy_linearizable_fenced(
            min_discard_bytes,
            max_discard_bytes,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        report.verify_expected_proof_checksum(expected_object_gc_report_proof_checksum)?;
        Ok((report, readiness))
    }

    pub fn object_gc_report_with_read_proof_linearizable(
        &self,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<GcReport>, ObjectError> {
        self.object_gc_report_with_discard_budget_and_read_proof_linearizable(0, now_ms)
    }

    pub fn object_gc_report_with_discard_budget_and_read_proof_linearizable(
        &self,
        max_discard_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<GcReport>, ObjectError> {
        self.object_gc_report_with_discard_policy_and_read_proof_linearizable(
            1,
            max_discard_bytes,
            now_ms,
        )
    }

    pub fn object_gc_report_with_discard_policy_and_read_proof_linearizable(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<GcReport>, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (report, readiness) = self.object_gc_report_with_discard_policy_linearizable_fenced(
            min_discard_bytes,
            max_discard_bytes,
            now_ms,
            &route_checksum,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn object_gc_report_with_read_proof_linearizable_proof_fenced(
        &self,
        now_ms: u64,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<GcReport>, ObjectError> {
        self.object_gc_report_with_discard_budget_and_read_proof_linearizable_proof_fenced(
            0,
            now_ms,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn object_gc_report_with_discard_budget_and_read_proof_linearizable_proof_fenced(
        &self,
        max_discard_bytes: u64,
        now_ms: u64,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<GcReport>, ObjectError> {
        self.object_gc_report_with_discard_policy_and_read_proof_linearizable_proof_fenced(
            1,
            max_discard_bytes,
            now_ms,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn object_gc_report_with_discard_policy_and_read_proof_linearizable_proof_fenced(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        now_ms: u64,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<GcReport>, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (report, readiness) = self
            .object_gc_report_with_discard_policy_linearizable_proof_fenced(
                min_discard_bytes,
                max_discard_bytes,
                now_ms,
                &route_checksum,
                expected_object_gc_report_proof_checksum,
            )?;
        Ok(proven_metadata_read(report, &readiness))
    }
}
