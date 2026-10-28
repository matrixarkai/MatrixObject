// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn can_accept_object_range_read_linearizable(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        self.can_accept_object_range_read_linearizable_fenced(
            bucket,
            key,
            offset,
            length,
            max_payload_bytes,
            now_ms,
            "",
        )
    }

    pub fn can_accept_object_range_read_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        let (options, readiness) = self.metadata_read_options_at_raft_position(
            "can accept object range read linearizable fenced",
            now_ms,
            0,
        )?;
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "object range read admission",
            expected_page_route_control_checksum,
        )?;
        self.store
            .can_accept_object_range_read_with_metadata_read(
                bucket,
                key,
                offset,
                length,
                max_payload_bytes,
                options,
            )
            .map(|(mut admission, _)| {
                admission.expected_route_control_checksum =
                    expected_page_route_control_checksum.to_string();
                stamp_object_range_admission_proof("CanReadObjectRange", &mut admission);
                (admission, readiness)
            })
    }
}
