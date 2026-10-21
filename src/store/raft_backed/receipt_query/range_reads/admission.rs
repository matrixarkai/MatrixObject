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
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
            .map(|(mut admission, readiness)| {
                admission.expected_route_control_checksum =
                    expected_page_route_control_checksum.to_string();
                stamp_object_range_admission_proof("CanReadObjectRange", &mut admission);
                (admission, readiness)
            })
    }
}
