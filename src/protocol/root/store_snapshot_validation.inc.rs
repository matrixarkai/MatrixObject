// SPDX-License-Identifier: Apache-2.0
include!("store_snapshot_validation/configuration.rs");
include!("store_snapshot_validation/bucket_policies.rs");
include!("store_snapshot_validation/raft_state.rs");
include!("store_snapshot_validation/snapshot_checksum.rs");
include!("store_snapshot_validation/chunks.rs");
include!("store_snapshot_validation/objects.rs");
include!("store_snapshot_validation/multipart_uploads.rs");
include!("store_snapshot_validation/changefeed_state.rs");

impl StoreSnapshot {
    pub fn validate(&self) -> Result<(), ObjectError> {
        self.validate_configuration()?;
        self.meta_snapshot.validate()?;
        self.root_snapshot.validate()?;
        self.validate_bucket_policies()?;
        self.validate_raft_state()?;
        self.validate_snapshot_checksum()?;
        self.validate_chunks()?;
        self.validate_objects()?;
        self.validate_multipart_uploads()?;
        self.validate_changefeed_state()
    }
}
