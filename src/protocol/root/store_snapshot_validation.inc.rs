include!("store_snapshot_validation/configuration_and_policies.rs");
include!("store_snapshot_validation/raft_and_checksum.rs");
include!("store_snapshot_validation/segments_and_objects.rs");
include!("store_snapshot_validation/multipart_uploads.rs");
include!("store_snapshot_validation/changefeed_state.rs");

impl StoreSnapshot {
    pub fn validate(&self) -> Result<(), ObjectError> {
        self.validate_configuration_and_policies()?;
        self.validate_raft_and_checksum()?;
        self.validate_slabs_and_objects()?;
        self.validate_multipart_uploads()?;
        self.validate_changefeed_state()
    }
}
