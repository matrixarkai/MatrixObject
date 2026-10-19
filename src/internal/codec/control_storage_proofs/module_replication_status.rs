// SPDX-License-Identifier: Apache-2.0
include!("module_replication_status/replication_checksum.rs");
include!("module_replication_status/replication_proof.rs");
include!("module_replication_status/status_verification.rs");
include!("module_replication_status/checksum_identity.rs");
include!("module_replication_status/checksum_root_storage.rs");
include!("module_replication_status/checksum_root_metadata.rs");
include!("module_replication_status/checksum_metaserver.rs");
include!("module_replication_status/checksum_storage.rs");
include!("module_replication_status/checksum_health.rs");

pub(crate) fn objectstore_module_status_checksum(status: &ObjectStoreModuleStatus) -> String {
    let mut material = Vec::new();
    append_module_identity(&mut material, status);
    append_root_storage_status(&mut material, status);
    append_root_metadata_status(&mut material, status);
    append_metaserver_status(&mut material, status);
    append_storage_status(&mut material, status);
    append_health_fingerprints(&mut material, status);
    etag(&material)
}
