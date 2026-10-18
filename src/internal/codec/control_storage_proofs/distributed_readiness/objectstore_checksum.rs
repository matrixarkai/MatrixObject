include!("objectstore_checksum/identity.rs");
include!("objectstore_checksum/metadata.rs");
include!("objectstore_checksum/metadata_control_proof.rs");
include!("objectstore_checksum/page.rs");
include!("objectstore_checksum/control_plane.rs");
include!("objectstore_checksum/root_pageserver.rs");
include!("objectstore_checksum/root_chunkserver.rs");
include!("objectstore_checksum/root_metadata.rs");
include!("objectstore_checksum/storage.rs");

pub(crate) fn objectstore_distributed_readiness_checksum(
    report: &ObjectStoreDistributedReadiness,
) -> String {
    let mut material = Vec::new();
    append_readiness_identity(&mut material, report);
    append_metadata_readiness(&mut material, report);
    append_metadata_control_proof(&mut material, report);
    append_page_readiness(&mut material, report);
    append_control_plane_readiness(&mut material, report);
    append_root_pageserver_topology(&mut material, report);
    append_root_chunkserver_topology(&mut material, report);
    append_root_metadata_topology(&mut material, report);
    append_storage_readiness(&mut material, report);
    etag(&material)
}
