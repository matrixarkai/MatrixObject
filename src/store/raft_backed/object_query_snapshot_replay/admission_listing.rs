// Admission and listing APIs are grouped by operation and consistency path.
include!("range_read_admission.rs");
include!("delete_discard_admission.rs");
include!("basic_object_listing.rs");
include!("linearizable_object_listing.rs");
include!("shard_object_listing.rs");
include!("proven_object_listing.rs");
