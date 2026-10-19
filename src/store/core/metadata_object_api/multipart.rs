// SPDX-License-Identifier: Apache-2.0
// Multipart APIs are grouped by lifecycle operation and listing contract.
include!("multipart_create.rs");
include!("multipart_upload_part.rs");
include!("multipart_complete.rs");
include!("multipart_abort.rs");
include!("multipart_upload_listing.rs");
include!("multipart_metadata_listing.rs");
include!("multipart_part_listing.rs");
