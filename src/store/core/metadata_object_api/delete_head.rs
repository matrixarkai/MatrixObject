// SPDX-License-Identifier: Apache-2.0
// Object metadata APIs are grouped by mutation and read ownership.
include!("placement_put.rs");
include!("object_delete.rs");
include!("batch_delete.rs");
include!("prefix_delete.rs");
include!("object_head.rs");
include!("versioned_head.rs");
