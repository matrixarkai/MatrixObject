// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
include!("service_catalog_root_bootstrap.rs");
include!("service_catalog_gateway_liveness.rs");
include!("service_catalog_pageserver_registration.rs");
include!("service_catalog_pageserver_heartbeat.rs");
include!("service_catalog_pageserver_offline.rs");
include!("service_catalog_chunkserver_registration.rs");
include!("service_catalog_chunkserver_heartbeat.rs");
include!("service_catalog_chunkserver_offline.rs");
