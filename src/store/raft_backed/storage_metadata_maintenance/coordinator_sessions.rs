// SPDX-License-Identifier: Apache-2.0
// Metadata coordinator sessions are grouped by configuration, mutation, and expiry phase.
include!("coordinator_configuration.rs");
include!("coordinator_configuration_prepare.rs");
include!("coordinator_session_mutations.rs");
include!("coordinator_session_prepare.rs");
include!("coordinator_expiry_prepare.rs");
include!("coordinator_expiry_envelopes.rs");
include!("coordinator_expiry_propose.rs");
