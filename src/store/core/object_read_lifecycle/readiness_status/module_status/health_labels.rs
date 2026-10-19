// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ModuleHealthLabels {
    root: String,
    metadata: String,
    page: String,
    chunk: String,
    cache: String,
    capacity: String,
    repair: String,
    compression: String,
    erasure_coding: String,
}

impl ModuleHealthLabels {
    fn derive(
        readiness: &ObjectStoreDistributedReadiness,
        min_capacity_free_bytes: u64,
    ) -> Self {
        let metadata = if readiness.metadata_ready {
            "ready".to_string()
        } else if !readiness.metadata_quorum_ready {
            "quorum_unavailable".to_string()
        } else if !readiness.metadata_caught_up {
            "apply_lag".to_string()
        } else {
            readiness.metadata.reason.clone()
        };
        let page = if readiness.page_ready {
            "ready".to_string()
        } else {
            readiness.page.reason.clone()
        };
        let chunk = if readiness.storage_writable_replicas >= readiness.page.write_quorum {
            "ready"
        } else if readiness.storage_readable_replicas >= readiness.page.read_quorum {
            "read_only"
        } else if readiness.storage_active_replicas == 0 {
            "none"
        } else {
            "degraded"
        }
        .to_string();
        let cache = if readiness.storage_read_cache_pressure {
            "pressure"
        } else if readiness.storage_read_cache_entries == 0 {
            "empty"
        } else {
            "ready"
        }
        .to_string();
        let capacity = if readiness.storage_capacity_pressure_replicas != 0
            || readiness.storage_total_capacity_free_bytes < min_capacity_free_bytes
        {
            "pressure"
        } else {
            "ready"
        }
        .to_string();
        let repair = if readiness.placement_degraded_objects != 0
            || readiness.storage_cumulative_repair_degraded_chunks
                > readiness.storage_cumulative_repaired_replicas
            || readiness.storage_cumulative_failed_repairs != 0
        {
            "needed"
        } else {
            "ready"
        }
        .to_string();
        let compression = if readiness.storage_compressed_chunk_count == 0 {
            "idle"
        } else {
            "active"
        }
        .to_string();
        let erasure_coding = if readiness.storage_ec_reconstruction_failures != 0 {
            "repair_failures"
        } else if readiness.storage_ec_reconstructed_reads != 0 {
            "reconstructing"
        } else {
            "ready"
        }
        .to_string();
        Self {
            root: readiness.root_status.clone(),
            metadata,
            page,
            chunk,
            cache,
            capacity,
            repair,
            compression,
            erasure_coding,
        }
    }
}
