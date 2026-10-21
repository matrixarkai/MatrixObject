// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataRouteCacheEntry {
    /// Shared, because every key resolving to one shard resolves to the same
    /// route: the shard's four strings, the owner session's three, the
    /// preferred endpoint and the fallbacks. One allocation per distinct route
    /// rather than one per cached key.
    pub route: std::sync::Arc<MetadataShardRoute>,
    pub topology_epoch: u64,
    /// The three checksums below are store-wide: every entry created in one
    /// generation holds the same digests, so the allocations are shared. They
    /// stay per entry because `status_audit` compares each against the current
    /// value to report WHICH entries are stale, and entries inserted either
    /// side of a session change legitimately differ.
    pub topology_checksum: SharedText,
    pub shard_topology_checksum: SharedText,
    pub session_checksum: SharedText,
    pub expire_time_ms: u64,
    pub last_access_sequence: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataRouteCacheStatus {
    pub entries: u64,
    pub capacity: u64,
    pub ttl_ms: u64,
    pub hits: u64,
    pub misses: u64,
    pub invalidations: u64,
    pub expired_invalidations: u64,
    pub capacity_evictions: u64,
    pub pressure: bool,
    pub topology_epoch: u64,
    pub topology_checksum: String,
    pub shard_topology_checksum: String,
    pub session_checksum: String,
    pub fresh_entries: u64,
    pub expired_entries: u64,
    pub stale_topology_entries: u64,
    pub stale_session_entries: u64,
    pub stale_entries: u64,
    pub fresh: bool,
}

impl MetadataRouteCacheStatus {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("entries", self.entries),
            ("capacity", self.capacity),
            ("ttl_ms", self.ttl_ms),
            ("hits", self.hits),
            ("misses", self.misses),
            ("invalidations", self.invalidations),
            ("expired_invalidations", self.expired_invalidations),
            ("capacity_evictions", self.capacity_evictions),
            ("topology_epoch", self.topology_epoch),
            ("fresh_entries", self.fresh_entries),
            ("expired_entries", self.expired_entries),
            ("stale_topology_entries", self.stale_topology_entries),
            ("stale_session_entries", self.stale_session_entries),
            ("stale_entries", self.stale_entries),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}pressure"), self.pressure.to_string());
        fields.insert(
            format!("{field_prefix}topology_checksum"),
            self.topology_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}shard_topology_checksum"),
            self.shard_topology_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}session_checksum"),
            self.session_checksum.clone(),
        );
        fields.insert(format!("{field_prefix}fresh"), self.fresh.to_string());
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataRouteCacheAudit {
    pub status: MetadataRouteCacheStatus,
    pub entries_before: u64,
    pub entries_after: u64,
    pub evicted_entries: u64,
    pub eviction_requested: bool,
    pub expected_metadata_shard_topology_epoch: Option<u64>,
    pub expected_metadata_shard_topology_checksum: String,
    pub expected_metadata_coordinator_session_checksum: String,
    pub stale_samples: Vec<MetadataRouteCacheStaleSample>,
}

impl MetadataRouteCacheAudit {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .status
            .to_control_fields(&format!("{field_prefix}status."));
        for (name, value) in [
            ("entries_before", self.entries_before),
            ("entries_after", self.entries_after),
            ("evicted_entries", self.evicted_entries),
            ("stale_sample_count", self.stale_samples.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}eviction_requested"),
            self.eviction_requested.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expected_metadata_shard_topology_epoch_present"),
            self.expected_metadata_shard_topology_epoch
                .is_some()
                .to_string(),
        );
        if let Some(expected_epoch) = self.expected_metadata_shard_topology_epoch {
            fields.insert(
                format!("{field_prefix}expected_metadata_shard_topology_epoch"),
                expected_epoch.to_string(),
            );
        }
        fields.insert(
            format!("{field_prefix}expected_metadata_shard_topology_checksum"),
            self.expected_metadata_shard_topology_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_metadata_coordinator_session_checksum"),
            self.expected_metadata_coordinator_session_checksum.clone(),
        );
        for (index, sample) in self.stale_samples.iter().enumerate() {
            fields.extend(sample.to_control_fields(&format!("{field_prefix}stale.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataRouteCacheStaleSample {
    pub key: String,
    pub shard_id: String,
    pub reason: String,
    pub expire_time_ms: u64,
    pub entry_topology_checksum: String,
    pub entry_session_checksum: String,
    pub current_topology_checksum: String,
    pub current_session_checksum: String,
}

impl MetadataRouteCacheStaleSample {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        fields.insert(format!("{field_prefix}shard_id"), self.shard_id.clone());
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        fields.insert(
            format!("{field_prefix}expire_time_ms"),
            self.expire_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}entry_topology_checksum"),
            self.entry_topology_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}entry_session_checksum"),
            self.entry_session_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}current_topology_checksum"),
            self.current_topology_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}current_session_checksum"),
            self.current_session_checksum.clone(),
        );
        fields
    }
}
