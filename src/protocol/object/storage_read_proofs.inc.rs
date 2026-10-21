// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageReadProof {
    pub requested_storage_read_index: u64,
    pub requested_storage_commit_index: u64,
    pub storage_applied_index: u64,
    pub storage_read_index_lag: u64,
    pub storage_commit_index_lag: u64,
    pub control_kind: String,
    pub control_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenStorageRead<T> {
    pub value: T,
    pub proof: StorageReadProof,
}

impl StorageReadProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            (
                "requested_storage_read_index",
                self.requested_storage_read_index,
            ),
            (
                "requested_storage_commit_index",
                self.requested_storage_commit_index,
            ),
            ("storage_applied_index", self.storage_applied_index),
            ("storage_read_index_lag", self.storage_read_index_lag),
            ("storage_commit_index_lag", self.storage_commit_index_lag),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}control_kind"),
            self.control_kind.clone(),
        );
        fields.insert(
            format!("{field_prefix}control_checksum"),
            self.control_checksum.clone(),
        );
        fields
    }
}

impl<T> ProvenStorageRead<T> {
    pub fn proof_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        self.proof.to_control_fields(field_prefix)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPageRouteFencedStorageRead<T> {
    pub value: T,
    pub proof: StorageReadProof,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl<T> ProvenPageRouteFencedStorageRead<T> {
    pub fn into_storage_read(self) -> ProvenStorageRead<T> {
        ProvenStorageRead {
            value: self.value,
            proof: self.proof,
        }
    }

    pub fn proof_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control_proof.")),
        );
        fields.insert(
            format!("{field_prefix}expected_route_control_checksum"),
            self.expected_route_control_checksum.clone(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenChunkControlFencedStorageRead<T> {
    pub value: T,
    pub proof: StorageReadProof,
    pub control_proof: ReplicaServiceControlProof,
    pub expected_control_checksum: String,
}

impl<T> ProvenChunkControlFencedStorageRead<T> {
    pub fn into_storage_read(self) -> ProvenStorageRead<T> {
        ProvenStorageRead {
            value: self.value,
            proof: self.proof,
        }
    }

    pub fn proof_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields.extend(
            self.control_proof
                .to_control_fields(&format!("{field_prefix}chunk_control_proof.")),
        );
        fields.insert(
            format!("{field_prefix}expected_control_checksum"),
            self.expected_control_checksum.clone(),
        );
        fields
    }
}

