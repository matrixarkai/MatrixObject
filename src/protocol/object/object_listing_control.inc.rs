// SPDX-License-Identifier: Apache-2.0
impl ObjectListPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        if let Some(delimiter) = &self.delimiter {
            fields.insert(format!("{field_prefix}delimiter"), delimiter.clone());
        }
        if let Some(marker) = &self.marker {
            fields.insert(format!("{field_prefix}marker"), marker.clone());
        }
        if let Some(next_marker) = &self.next_marker {
            fields.insert(format!("{field_prefix}next_marker"), next_marker.clone());
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}object_count"),
            self.objects.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}common_prefix_count"),
            self.common_prefixes.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}scanned_objects"),
            self.scanned_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}emitted_objects"),
            self.emitted_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}emitted_common_prefixes"),
            self.emitted_common_prefixes.to_string(),
        );
        append_object_list_page_control_fields(self, field_prefix, &mut fields);
        for (index, object) in self.objects.iter().enumerate() {
            fields.extend(object.to_control_fields(&format!("{field_prefix}object.{index}.")));
        }
        for (index, common_prefix) in self.common_prefixes.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}common_prefix.{index}"),
                common_prefix.clone(),
            );
        }
        fields
    }
}

impl ObjectVersionPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        if let Some(marker) = self.marker {
            fields.insert(format!("{field_prefix}marker"), marker.to_string());
        }
        if let Some(next_marker) = self.next_version_marker {
            fields.insert(
                format!("{field_prefix}next_marker"),
                next_marker.to_string(),
            );
            fields.insert(
                format!("{field_prefix}next_version_marker"),
                next_marker.to_string(),
            );
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}version_count"),
            self.versions.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}requested_versions"),
            self.requested_versions.to_string(),
        );
        fields.insert(
            format!("{field_prefix}emitted_versions"),
            self.emitted_versions.to_string(),
        );
        append_object_list_page_control_fields(self, field_prefix, &mut fields);
        for (index, version) in self.versions.iter().enumerate() {
            fields.extend(version.to_control_fields(&format!("{field_prefix}version.{index}.")));
        }
        fields
    }
}

impl BucketObjectVersionPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        if let Some(delimiter) = &self.delimiter {
            fields.insert(format!("{field_prefix}delimiter"), delimiter.clone());
        }
        if let Some(key_marker) = &self.key_marker {
            fields.insert(format!("{field_prefix}key_marker"), key_marker.clone());
        }
        if let Some(version_marker) = self.version_marker {
            fields.insert(
                format!("{field_prefix}version_marker"),
                version_marker.to_string(),
            );
        }
        if let Some(next_key_marker) = &self.next_key_marker {
            fields.insert(
                format!("{field_prefix}next_key_marker"),
                next_key_marker.clone(),
            );
        }
        if let Some(next_version_marker) = self.next_version_marker {
            fields.insert(
                format!("{field_prefix}next_version_marker"),
                next_version_marker.to_string(),
            );
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}version_count"),
            self.versions.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}common_prefix_count"),
            self.common_prefixes.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}scanned_keys"),
            self.scanned_keys.to_string(),
        );
        fields.insert(
            format!("{field_prefix}emitted_versions"),
            self.emitted_versions.to_string(),
        );
        fields.insert(
            format!("{field_prefix}emitted_common_prefixes"),
            self.emitted_common_prefixes.to_string(),
        );
        append_object_list_page_control_fields(self, field_prefix, &mut fields);
        for (index, version) in self.versions.iter().enumerate() {
            fields.extend(version.to_control_fields(&format!("{field_prefix}version.{index}.")));
        }
        for (index, common_prefix) in self.common_prefixes.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}common_prefix.{index}"),
                common_prefix.clone(),
            );
        }
        fields
    }
}

impl ObjectTagPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}tag_key"), self.tag_key.clone());
        fields.insert(format!("{field_prefix}tag_value"), self.tag_value.clone());
        if let Some(marker) = &self.marker {
            fields.insert(format!("{field_prefix}marker"), marker.clone());
        }
        if let Some(next_marker) = &self.next_marker {
            fields.insert(format!("{field_prefix}next_marker"), next_marker.clone());
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}object_count"),
            self.objects.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}scanned_objects"),
            self.scanned_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}emitted_objects"),
            self.emitted_objects.to_string(),
        );
        append_object_list_page_control_fields(self, field_prefix, &mut fields);
        for (index, object) in self.objects.iter().enumerate() {
            fields.extend(object.to_control_fields(&format!("{field_prefix}object.{index}.")));
        }
        fields
    }
}

trait ObjectListPageControl {
    fn payload_bytes(&self) -> u64;
    fn max_payload_bytes(&self) -> u64;
    fn read_proof(&self) -> &ObjectReadProof;
    fn object_list_page_proof_format(&self) -> &str;
    fn object_list_page_proof_checksum(&self) -> &str;
    fn expected_object_list_page_proof_checksum(&self) -> &str;
}

impl ObjectListPageControl for ObjectListPage {
    fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    fn max_payload_bytes(&self) -> u64 {
        self.max_payload_bytes
    }

    fn read_proof(&self) -> &ObjectReadProof {
        &self.read_proof
    }

    fn object_list_page_proof_format(&self) -> &str {
        &self.object_list_page_proof_format
    }

    fn object_list_page_proof_checksum(&self) -> &str {
        &self.object_list_page_proof_checksum
    }

    fn expected_object_list_page_proof_checksum(&self) -> &str {
        &self.expected_object_list_page_proof_checksum
    }
}

impl ObjectListPageControl for ObjectVersionPage {
    fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    fn max_payload_bytes(&self) -> u64 {
        self.max_payload_bytes
    }

    fn read_proof(&self) -> &ObjectReadProof {
        &self.read_proof
    }

    fn object_list_page_proof_format(&self) -> &str {
        &self.object_list_page_proof_format
    }

    fn object_list_page_proof_checksum(&self) -> &str {
        &self.object_list_page_proof_checksum
    }

    fn expected_object_list_page_proof_checksum(&self) -> &str {
        &self.expected_object_list_page_proof_checksum
    }
}

impl ObjectListPageControl for BucketObjectVersionPage {
    fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    fn max_payload_bytes(&self) -> u64 {
        self.max_payload_bytes
    }

    fn read_proof(&self) -> &ObjectReadProof {
        &self.read_proof
    }

    fn object_list_page_proof_format(&self) -> &str {
        &self.object_list_page_proof_format
    }

    fn object_list_page_proof_checksum(&self) -> &str {
        &self.object_list_page_proof_checksum
    }

    fn expected_object_list_page_proof_checksum(&self) -> &str {
        &self.expected_object_list_page_proof_checksum
    }
}

impl ObjectListPageControl for ObjectTagPage {
    fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    fn max_payload_bytes(&self) -> u64 {
        self.max_payload_bytes
    }

    fn read_proof(&self) -> &ObjectReadProof {
        &self.read_proof
    }

    fn object_list_page_proof_format(&self) -> &str {
        &self.object_list_page_proof_format
    }

    fn object_list_page_proof_checksum(&self) -> &str {
        &self.object_list_page_proof_checksum
    }

    fn expected_object_list_page_proof_checksum(&self) -> &str {
        &self.expected_object_list_page_proof_checksum
    }
}

fn append_object_list_page_control_fields<Page: ObjectListPageControl>(
    page: &Page,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}payload_bytes"),
        page.payload_bytes().to_string(),
    );
    fields.insert(
        format!("{field_prefix}max_payload_bytes"),
        page.max_payload_bytes().to_string(),
    );
    fields.extend(page.read_proof().to_control_fields(field_prefix));
    fields.insert(
        format!("{field_prefix}object_list_page_proof_format"),
        page.object_list_page_proof_format().to_string(),
    );
    fields.insert(
        format!("{field_prefix}object_list_page_proof_checksum"),
        page.object_list_page_proof_checksum().to_string(),
    );
    fields.insert(
        format!("{field_prefix}expected_object_list_page_proof_checksum"),
        page.expected_object_list_page_proof_checksum().to_string(),
    );
}

