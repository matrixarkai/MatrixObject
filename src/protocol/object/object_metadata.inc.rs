#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectExtent {
    pub slab_id: SlabId,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    pub etag: Box<str>,
    pub content_type: SharedText,
    pub user_metadata: ObjectAttributes,
    pub storage_class: SharedText,
    pub tags: ObjectAttributes,
    pub lock: ObjectLock,
    pub update_time_ms: u64,
    pub extents: Box<[ObjectExtent]>,
    pub deleted: bool,
}

impl ObjectMetadata {
    pub fn metadata_to_flat_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_flat_fields(&self.user_metadata, field_prefix)
    }

    pub fn metadata_to_indexed_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_indexed_fields(&self.user_metadata, field_prefix)
    }

    pub fn tags_to_flat_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_flat_fields(&self.tags, field_prefix)
    }

    pub fn tags_to_indexed_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_indexed_fields(&self.tags, field_prefix)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}bucket"),
            self.object_id.bucket.to_string(),
        );
        fields.insert(format!("{field_prefix}key"), self.object_id.key.to_string());
        fields.insert(format!("{field_prefix}etag"), self.etag.clone().into());
        fields.insert(
            format!("{field_prefix}content_type"),
            self.content_type.clone().into(),
        );
        fields.insert(format!("{field_prefix}size"), self.length.to_string().into());
        fields.insert(format!("{field_prefix}version"), self.version.to_string().into());
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}delete_marker"),
            self.deleted.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}legal_hold"),
            self.lock.legal_hold.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}legal_hold_update_time_ms"),
            self.lock.legal_hold_update_time_ms.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}retention_until_ms"),
            self.lock.retention_until_ms.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}storage_class"),
            if self.storage_class.is_empty() {
                "STANDARD".to_string().into()
            } else {
                self.storage_class.clone().into()
            },
        );
        fields.insert(
            format!("{field_prefix}restore_until_ms"),
            self.lock.restore_until_ms.to_string().into(),
        );
        fields.extend(self.metadata_to_indexed_fields(&format!("{field_prefix}metadata.")));
        fields.extend(self.tags_to_indexed_fields(&format!("{field_prefix}tag.")));
        fields.insert(
            format!("{field_prefix}extent_count"),
            self.extents.len().to_string().into(),
        );
        for (index, extent) in self.extents.iter().enumerate() {
            let prefix = format!("{field_prefix}extent.{index}.");
            fields.insert(
                format!("{prefix}segment_volume_id"),
                extent.slab_id.volume_id.to_string().into(),
            );
            fields.insert(
                format!("{prefix}segment_id"),
                extent.slab_id.id.to_string().into(),
            );
            fields.insert(format!("{prefix}offset"), extent.offset.to_string().into());
            fields.insert(format!("{prefix}length"), extent.length.to_string().into());
        }
        fields
    }
}

