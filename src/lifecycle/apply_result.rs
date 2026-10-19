// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LifecycleApplyResult {
    pub expired_noncurrent_versions: u64,
    pub transitioned_current_objects: u64,
    pub expired_current_objects: u64,
    pub removed_delete_markers: u64,
    pub aborted_multipart_uploads: u64,
    pub scanned_objects: u64,
    pub truncated: bool,
}

impl LifecycleApplyResult {
    pub(crate) fn total_actions(&self) -> u64 {
        self.expired_noncurrent_versions
            .saturating_add(self.transitioned_current_objects)
            .saturating_add(self.expired_current_objects)
            .saturating_add(self.removed_delete_markers)
            .saturating_add(self.aborted_multipart_uploads)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}expired_version_count"),
            self.expired_noncurrent_versions.to_string(),
        );
        fields.insert(
            format!("{field_prefix}transitioned_object_count"),
            self.transitioned_current_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expired_current_object_count"),
            self.expired_current_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}removed_delete_marker_count"),
            self.removed_delete_markers.to_string(),
        );
        fields.insert(
            format!("{field_prefix}aborted_multipart_upload_count"),
            self.aborted_multipart_uploads.to_string(),
        );
        fields.insert(
            format!("{field_prefix}scanned_object_count"),
            self.scanned_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}total_action_count"),
            self.total_actions().to_string(),
        );
        fields.insert(
            format!("{field_prefix}truncated"),
            self.truncated.to_string(),
        );
        fields
    }
}
