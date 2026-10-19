// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataChange {
    pub sequence: u64,
    pub commit_index: u64,
    pub key: SharedText,
    pub version: u64,
    pub deleted: bool,
    pub update_time_ms: u64,
}

impl MetadataChange {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}sequence"), self.sequence.to_string());
        fields.insert(
            format!("{field_prefix}commit_index"),
            self.commit_index.to_string(),
        );
        fields.insert(format!("{field_prefix}key"), self.key.to_string());
        fields.insert(format!("{field_prefix}version"), self.version.to_string());
        fields.insert(format!("{field_prefix}deleted"), self.deleted.to_string());
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataChangePage {
    pub changes: Vec<MetadataChange>,
    pub prefix: String,
    pub after_sequence: u64,
    pub next_sequence_marker: Option<u64>,
    pub scanned_changes: u64,
    pub skipped_changes: u64,
    pub payload_bytes: u64,
    pub max_commit_index: u64,
    pub has_more: bool,
}

impl MetadataChangePage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        metadata_change_page_control_fields(
            &self.changes,
            &self.prefix,
            self.after_sequence,
            self.after_sequence,
            &self.next_sequence_marker,
            false,
            false,
            0,
            0,
            0,
            0,
            0,
            self.scanned_changes,
            self.skipped_changes,
            self.payload_bytes,
            self.max_commit_index,
            self.has_more,
            field_prefix,
        )
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataWatchPage {
    pub changes: Vec<MetadataChange>,
    pub prefix: String,
    pub requested_after_sequence: u64,
    pub effective_after_sequence: u64,
    pub next_sequence_marker: Option<u64>,
    pub cursor_compacted: bool,
    pub resync_required: bool,
    pub resync_from_sequence: u64,
    pub first_retained_change_sequence: u64,
    pub last_retained_change_sequence: u64,
    pub first_retained_commit_index: u64,
    pub last_retained_commit_index: u64,
    pub scanned_changes: u64,
    pub skipped_changes: u64,
    pub payload_bytes: u64,
    pub max_commit_index: u64,
    pub has_more: bool,
}

impl MetadataWatchPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        metadata_change_page_control_fields(
            &self.changes,
            &self.prefix,
            self.requested_after_sequence,
            self.effective_after_sequence,
            &self.next_sequence_marker,
            self.cursor_compacted,
            self.resync_required,
            self.resync_from_sequence,
            self.first_retained_change_sequence,
            self.last_retained_change_sequence,
            self.first_retained_commit_index,
            self.last_retained_commit_index,
            self.scanned_changes,
            self.skipped_changes,
            self.payload_bytes,
            self.max_commit_index,
            self.has_more,
            field_prefix,
        )
    }
}

pub(crate) fn metadata_change_page_control_fields(
    changes: &[MetadataChange],
    prefix: &str,
    requested_after_sequence: u64,
    effective_after_sequence: u64,
    next_sequence_marker: &Option<u64>,
    cursor_compacted: bool,
    resync_required: bool,
    resync_from_sequence: u64,
    first_retained_change_sequence: u64,
    last_retained_change_sequence: u64,
    first_retained_commit_index: u64,
    last_retained_commit_index: u64,
    scanned_changes: u64,
    skipped_changes: u64,
    payload_bytes: u64,
    max_commit_index: u64,
    has_more: bool,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert(format!("{field_prefix}prefix"), prefix.to_string());
    fields.insert(
        format!("{field_prefix}requested_after_sequence"),
        requested_after_sequence.to_string(),
    );
    fields.insert(
        format!("{field_prefix}effective_after_sequence"),
        effective_after_sequence.to_string(),
    );
    if let Some(next_sequence_marker) = next_sequence_marker {
        fields.insert(
            format!("{field_prefix}next_sequence_marker"),
            next_sequence_marker.to_string(),
        );
    }
    for (name, value) in [
        ("change_count", changes.len() as u64),
        ("resync_from_sequence", resync_from_sequence),
        (
            "first_retained_change_sequence",
            first_retained_change_sequence,
        ),
        (
            "last_retained_change_sequence",
            last_retained_change_sequence,
        ),
        ("first_retained_commit_index", first_retained_commit_index),
        ("last_retained_commit_index", last_retained_commit_index),
        ("scanned_changes", scanned_changes),
        ("skipped_changes", skipped_changes),
        ("payload_bytes", payload_bytes),
        ("max_commit_index", max_commit_index),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields.insert(
        format!("{field_prefix}cursor_compacted"),
        cursor_compacted.to_string(),
    );
    fields.insert(
        format!("{field_prefix}resync_required"),
        resync_required.to_string(),
    );
    fields.insert(format!("{field_prefix}has_more"), has_more.to_string());
    fields.insert(format!("{field_prefix}truncated"), has_more.to_string());
    for (index, change) in changes.iter().enumerate() {
        fields.extend(change.to_control_fields(&format!("{field_prefix}change.{index}.")));
    }
    fields
}
