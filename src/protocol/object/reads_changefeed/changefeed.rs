
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectChangeRecord {
    pub sequence: u64,
    pub previous_sequence: u64,
    /// One of a fixed set of twelve operation names, and a `Box<str>` rather
    /// than a `String` because it is written once and never grown: a `String`
    /// carries a capacity word the feed has no use for.
    ///
    /// An enum would be smaller still -- one byte against sixteen -- but the
    /// snapshot stores this as text, so an enum would have to decide what a
    /// decode does with a name it does not know. That is a compatibility
    /// decision rather than a representation one, and it is in the issue
    /// rather than here.
    pub operation: Box<str>,
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    /// The object metadata holds this too, and the metadata record's value
    /// restates it a third time. Boxed for the same reason as the operation.
    pub etag: Box<str>,
    pub deleted: bool,
    pub event_time_ms: u64,
}

impl ObjectChangeRecord {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}sequence"), self.sequence.to_string());
        fields.insert(
            format!("{field_prefix}previous_sequence"),
            self.previous_sequence.to_string(),
        );
        fields.insert(format!("{field_prefix}record_id"), String::new());
        fields.insert(format!("{field_prefix}record_crc64"), String::new());
        fields.insert(
            format!("{field_prefix}operation"),
            self.operation.to_string(),
        );
        fields.insert(
            format!("{field_prefix}bucket"),
            self.object_id.bucket.to_string(),
        );
        fields.insert(format!("{field_prefix}key"), self.object_id.key.to_string());
        fields.insert(format!("{field_prefix}version"), self.version.to_string());
        fields.insert(
            format!("{field_prefix}metadata_commit_index"),
            self.sequence.to_string(),
        );
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields.insert(format!("{field_prefix}etag"), self.etag.to_string());
        fields.insert(format!("{field_prefix}deleted"), self.deleted.to_string());
        fields.insert(
            format!("{field_prefix}event_time_ms"),
            self.event_time_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}json"), String::new());
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectChangeCursor {
    pub consumer_id: String,
    pub sequence: u64,
    pub update_time_ms: u64,
}

impl ObjectChangeCursor {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}consumer_id"),
            self.consumer_id.clone(),
        );
        fields.insert(format!("{field_prefix}sequence"), self.sequence.to_string());
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectChangePage {
    pub records: Vec<ObjectChangeRecord>,
    pub next_sequence: u64,
    pub retained_floor_sequence: u64,
    pub cursor_compacted: bool,
    pub resync_required: bool,
    pub resync_from_sequence: u64,
    pub truncated: bool,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub read_proof: ObjectReadProof,
    pub object_change_page_proof_format: String,
    pub object_change_page_proof_checksum: String,
    pub expected_object_change_page_proof_checksum: String,
}

impl ObjectChangePage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}next_sequence"),
            self.next_sequence.to_string(),
        );
        fields.insert(
            format!("{field_prefix}retained_floor_sequence"),
            self.retained_floor_sequence.to_string(),
        );
        fields.insert(
            format!("{field_prefix}cursor_compacted"),
            self.cursor_compacted.to_string(),
        );
        fields.insert(
            format!("{field_prefix}resync_required"),
            self.resync_required.to_string(),
        );
        fields.insert(
            format!("{field_prefix}resync_from_sequence"),
            self.resync_from_sequence.to_string(),
        );
        fields.insert(
            format!("{field_prefix}truncated"),
            self.truncated.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.payload_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_payload_bytes"),
            self.max_payload_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        fields.insert(
            format!("{field_prefix}record_count"),
            self.records.len().to_string(),
        );
        fields.extend(self.read_proof.to_control_fields(field_prefix));
        fields.insert(
            format!("{field_prefix}object_change_page_proof_format"),
            self.object_change_page_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}object_change_page_proof_checksum"),
            self.object_change_page_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_object_change_page_proof_checksum"),
            self.expected_object_change_page_proof_checksum.clone(),
        );
        for (index, record) in self.records.iter().enumerate() {
            fields.extend(record.to_control_fields(&format!("{field_prefix}record.{index}.")));
        }
        fields
    }
}

