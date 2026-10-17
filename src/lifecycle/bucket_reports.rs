#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketLifecyclePolicy {
    pub noncurrent_versions_to_keep: usize,
    pub noncurrent_older_than_ms: u64,
    pub transition_after_ms: u64,
    pub transition_storage_class: Option<String>,
    pub expire_current_after_ms: u64,
    pub expire_delete_markers: bool,
    pub abort_incomplete_multipart_after_ms: u64,
    pub filter_tag_key: Option<String>,
    pub filter_tag_value: Option<String>,
    pub filter_prefix: Option<String>,
}

impl BucketLifecyclePolicy {
    pub fn disabled(&self) -> bool {
        self.noncurrent_versions_to_keep == 0
            && self.noncurrent_older_than_ms == 0
            && self.transition_after_ms == 0
            && self.transition_storage_class.is_none()
            && self.expire_current_after_ms == 0
            && !self.expire_delete_markers
            && self.abort_incomplete_multipart_after_ms == 0
            && self.filter_tag_key.is_none()
            && self.filter_tag_value.is_none()
            && self.filter_prefix.is_none()
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}noncurrent_versions_to_keep"),
            self.noncurrent_versions_to_keep.to_string(),
        );
        fields.insert(
            format!("{field_prefix}noncurrent_older_than_ms"),
            self.noncurrent_older_than_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}transition_after_ms"),
            self.transition_after_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}transition_storage_class"),
            self.transition_storage_class.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}filter_tag_key"),
            self.filter_tag_key.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}filter_tag_value"),
            self.filter_tag_value.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}filter_prefix"),
            self.filter_prefix.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}expire_delete_markers"),
            self.expire_delete_markers.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expire_current_after_ms"),
            self.expire_current_after_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}abort_incomplete_multipart_after_ms"),
            self.abort_incomplete_multipart_after_ms.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketVersioningMutationReport {
    pub metadata: ObjectMetadata,
    pub enabled: bool,
}

impl BucketVersioningMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(format!("{field_prefix}enabled"), self.enabled.to_string());
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketRetentionMutationReport {
    pub metadata: ObjectMetadata,
    pub retain_for_ms: u64,
}

impl BucketRetentionMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}retain_for_ms"),
            self.retain_for_ms.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketQuotaMutationReport {
    pub metadata: ObjectMetadata,
    pub quota: BucketQuota,
    pub live_bytes: u64,
    pub live_objects: u64,
}

impl BucketQuotaMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.extend(self.quota.to_control_fields(
            self.live_bytes,
            self.live_objects,
            field_prefix,
        ));
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketLifecycleMutationReport {
    pub metadata: ObjectMetadata,
    pub policy: BucketLifecyclePolicy,
}

impl BucketLifecycleMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.extend(self.policy.to_control_fields(field_prefix));
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketPolicyMutationReceipt {
    pub idempotency_key: String,
    pub method: String,
    pub bucket: String,
    pub request_checksum: String,
    pub metadata: ObjectMetadata,
    pub commit_index: u64,
    pub update_time_ms: u64,
}

impl BucketPolicyMutationReceipt {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}method"), self.method.clone());
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}commit_index"),
            self.commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketCorsMutationReport {
    pub metadata: ObjectMetadata,
    pub policy: BucketCorsPolicy,
}

impl BucketCorsMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.extend(self.policy.to_control_fields(field_prefix));
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketPolicyDocumentMutationReport {
    pub metadata: ObjectMetadata,
    pub policy: BucketPolicyDocument,
}

impl BucketPolicyDocumentMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.extend(self.policy.to_control_fields(field_prefix));
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketTaggingMutationReport {
    pub metadata: ObjectMetadata,
    pub tag_set: BucketTagSet,
}

impl BucketTaggingMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.extend(self.tag_set.to_control_fields(field_prefix));
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketCorsMutationReceipt {
    pub idempotency_key: String,
    pub bucket: String,
    pub request_checksum: String,
    pub report: BucketCorsMutationReport,
    pub commit_index: u64,
    pub update_time_ms: u64,
}

impl BucketCorsMutationReceipt {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .report
            .to_control_fields(&format!("{field_prefix}report."));
        fields.insert(
            format!("{field_prefix}idempotency_key"),
            self.idempotency_key.clone(),
        );
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(
            format!("{field_prefix}request_checksum"),
            self.request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}commit_index"),
            self.commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields
    }
}
