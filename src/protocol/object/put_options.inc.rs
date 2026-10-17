#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PutOptions {
    pub if_none_match: bool,
    pub if_match_etag: Option<String>,
    pub if_match_etags: Vec<String>,
    pub expected_checksum: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_epoch: Option<u64>,
    pub user_metadata: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub storage_class: Option<String>,
    pub min_read_index: u64,
    pub min_commit_index: u64,
}

impl PutOptions {
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

    pub fn with_metadata_from_fields(
        mut self,
        fields: &BTreeMap<String, String>,
        field_prefix: &str,
    ) -> Result<Self, ObjectError> {
        self.user_metadata = string_map_from_fields(fields, field_prefix)?;
        validate_user_metadata(&self.user_metadata)?;
        Ok(self)
    }

    pub fn with_tags_from_fields(
        mut self,
        fields: &BTreeMap<String, String>,
        field_prefix: &str,
    ) -> Result<Self, ObjectError> {
        self.tags = string_map_from_fields(fields, field_prefix)?;
        validate_tags(&self.tags)?;
        Ok(self)
    }
}
