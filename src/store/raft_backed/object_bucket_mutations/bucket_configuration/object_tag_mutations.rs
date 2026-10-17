impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_set_object_tags(
        &mut self,
        bucket: &str,
        key: &str,
        tags: BTreeMap<String, String>,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::SetObjectTags {
            bucket: bucket.to_string(),
            key: key.to_string(),
            tags,
            update_time_ms,
        })
    }
}
