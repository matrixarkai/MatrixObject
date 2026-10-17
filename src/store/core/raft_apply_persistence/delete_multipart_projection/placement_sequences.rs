impl MatrixObjectStore {
    fn release_root_placement_if_present(
        &mut self,
        object_id: &ObjectId,
        keep: Option<&ObjectPlacement>,
    ) {
        let Some(existing) = self.root_service.placements.get(object_id).cloned() else {
            return;
        };
        if keep.map_or(false, |placement| {
            placement.version == existing.version
                && placement.node_ids == existing.node_ids
                && placement.length == existing.length
        }) {
            return;
        }
        let _ = self.root_service.release_object_placement(object_id);
    }

    fn next_object_version(&self, object_id: &ObjectId) -> u64 {
        let mut version = self
            .objects
            .get(object_id)
            .map_or(0, |metadata| metadata.version);
        if let Some(versions) = self.versions.get(object_id) {
            for metadata in versions {
                version = version.max(metadata.version);
            }
        }
        version.saturating_add(1).max(1)
    }

    fn next_upload_id(&mut self) -> String {
        let sequence = self.next_upload_sequence;
        self.next_upload_sequence = self.next_upload_sequence.saturating_add(1).max(1);
        format!("mpu-{sequence}")
    }

    fn advance_upload_sequence_from_id(&mut self, upload_id: &str) {
        if let Some(suffix) = upload_id.strip_prefix("mpu-") {
            if let Ok(sequence) = suffix.parse::<u64>() {
                self.next_upload_sequence =
                    self.next_upload_sequence.max(sequence.saturating_add(1));
            }
        }
    }
}
