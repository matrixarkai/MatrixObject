// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    /// Whether this bucket has been created.
    ///
    /// # WHY THIS IS NOT THE SAME QUESTION AS "DOES IT HOLD ANY OBJECTS"
    ///
    /// [`Self::list_objects`] validates the bucket NAME and then ranges over the object map, so a
    /// bucket that was never created answers an empty listing rather than an error -- the two
    /// states are indistinguishable from the listing alone. They are not the same thing to a
    /// caller: one is a bucket with nothing in it, the other is a bucket that is not there, and a
    /// caller told "empty" for the second will keep writing into a name that does not exist.
    pub fn bucket_exists(&self, bucket: &str) -> Result<bool, ObjectError> {
        validate_bucket(bucket)?;
        Ok(self.buckets.contains(bucket))
    }

    pub fn list_buckets(
        &self,
        marker: Option<&str>,
        limit: usize,
    ) -> Result<BucketInventoryPage, ObjectError> {
        if let Some(marker) = marker {
            validate_bucket(marker)?;
        }
        let limit = limit.clamp(1, MAX_BUCKET_LIST_LIMIT);
        let mut page = BucketInventoryPage {
            marker: marker.map(str::to_string),
            limit: limit as u64,
            ..BucketInventoryPage::default()
        };
        let iter: Box<dyn Iterator<Item = &SharedText> + '_> = if let Some(marker) = marker {
            Box::new(self.buckets.range::<str, _>((
                std::ops::Bound::Excluded(marker),
                std::ops::Bound::Unbounded,
            )))
        } else {
            Box::new(self.buckets.iter())
        };
        for bucket in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if page.buckets.len() == limit {
                page.has_more = true;
                break;
            }
            page.next_marker = Some(bucket.to_string());
            page.buckets.push(bucket.to_string());
        }
        page.returned_count = page.buckets.len() as u64;
        if page.buckets.is_empty() {
            page.next_marker = None;
        }
        Ok(page)
    }

    pub fn list_buckets_with_metadata_read(
        &self,
        marker: Option<&str>,
        limit: usize,
        options: MetadataReadOptions,
    ) -> Result<BucketInventoryPage, ObjectError> {
        let readiness = self.prepare_metadata_read("bucket/", &options)?;
        let mut page = self.list_buckets(marker, limit)?;
        page.readiness = readiness;
        Ok(page)
    }
}
