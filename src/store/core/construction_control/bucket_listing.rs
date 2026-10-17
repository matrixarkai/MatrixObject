impl MatrixObjectStore {
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
        let iter: Box<dyn Iterator<Item = &String> + '_> = if let Some(marker) = marker {
            Box::new(self.buckets.range((
                std::ops::Bound::Excluded(marker.to_string()),
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
            page.next_marker = Some(bucket.clone());
            page.buckets.push(bucket.clone());
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
