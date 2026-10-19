// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {

    pub fn plan_object_placement(
        &self,
        bucket: &str,
        key: &str,
        length: u64,
        replica_count: usize,
    ) -> Result<PlacementPlan, ObjectError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        self.meta_service
            .plan_object_placement(ObjectId::new(bucket, key), length, replica_count)
    }


    pub fn commit_object_placement(
        &mut self,
        placement: ObjectPlacement,
        expected_plan_checksum: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.meta_service
            .commit_object_placement(placement, expected_plan_checksum)
    }


    pub fn release_object_placement(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.meta_service
            .release_object_placement(&ObjectId::new(bucket, key))
    }
}
