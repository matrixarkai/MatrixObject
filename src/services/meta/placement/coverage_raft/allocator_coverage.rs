// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn page_allocator_shard_coverage(&self) -> RootPageAllocatorShardCoverage {
        let mut coverage = RootPageAllocatorShardCoverage {
            status: "none".to_string(),
            ..RootPageAllocatorShardCoverage::default()
        };
        let mut seen_shards = BTreeSet::new();
        let mut mixed_shard_count = false;
        let mut duplicate_shard = false;
        let mut invalid_shard = false;
        for node in self.nodes.values() {
            if node.service_kind != "blockserver"
                || !node.data_eligible
                || !node.online
                || node.draining
            {
                continue;
            }
            coverage.active_pageserver_count = coverage.active_pageserver_count.saturating_add(1);
            if node.page_allocator_shard_count == 0
                || node.page_allocator_shard_index >= node.page_allocator_shard_count
            {
                invalid_shard = true;
                continue;
            }
            if coverage.shard_count == 0 {
                coverage.shard_count = node.page_allocator_shard_count;
            } else if coverage.shard_count != node.page_allocator_shard_count {
                mixed_shard_count = true;
            }
            if !seen_shards.insert(node.page_allocator_shard_index) {
                duplicate_shard = true;
            }
        }
        coverage.covered_shards = seen_shards.len() as u64;
        coverage.complete =
            coverage.shard_count > 0 && coverage.covered_shards == coverage.shard_count;
        coverage.status = if coverage.active_pageserver_count == 0 {
            "none"
        } else if invalid_shard {
            "invalid_shard"
        } else if mixed_shard_count {
            "mixed_shard_count"
        } else if duplicate_shard {
            "duplicate_shard"
        } else if coverage.complete {
            "complete"
        } else {
            "incomplete"
        }
        .to_string();
        coverage
    }
}
