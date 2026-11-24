// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Included into `object_client.rs`, beside the chunk object client, for the group connections. The
// decision of which pages may be freed is `crate::chunk_reclaim`, which is pure; this moves the
// inventory in and the discards out.

use crate::chunk_reclaim::{confirm, live_pages_of, Candidates, PageKey};

/// What one sweep saw and did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReclaimReport {
    /// Pages the groups hold.
    pub inventory_pages: usize,
    /// Of those, the ones no manifest names and nobody has swept: the pages being watched.
    pub unnamed_pages: usize,
    /// Pages whose grace ran out and that the catalog, read again, still does not name.
    pub freed: Vec<PageKey>,
}

/// What a scan decided: the pages whose grace has run out, and the group connections it used.
pub struct ReclaimPlan {
    groups: Vec<(String, RemoteGroupClient)>,
    due: Vec<PageKey>,
    report: ReclaimReport,
}

impl ReclaimPlan {
    /// The pages that are due, which [`RemoteMetaClient::reclaim_apply`] will check once more.
    pub fn due_pages(&self) -> &[PageKey] {
        &self.due
    }
}

impl RemoteGroupClient {
    /// The pages this group holds after `after`, with their write sequences.
    fn inventory_batch(
        &mut self,
        after: Option<&crate::PageId>,
        limit: usize,
    ) -> Result<crate::cluster::message::PageInventory, ObjectError> {
        match self {
            Self::Raft(client) => client.page_inventory(after, limit as u64),
            Self::Star(set) => set.page_inventory_union(after, limit),
        }
    }

    /// Discard a page that still has its first-write sequence, all of its logical length. A page that
    /// has been swept or rewritten since the inventory, or holds nothing, is left alone: `false`.
    fn discard_first_write(&mut self, page: &crate::PageId) -> Result<bool, ObjectError> {
        let head = match self {
            Self::Raft(client) => client.head_chunk(page),
            Self::Star(set) => set.head_chunk(page),
        };
        let head = match head {
            Ok(head) => head,
            // gone since the inventory: nothing to free
            Err(ObjectError::NotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        if head.write_sequence != crate::chunk_reclaim::FIRST_WRITE_SEQUENCE || head.logical_length == 0 {
            return Ok(false);
        }
        match self {
            Self::Raft(client) => client.discard_chunk_if_sequence(
                page,
                0,
                head.logical_length,
                crate::chunk_reclaim::FIRST_WRITE_SEQUENCE,
            )?,
            Self::Star(set) => {
                set.discard_chunk(page, 0, head.logical_length)?;
            }
        }
        Ok(true)
    }
}

impl RemoteMetaClient {
    /// Every group the metadata plane routes, connected once.
    fn reclaim_groups(&mut self, options: &ObjectWriteOptions) -> Result<Vec<(String, RemoteGroupClient)>, ObjectError> {
        let mut groups = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let page = self.list_chunk_routes(after.as_deref(), 32)?;
            for route in &page.routes {
                let name = route
                    .group
                    .as_ref()
                    .map(|group| group.placement_id.clone())
                    .ok_or_else(|| ObjectError::Corruption("a chunk route names no group".into()))?;
                groups.push((name, connect_group(route, &options.replica)?));
            }
            match (page.has_more, page.routes.last()) {
                (true, Some(last)) => after = last.group.as_ref().map(|group| group.placement_id.clone()),
                _ => break,
            }
        }
        Ok(groups)
    }

    /// Every page every routed group holds, with its write sequence.
    pub fn reclaim_inventory(&mut self, options: &ObjectWriteOptions) -> Result<Vec<(PageKey, u64)>, ObjectError> {
        let mut groups = self.reclaim_groups(options)?;
        self.inventory_of(&mut groups)
    }

    fn inventory_of(&mut self, groups: &mut [(String, RemoteGroupClient)]) -> Result<Vec<(PageKey, u64)>, ObjectError> {
        let mut pages = Vec::new();
        for (name, client) in groups.iter_mut() {
            let mut after: Option<crate::PageId> = None;
            loop {
                let batch = client.inventory_batch(after.as_ref(), 1024)?;
                for (page, sequence) in &batch.entries {
                    pages.push((
                        PageKey {
                            group: name.clone(),
                            volume_id: page.chunk_id.volume_id(),
                            chunk_id: page.chunk_id.id(),
                            page_index: page.page_index,
                        },
                        *sequence,
                    ));
                }
                match (batch.has_more, batch.entries.last()) {
                    (true, Some((last, _))) => after = Some(last.clone()),
                    _ => break,
                }
            }
        }
        Ok(pages)
    }

    /// The pages the catalog names now, from the manifests it holds (a page of the listing at a time).
    fn reclaim_live_pages(&mut self) -> Result<std::collections::BTreeSet<PageKey>, ObjectError> {
        let mut manifests = Vec::new();
        let mut after = String::new();
        let mut buckets = Vec::new();
        // the global cursor walks every bucket in order
        let mut after_bucket = String::new();
        loop {
            let page = self.list_chunk_manifests(crate::object_manifest::ManifestListRequest {
                version: 3,
                bucket: String::new(),
                after: after.clone(),
                limit: 32,
                expected_revision: Vec::new(),
                after_bucket: after_bucket.clone(),
            })?;
            for entry in &page.entries {
                if !entry.deleted {
                    buckets.push((entry.bucket.clone(), entry.key.clone()));
                }
            }
            match (page.has_more, page.entries.last()) {
                (true, Some(last)) => {
                    after = last.key.clone();
                    after_bucket = last.bucket.clone();
                }
                _ => break,
            }
        }
        for (bucket, key) in buckets {
            match self.read_chunk_manifest(&bucket, &key) {
                Ok(manifest) => manifests.push(manifest),
                // deleted between the listing and the read: it names nothing
                Err(ObjectError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(live_pages_of(manifests.iter()))
    }

    /// The first half of a sweep: the inventory first, then the catalog, then which pages have been
    /// unnamed for the whole grace. Nothing is discarded. `candidates` carries what earlier sweeps saw:
    /// keep it between calls, and a restart that loses it only starts every clock again.
    pub fn reclaim_scan(
        &mut self,
        candidates: &mut Candidates,
        now_ms: u64,
        grace_ms: u64,
        options: &ObjectWriteOptions,
    ) -> Result<ReclaimPlan, ObjectError> {
        let mut groups = self.reclaim_groups(options)?;
        let inventory = self.inventory_of(&mut groups)?;
        let live = self.reclaim_live_pages()?;
        candidates.scan(&inventory, &live, now_ms);
        let due = candidates.due(now_ms, grace_ms);
        let report = ReclaimReport {
            inventory_pages: inventory.len(),
            unnamed_pages: candidates.len(),
            freed: Vec::new(),
        };
        Ok(ReclaimPlan { groups, due, report })
    }

    /// The second half: read the catalog AGAIN, just before the discards, and free only the due pages
    /// it still does not name. A manifest published since the scan keeps its pages.
    pub fn reclaim_apply(&mut self, plan: ReclaimPlan) -> Result<ReclaimReport, ObjectError> {
        let ReclaimPlan { mut groups, due, mut report } = plan;
        if due.is_empty() {
            return Ok(report);
        }
        let live_now = self.reclaim_live_pages()?;
        for page in confirm(due, &live_now) {
            let Some((_, client)) = groups.iter_mut().find(|(name, _)| *name == page.group) else { continue };
            let id = crate::PageId { chunk_id: crate::ChunkId::new(page.volume_id, page.chunk_id)?, page_index: page.page_index };
            if client.discard_first_write(&id)? {
                report.freed.push(page);
            }
        }
        Ok(report)
    }

    /// One whole sweep: [`Self::reclaim_scan`] and then [`Self::reclaim_apply`].
    pub fn reclaim_sweep(
        &mut self,
        candidates: &mut Candidates,
        now_ms: u64,
        grace_ms: u64,
        options: &ObjectWriteOptions,
    ) -> Result<ReclaimReport, ObjectError> {
        let plan = self.reclaim_scan(candidates, now_ms, grace_ms, options)?;
        self.reclaim_apply(plan)
    }
}
