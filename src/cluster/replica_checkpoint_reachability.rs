// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Segment ownership of a published root, its reused records, and payload spans.
use super::*;

fn keep_id(keep: &mut BTreeSet<Vec<u8>>, id: &[u8]) -> io::Result<()> {
    if id.len() != 16 {
        return Err(invalid("invalid protected segment identity"));
    }
    if !keep.contains(id) {
        keep.insert(id.to_vec());
    }
    Ok(())
}
fn protect_catalog(
    log: &SegmentedOutcomeLog,
    bytes: &[u8],
    epoch: u64,
    keep: &mut BTreeSet<Vec<u8>>,
) -> io::Result<()> {
    let commit = decode_catalog(bytes)?;
    if commit.log_id != log.store_id() || commit.epoch > epoch {
        return Err(invalid("protected catalog identity or epoch mismatch"));
    }
    log.protect_interval(
        commit
            .first
            .as_ref()
            .ok_or_else(|| invalid("missing protected catalog start"))?,
        commit
            .last
            .as_ref()
            .ok_or_else(|| invalid("missing protected catalog end"))?,
        keep,
    )
}

impl IndexState {
    pub(in crate::cluster) fn compaction_metadata_ranges(
        &self,
        log: &SegmentedOutcomeLog,
        candidates: &[crate::durable_payload::outcome_segments::CompactionSegment],
        epoch: u64,
        ranges: &mut BTreeMap<[u8; 16], Vec<(u64, u64)>>,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        fn add(
            ranges: &mut BTreeMap<[u8; 16], Vec<(u64, u64)>>,
            record: ExtentRecord,
        ) -> io::Result<()> {
            if let Some(list) = ranges.get_mut(&record.store_id) {
                let end = record
                    .offset
                    .checked_add(record.length)
                    .ok_or_else(|| invalid("compaction record overflow"))?;
                list.push((record.offset, end));
            }
            Ok(())
        }
        let mut stack: Vec<_> = self.root.iter().map(|node| node.as_ref()).collect();
        while let Some(node) = stack.pop() {
            check()?;
            let saved = node
                .saved
                .as_ref()
                .ok_or_else(|| invalid("unpublished compaction node"))?;
            add(ranges, ExtentRecord::new(saved.position()?)?)?;
            match (&node.value.page, node.value.catalog.is_empty()) {
                (Some(page), true) => {
                    add(ranges, ExtentRecord::new(page.position()?)?)?;
                    for record in &node.extent_records {
                        add(ranges, *record)?;
                    }
                }
                (None, false) => {
                    let commit = decode_catalog(&node.value.catalog)?;
                    if commit.log_id != log.store_id() || commit.epoch > epoch {
                        return Err(invalid("compaction catalog identity or epoch mismatch"));
                    }
                    let first = commit
                        .first
                        .as_ref()
                        .ok_or_else(|| invalid("missing compaction catalog start"))?;
                    let last = commit
                        .last
                        .as_ref()
                        .ok_or_else(|| invalid("missing compaction catalog end"))?;
                    for (id, start, end) in
                        log.compaction_catalog_ranges(first, last, candidates)?
                    {
                        ranges.get_mut(&id).unwrap().push((start, end));
                    }
                }
                _ => return Err(invalid("invalid compaction page binding")),
            }
            stack.extend(
                node.left
                    .iter()
                    .chain(node.right.iter())
                    .map(|node| node.as_ref()),
            );
        }
        for extent in self.extents.values() {
            check()?;
            if let Some(saved) = &extent.saved {
                add(ranges, ExtentRecord::new(saved.position()?)?)?;
            }
        }
        Ok(())
    }

    pub(in crate::cluster) fn evacuate_metadata(
        &mut self,
        log: &SegmentedOutcomeLog,
        selected: &[crate::durable_payload::outcome_segments::CompactionSegment],
        dirty: &mut BTreeSet<PageId>,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        check()?;
        // Published coverage includes leaf bindings and reusable descriptor records.
        if self.ordinary_coverage()
            && selected
                .iter()
                .all(|segment| !self.planner.has_metadata(&segment.store_id))
        {
            return Ok(());
        }
        let ids: BTreeSet<_> = selected.iter().map(|segment| segment.store_id).collect();
        fn node(
            value: &mut Node,
            log: &SegmentedOutcomeLog,
            selected: &[crate::durable_payload::outcome_segments::CompactionSegment],
            ids: &BTreeSet<[u8; 16]>,
            dirty: &mut BTreeSet<PageId>,
            planner: &mut PlannerCoverage,
            check: &dyn Fn() -> io::Result<()>,
        ) -> io::Result<bool> {
            check()?;
            let mut changed = false;
            for child in value.left.iter_mut().chain(value.right.iter_mut()) {
                changed |= node(child, log, selected, ids, dirty, planner, check)?;
            }
            let rewrite_page = match (&value.value.page, value.value.catalog.is_empty()) {
                (Some(page), true) => {
                    ids.contains(&ExtentRecord::new(page.position()?)?.store_id)
                        || value
                            .extent_records
                            .iter()
                            .any(|record| ids.contains(&record.store_id))
                }
                (None, false) => {
                    let commit = decode_catalog(&value.value.catalog)?;
                    !log.compaction_catalog_ranges(
                        commit
                            .first
                            .as_ref()
                            .ok_or_else(|| invalid("missing compaction catalog start"))?,
                        commit
                            .last
                            .as_ref()
                            .ok_or_else(|| invalid("missing compaction catalog end"))?,
                        selected,
                    )?
                    .is_empty()
                }
                _ => return Err(invalid("invalid compaction page binding")),
            };
            if rewrite_page {
                dirty.insert(value.key.clone());
                changed = true;
            }
            if let Some(saved) = &value.saved {
                changed |= ids.contains(&ExtentRecord::new(saved.position()?)?.store_id);
            }
            if changed {
                planner.remove_saved(value.saved.as_ref())?;
                value.saved = None;
            }
            Ok(changed)
        }
        if let Some(root) = &mut self.root {
            node(root, log, selected, &ids, dirty, &mut self.planner, check)?;
        }
        for extent in self.extents.values_mut() {
            check()?;
            if extent
                .saved
                .as_ref()
                .map(|saved| {
                    Ok::<_, io::Error>(
                        ids.contains(&ExtentRecord::new(saved.position()?)?.store_id),
                    )
                })
                .transpose()?
                .unwrap_or(false)
            {
                self.planner.remove_saved(extent.saved.as_ref())?;
                extent.saved = None;
            }
        }
        Ok(())
    }

    // The checkpoint worker supplies its dirty set after publication. A changed
    // catalog must use the complete walk until preparation refreshes coverage.
    pub(in crate::cluster) fn protected_segments_indexed(
        &self,
        log: &SegmentedOutcomeLog,
        pages: &PageCatalog,
        commit: &CheckpointCommit,
        publication: &StoredOutcomePosition,
        dirty: &BTreeSet<PageId>,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<BTreeSet<Vec<u8>>> {
        if !dirty.is_empty() || !self.ordinary_coverage() {
            return self.protected_segments(log, pages, commit, publication, check);
        }
        self.protected_segments_checked(log, pages, commit, publication, true, check)
    }
    pub(in crate::cluster) fn protected_segments(
        &self,
        log: &SegmentedOutcomeLog,
        pages: &PageCatalog,
        commit: &CheckpointCommit,
        publication: &StoredOutcomePosition,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<BTreeSet<Vec<u8>>> {
        self.protected_segments_checked(log, pages, commit, publication, false, check)
    }
    fn protected_segments_checked(
        &self,
        log: &SegmentedOutcomeLog,
        pages: &PageCatalog,
        commit: &CheckpointCommit,
        publication: &StoredOutcomePosition,
        cached: bool,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<BTreeSet<Vec<u8>>> {
        check()?;
        validate_commit(log, commit)?;
        let root = commit.index_root.as_ref().unwrap();
        if self.root.as_ref().and_then(|n| n.saved.as_ref()) != root.node.as_ref()
            || root.pages != pages.pages.len() as u64
        {
            return Err(invalid("cached index does not match published root"));
        }
        let outcomes = log.read_outcomes(publication)?;
        let [RecoveredMutation::CheckpointFragment(fragment)] = outcomes.as_slice() else {
            return Err(invalid("missing protected checkpoint publication"));
        };
        if fragment.kind != CheckpointFragment::COMMIT
            || fragment.checkpoint_id != commit.checkpoint_id
            || fragment.ordinal != commit.epoch
            || fragment.data != commit.encode_to_vec()
        {
            return Err(invalid("protected checkpoint publication changed"));
        }
        let mut keep = BTreeSet::new();
        keep_id(&mut keep, &publication.store_id)?;
        protect_catalog(log, &root.metadata_catalog, commit.epoch, &mut keep)?;
        if cached {
            self.planner.protect(pages.pages.len(), &mut keep, check)?;
            return Ok(keep);
        }
        let mut stack: Vec<_> = self.root.iter().map(|n| n.as_ref()).collect();
        while let Some(node) = stack.pop() {
            check()?;
            keep_id(
                &mut keep,
                &node
                    .saved
                    .as_ref()
                    .ok_or_else(|| invalid("unpublished index node"))?
                    .position()?
                    .store_id,
            )?;
            match (&node.value.page, node.value.catalog.is_empty()) {
                (Some(page), true) => {
                    page.plain()?;
                    keep_id(&mut keep, &page.position()?.store_id)?;
                    // Keep each leaf's actual extent-record locations. Multiple
                    // valid records may describe the same physical payload.
                    for record in &node.extent_records {
                        keep_id(&mut keep, &record.store_id)?;
                    }
                }
                (None, false) => {
                    protect_catalog(log, &node.value.catalog, commit.epoch, &mut keep)?
                }
                _ => return Err(invalid("invalid protected page binding")),
            }
            if let Some(left) = &node.left {
                stack.push(left.as_ref());
            }
            if let Some(right) = &node.right {
                stack.push(right.as_ref());
            }
        }
        for page in pages.pages.values() {
            check()?;
            for span in &page.spans {
                keep_id(&mut keep, &span.extent.store_id)?;
            }
        }
        // Cached descriptors can be reused by a later dirty page, even when a
        // different descriptor record currently appears in another leaf.
        for extent in self.extents.values() {
            check()?;
            keep_id(&mut keep, &extent.descriptor.store_id)?;
            if let Some(saved) = &extent.saved {
                saved.plain()?;
                keep_id(&mut keep, &saved.position()?.store_id)?;
            }
        }
        Ok(keep)
    }
}
