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
    pub(in crate::cluster) fn protected_segments(
        &self,
        log: &SegmentedOutcomeLog,
        pages: &PageCatalog,
        commit: &CheckpointCommit,
        publication: &StoredOutcomePosition,
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
                    for id in &node.extent_segments {
                        keep_id(&mut keep, id)?;
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
