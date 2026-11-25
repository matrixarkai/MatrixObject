// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Immutable page, extent and balanced-index records. Only changed paths are written.
use super::replica_checkpoint_catalog::{
    self as catalog, CatalogHeader, CheckpointCommit, LoadedCheckpoint, PreparedCheckpoint,
};
use super::replica_checkpoint_spans::{CatalogPage, CatalogPageMetadata, PageCatalog, PayloadSpan};
use crate::durable_payload::{
    outcome_log::{CheckpointFragment, DecidedMutation, RecoveredMutation, StoredOutcomePosition},
    outcome_segments::SegmentedOutcomeLog,
    PayloadExtent,
};
use crate::{ChunkId, PageId};
use prost::Message;
use ring::{
    digest,
    rand::{SecureRandom, SystemRandom},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    sync::Arc,
};
const EXTENT: u32 = 1;
const PAGE: u32 = 2;
const NODE: u32 = 3;
const MAX_NODE_BYTES: usize = 16 * 1024;
const MAX_HEIGHT: u32 = 96;
const BATCH_BYTES: usize = 1024 * 1024;
const CACHE_BYTES: usize = 8 * 1024 * 1024;
type ExtentKey = ([u8; 16], u64);
fn invalid(text: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, text.to_string())
}
fn identity() -> io::Result<Vec<u8>> {
    let mut id = vec![0; 16];
    SystemRandom::new()
        .fill(&mut id)
        .map_err(|_| invalid("index identity generation failed"))?;
    Ok(id)
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct BlobRef {
    #[prost(message, optional, tag = "1")]
    position: Option<StoredOutcomePosition>,
    #[prost(uint32, tag = "2")]
    decision: u32,
    #[prost(bytes = "vec", tag = "3")]
    checkpoint_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    ordinal: u64,
    #[prost(bytes = "vec", tag = "5")]
    sha256: Vec<u8>,
    #[prost(uint32, tag = "6")]
    length: u32,
    #[prost(uint32, tag = "7")]
    height: u32,
    #[prost(uint64, tag = "8")]
    count: u64,
}
impl BlobRef {
    fn position(&self) -> io::Result<&StoredOutcomePosition> {
        if self.checkpoint_id.len() != 16
            || self.sha256.len() != 32
            || self.length == 0
            || self.length as usize > CheckpointFragment::MAX_DATA_BYTES
            || self.decision >= 64
        {
            return Err(invalid("invalid index record pointer"));
        }
        let position = self
            .position
            .as_ref()
            .ok_or_else(|| invalid("missing index record position"))?;
        if position.version != 1 || position.store_id.len() != 16 || position.length == 0 {
            return Err(invalid("invalid index record position"));
        }
        Ok(position)
    }
    fn plain(&self) -> io::Result<()> {
        self.position()?;
        if self.height != 0 || self.count != 0 {
            return Err(invalid("unexpected index geometry"));
        }
        Ok(())
    }
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct IndexRoot {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(message, optional, tag = "2")]
    node: Option<BlobRef>,
    #[prost(bytes = "vec", tag = "3")]
    metadata_catalog: Vec<u8>,
    #[prost(message, optional, tag = "4")]
    captured_cut: Option<StoredOutcomePosition>,
    #[prost(uint64, tag = "5")]
    pages: u64,
}
#[derive(Clone, PartialEq, Message)]
struct Key {
    #[prost(uint64, tag = "1")]
    volume: u64,
    #[prost(uint64, tag = "2")]
    chunk: u64,
    #[prost(uint64, tag = "3")]
    page: u64,
}
impl Key {
    fn new(id: &PageId) -> Self {
        Self {
            volume: id.chunk_id.volume_id(),
            chunk: id.chunk_id.id(),
            page: id.page_index,
        }
    }
    fn id(&self) -> io::Result<PageId> {
        Ok(PageId {
            chunk_id: ChunkId::new(self.volume, self.chunk).map_err(invalid)?,
            page_index: self.page,
        })
    }
}
#[derive(Clone, PartialEq, Message)]
struct Span {
    #[prost(uint64, tag = "1")]
    offset: u64,
    #[prost(uint64, tag = "2")]
    length: u64,
    #[prost(uint64, tag = "3")]
    extent_offset: u64,
    #[prost(message, optional, tag = "4")]
    extent: Option<BlobRef>,
}
#[derive(Clone, PartialEq, Message)]
struct Page {
    #[prost(message, optional, tag = "1")]
    metadata: Option<CatalogPageMetadata>,
    #[prost(message, repeated, tag = "2")]
    spans: Vec<Span>,
}
#[derive(Clone, PartialEq, Message)]
struct Value {
    #[prost(message, optional, tag = "1")]
    page: Option<BlobRef>,
    #[prost(bytes = "vec", tag = "2")]
    catalog: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
struct DiskNode {
    #[prost(message, optional, tag = "1")]
    key: Option<Key>,
    #[prost(message, optional, tag = "2")]
    value: Option<Value>,
    #[prost(message, optional, tag = "3")]
    left: Option<BlobRef>,
    #[prost(message, optional, tag = "4")]
    right: Option<BlobRef>,
    #[prost(uint32, tag = "5")]
    height: u32,
    #[prost(uint64, tag = "6")]
    count: u64,
}
#[derive(Clone, PartialEq, Message)]
struct Block {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    log_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    epoch: u64,
    #[prost(uint32, tag = "4")]
    kind: u32,
    #[prost(message, optional, tag = "5")]
    extent: Option<PayloadExtent>,
    #[prost(message, optional, tag = "6")]
    page: Option<Page>,
    #[prost(message, optional, tag = "7")]
    node: Option<DiskNode>,
}
impl Block {
    fn new(log: &SegmentedOutcomeLog, epoch: u64, kind: u32) -> Self {
        Self {
            version: 1,
            log_id: log.store_id().to_vec(),
            epoch,
            kind,
            extent: None,
            page: None,
            node: None,
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ExtentRecord {
    store_id: [u8; 16],
    offset: u64,
    length: u64,
}
impl ExtentRecord {
    fn new(position: &StoredOutcomePosition) -> io::Result<Self> {
        if position.version != 1 || position.length == 0 {
            return Err(invalid("invalid extent record position"));
        }
        Ok(Self {
            store_id: position.store_id.as_slice().try_into().map_err(invalid)?,
            offset: position.offset,
            length: position.length,
        })
    }
}
struct Node {
    key: PageId,
    value: Value,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
    height: u32,
    count: u64,
    saved: Option<BlobRef>,
    extent_records: Vec<ExtentRecord>,
}
fn height(node: &Option<Box<Node>>) -> u32 {
    node.as_ref().map_or(0, |n| n.height)
}
fn count(node: &Option<Box<Node>>) -> u64 {
    node.as_ref().map_or(0, |n| n.count)
}
impl Node {
    fn update(&mut self) {
        self.height = 1 + height(&self.left).max(height(&self.right));
        self.count = 1 + count(&self.left) + count(&self.right);
        self.saved = None;
    }
    fn rotate_left(mut self: Box<Self>) -> Box<Self> {
        let mut right = self.right.take().unwrap();
        self.right = right.left.take();
        self.update();
        right.left = Some(self);
        right.update();
        right
    }
    fn rotate_right(mut self: Box<Self>) -> Box<Self> {
        let mut left = self.left.take().unwrap();
        self.left = left.right.take();
        self.update();
        left.right = Some(self);
        left.update();
        left
    }
    fn put(
        node: Option<Box<Self>>,
        key: PageId,
        value: Value,
        extent_records: Vec<ExtentRecord>,
    ) -> Box<Self> {
        let Some(mut node) = node else {
            return Box::new(Self {
                key,
                value,
                extent_records,
                left: None,
                right: None,
                height: 1,
                count: 1,
                saved: None,
            });
        };
        match key.cmp(&node.key) {
            std::cmp::Ordering::Less => {
                node.left = Some(Self::put(node.left.take(), key, value, extent_records))
            }
            std::cmp::Ordering::Greater => {
                node.right = Some(Self::put(node.right.take(), key, value, extent_records))
            }
            std::cmp::Ordering::Equal => {
                node.value = value;
                node.extent_records = extent_records;
            }
        }
        node.update();
        if height(&node.left) > height(&node.right) + 1 {
            let left = node.left.as_ref().unwrap();
            if height(&left.right) > height(&left.left) {
                node.left = Some(node.left.take().unwrap().rotate_left());
            }
            node = node.rotate_right();
        } else if height(&node.right) > height(&node.left) + 1 {
            let right = node.right.as_ref().unwrap();
            if height(&right.left) > height(&right.right) {
                node.right = Some(node.right.take().unwrap().rotate_right());
            }
            node = node.rotate_left();
        }
        node
    }
    fn find_mut(&mut self, key: &PageId) -> &mut Self {
        match key.cmp(&self.key) {
            std::cmp::Ordering::Less => self.left.as_mut().unwrap().find_mut(key),
            std::cmp::Ordering::Greater => self.right.as_mut().unwrap().find_mut(key),
            std::cmp::Ordering::Equal => self,
        }
    }
    fn dirty(&self, out: &mut Vec<(u32, PageId)>) {
        if self.saved.is_some() {
            return;
        }
        if let Some(left) = &self.left {
            left.dirty(out);
        }
        if let Some(right) = &self.right {
            right.dirty(out);
        }
        out.push((self.height, self.key.clone()));
    }
    fn disk(&self) -> io::Result<DiskNode> {
        let child = |node: &Option<Box<Node>>| -> io::Result<Option<BlobRef>> {
            node.as_ref()
                .map(|node| {
                    node.saved
                        .clone()
                        .ok_or_else(|| invalid("index child not persisted"))
                })
                .transpose()
        };
        Ok(DiskNode {
            key: Some(Key::new(&self.key)),
            value: Some(self.value.clone()),
            left: child(&self.left)?,
            right: child(&self.right)?,
            height: self.height,
            count: self.count,
        })
    }
}
#[path = "replica_checkpoint_coverage.rs"]
mod planner_coverage;
use planner_coverage::PlannerCoverage;

struct SavedExtent {
    descriptor: Arc<PayloadExtent>,
    saved: Option<BlobRef>,
}
#[derive(Default)]
pub(super) struct IndexState {
    root: Option<Box<Node>>,
    extents: BTreeMap<ExtentKey, SavedExtent>,
    planner: PlannerCoverage,
}
impl IndexState {
    pub fn prune(&mut self, candidates: &BTreeSet<ExtentKey>) -> io::Result<()> {
        for key in candidates {
            if self
                .extents
                .get(key)
                .is_some_and(|e| Arc::strong_count(&e.descriptor) == 1)
            {
                self.planner
                    .remove_saved(self.extents[key].saved.as_ref())?;
                self.planner.remove_source(&key.0)?;
                self.extents.remove(key);
            }
        }
        Ok(())
    }
    #[cfg(all(test, unix))]
    fn put(&mut self, key: PageId, value: Value) {
        self.put_with_extents(key, value, Vec::new()).unwrap();
    }
    fn put_with_extents(
        &mut self,
        key: PageId,
        value: Value,
        extent_records: Vec<ExtentRecord>,
    ) -> io::Result<()> {
        PlannerCoverage::validate_value(&value, &extent_records)?;
        let mut current = self.root.as_deref();
        while let Some(node) = current {
            self.planner.remove_saved(node.saved.as_ref())?;
            match key.cmp(&node.key) {
                std::cmp::Ordering::Less => current = node.left.as_deref(),
                std::cmp::Ordering::Greater => current = node.right.as_deref(),
                std::cmp::Ordering::Equal => {
                    self.planner
                        .remove_value(&node.value, &node.extent_records)?;
                    break;
                }
            }
        }
        self.planner.add_value(&value, &extent_records)?;
        self.root = Some(Node::put(self.root.take(), key, value, extent_records));
        Ok(())
    }
    fn save_node(&mut self, key: &PageId, saved: BlobRef) -> io::Result<()> {
        let node = self.root.as_mut().unwrap().find_mut(key);
        self.planner.remove_saved(node.saved.as_ref())?;
        self.planner.add_saved(Some(&saved))?;
        node.saved = Some(saved);
        Ok(())
    }
    fn save_extent(
        &mut self,
        key: ExtentKey,
        descriptor: Arc<PayloadExtent>,
        saved: Option<BlobRef>,
    ) -> io::Result<()> {
        if catalog::extent_key(&descriptor)? != key {
            return Err(invalid("cached extent source identity differs"));
        }
        if let Some(old) = self.extents.get(&key) {
            if old.descriptor.as_ref() != descriptor.as_ref() {
                return Err(invalid("conflicting cached extent"));
            }
            self.planner.remove_saved(old.saved.as_ref())?;
        }
        if let Some(location) = &saved {
            location.plain()?;
            location.position()?;
        }
        self.planner.add_saved(saved.as_ref())?;
        if !self.extents.contains_key(&key) {
            self.planner.add_source(key.0)?;
        }
        self.extents.insert(key, SavedExtent { descriptor, saved });
        Ok(())
    }
    #[cfg(all(test, unix))]
    fn set_extent_saved(&mut self, key: ExtentKey, saved: BlobRef) -> io::Result<()> {
        let descriptor = self.extents[&key].descriptor.clone();
        self.save_extent(key, descriptor, Some(saved))
    }
    fn rebuild_planner(&mut self, pages: &PageCatalog) -> io::Result<()> {
        let mut planner = PlannerCoverage::default();
        let mut stack: Vec<_> = self.root.iter().map(|node| node.as_ref()).collect();
        while let Some(node) = stack.pop() {
            planner.add_saved(node.saved.as_ref())?;
            planner.add_value(&node.value, &node.extent_records)?;
            stack.extend(
                node.left
                    .iter()
                    .chain(node.right.iter())
                    .map(|node| node.as_ref()),
            );
        }
        for (key, extent) in &self.extents {
            planner.add_saved(extent.saved.as_ref())?;
            planner.add_source(key.0)?;
        }
        planner.refresh_payloads(pages, pages.pages.keys(), &|| Ok(()))?;
        self.planner = planner;
        Ok(())
    }
    pub(in crate::cluster) fn refresh_planned_payloads(
        &mut self,
        pages: &PageCatalog,
        dirty: &BTreeSet<PageId>,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        self.planner.refresh_payloads(pages, dirty.iter(), check)
    }
    pub(in crate::cluster) fn planned_payload_pages(
        &self,
        segment: &[u8; 16],
        pages: usize,
    ) -> io::Result<Vec<PageId>> {
        self.planner.payload_pages(segment, pages)
    }
    pub(in crate::cluster) fn planned_payload_lower_bound(&self, pages: usize) -> io::Result<u64> {
        self.planner.payload_lower_bound(pages)
    }
    pub(in crate::cluster) fn planned_live_bytes(
        &self,
        segment: &[u8; 16],
        length: u64,
    ) -> io::Result<u64> {
        self.planner.live_bytes(segment, length)
    }
    pub(in crate::cluster) fn ordinary_coverage(&self) -> bool {
        self.planner.ordinary()
            && self.root.as_ref().is_none_or(|root| {
                root.saved
                    .as_ref()
                    .is_some_and(|saved| saved.position.is_some())
            })
    }
}
enum Slot {
    Extent(ExtentKey, Arc<PayloadExtent>),
    Page(PageId, Vec<ExtentRecord>),
    Node(PageId),
}
struct Pending {
    slot: Slot,
    fragment: CheckpointFragment,
    height: u32,
    count: u64,
}
struct Writer<'a> {
    log: &'a SegmentedOutcomeLog,
    id: Vec<u8>,
    ordinal: u64,
    pending: Vec<Pending>,
    bytes: usize,
    check: &'a dyn Fn() -> io::Result<()>,
}
impl Writer<'_> {
    fn push(
        &mut self,
        state: &mut IndexState,
        slot: Slot,
        block: Block,
        height: u32,
        count: u64,
    ) -> io::Result<()> {
        (self.check)()?;
        let bytes = block.encode_to_vec();
        if bytes.len() > CheckpointFragment::MAX_DATA_BYTES
            || (block.kind == NODE && bytes.len() > MAX_NODE_BYTES)
        {
            return Err(invalid("index record exceeds size limit"));
        }
        if self.pending.len() == 64 || self.bytes + bytes.len() > BATCH_BYTES {
            self.flush(state)?;
        }
        let fragment = CheckpointFragment {
            version: 1,
            checkpoint_id: self.id.clone(),
            kind: CheckpointFragment::CATALOG,
            ordinal: self.ordinal,
            data: bytes,
        };
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("index ordinal overflow"))?;
        if let Slot::Node(key) = &slot {
            let saved = BlobRef {
                position: None,
                decision: self.pending.len() as u32,
                checkpoint_id: fragment.checkpoint_id.clone(),
                ordinal: fragment.ordinal,
                sha256: digest::digest(&digest::SHA256, &fragment.data)
                    .as_ref()
                    .to_vec(),
                length: fragment.data.len() as u32,
                height,
                count,
            };
            state.save_node(key, saved)?;
        }
        self.bytes += fragment.data.len();
        self.pending.push(Pending {
            slot,
            fragment,
            height,
            count,
        });
        Ok(())
    }
    fn flush(&mut self, state: &mut IndexState) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        (self.check)()?;
        let mutations: Vec<_> = self
            .pending
            .iter()
            .map(|p| DecidedMutation::CheckpointFragment(&p.fragment))
            .collect();
        let record = self.log.append_decided_batch(&mutations)?;
        for (decision, pending) in self.pending.drain(..).enumerate() {
            let saved = BlobRef {
                position: Some(record.position.clone()),
                decision: decision as u32,
                checkpoint_id: pending.fragment.checkpoint_id,
                ordinal: pending.fragment.ordinal,
                sha256: digest::digest(&digest::SHA256, &pending.fragment.data)
                    .as_ref()
                    .to_vec(),
                length: pending.fragment.data.len() as u32,
                height: pending.height,
                count: pending.count,
            };
            match pending.slot {
                Slot::Extent(key, descriptor) => state.save_extent(key, descriptor, Some(saved))?,
                Slot::Page(key, extent_records) => state.put_with_extents(
                    key,
                    Value {
                        page: Some(saved),
                        catalog: Vec::new(),
                    },
                    extent_records,
                )?,
                Slot::Node(key) => state.save_node(&key, saved)?,
            }
        }
        self.bytes = 0;
        Ok(())
    }
}
fn decode_catalog(bytes: &[u8]) -> io::Result<CheckpointCommit> {
    if bytes.len() > MAX_NODE_BYTES {
        return Err(invalid("oversized catalog pointer"));
    }
    let commit = CheckpointCommit::decode(bytes).map_err(invalid)?;
    if commit.version != 1
        || commit.index_root.is_some()
        || commit.install
        || commit.through.is_some()
        || commit.encode_to_vec() != bytes
    {
        return Err(invalid("invalid index catalog pointer"));
    }
    Ok(commit)
}
fn before(
    log: &SegmentedOutcomeLog,
    child: &StoredOutcomePosition,
    parent: &StoredOutcomePosition,
) -> io::Result<()> {
    if child == parent {
        return Err(invalid("index pointer is not before its parent"));
    }
    log.metadata_cursor_after_through(Some(child), parent)?;
    Ok(())
}
pub(super) fn prepare(
    log: &SegmentedOutcomeLog,
    catalog: &PageCatalog,
    state: &mut Option<IndexState>,
    dirty: &mut BTreeSet<PageId>,
    metadata: Vec<u8>,
    cut: Option<StoredOutcomePosition>,
    epoch: u64,
    install: bool,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<PreparedCheckpoint> {
    check()?;
    if epoch == 0 {
        return Err(invalid("zero checkpoint epoch"));
    }
    let keys: Vec<_> = if state.is_none() {
        catalog.pages.keys().cloned().collect()
    } else {
        dirty.iter().cloned().collect()
    };
    let state = state.get_or_insert_with(IndexState::default);
    state
        .planner
        .refresh_payloads(catalog, keys.iter(), check)?;
    let id = identity()?;
    let mut writer = Writer {
        log,
        id: id.clone(),
        ordinal: 0,
        pending: Vec::new(),
        bytes: 0,
        check,
    };
    let mut scheduled = BTreeSet::new();
    for key in &keys {
        let page = catalog
            .pages
            .get(key)
            .ok_or_else(|| invalid("dirty page is absent"))?;
        page.validate_spans()?;
        for span in &page.spans {
            let key = catalog::extent_key(&span.extent)?;
            if let Some(saved) = state.extents.get(&key) {
                if saved.descriptor.as_ref() != span.extent.as_ref() {
                    return Err(invalid("conflicting index extent"));
                }
                if saved.saved.is_some() {
                    continue;
                }
            }
            if scheduled.insert(key) {
                log.read_payload_range(&span.extent, 0, 0)?;
                let mut block = Block::new(log, epoch, EXTENT);
                block.extent = Some((*span.extent).clone());
                writer.push(state, Slot::Extent(key, span.extent.clone()), block, 0, 0)?;
            }
        }
    }
    writer.flush(state)?;
    for key in &keys {
        check()?;
        let page = &catalog.pages[key];
        let mut block = Block::new(log, epoch, PAGE);
        let mut extent_records = BTreeSet::new();
        let mut large = page.metadata.encoded_len() > CheckpointFragment::MAX_DATA_BYTES / 2;
        if !large {
            block.page = Some(Page {
                metadata: Some(page.metadata.clone()),
                spans: Vec::new(),
            });
            let mut size = block.encoded_len();
            for span in &page.spans {
                let extent = state.extents[&catalog::extent_key(&span.extent)?]
                    .saved
                    .clone()
                    .ok_or_else(|| invalid("unsaved page extent"))?;
                extent_records.insert(ExtentRecord::new(extent.position()?)?);
                let value = Span {
                    offset: span.offset,
                    length: span.length,
                    extent_offset: span.extent_offset,
                    extent: Some(extent),
                };
                size += value.encoded_len() + 8;
                if size > CheckpointFragment::MAX_DATA_BYTES {
                    large = true;
                    break;
                }
                block.page.as_mut().unwrap().spans.push(value);
            }
        }
        if large {
            writer.flush(state)?;
            let prepared = catalog::prepare_page_checked(log, key, page, epoch, check)?;
            state.put_with_extents(
                key.clone(),
                Value {
                    page: None,
                    catalog: prepared.commit.encode_to_vec(),
                },
                Vec::new(),
            )?;
        } else {
            writer.push(
                state,
                Slot::Page(key.clone(), extent_records.into_iter().collect()),
                block,
                0,
                0,
            )?;
        }
    }
    writer.flush(state)?;
    let mut nodes = Vec::new();
    if let Some(root) = &state.root {
        root.dirty(&mut nodes);
    }
    nodes.sort_unstable();
    for (height, key) in nodes {
        // Flush before encoding references: a new node may name pending children
        // by an earlier decision in this same batch. Never move it to a later
        // batch after those relative pointers have been encoded.
        if writer.pending.len() == 64 || writer.bytes + MAX_NODE_BYTES > BATCH_BYTES {
            writer.flush(state)?;
        }
        let node = state.root.as_mut().unwrap().find_mut(&key);
        let count = node.count;
        let mut block = Block::new(log, epoch, NODE);
        block.node = Some(node.disk()?);
        writer.push(state, Slot::Node(key), block, height, count)?;
    }
    writer.flush(state)?;
    let pages = count(&state.root);
    if pages != catalog.pages.len() as u64 {
        return Err(invalid("index page count mismatch"));
    }
    let meta = catalog::prepare_metadata_checked(log, catalog, metadata, epoch, check)?.commit;
    let last = meta.last.clone();
    let root = IndexRoot {
        version: 1,
        node: state.root.as_ref().map(|n| n.saved.clone().unwrap()),
        metadata_catalog: meta.encode_to_vec(),
        captured_cut: cut.clone(),
        pages,
    };
    dirty.clear();
    Ok(PreparedCheckpoint {
        commit: CheckpointCommit {
            version: 2,
            log_id: log.store_id().to_vec(),
            checkpoint_id: id,
            epoch,
            first: None,
            last: last.clone(),
            fragments: 0,
            bytes: 0,
            sha256: Vec::new(),
            through: if install { last } else { cut },
            install,
            index_root: Some(root),
        },
    })
}
pub(super) fn validate_commit(
    log: &SegmentedOutcomeLog,
    commit: &CheckpointCommit,
) -> io::Result<()> {
    let root = commit
        .index_root
        .as_ref()
        .ok_or_else(|| invalid("missing checkpoint index root"))?;
    if commit.version != 2
        || commit.log_id != log.store_id()
        || commit.checkpoint_id.len() != 16
        || commit.epoch == 0
        || commit.first.is_some()
        || commit.fragments != 0
        || commit.bytes != 0
        || !commit.sha256.is_empty()
        || root.version != 1
    {
        return Err(invalid("invalid indexed checkpoint commit"));
    }
    let meta = decode_catalog(&root.metadata_catalog)?;
    let last = commit
        .last
        .as_ref()
        .ok_or_else(|| invalid("missing index preparation end"))?;
    if meta.log_id != commit.log_id
        || meta.epoch != commit.epoch
        || meta.last.as_ref() != Some(last)
        || meta.first.is_none()
        || (commit.install && commit.through != commit.last)
        || (!commit.install && commit.through != root.captured_cut)
    {
        return Err(invalid("index metadata or replay cut mismatch"));
    }
    log.read_metadata(last)?;
    if let Some(cut) = &root.captured_cut {
        before(log, cut, meta.first.as_ref().unwrap())?;
    }
    match &root.node {
        Some(node)
            if root.pages != 0
                && node.count == root.pages
                && node.height != 0
                && node.height <= MAX_HEIGHT
                && node.length as usize <= MAX_NODE_BYTES =>
        {
            before(log, node.position()?, meta.first.as_ref().unwrap())?
        }
        None if root.pages == 0 => {}
        _ => return Err(invalid("invalid index root geometry")),
    }
    Ok(())
}
struct Cache {
    position: StoredOutcomePosition,
    fragments: Vec<CheckpointFragment>,
    bytes: usize,
}
struct Reader<'a> {
    log: &'a SegmentedOutcomeLog,
    epoch: u64,
    cache: VecDeque<Cache>,
    cache_bytes: usize,
    visited: usize,
    max_pages: usize,
    extent_cache: BTreeMap<([u8; 16], u64, u32), (BlobRef, Arc<PayloadExtent>)>,
}
impl Reader<'_> {
    fn fragment(&mut self, location: &BlobRef) -> io::Result<CheckpointFragment> {
        let position = location.position()?;
        let hit = self
            .cache
            .iter()
            .position(|entry| entry.position == *position);
        if let Some(at) = hit {
            let entry = self.cache.remove(at).unwrap();
            self.cache.push_back(entry);
        } else {
            let metadata = self.log.read_metadata(position)?;
            if metadata.batch.version != 2 {
                return Err(invalid("index pointer names a logical mutation"));
            }
            let outcomes = self.log.read_outcomes(position)?;
            let mut fragments = Vec::new();
            let mut bytes = 0;
            for outcome in outcomes {
                let RecoveredMutation::CheckpointFragment(fragment) = outcome else {
                    return Err(invalid("index batch contains a logical mutation"));
                };
                bytes += fragment.data.len();
                fragments.push(fragment);
            }
            while !self.cache.is_empty()
                && (self.cache_bytes + bytes > CACHE_BYTES || self.cache.len() >= 32)
            {
                self.cache_bytes -= self.cache.pop_front().unwrap().bytes;
            }
            self.cache_bytes += bytes;
            self.cache.push_back(Cache {
                position: position.clone(),
                fragments,
                bytes,
            });
        }
        let fragment = self
            .cache
            .back()
            .unwrap()
            .fragments
            .get(location.decision as usize)
            .ok_or_else(|| invalid("index decision is out of range"))?;
        if fragment.kind != CheckpointFragment::CATALOG
            || fragment.checkpoint_id != location.checkpoint_id
            || fragment.ordinal != location.ordinal
            || fragment.data.len() != location.length as usize
            || digest::digest(&digest::SHA256, &fragment.data).as_ref() != location.sha256
        {
            return Err(invalid("index record identity or digest mismatch"));
        }
        Ok(fragment.clone())
    }
    fn block(
        &mut self,
        location: &BlobRef,
        parent: &StoredOutcomePosition,
        kind: u32,
        same_record: bool,
    ) -> io::Result<Block> {
        if location.position()? != parent {
            before(self.log, location.position()?, parent)?;
        } else if !same_record {
            return Err(invalid("invalid same-record index pointer"));
        }
        let fragment = self.fragment(location)?;
        let block = Block::decode(fragment.data.as_slice()).map_err(invalid)?;
        if block.encode_to_vec() != fragment.data
            || block.version != 1
            || block.log_id != self.log.store_id()
            || block.epoch == 0
            || block.epoch > self.epoch
            || block.kind != kind
            || usize::from(block.extent.is_some())
                + usize::from(block.page.is_some())
                + usize::from(block.node.is_some())
                != 1
            || (kind == EXTENT && block.extent.is_none())
            || (kind == PAGE && block.page.is_none())
            || (kind == NODE && block.node.is_none())
        {
            return Err(invalid("invalid index record encoding or type"));
        }
        Ok(block)
    }
    fn extent(
        &mut self,
        location: &BlobRef,
        parent: &StoredOutcomePosition,
        state: &mut IndexState,
    ) -> io::Result<Arc<PayloadExtent>> {
        location.plain()?;
        if location.length as usize > MAX_NODE_BYTES {
            return Err(invalid("oversized extent record"));
        }
        let position = location.position()?;
        let key = (
            position.store_id.as_slice().try_into().map_err(invalid)?,
            position.offset,
            location.decision,
        );
        if let Some((old, extent)) = self.extent_cache.get(&key) {
            if old != location {
                return Err(invalid("conflicting extent pointers"));
            }
            before(self.log, position, parent)?;
            return Ok(extent.clone());
        }
        let extent = self.block(location, parent, EXTENT, false)?.extent.unwrap();
        let extent = self.intern_extent(Arc::new(extent), Some(location.clone()), state)?;
        self.extent_cache
            .insert(key, (location.clone(), extent.clone()));
        Ok(extent)
    }
    fn intern_extent(
        &self,
        extent: Arc<PayloadExtent>,
        location: Option<BlobRef>,
        state: &mut IndexState,
    ) -> io::Result<Arc<PayloadExtent>> {
        let key = catalog::extent_key(&extent)?;
        if let Some(old) = state.extents.get_mut(&key) {
            if old.descriptor.as_ref() != extent.as_ref() {
                return Err(invalid("conflicting index extent descriptors"));
            }
            if old.saved.is_none() {
                old.saved = location;
            }
            return Ok(old.descriptor.clone());
        }
        self.log.read_payload_range(&extent, 0, 0)?;
        state.extents.insert(
            key,
            SavedExtent {
                descriptor: extent.clone(),
                saved: location,
            },
        );
        Ok(extent)
    }
    fn page(
        &mut self,
        value: &Value,
        key: &PageId,
        parent: &StoredOutcomePosition,
        state: &mut IndexState,
    ) -> io::Result<(CatalogPage, Vec<ExtentRecord>)> {
        let mut extent_records = BTreeSet::new();
        let page = match (&value.page, value.catalog.is_empty()) {
            (Some(location), true) => {
                location.plain()?;
                let saved = self.block(location, parent, PAGE, false)?.page.unwrap();
                let metadata = saved
                    .metadata
                    .ok_or_else(|| invalid("missing indexed page metadata"))?;
                let mut spans = Vec::with_capacity(saved.spans.len());
                for span in saved.spans {
                    let saved_extent = span
                        .extent
                        .as_ref()
                        .ok_or_else(|| invalid("missing indexed page extent"))?;
                    extent_records.insert(ExtentRecord::new(saved_extent.position()?)?);
                    let extent = self.extent(saved_extent, location.position()?, state)?;
                    spans.push(PayloadSpan {
                        offset: span.offset,
                        length: span.length,
                        extent_offset: span.extent_offset,
                        extent,
                    });
                }
                CatalogPage { metadata, spans }
            }
            (None, false) => {
                let commit = decode_catalog(&value.catalog)?;
                if commit.epoch > self.epoch {
                    return Err(invalid("page catalog is newer than root"));
                }
                before(
                    self.log,
                    commit
                        .last
                        .as_ref()
                        .ok_or_else(|| invalid("missing page catalog end"))?,
                    parent,
                )?;
                let mut loaded = catalog::load(self.log, commit, 1)?;
                if !loaded.header.metadata.is_empty() || loaded.catalog.pages.len() != 1 {
                    return Err(invalid("invalid large page catalog"));
                }
                let mut page = loaded
                    .catalog
                    .pages
                    .remove(key)
                    .ok_or_else(|| invalid("large page key mismatch"))?;
                for span in &mut page.spans {
                    span.extent = self.intern_extent(span.extent.clone(), None, state)?;
                }
                page
            }
            _ => return Err(invalid("invalid indexed page binding")),
        };
        if catalog::page_id(&page.metadata)? != *key {
            return Err(invalid("index page key mismatch"));
        }
        page.validate_spans()?;
        Ok((page, extent_records.into_iter().collect()))
    }
    fn node(
        &mut self,
        location: &BlobRef,
        parent: &StoredOutcomePosition,
        parent_ref: Option<&BlobRef>,
        lower: Option<&PageId>,
        upper: Option<&PageId>,
        depth: u32,
        state: &mut IndexState,
        pages: &mut PageCatalog,
    ) -> io::Result<Box<Node>> {
        self.visited = self
            .visited
            .checked_add(1)
            .ok_or_else(|| invalid("index node count overflow"))?;
        if self.visited > self.max_pages
            || depth > MAX_HEIGHT
            || location.height == 0
            || location.height > MAX_HEIGHT
            || location.count == 0
            || location.count > self.max_pages as u64
            || location.length as usize > MAX_NODE_BYTES
        {
            return Err(invalid("index geometry exceeds bounds"));
        }
        let same_record = location.position()? == parent;
        if same_record {
            let parent_ref = parent_ref.ok_or_else(|| invalid("relative root pointer"))?;
            if location.decision >= parent_ref.decision
                || location.ordinal >= parent_ref.ordinal
                || location.checkpoint_id != parent_ref.checkpoint_id
            {
                return Err(invalid("unordered same-record index pointer"));
            }
        }
        let node = self
            .block(location, parent, NODE, same_record)?
            .node
            .unwrap();
        let key = node
            .key
            .as_ref()
            .ok_or_else(|| invalid("missing index key"))?
            .id()?;
        if lower.is_some_and(|p| p >= &key) || upper.is_some_and(|p| p <= &key) {
            return Err(invalid("unordered index keys"));
        }
        let child_height = |value: &Option<BlobRef>| value.as_ref().map_or(0, |p| p.height);
        let child_count = |value: &Option<BlobRef>| value.as_ref().map_or(0, |p| p.count);
        let expected_count = child_count(&node.left)
            .checked_add(child_count(&node.right))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| invalid("index count overflow"))?;
        if child_height(&node.left) >= location.height
            || child_height(&node.right) >= location.height
            || node.height != location.height
            || node.count != location.count
            || node.height != 1 + child_height(&node.left).max(child_height(&node.right))
            || node.count != expected_count
            || child_height(&node.left).abs_diff(child_height(&node.right)) > 1
        {
            return Err(invalid("invalid index child geometry"));
        }
        let value = node.value.ok_or_else(|| invalid("missing indexed page"))?;
        let position = location.position()?;
        let resolve = |value: &BlobRef| -> BlobRef {
            let mut value = value.clone();
            if value.position.is_none() {
                value.position = Some(position.clone());
            }
            value
        };
        let left = node
            .left
            .as_ref()
            .map(|p| {
                self.node(
                    &resolve(p),
                    position,
                    Some(location),
                    lower,
                    Some(&key),
                    depth + 1,
                    state,
                    pages,
                )
            })
            .transpose()?;
        let right = node
            .right
            .as_ref()
            .map(|p| {
                self.node(
                    &resolve(p),
                    position,
                    Some(location),
                    Some(&key),
                    upper,
                    depth + 1,
                    state,
                    pages,
                )
            })
            .transpose()?;
        let height = 1 + height(&left).max(height(&right));
        let count = count(&left)
            .checked_add(count(&right))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| invalid("index count overflow"))?;
        if height != node.height
            || count != node.count
            || height != location.height
            || count != location.count
            || height_of_difference(&left, &right) > 1
        {
            return Err(invalid("unbalanced or inconsistent index geometry"));
        }
        let (page, extent_records) = self.page(&value, &key, position, state)?;
        if pages.pages.insert(key.clone(), page).is_some() {
            return Err(invalid("duplicate indexed page"));
        }
        Ok(Box::new(Node {
            key,
            value,
            left,
            right,
            height,
            count,
            saved: Some(location.clone()),
            extent_records,
        }))
    }
}
fn height_of_difference(a: &Option<Box<Node>>, b: &Option<Box<Node>>) -> u32 {
    height(a).abs_diff(height(b))
}
pub(super) fn load(
    log: &SegmentedOutcomeLog,
    commit: CheckpointCommit,
    max_pages: usize,
) -> io::Result<LoadedCheckpoint> {
    validate_commit(log, &commit)?;
    let root = commit.index_root.as_ref().unwrap();
    if root.pages > max_pages as u64 {
        return Err(invalid("indexed checkpoint exceeds page limit"));
    }
    if commit.install {
        let mut cursor = log.metadata_cursor_after_through(
            root.captured_cut.as_ref(),
            commit.last.as_ref().unwrap(),
        )?;
        while let Some(record) = cursor.next_record()? {
            if record.batch.version != 2 {
                return Err(invalid("logical mutation crossed an indexed install"));
            }
        }
    }
    let meta = catalog::load(log, decode_catalog(&root.metadata_catalog)?, 0)?;
    if !meta.catalog.pages.is_empty() {
        return Err(invalid("index metadata catalog contains pages"));
    }
    let mut reader = Reader {
        log,
        epoch: commit.epoch,
        cache: VecDeque::new(),
        cache_bytes: 0,
        visited: 0,
        max_pages: usize::try_from(root.pages).map_err(invalid)?,
        extent_cache: BTreeMap::new(),
    };
    let mut index = IndexState::default();
    let mut pages = PageCatalog {
        logs: meta.catalog.logs,
        ..PageCatalog::default()
    };
    let tree = root
        .node
        .as_ref()
        .map(|p| {
            reader.node(
                p,
                commit.last.as_ref().unwrap(),
                None,
                None,
                None,
                1,
                &mut index,
                &mut pages,
            )
        })
        .transpose()?;
    index.root = tree;
    pages.validate_log_pages()?;
    index.rebuild_planner(&pages)?;
    if count(&index.root) != root.pages || pages.pages.len() as u64 != root.pages {
        return Err(invalid("indexed checkpoint page count mismatch"));
    }
    let header = CatalogHeader {
        version: 2,
        log_id: commit.log_id.clone(),
        checkpoint_id: commit.checkpoint_id.clone(),
        epoch: commit.epoch,
        captured_cut: root.captured_cut.clone(),
        install: commit.install,
        metadata: meta.header.metadata,
        extent_count: index.extents.len() as u64,
        page_count: root.pages,
        log_count: pages.logs.chunks.len() as u64,
    };
    Ok(LoadedCheckpoint {
        header,
        catalog: pages,
        commit,
        index: Some(index),
        publication_position: None,
    })
}
#[cfg(all(test, unix))]
#[path = "replica_checkpoint_index_tests.rs"]
mod tests;

#[path = "replica_checkpoint_reachability.rs"]
mod reachability;
