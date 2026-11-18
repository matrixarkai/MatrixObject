// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Streaming metadata-only checkpoints in the replica's immutable outcome log.
//! Preparing a catalog does not publish it. The journal serializes publication
//! with installs and appends the small commit record only for the current epoch.
use super::replica_checkpoint_spans::{CatalogPage, CatalogPageMetadata, PageCatalog, PayloadSpan};
use crate::durable_payload::{
    outcome_log::{CheckpointFragment, DecidedMutation, RecoveredMutation, StoredOutcomePosition},
    outcome_segments::{SegmentedOutcomeLog, StoredMetadataCursor},
    PayloadExtent,
};
use crate::{ChunkId, PageId};
use prost::Message;
use ring::{
    digest,
    rand::{SecureRandom, SystemRandom},
};
use std::{
    collections::BTreeMap,
    io::{self, Read},
    sync::Arc,
};
const MAX_ROW_BYTES: usize = 256 * 1024 * 1024;
const MAX_CATALOG_BYTES: u64 = super::replica_image::MAX_FILE_CHECKPOINT_BYTES;
fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct CatalogHeader {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub log_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub checkpoint_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub epoch: u64,
    #[prost(message, optional, tag = "5")]
    pub captured_cut: Option<StoredOutcomePosition>,
    #[prost(bool, tag = "6")]
    pub install: bool,
    #[prost(bytes = "vec", tag = "7")]
    pub metadata: Vec<u8>,
    #[prost(uint64, tag = "8")]
    pub extent_count: u64,
    #[prost(uint64, tag = "9")]
    pub page_count: u64,
}
#[derive(Clone, PartialEq, Message)]
struct PageHeader {
    #[prost(message, optional, tag = "1")]
    metadata: Option<CatalogPageMetadata>,
    #[prost(uint64, tag = "2")]
    spans: u64,
}
#[derive(Clone, PartialEq, Message)]
struct Span {
    #[prost(uint64, tag = "1")]
    offset: u64,
    #[prost(uint64, tag = "2")]
    length: u64,
    #[prost(uint64, tag = "3")]
    extent_index: u64,
    #[prost(uint64, tag = "4")]
    extent_offset: u64,
}
#[derive(Clone, PartialEq, Message)]
struct Row {
    #[prost(oneof = "row::Value", tags = "1, 2, 3, 4")]
    value: Option<row::Value>,
}
mod row {
    use super::*;
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        #[prost(message, tag = "1")]
        Header(CatalogHeader),
        #[prost(message, tag = "2")]
        Extent(PayloadExtent),
        #[prost(message, tag = "3")]
        Page(PageHeader),
        #[prost(message, tag = "4")]
        Span(Span),
    }
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct CheckpointCommit {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub log_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub checkpoint_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub epoch: u64,
    #[prost(message, optional, tag = "5")]
    pub first: Option<StoredOutcomePosition>,
    #[prost(message, optional, tag = "6")]
    pub last: Option<StoredOutcomePosition>,
    #[prost(uint64, tag = "7")]
    pub fragments: u64,
    #[prost(uint64, tag = "8")]
    pub bytes: u64,
    #[prost(bytes = "vec", tag = "9")]
    pub sha256: Vec<u8>,
    #[prost(message, optional, tag = "10")]
    pub through: Option<StoredOutcomePosition>,
    #[prost(bool, tag = "11")]
    pub install: bool,
    #[prost(message, optional, tag = "12")]
    pub index_root: Option<super::replica_checkpoint_index::IndexRoot>,
}
pub(super) struct PreparedCheckpoint {
    pub commit: CheckpointCommit,
}
pub(super) struct LoadedCheckpoint {
    pub header: CatalogHeader,
    pub catalog: PageCatalog,
    pub commit: CheckpointCommit,
    pub index: Option<super::replica_checkpoint_index::IndexState>,
    pub publication_position: Option<StoredOutcomePosition>,
}
pub(super) fn extent_key(extent: &PayloadExtent) -> io::Result<([u8; 16], u64)> {
    let id = extent
        .store_id
        .as_slice()
        .try_into()
        .map_err(|_| invalid("invalid catalog extent identity"))?;
    Ok((id, extent.offset))
}
struct CatalogWriter<'a> {
    log: &'a SegmentedOutcomeLog,
    check: Option<&'a dyn Fn() -> io::Result<()>>,
    id: Vec<u8>,
    buffer: Vec<u8>,
    hash: digest::Context,
    bytes: u64,
    count: u64,
    first: Option<StoredOutcomePosition>,
    last: Option<StoredOutcomePosition>,
}
impl CatalogWriter<'_> {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        let next = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("catalog length overflow"))?;
        if next > MAX_CATALOG_BYTES {
            return Err(invalid("catalog exceeds byte limit"));
        }
        self.hash.update(bytes);
        self.bytes = next;
        while !bytes.is_empty() {
            let n = bytes
                .len()
                .min(CheckpointFragment::MAX_DATA_BYTES - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if self.buffer.len() == CheckpointFragment::MAX_DATA_BYTES {
                self.flush()?;
            }
        }
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        if let Some(check) = self.check {
            check()?;
        }
        if self.buffer.is_empty() {
            return Ok(());
        }
        let fragment = CheckpointFragment {
            version: 1,
            checkpoint_id: self.id.clone(),
            kind: CheckpointFragment::CATALOG,
            ordinal: self.count,
            data: std::mem::take(&mut self.buffer),
        };
        let record = self
            .log
            .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])?;
        self.buffer = fragment.data;
        self.buffer.clear();
        self.first.get_or_insert_with(|| record.position.clone());
        self.last = Some(record.position);
        self.count += 1;
        Ok(())
    }
    // Encode a borrowed nested message. Page discard maps and sector digests
    // never need to be cloned to construct a protobuf oneof row.
    fn row(&mut self, tag: u8, value: &impl Message, scratch: &mut Vec<u8>) -> io::Result<()> {
        scratch.clear();
        scratch.push((tag << 3) | 2);
        prost::encoding::encode_varint(value.encoded_len() as u64, scratch);
        value.encode(scratch).map_err(invalid)?;
        self.raw_row(scratch)
    }
    fn raw_row(&mut self, row: &[u8]) -> io::Result<()> {
        if row.is_empty() || row.len() > MAX_ROW_BYTES {
            return Err(invalid("invalid catalog row length"));
        }
        self.write(&(row.len() as u32).to_le_bytes())?;
        self.write(row)
    }
}
/// Runs outside the node mutex. It reads descriptors and metadata only; it does
/// not hydrate page bytes. The caller owns the catalog exclusively until done.
#[cfg(all(test, unix))]
pub(super) fn prepare(
    log: &SegmentedOutcomeLog,
    catalog: &PageCatalog,
    metadata: Vec<u8>,
    captured_cut: Option<StoredOutcomePosition>,
    epoch: u64,
    install: bool,
) -> io::Result<PreparedCheckpoint> {
    prepare_checked(
        log,
        catalog,
        metadata,
        captured_cut,
        epoch,
        install,
        &|| Ok(()),
    )
}
pub(super) fn prepare_checked(
    log: &SegmentedOutcomeLog,
    catalog: &PageCatalog,
    metadata: Vec<u8>,
    captured_cut: Option<StoredOutcomePosition>,
    epoch: u64,
    install: bool,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<PreparedCheckpoint> {
    prepare_iter_checked(
        log,
        catalog.pages.iter(),
        catalog.pages.len(),
        metadata,
        captured_cut,
        epoch,
        install,
        check,
    )
}

pub(super) fn prepare_page_checked(
    log: &SegmentedOutcomeLog,
    id: &PageId,
    page: &CatalogPage,
    epoch: u64,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<PreparedCheckpoint> {
    prepare_iter_checked(
        log,
        std::iter::once((id, page)),
        1,
        Vec::new(),
        None,
        epoch,
        false,
        check,
    )
}

fn prepare_iter_checked<'a>(
    log: &SegmentedOutcomeLog,
    pages: impl Iterator<Item = (&'a PageId, &'a CatalogPage)> + Clone,
    page_count: usize,
    metadata: Vec<u8>,
    captured_cut: Option<StoredOutcomePosition>,
    epoch: u64,
    install: bool,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<PreparedCheckpoint> {
    check()?;
    if epoch == 0 {
        return Err(invalid("zero checkpoint epoch"));
    }
    if let Some(cut) = &captured_cut {
        log.read_metadata(cut)?;
    }
    let mut extents = BTreeMap::new();
    for (_, page) in pages.clone() {
        check()?;
        page.validate_spans()?;
        for span in &page.spans {
            let key = extent_key(&span.extent)?;
            match extents.get(&key) {
                Some((_, old)) if old != &span.extent => {
                    return Err(invalid("conflicting catalog extent descriptors"))
                }
                Some(_) => {}
                None => {
                    extents.insert(key, (0u64, span.extent.clone()));
                }
            }
        }
    }
    for (index, (_, (number, _))) in extents.iter_mut().enumerate() {
        *number = index as u64;
    }
    let mut id = vec![0; 16];
    SystemRandom::new()
        .fill(&mut id)
        .map_err(|_| invalid("checkpoint identity generation failed"))?;
    let header = CatalogHeader {
        version: 1,
        log_id: log.store_id().to_vec(),
        checkpoint_id: id.clone(),
        epoch,
        captured_cut: captured_cut.clone(),
        install,
        metadata,
        extent_count: extents.len() as u64,
        page_count: page_count as u64,
    };
    let mut writer = CatalogWriter {
        log,
        check: Some(check),
        id: id.clone(),
        buffer: Vec::with_capacity(CheckpointFragment::MAX_DATA_BYTES),
        hash: digest::Context::new(&digest::SHA256),
        bytes: 0,
        count: 0,
        first: None,
        last: None,
    };
    let mut scratch = Vec::new();
    writer.row(1, &header, &mut scratch)?;
    for (_, extent) in extents.values() {
        check()?;
        writer.row(2, extent.as_ref(), &mut scratch)?;
    }
    for (id, page) in pages {
        check()?;
        if page_id(&page.metadata)? != *id {
            return Err(invalid("catalog page key mismatch"));
        }
        // PageHeader is encoded directly to borrow its possibly large metadata.
        scratch.clear();
        let meta_len = page.metadata.encoded_len();
        let page_len = 1
            + prost::encoding::encoded_len_varint(meta_len as u64)
            + meta_len
            + if page.spans.is_empty() {
                0
            } else {
                1 + prost::encoding::encoded_len_varint(page.spans.len() as u64)
            };
        scratch.push(26);
        prost::encoding::encode_varint(page_len as u64, &mut scratch);
        scratch.push(10);
        prost::encoding::encode_varint(meta_len as u64, &mut scratch);
        page.metadata.encode(&mut scratch).map_err(invalid)?;
        if !page.spans.is_empty() {
            scratch.push(16);
            prost::encoding::encode_varint(page.spans.len() as u64, &mut scratch);
        }
        writer.raw_row(&scratch)?;
        for span in &page.spans {
            let binding = Span {
                offset: span.offset,
                length: span.length,
                extent_index: extents[&extent_key(&span.extent)?].0,
                extent_offset: span.extent_offset,
            };
            writer.row(4, &binding, &mut scratch)?;
        }
    }
    writer.flush()?;
    let through = if install {
        writer.last.clone()
    } else {
        captured_cut
    };
    Ok(PreparedCheckpoint {
        commit: CheckpointCommit {
            version: 1,
            log_id: log.store_id().to_vec(),
            checkpoint_id: id,
            epoch,
            first: writer.first,
            last: writer.last,
            fragments: writer.count,
            bytes: writer.bytes,
            sha256: writer.hash.finish().as_ref().to_vec(),
            through,
            install,
            index_root: None,
        },
    })
}
impl PreparedCheckpoint {
    /// Call under the journal publication gate after checking its current epoch.
    /// A failed append publishes nothing usable and fences the log on uncertain I/O.
    #[cfg(all(test, unix))]
    pub fn publish(self, log: &SegmentedOutcomeLog) -> io::Result<CheckpointCommit> {
        self.publish_record(log).map(|(commit, _)| commit)
    }
    pub fn publish_record(
        self,
        log: &SegmentedOutcomeLog,
    ) -> io::Result<(CheckpointCommit, StoredOutcomePosition)> {
        validate_commit(log, &self.commit)?;
        let fragment = CheckpointFragment {
            version: 1,
            checkpoint_id: self.commit.checkpoint_id.clone(),
            kind: CheckpointFragment::COMMIT,
            ordinal: self.commit.epoch,
            data: self.commit.encode_to_vec(),
        };
        let record = log.append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])?;
        Ok((self.commit, record.position))
    }
}
fn validate_commit(log: &SegmentedOutcomeLog, commit: &CheckpointCommit) -> io::Result<()> {
    if commit.version == 2 {
        return super::replica_checkpoint_index::validate_commit(log, commit);
    }
    if commit.version != 1
        || commit.index_root.is_some()
        || commit.log_id != log.store_id()
        || commit.checkpoint_id.len() != 16
        || commit.epoch == 0
        || commit.fragments == 0
        || commit.bytes == 0
        || commit.bytes > MAX_CATALOG_BYTES
        || commit.fragments
            != commit
                .bytes
                .div_ceil(CheckpointFragment::MAX_DATA_BYTES as u64)
        || commit.sha256.len() != 32
        || commit.first.is_none()
        || commit.last.is_none()
    {
        return Err(invalid("invalid checkpoint commit"));
    }
    log.metadata_cursor_between(
        commit.first.as_ref().unwrap(),
        commit.last.as_ref().unwrap(),
    )?;
    if let Some(cut) = &commit.through {
        log.read_metadata(cut)?;
    }
    if commit.install && commit.through != commit.last {
        return Err(invalid("invalid install checkpoint cut"));
    }
    Ok(())
}
pub(super) fn page_id(metadata: &CatalogPageMetadata) -> io::Result<PageId> {
    Ok(PageId {
        chunk_id: ChunkId::new(metadata.volume_id, metadata.chunk_id).map_err(invalid)?,
        page_index: metadata.page_index,
    })
}
struct CatalogReader<'a> {
    log: &'a SegmentedOutcomeLog,
    cursor: StoredMetadataCursor<'a>,
    commit: &'a CheckpointCommit,
    buffer: Vec<u8>,
    offset: usize,
    ordinal: u64,
    bytes: u64,
    hash: digest::Context,
    last: Option<StoredOutcomePosition>,
}
impl Read for CatalogReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        while self.offset == self.buffer.len() {
            let Some(record) = self.cursor.next_record()? else {
                return Ok(0);
            };
            let matching = record
                .batch
                .decisions
                .iter()
                .filter_map(|d| d.checkpoint_fragment.as_deref())
                .any(|f| f.checkpoint_id == self.commit.checkpoint_id);
            if !matching {
                continue;
            }
            if record.batch.version != 2 || record.batch.decisions.len() != 1 {
                return Err(invalid("invalid catalog fragment batch"));
            }
            let outcomes = self.log.read_outcomes(&record.position)?;
            let Some(RecoveredMutation::CheckpointFragment(fragment)) = outcomes.into_iter().next()
            else {
                return Err(invalid("missing catalog fragment"));
            };
            if fragment.kind != CheckpointFragment::CATALOG
                || fragment.ordinal != self.ordinal
                || fragment.checkpoint_id != self.commit.checkpoint_id
                || self.ordinal >= self.commit.fragments
                || (self.ordinal == 0 && Some(&record.position) != self.commit.first.as_ref())
                || (self.ordinal + 1 < self.commit.fragments
                    && fragment.data.len() != CheckpointFragment::MAX_DATA_BYTES)
            {
                return Err(invalid("catalog fragment order or identity mismatch"));
            }
            self.bytes = self
                .bytes
                .checked_add(fragment.data.len() as u64)
                .ok_or_else(|| invalid("catalog length overflow"))?;
            if self.bytes > self.commit.bytes {
                return Err(invalid("catalog exceeds committed length"));
            }
            self.hash.update(&fragment.data);
            self.buffer = fragment.data;
            self.offset = 0;
            self.ordinal += 1;
            self.last = Some(record.position);
        }
        let n = out.len().min(self.buffer.len() - self.offset);
        out[..n].copy_from_slice(&self.buffer[self.offset..self.offset + n]);
        self.offset += n;
        Ok(n)
    }
}
impl CatalogReader<'_> {
    fn row(&mut self) -> io::Result<Row> {
        let mut length = [0; 4];
        self.read_exact(&mut length)?;
        let length = u32::from_le_bytes(length) as usize;
        if length == 0 || length > MAX_ROW_BYTES || length as u64 > self.commit.bytes {
            return Err(invalid("invalid catalog row length"));
        }
        let mut bytes = vec![0; length];
        self.read_exact(&mut bytes)?;
        let row = Row::decode(bytes.as_slice()).map_err(invalid)?;
        if row.encode_to_vec() != bytes {
            return Err(invalid("noncanonical catalog row"));
        }
        Ok(row)
    }
    fn finish(mut self) -> io::Result<()> {
        let mut extra = [0];
        if self.read(&mut extra)? != 0
            || self.ordinal != self.commit.fragments
            || self.bytes != self.commit.bytes
            || self.last != self.commit.last
            || self.hash.finish().as_ref() != self.commit.sha256
        {
            return Err(invalid(
                "catalog length, digest or record boundary mismatch",
            ));
        }
        Ok(())
    }
}
pub(super) fn load(
    log: &SegmentedOutcomeLog,
    commit: CheckpointCommit,
    max_pages: usize,
) -> io::Result<LoadedCheckpoint> {
    if commit.version == 2 {
        return super::replica_checkpoint_index::load(log, commit, max_pages);
    }
    validate_commit(log, &commit)?;
    let mut reader = CatalogReader {
        log,
        cursor: log.metadata_cursor_between(
            commit.first.as_ref().unwrap(),
            commit.last.as_ref().unwrap(),
        )?,
        commit: &commit,
        buffer: Vec::new(),
        offset: 0,
        ordinal: 0,
        bytes: 0,
        hash: digest::Context::new(&digest::SHA256),
        last: None,
    };
    let Some(row::Value::Header(header)) = reader.row()?.value else {
        return Err(invalid("missing catalog header"));
    };
    if header.version != 1
        || header.log_id != commit.log_id
        || header.checkpoint_id != commit.checkpoint_id
        || header.epoch != commit.epoch
        || header.install != commit.install
        || header.page_count > max_pages as u64
        || (max_pages == 0 && header.extent_count != 0)
        || header.extent_count > commit.bytes
        || header.page_count > commit.bytes
        || (!header.install && header.captured_cut != commit.through)
    {
        return Err(invalid(
            "catalog header does not match commit or configured limits",
        ));
    }
    // An install must not move its replay cut past intervening logical writes.
    // For a background checkpoint, through stays at the captured pre-write cut.
    if header.install {
        let mut between = log.metadata_cursor_after_through(
            header.captured_cut.as_ref(),
            commit.last.as_ref().unwrap(),
        )?;
        while let Some(record) = between.next_record()? {
            if record.batch.version != 2 {
                return Err(invalid("logical mutation crossed an install checkpoint"));
            }
        }
    } else if let Some(cut) = &header.captured_cut {
        log.metadata_cursor_after_through(Some(cut), commit.first.as_ref().unwrap())?;
    }
    let mut extents = Vec::new();
    let mut previous = None;
    for _ in 0..header.extent_count {
        let Some(row::Value::Extent(extent)) = reader.row()?.value else {
            return Err(invalid("missing catalog extent"));
        };
        let key = extent_key(&extent)?;
        if previous.is_some_and(|p| p >= key) {
            return Err(invalid("duplicate or unordered catalog extent"));
        }
        // Checks ownership, physical bounds and descriptor shape without payload I/O.
        log.read_payload_range(&extent, 0, 0)?;
        previous = Some(key);
        extents.push(Arc::new(extent));
    }
    let mut catalog = PageCatalog::default();
    let mut previous = None;
    for _ in 0..header.page_count {
        let Some(row::Value::Page(page)) = reader.row()?.value else {
            return Err(invalid("missing catalog page"));
        };
        let metadata = page
            .metadata
            .ok_or_else(|| invalid("missing page metadata"))?;
        let id = page_id(&metadata)?;
        if previous.is_some_and(|p| p >= id) || page.spans > commit.bytes {
            return Err(invalid("duplicate, unordered or oversized catalog page"));
        }
        previous = Some(id.clone());
        let mut spans = Vec::new();
        for _ in 0..page.spans {
            let Some(row::Value::Span(span)) = reader.row()?.value else {
                return Err(invalid("missing catalog span"));
            };
            let extent = extents
                .get(usize::try_from(span.extent_index).map_err(invalid)?)
                .ok_or_else(|| invalid("catalog span has unknown extent"))?
                .clone();
            spans.push(PayloadSpan {
                offset: span.offset,
                length: span.length,
                extent,
                extent_offset: span.extent_offset,
            });
        }
        let page = CatalogPage { metadata, spans };
        page.validate_spans()?;
        catalog.pages.insert(id, page);
    }
    reader.finish()?;
    Ok(LoadedCheckpoint {
        header,
        catalog,
        commit,
        index: None,
        publication_position: None,
    })
}
/// Discover commits with metadata reads only. An invalid newest published
/// checkpoint is an error; recovery never falls back to an obsolete checkpoint.
pub(super) fn latest(
    log: &SegmentedOutcomeLog,
    max_pages: usize,
) -> io::Result<Option<LoadedCheckpoint>> {
    let mut cursor = log.metadata_cursor_after(None)?;
    let mut found = None;
    let mut epoch = 0;
    while let Some(record) = cursor.next_record()? {
        for decision in &record.batch.decisions {
            let Some(fragment) = &decision.checkpoint_fragment else {
                continue;
            };
            if fragment.kind != CheckpointFragment::COMMIT {
                continue;
            }
            if record.batch.version != 2
                || record.batch.decisions.len() != 1
                || fragment.ordinal <= epoch
            {
                return Err(invalid("invalid checkpoint publication order"));
            }
            epoch = fragment.ordinal;
            found = Some((record.position.clone(), fragment.checkpoint_id.clone()));
        }
    }
    let Some((position, id)) = found else {
        return Ok(None);
    };
    let outcomes = log.read_outcomes(&position)?;
    let Some(RecoveredMutation::CheckpointFragment(fragment)) = outcomes.into_iter().next() else {
        return Err(invalid("missing checkpoint commit"));
    };
    let commit = CheckpointCommit::decode(fragment.data.as_slice()).map_err(invalid)?;
    if commit.encode_to_vec() != fragment.data
        || commit.epoch != epoch
        || commit.checkpoint_id != id
    {
        return Err(invalid("checkpoint commit identity or encoding mismatch"));
    }
    // Ensure the catalog was fully present before its publication record.
    log.metadata_cursor_after_through(commit.last.as_ref(), &position)?;
    let mut loaded = load(log, commit, max_pages)?;
    loaded.publication_position = Some(position);
    Ok(Some(loaded))
}
#[cfg(all(test, unix))]
#[path = "replica_checkpoint_catalog_tests.rs"]
mod tests;
