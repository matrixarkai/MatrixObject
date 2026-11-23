// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable creation ownership. The anchor commits a checksummed registry prefix before ACK.
//! One entry per chunk transition; appends and reads only consult the resident index.
use prost::Message;
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
const REGISTRY_MAGIC: &[u8] = b"MOSRSOWNER1";
const ANCHOR_MAGIC: &[u8] = b"MOSRSANCHOR1";
const MAX_RECORD: usize = 512;
const PENDING: u32 = 1;
const READY: u32 = 2;
const RETIRED: u32 = 3;
fn invalid(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s)
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    crate::chunk_integrity::sha256(bytes)
}
fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
#[derive(Clone, PartialEq, Eq, Message)]
pub(super) struct StorageOwner {
    #[prost(uint64, tag = "1")]
    pub volume: u64,
    #[prost(uint64, tag = "2")]
    pub chunk: u64,
    #[prost(uint32, tag = "3")]
    pub generation: u32,
    #[prost(uint64, tag = "4")]
    pub epoch: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub initialization: Vec<u8>,
    #[prost(uint32, tag = "6")]
    pub state: u32,
}
impl StorageOwner {
    fn validate(&self) -> io::Result<()> {
        crate::ChunkId::new(self.volume, self.chunk)
            .map_err(|_| invalid("invalid owner coordinates"))?;
        if self.generation == 0
            || self.epoch == 0
            || self.initialization.len() != 16
            || self.initialization.iter().all(|b| *b == 0)
            || !matches!(self.state, PENDING | READY | RETIRED)
        {
            return Err(invalid("invalid native storage owner"));
        }
        Ok(())
    }
    fn same_identity(&self, other: &Self) -> bool {
        self.volume == other.volume
            && self.chunk == other.chunk
            && self.generation == other.generation
            && self.epoch == other.epoch
            && self.initialization == other.initialization
    }
}
#[derive(Clone, PartialEq, Message)]
struct Entry {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    sequence: u64,
    #[prost(bytes = "vec", tag = "3")]
    previous: Vec<u8>,
    #[prost(message, optional, tag = "4")]
    owner: Option<StorageOwner>,
}
#[derive(Clone, PartialEq, Message)]
struct Anchor {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    incarnation: Vec<u8>,
    #[prost(uint64, tag = "3")]
    length: u64,
    #[prost(uint64, tag = "4")]
    sequence: u64,
    #[prost(bytes = "vec", tag = "5")]
    digest: Vec<u8>,
}
pub(super) struct NativeStorageAuthority {
    directory: PathBuf,
    registry: File,
    anchor: Anchor,
    owners: BTreeMap<(u64, u64), StorageOwner>,
    poisoned: bool,
}
impl NativeStorageAuthority {
    pub(super) fn path(root: &Path) -> PathBuf {
        root.join(".native-authority")
    }
    pub(super) fn open(root: &Path) -> io::Result<Self> {
        let directory = Self::path(root);
        let fresh = match std::fs::create_dir(&directory) {
            Ok(()) => {
                sync_directory(root)?;
                true
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => false,
            Err(e) => return Err(e),
        };
        let mut registry = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(fresh)
            .open(directory.join("owners"))?;
        let anchor = if fresh {
            let mut incarnation = [0; 16];
            SystemRandom::new()
                .fill(&mut incarnation)
                .map_err(|_| invalid("storage randomness unavailable"))?;
            if incarnation == [0; 16] {
                return Err(invalid("storage randomness returned zero"));
            }
            let mut header = REGISTRY_MAGIC.to_vec();
            header.extend_from_slice(&incarnation);
            registry.write_all(&header)?;
            registry.sync_all()?;
            sync_directory(&directory)?;
            let anchor = Anchor {
                version: 1,
                incarnation: incarnation.to_vec(),
                length: header.len() as u64,
                sequence: 0,
                digest: digest(&header).to_vec(),
            };
            Self::persist_anchor(&directory, &anchor)?;
            anchor
        } else {
            Self::read_anchor(&directory)?
        };
        if anchor.version != 1
            || anchor.incarnation.len() != 16
            || anchor.incarnation.iter().all(|b| *b == 0)
            || anchor.digest.len() != 32
            || anchor.length < (REGISTRY_MAGIC.len() + 16) as u64
            || registry.metadata()?.len() < anchor.length
        {
            return Err(invalid("invalid native storage anchor"));
        }
        registry.seek(SeekFrom::Start(0))?;
        let mut header = vec![0; REGISTRY_MAGIC.len() + 16];
        registry.read_exact(&mut header)?;
        if !header.starts_with(REGISTRY_MAGIC)
            || header[REGISTRY_MAGIC.len()..] != anchor.incarnation
        {
            return Err(invalid("storage identity differs from registry"));
        }
        let mut position = header.len() as u64;
        let mut previous = digest(&header);
        let mut sequence = 0;
        let mut owners = BTreeMap::new();
        while position < anchor.length {
            if anchor.length - position < 4 {
                return Err(invalid("truncated committed storage entry"));
            }
            let mut size = [0; 4];
            registry.read_exact(&mut size)?;
            let size = u32::from_be_bytes(size) as usize;
            if size == 0 || size > MAX_RECORD || anchor.length - position < 4 + size as u64 + 32 {
                return Err(invalid("invalid committed storage entry size"));
            }
            let mut payload = vec![0; size];
            registry.read_exact(&mut payload)?;
            let mut checksum = [0; 32];
            registry.read_exact(&mut checksum)?;
            if digest(&payload) != checksum {
                return Err(invalid("storage entry checksum differs"));
            }
            let entry = Entry::decode(payload.as_slice())
                .map_err(|_| invalid("invalid storage entry protobuf"))?;
            if entry.encode_to_vec() != payload
                || entry.version != 1
                || entry.sequence != sequence + 1
                || entry.previous != previous
            {
                return Err(invalid(
                    "storage entry history is not canonical and consecutive",
                ));
            }
            let owner = entry
                .owner
                .ok_or_else(|| invalid("storage entry lacks owner"))?;
            Self::check_transition(&owners, &owner)?;
            owners.insert((owner.volume, owner.chunk), owner);
            sequence += 1;
            previous = checksum;
            position += 4 + size as u64 + 32;
        }
        if sequence != anchor.sequence || previous.as_slice() != anchor.digest {
            return Err(invalid("storage anchor does not bind committed history"));
        }
        // Data-file changes follow an anchored PENDING entry. An unanchored suffix cannot
        // authorize creation or reads, and may be removed after an interrupted commit.
        if registry.metadata()?.len() != position {
            registry.set_len(position)?;
            registry.sync_all()?;
        }
        registry.seek(SeekFrom::Start(position))?;
        Ok(Self {
            directory,
            registry,
            anchor,
            owners,
            poisoned: false,
        })
    }
    fn read_anchor(directory: &Path) -> io::Result<Anchor> {
        let mut bytes = Vec::new();
        File::open(directory.join("anchor"))?
            .take(2049)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 2048
            || bytes.len() < ANCHOR_MAGIC.len() + 32
            || !bytes.starts_with(ANCHOR_MAGIC)
        {
            return Err(invalid("invalid storage anchor envelope"));
        }
        let end = bytes.len() - 32;
        if digest(&bytes[..end]).as_slice() != &bytes[end..] {
            return Err(invalid("storage anchor checksum differs"));
        }
        let payload = &bytes[ANCHOR_MAGIC.len()..end];
        let anchor =
            Anchor::decode(payload).map_err(|_| invalid("invalid storage anchor protobuf"))?;
        if anchor.encode_to_vec() != payload {
            return Err(invalid("noncanonical storage anchor"));
        }
        Ok(anchor)
    }
    fn persist_anchor(directory: &Path, anchor: &Anchor) -> io::Result<()> {
        let mut bytes = ANCHOR_MAGIC.to_vec();
        anchor
            .encode(&mut bytes)
            .map_err(|_| invalid("storage anchor encoding failed"))?;
        bytes.extend_from_slice(&digest(&bytes));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(directory.join("anchor.pending"))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(directory.join("anchor.pending"), directory.join("anchor"))?;
        sync_directory(directory)
    }
    fn check_transition(
        owners: &BTreeMap<(u64, u64), StorageOwner>,
        owner: &StorageOwner,
    ) -> io::Result<()> {
        owner.validate()?;
        match owners.get(&(owner.volume, owner.chunk)) {
            None if owner.state == PENDING => Ok(()),
            Some(old)
                if old.same_identity(owner)
                    && ((old.state == PENDING && owner.state == READY)
                        || (old.state != RETIRED && owner.state == RETIRED)) =>
            {
                Ok(())
            }
            _ => Err(invalid("native storage ownership cannot be reassigned")),
        }
    }
    fn healthy(&self) -> io::Result<()> {
        if self.poisoned {
            Err(invalid("native storage authority needs recovery"))
        } else {
            Ok(())
        }
    }
    fn commit(&mut self, owner: StorageOwner) -> io::Result<()> {
        self.healthy()?;
        Self::check_transition(&self.owners, &owner)?;
        let sequence = self
            .anchor
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("storage sequence exhausted"))?;
        let entry = Entry {
            version: 1,
            sequence,
            previous: self.anchor.digest.clone(),
            owner: Some(owner.clone()),
        };
        let payload = entry.encode_to_vec();
        if payload.len() > MAX_RECORD {
            return Err(invalid("storage entry too large"));
        }
        let checksum = digest(&payload);
        let mut next = self.anchor.clone();
        next.sequence = sequence;
        next.length = next
            .length
            .checked_add(4 + payload.len() as u64 + 32)
            .ok_or_else(|| invalid("storage registry exhausted"))?;
        next.digest = checksum.to_vec();
        self.poisoned = true;
        self.registry
            .write_all(&(payload.len() as u32).to_be_bytes())?;
        self.registry.write_all(&payload)?;
        self.registry.write_all(&checksum)?;
        self.registry.sync_all()?;
        Self::persist_anchor(&self.directory, &next)?;
        self.anchor = next;
        self.owners.insert((owner.volume, owner.chunk), owner);
        self.poisoned = false;
        Ok(())
    }
    pub(super) fn incarnation(&self) -> io::Result<[u8; 16]> {
        self.healthy()?;
        Ok(self.anchor.incarnation.as_slice().try_into().unwrap())
    }
    pub(super) fn is_ready(&self, volume: u64, chunk: u64) -> bool {
        self.owners
            .get(&(volume, chunk))
            .is_some_and(|o| o.state == READY)
    }
    pub(super) fn contains(&self, volume: u64, chunk: u64) -> bool {
        self.owners.contains_key(&(volume, chunk))
    }
    pub(super) fn verify_incarnation(&self, incarnation: [u8; 16]) -> io::Result<()> {
        if self.incarnation()? != incarnation {
            return Err(invalid("physical storage incarnation differs"));
        }
        Ok(())
    }
    pub(super) fn begin(&mut self, mut owner: StorageOwner, file_exists: bool) -> io::Result<()> {
        self.healthy()?;
        owner.state = PENDING;
        owner.validate()?;
        if let Some(old) = self.owners.get(&(owner.volume, owner.chunk)) {
            if !old.same_identity(&owner)
                || old.state == RETIRED
                || (old.state == READY && !file_exists)
            {
                return Err(invalid(
                    "initialized storage cannot be recreated or reassigned",
                ));
            }
            return Ok(());
        }
        if file_exists {
            return Err(invalid(
                "unowned existing file cannot become native storage",
            ));
        }
        self.commit(owner)
    }
    pub(super) fn complete(&mut self, volume: u64, chunk: u64) -> io::Result<()> {
        let mut owner = self
            .owners
            .get(&(volume, chunk))
            .cloned()
            .ok_or_else(|| invalid("native storage creation was not reserved"))?;
        if owner.state == READY {
            return self.healthy();
        }
        owner.state = READY;
        self.commit(owner)
    }
    pub(super) fn authorize(
        &self,
        volume: u64,
        chunk: u64,
        generation: u32,
        initialization: [u8; 16],
    ) -> io::Result<()> {
        self.healthy()?;
        if !self.owners.get(&(volume, chunk)).is_some_and(|o| {
            o.state == READY && o.generation == generation && o.initialization == initialization
        }) {
            return Err(invalid("native storage member is not qualified"));
        }
        Ok(())
    }
    #[cfg(test)]
    fn retire(&mut self, volume: u64, chunk: u64) -> io::Result<()> {
        let mut owner = self
            .owners
            .get(&(volume, chunk))
            .cloned()
            .ok_or_else(|| invalid("native retirement lacks its owner"))?;
        if owner.state == RETIRED {
            return self.healthy();
        }
        owner.state = RETIRED;
        self.commit(owner)
    }
}
#[cfg(test)]
#[path = "native_storage_authority_tests.rs"]
mod tests;
