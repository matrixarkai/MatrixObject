// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn saturating_add(left: u64, right: u64) -> u64 {
    left.saturating_add(right)
}

pub(crate) fn ranges_intersect(a_offset: u64, a_length: u64, b_offset: u64, b_length: u64) -> bool {
    if a_length == 0 || b_length == 0 {
        return false;
    }
    let Some(a_end) = a_offset.checked_add(a_length) else {
        return false;
    };
    let Some(b_end) = b_offset.checked_add(b_length) else {
        return false;
    };
    a_offset < b_end && b_offset < a_end
}

pub(crate) const PERSISTENT_JOURNAL_MAGIC: &str = "MOSRSJNL1";
pub(crate) const MAX_PERSISTENT_JOURNAL_RECORD_BYTES: usize = 128 * 1024 * 1024;

#[derive(Default)]
struct PersistentJournalSyncState {
    pending_frames: u64,
    pending_bytes: u64,
    first_unsynced_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PersistentJournalSyncPolicy {
    every_frames: u64,
    every_bytes: u64,
    max_unsynced_ms: u64,
}

fn persistent_journal_sync_states(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, PersistentJournalSyncState>> {
    static STATES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, PersistentJournalSyncState>>,
    > = std::sync::OnceLock::new();
    STATES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

fn persistent_journal_known_synced_paths(
) -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static PATHS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    PATHS.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

fn persistent_journal_known_initialized_paths(
) -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static PATHS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    PATHS.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

fn persistent_journal_known_parent_dirs(
) -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static DIRS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    DIRS.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

fn persistent_journal_append_handles(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, std::fs::File>> {
    static HANDLES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
    > = std::sync::OnceLock::new();
    HANDLES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

const MAX_PERSISTENT_JOURNAL_PARENT_DIR_CACHE_ENTRIES: usize = 4096;
const MAX_PERSISTENT_JOURNAL_PATH_STATE_CACHE_ENTRIES: usize = 4096;
const MAX_PERSISTENT_JOURNAL_APPEND_HANDLES: usize = 256;

fn trim_persistent_journal_path_cache(paths: &mut std::collections::HashSet<String>) {
    let excess = paths
        .len()
        .saturating_sub(MAX_PERSISTENT_JOURNAL_PATH_STATE_CACHE_ENTRIES);
    if excess == 0 {
        return;
    }
    let evicted: Vec<_> = paths.iter().take(excess).cloned().collect();
    for path in evicted {
        paths.remove(&path);
    }
}

fn sync_persistent_journal_handle_before_eviction(
    path_key: &str,
    file: &mut std::fs::File,
) -> Result<(), ObjectError> {
    if persistent_journal_is_known_synced(path_key) {
        return Ok(());
    }
    file.sync_data().map_err(|err| {
        ObjectError::Corruption(format!(
            "failed to sync persistent journal {} before handle eviction: {}",
            path_key, err
        ))
    })?;
    mark_persistent_journal_synced(path_key);
    Ok(())
}

fn trim_persistent_journal_append_handles(
    handles: &mut std::collections::HashMap<String, std::fs::File>,
) -> Result<(), ObjectError> {
    if handles.len() <= MAX_PERSISTENT_JOURNAL_APPEND_HANDLES {
        return Ok(());
    }
    if let Ok(synced_paths) = persistent_journal_known_synced_paths().lock() {
        let excess = handles
            .len()
            .saturating_sub(MAX_PERSISTENT_JOURNAL_APPEND_HANDLES);
        let evicted: Vec<_> = handles
            .keys()
            .filter(|path| synced_paths.contains(*path))
            .take(excess)
            .cloned()
            .collect();
        for path in evicted {
            handles.remove(&path);
        }
    }
    let excess = handles
        .len()
        .saturating_sub(MAX_PERSISTENT_JOURNAL_APPEND_HANDLES);
    if excess == 0 {
        return Ok(());
    }
    let evicted: Vec<_> = handles.keys().take(excess).cloned().collect();
    for path in evicted {
        if let Some(mut file) = handles.remove(&path) {
            sync_persistent_journal_handle_before_eviction(&path, &mut file)?;
        }
    }
    Ok(())
}

fn env_u64_or_default(name: &str, default_value: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value.trim().parse::<u64>().unwrap_or(default_value),
        Err(_) => default_value,
    }
}

fn env_bool_or_default(name: &str, default_value: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default_value,
        },
        Err(_) => default_value,
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn persistent_journal_sync_policy() -> PersistentJournalSyncPolicy {
    static POLICY: std::sync::OnceLock<PersistentJournalSyncPolicy> = std::sync::OnceLock::new();
    *POLICY.get_or_init(|| PersistentJournalSyncPolicy {
        every_frames: env_u64_or_default("MATRIX_OBJECTSTORE_RUST_JOURNAL_SYNC_EVERY_FRAMES", 1),
        every_bytes: env_u64_or_default("MATRIX_OBJECTSTORE_RUST_JOURNAL_SYNC_EVERY_BYTES", 0),
        max_unsynced_ms: env_u64_or_default(
            "MATRIX_OBJECTSTORE_RUST_JOURNAL_SYNC_MAX_UNSYNCED_MS",
            0,
        ),
    })
}

fn note_persistent_journal_append(path_key: &str, frame_bytes: u64) -> bool {
    let policy = persistent_journal_sync_policy();
    if policy.every_frames <= 1 && policy.every_bytes == 0 && policy.max_unsynced_ms == 0 {
        return true;
    }
    let Ok(mut states) = persistent_journal_sync_states().lock() else {
        if let Ok(mut paths) = persistent_journal_known_synced_paths().lock() {
            paths.remove(path_key);
        }
        return true;
    };
    let state = states.entry(path_key.to_string()).or_default();
    let now_ms = current_time_ms();
    if state.pending_frames == 0 && state.pending_bytes == 0 {
        state.first_unsynced_ms = now_ms;
        if let Ok(mut paths) = persistent_journal_known_synced_paths().lock() {
            paths.remove(path_key);
        }
    }
    state.pending_frames = state.pending_frames.saturating_add(1);
    state.pending_bytes = state.pending_bytes.saturating_add(frame_bytes);
    if policy.every_frames == 0 && policy.every_bytes == 0 && policy.max_unsynced_ms == 0 {
        return false;
    }
    let frame_due = policy.every_frames > 0 && state.pending_frames >= policy.every_frames;
    let byte_due = policy.every_bytes > 0 && state.pending_bytes >= policy.every_bytes;
    let age_due = policy.max_unsynced_ms > 0
        && now_ms.saturating_sub(state.first_unsynced_ms) >= policy.max_unsynced_ms;
    if frame_due || byte_due || age_due {
        state.pending_frames = 0;
        state.pending_bytes = 0;
        state.first_unsynced_ms = 0;
        true
    } else {
        false
    }
}

fn persistent_journal_is_known_synced(path_key: &str) -> bool {
    let Ok(paths) = persistent_journal_known_synced_paths().lock() else {
        return false;
    };
    paths.contains(path_key)
}

fn persistent_journal_append_handle_cache_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        env_bool_or_default("MATRIX_OBJECTSTORE_RUST_JOURNAL_CACHE_APPEND_HANDLES", true)
    })
}

fn mark_persistent_journal_synced(path_key: &str) {
    if let Ok(mut states) = persistent_journal_sync_states().lock() {
        states.remove(path_key);
    }
    if let Ok(mut paths) = persistent_journal_known_synced_paths().lock() {
        paths.insert(path_key.to_string());
        trim_persistent_journal_path_cache(&mut paths);
    }
}

fn persistent_journal_is_known_initialized(path_key: &str) -> bool {
    let Ok(paths) = persistent_journal_known_initialized_paths().lock() else {
        return false;
    };
    paths.contains(path_key)
}

fn mark_persistent_journal_initialized(path_key: &str) {
    if let Ok(mut paths) = persistent_journal_known_initialized_paths().lock() {
        paths.insert(path_key.to_string());
        trim_persistent_journal_path_cache(&mut paths);
    }
}

pub(crate) fn forget_persistent_journal_path(path_key: &str) {
    if let Ok(mut states) = persistent_journal_sync_states().lock() {
        states.remove(path_key);
    }
    if let Ok(mut paths) = persistent_journal_known_synced_paths().lock() {
        paths.remove(path_key);
    }
    if let Ok(mut paths) = persistent_journal_known_initialized_paths().lock() {
        paths.remove(path_key);
    }
    if let Ok(mut handles) = persistent_journal_append_handles().lock() {
        handles.remove(path_key);
    }
}

fn ensure_persistent_journal_parent_dir(parent: &std::path::Path) -> Result<(), ObjectError> {
    let key = parent.to_string_lossy().into_owned();
    if persistent_journal_known_parent_dirs()
        .lock()
        .map(|dirs| dirs.contains(&key))
        .unwrap_or(false)
    {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(|err| {
        ObjectError::Corruption(format!(
            "failed to create persistent journal directory {}: {}",
            parent.display(),
            err
        ))
    })?;
    if let Ok(mut dirs) = persistent_journal_known_parent_dirs().lock() {
        dirs.insert(key);
        let excess = dirs
            .len()
            .saturating_sub(MAX_PERSISTENT_JOURNAL_PARENT_DIR_CACHE_ENTRIES);
        if excess > 0 {
            let evicted: Vec<_> = dirs.iter().take(excess).cloned().collect();
            for dir in evicted {
                dirs.remove(&dir);
            }
        }
    }
    Ok(())
}

fn reopen_persistent_journal_after_parent_miss(
    path: &std::path::Path,
    parent: Option<&std::path::Path>,
) -> Result<std::fs::File, ObjectError> {
    if let Some(parent) = parent {
        let key = parent.to_string_lossy().into_owned();
        if let Ok(mut dirs) = persistent_journal_known_parent_dirs().lock() {
            dirs.remove(&key);
        }
        ensure_persistent_journal_parent_dir(parent)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| {
            ObjectError::Corruption(format!(
                "failed to open persistent journal {}: {}",
                path.display(),
                err
            ))
        })
}

pub(crate) fn sync_persistent_journal(path: &str) -> Result<(), ObjectError> {
    if path.is_empty() {
        return Ok(());
    }
    let path_key = path;
    let path = std::path::Path::new(path_key);
    if persistent_journal_is_known_synced(path_key) {
        return Ok(());
    }
    if persistent_journal_append_handle_cache_enabled() {
        let mut synced_cached_handle = false;
        let mut sync_error = None;
        if let Ok(mut handles) = persistent_journal_append_handles().lock() {
            if let Some(file) = handles.get_mut(path_key) {
                if let Err(err) = file.sync_data() {
                    handles.remove(path_key);
                    sync_error = Some(err);
                } else {
                    synced_cached_handle = true;
                }
            }
        }
        if let Some(err) = sync_error {
            forget_persistent_journal_path(path_key);
            return Err(ObjectError::Corruption(format!(
                "failed to sync persistent journal {}: {}",
                path.display(),
                err
            )));
        }
        if synced_cached_handle {
            mark_persistent_journal_synced(path_key);
            return Ok(());
        }
    }
    let file = match std::fs::OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            forget_persistent_journal_path(path_key);
            return Ok(());
        }
        Err(err) => {
            return Err(ObjectError::Corruption(format!(
                "failed to open persistent journal {} for sync: {}",
                path.display(),
                err
            )));
        }
    };
    file.sync_data().map_err(|err| {
        ObjectError::Corruption(format!(
            "failed to sync persistent journal {}: {}",
            path.display(),
            err
        ))
    })?;
    mark_persistent_journal_synced(path_key);
    Ok(())
}

fn write_all_vectored_frame(
    file: &mut std::fs::File,
    path: &std::path::Path,
    frame_header: &[u8],
    payload: &[u8],
    frame_trailer: &[u8],
) -> Result<(), ObjectError> {
    use std::io::{IoSlice, Write};
    let parts = [frame_header, payload, frame_trailer];
    let mut part_index = 0usize;
    let mut part_offset = 0usize;

    while part_index < parts.len() {
        while part_index < parts.len() && part_offset == parts[part_index].len() {
            part_index += 1;
            part_offset = 0;
        }
        if part_index >= parts.len() {
            break;
        }

        let slices = [
            IoSlice::new(&parts[part_index][part_offset..]),
            IoSlice::new(parts.get(part_index + 1).copied().unwrap_or_default()),
            IoSlice::new(parts.get(part_index + 2).copied().unwrap_or_default()),
        ];

        let written = file.write_vectored(&slices).map_err(|err| {
            ObjectError::Corruption(format!(
                "failed to append persistent journal {}: {}",
                path.display(),
                err
            ))
        })?;
        if written == 0 {
            return Err(ObjectError::Corruption(format!(
                "failed to append persistent journal {}: short vectored write",
                path.display()
            )));
        }

        let mut remaining = written;
        while part_index < parts.len() {
            let available = parts[part_index].len().saturating_sub(part_offset);
            if remaining < available {
                part_offset += remaining;
                break;
            }
            remaining -= available;
            part_index += 1;
            part_offset = 0;
            if remaining == 0 {
                break;
            }
        }
    }
    Ok(())
}

fn append_etag_bytes(out: &mut [u8], data: &[u8]) -> usize {
    const PREFIX: &[u8] = b"fnv1a64:";
    const HEX: &[u8; 16] = b"0123456789abcdef";
    debug_assert!(out.len() >= PREFIX.len() + 16);
    out[..PREFIX.len()].copy_from_slice(PREFIX);
    let mut value = fnv1a64(data);
    for index in (0..16).rev() {
        out[PREFIX.len() + index] = HEX[(value & 0xf) as usize];
        value >>= 4;
    }
    PREFIX.len() + 16
}

pub(crate) fn append_journal_put_options(out: &mut Vec<u8>, options: &PutOptions) {
    append_u64(out, 2);
    match &options.storage_class {
        Some(storage_class) => {
            append_u64(out, 1);
            append_string(out, storage_class);
        }
        None => append_u64(out, 0),
    }
    append_string_map(out, &options.user_metadata);
    append_string_map(out, &options.tags);
}

pub(crate) fn read_journal_put_options(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<PutOptions, ObjectError> {
    let version = cursor.read_u64()?;
    if version != 1 && version != 2 {
        return Err(ObjectError::Corruption(
            "persistent journal put options version is unsupported".to_string(),
        ));
    }
    let storage_class = match cursor.read_u64()? {
        0 => None,
        1 => Some(cursor.read_string()?),
        _ => {
            return Err(ObjectError::Corruption(
                "persistent journal storage class presence flag is invalid".to_string(),
            ))
        }
    };
    let user_metadata = if version >= 2 {
        read_string_map(cursor, "persistent journal put option user metadata count")?
    } else {
        BTreeMap::new()
    };
    let tags = read_string_map(cursor, "persistent journal put option tag count")?;
    let options = PutOptions {
        storage_class,
        user_metadata,
        tags,
        ..PutOptions::default()
    };
    validate_user_metadata(&options.user_metadata)?;
    validate_tags(&options.tags)?;
    if let Some(storage_class) = &options.storage_class {
        validate_storage_class(storage_class)?;
    }
    Ok(options)
}

pub(crate) fn read_journal_optional_replica_count(
    cursor: &mut SnapshotByteCursor<'_>,
    context: &str,
) -> Result<Option<usize>, ObjectError> {
    match cursor.read_u64()? {
        0 => Ok(None),
        1 => Ok(Some(cursor.read_count(context)?)),
        _ => Err(ObjectError::Corruption(format!(
            "{context} presence flag is invalid"
        ))),
    }
}

pub(crate) fn append_object_put_request(out: &mut Vec<u8>, request: &ObjectPutRequest) {
    append_string(out, &request.bucket);
    append_string(out, &request.key);
    append_string(out, &request.content_type);
    append_u64(out, request.data.len() as u64);
    out.extend_from_slice(&request.data);
    append_journal_put_options(out, &request.options);
}

pub(crate) fn read_object_put_request(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ObjectPutRequest, ObjectError> {
    let bucket = cursor.read_string()?;
    let key = cursor.read_string()?;
    let content_type = cursor.read_string()?;
    let data_len = cursor.read_u64()? as usize;
    let data = cursor.read_bytes(data_len)?.to_vec();
    let options = read_journal_put_options(cursor)?;
    Ok(ObjectPutRequest {
        bucket,
        key,
        data,
        content_type,
        options,
    })
}

pub(crate) fn append_object_copy_request(out: &mut Vec<u8>, request: &ObjectCopyRequest) {
    append_string(out, &request.source_bucket);
    append_string(out, &request.source_key);
    match request.source_version {
        Some(version) => {
            append_u64(out, 1);
            append_u64(out, version);
        }
        None => append_u64(out, 0),
    }
    append_string(out, &request.destination_bucket);
    append_string(out, &request.destination_key);
    match &request.content_type {
        Some(content_type) => {
            append_u64(out, 1);
            append_string(out, content_type);
        }
        None => append_u64(out, 0),
    }
    append_journal_put_options(out, &request.options);
}

pub(crate) fn read_object_copy_request(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ObjectCopyRequest, ObjectError> {
    let source_bucket = cursor.read_string()?;
    let source_key = cursor.read_string()?;
    let source_version = match cursor.read_u64()? {
        0 => None,
        1 => Some(cursor.read_u64()?),
        _ => {
            return Err(ObjectError::Corruption(
                "persistent journal copy request source-version presence flag is invalid"
                    .to_string(),
            ))
        }
    };
    let destination_bucket = cursor.read_string()?;
    let destination_key = cursor.read_string()?;
    let content_type = match cursor.read_u64()? {
        0 => None,
        1 => Some(cursor.read_string()?),
        _ => {
            return Err(ObjectError::Corruption(
                "persistent journal copy request content-type presence flag is invalid".to_string(),
            ))
        }
    };
    let options = read_journal_put_options(cursor)?;
    Ok(ObjectCopyRequest {
        source_bucket,
        source_key,
        source_version,
        destination_bucket,
        destination_key,
        content_type,
        options,
    })
}

pub(crate) fn committed_batch_copy_request_refs(
    report: &ObjectBatchCopyReport,
) -> Vec<&ObjectCopyRequest> {
    let mut committed = Vec::with_capacity(report.items.len());
    for item in &report.items {
        if item.metadata.is_some() {
            committed.push(&item.request);
        }
    }
    committed
}

pub(crate) fn append_object_delete_request(out: &mut Vec<u8>, request: &ObjectDeleteRequest) {
    append_string(out, &request.bucket);
    append_string(out, &request.key);
    match &request.if_match_etag {
        Some(etag) => {
            append_u64(out, 1);
            append_string(out, etag);
        }
        None => append_u64(out, 0),
    }
    match request.version {
        Some(version) => {
            append_u64(out, 1);
            append_u64(out, version);
        }
        None => append_u64(out, 0),
    }
}

pub(crate) fn read_object_delete_request(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ObjectDeleteRequest, ObjectError> {
    let bucket = cursor.read_string()?;
    let key = cursor.read_string()?;
    let if_match_etag = match cursor.read_u64()? {
        0 => None,
        1 => Some(cursor.read_string()?),
        _ => {
            return Err(ObjectError::Corruption(
                "persistent journal delete request if-match presence flag is invalid".to_string(),
            ))
        }
    };
    let version = match cursor.read_u64()? {
        0 => None,
        1 => Some(cursor.read_u64()?),
        _ => {
            return Err(ObjectError::Corruption(
                "persistent journal delete request version presence flag is invalid".to_string(),
            ))
        }
    };
    Ok(ObjectDeleteRequest {
        bucket,
        key,
        if_match_etag,
        version,
    })
}

pub(crate) fn committed_batch_delete_request_refs(
    report: &ObjectBatchDeleteReport,
) -> Vec<&ObjectDeleteRequest> {
    let mut committed = Vec::with_capacity(report.items.len());
    for item in &report.items {
        if item.metadata.is_some() {
            committed.push(&item.request);
        }
    }
    committed
}

pub(crate) fn append_persistent_journal_frame(
    path: &str,
    payload: &[u8],
) -> Result<(), ObjectError> {
    let path_key = path;
    let path = std::path::Path::new(path_key);
    let parent = path.parent();
    if let Some(parent) = parent {
        ensure_persistent_journal_parent_dir(parent)?;
    }
    if persistent_journal_append_handle_cache_enabled() {
        let mut file = {
            let mut handles = persistent_journal_append_handles().lock().map_err(|_| {
                ObjectError::Corruption(
                    "persistent journal append handle cache is poisoned".to_string(),
                )
            })?;
            if let Some(file) = handles.remove(path_key) {
                file
            } else {
                match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    drop(handles);
                    forget_persistent_journal_path(path_key);
                    let file = reopen_persistent_journal_after_parent_miss(path, parent)?;
                    file
                }
                Err(err) => {
                    return Err(ObjectError::Corruption(format!(
                        "failed to open persistent journal {}: {}",
                        path.display(),
                        err
                    )));
                }
            }
            }
        };
        let result = append_persistent_journal_frame_to_file(&mut file, path_key, path, payload);
        if result.is_ok() {
            let mut handles = persistent_journal_append_handles().lock().map_err(|_| {
                ObjectError::Corruption(
                    "persistent journal append handle cache is poisoned".to_string(),
                )
            })?;
            handles.insert(path_key.to_string(), file);
            trim_persistent_journal_append_handles(&mut handles)?;
        } else {
            forget_persistent_journal_path(path_key);
        }
        return result;
    }
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            forget_persistent_journal_path(path_key);
            reopen_persistent_journal_after_parent_miss(path, parent)?
        }
        Err(err) => {
            return Err(ObjectError::Corruption(format!(
                "failed to open persistent journal {}: {}",
                path.display(),
                err
            )));
        }
    };
    append_persistent_journal_frame_to_file(&mut file, path_key, path, payload)
}

fn append_persistent_journal_frame_to_file(
    file: &mut std::fs::File,
    path_key: &str,
    path: &std::path::Path,
    payload: &[u8],
) -> Result<(), ObjectError> {
    let needs_header = if persistent_journal_is_known_initialized(path_key) {
        false
    } else {
        file.metadata()
            .map_err(|err| {
                ObjectError::Corruption(format!(
                    "failed to stat persistent journal {}: {}",
                    path.display(),
                    err
                ))
            })?
            .len()
            == 0
    };
    let mut frame_header = [0u8; std::mem::size_of::<u64>() * 2 + PERSISTENT_JOURNAL_MAGIC.len()];
    let mut frame_header_len = 0usize;
    if needs_header {
        let magic_len = PERSISTENT_JOURNAL_MAGIC.len() as u64;
        frame_header[frame_header_len..frame_header_len + std::mem::size_of::<u64>()]
            .copy_from_slice(&magic_len.to_le_bytes());
        frame_header_len += std::mem::size_of::<u64>();
        frame_header[frame_header_len..frame_header_len + PERSISTENT_JOURNAL_MAGIC.len()]
            .copy_from_slice(PERSISTENT_JOURNAL_MAGIC.as_bytes());
        frame_header_len += PERSISTENT_JOURNAL_MAGIC.len();
    }
    let payload_len = payload.len() as u64;
    frame_header[frame_header_len..frame_header_len + std::mem::size_of::<u64>()]
        .copy_from_slice(&payload_len.to_le_bytes());
    frame_header_len += std::mem::size_of::<u64>();
    let mut frame_trailer = [0u8; std::mem::size_of::<u64>() + 64];
    let mut frame_trailer_len = 0usize;
    let checksum_len = 24u64;
    frame_trailer[frame_trailer_len..frame_trailer_len + std::mem::size_of::<u64>()]
        .copy_from_slice(&checksum_len.to_le_bytes());
    frame_trailer_len += std::mem::size_of::<u64>();
    frame_trailer_len += append_etag_bytes(&mut frame_trailer[frame_trailer_len..], payload);
    let frame_bytes = frame_header_len
        .saturating_add(payload.len())
        .saturating_add(frame_trailer_len);
    write_all_vectored_frame(
        &mut *file,
        path,
        &frame_header[..frame_header_len],
        payload,
        &frame_trailer[..frame_trailer_len],
    )?;
    mark_persistent_journal_initialized(path_key);
    if note_persistent_journal_append(path_key, frame_bytes as u64) {
        file.sync_data().map_err(|err| {
            forget_persistent_journal_path(path_key);
            ObjectError::Corruption(format!(
                "failed to sync persistent journal {}: {}",
                path.display(),
                err
            ))
        })?;
        mark_persistent_journal_synced(path_key);
    }
    Ok(())
}

/// How a journal file ends, found without applying anything in it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersistentJournalEnd {
    /// The last frame ends exactly where the file does.
    Clean,
    /// The file ends INSIDE a frame that begins at `at`.
    Torn { at: usize },
}

/// Walk a journal's frames and say how it ends, returning the number of COMPLETE frames before
/// the end.
///
/// A frame is `[u64 payload_len][payload][u64 checksum_len = 24]["fnv1a64:" + 16 hex]`, all lengths
/// little-endian, after a header of `[u64 len]["MOSRSJNL1"]`.
///
/// # WHAT COUNTS AS A TEAR, AND WHAT DOES NOT
///
/// Only one shape is a tear: the file ends before a frame it has begun is finished. A write that
/// was cut short leaves exactly that, because a frame is written front to back.
///
/// Everything else that is wrong is CORRUPTION and is returned as an error, never as a tear:
/// a length over [`MAX_PERSISTENT_JOURNAL_RECORD_BYTES`], a checksum length other than the 24 the
/// writer always writes, a magic that is not this format, and -- when `verify_checksums` is set --
/// a COMPLETE frame whose checksum does not match. The distinction is the one that matters: a
/// complete frame with a bad checksum is damage in the middle of the log, and treating it as
/// "the end" would silently discard every acknowledged frame after it.
///
/// # THE CASE THIS CANNOT TELL APART
///
/// A length field damaged in the MIDDLE of the log to a plausible value that runs past the end of
/// the file reads identically to a torn final frame. Nothing in the bytes distinguishes them, which
/// is why [`repair_persistent_journal_tail_file`] is opt-in and keeps what it drops.
pub(crate) fn scan_persistent_journal(
    bytes: &[u8],
    verify_checksums: bool,
) -> Result<(u64, PersistentJournalEnd), ObjectError> {
    const WORD: usize = std::mem::size_of::<u64>();
    const CHECKSUM_TEXT_LEN: usize = 24;
    let corrupt = |message: &str| ObjectError::Corruption(message.to_string());
    let word = |at: usize| -> u64 {
        let mut raw = [0u8; WORD];
        raw.copy_from_slice(&bytes[at..at + WORD]);
        u64::from_le_bytes(raw)
    };

    if bytes.is_empty() {
        return Ok((0, PersistentJournalEnd::Clean));
    }
    let magic = PERSISTENT_JOURNAL_MAGIC.as_bytes();
    let header_len = WORD + magic.len();
    if bytes.len() < header_len {
        // A file shorter than the header is a tear only if it is a PREFIX of the header.
        let mut expected = Vec::with_capacity(header_len);
        expected.extend_from_slice(&(magic.len() as u64).to_le_bytes());
        expected.extend_from_slice(magic);
        return if expected.starts_with(bytes) {
            Ok((0, PersistentJournalEnd::Torn { at: 0 }))
        } else {
            Err(corrupt("persistent journal magic mismatch"))
        };
    }
    if word(0) != magic.len() as u64 || &bytes[WORD..header_len] != magic {
        return Err(corrupt("persistent journal magic mismatch"));
    }

    let mut position = header_len;
    let mut frames = 0u64;
    while position < bytes.len() {
        if bytes.len() - position < WORD {
            return Ok((frames, PersistentJournalEnd::Torn { at: position }));
        }
        let payload_len = word(position);
        if payload_len > MAX_PERSISTENT_JOURNAL_RECORD_BYTES as u64 {
            return Err(corrupt("persistent journal record is too large"));
        }
        let payload_start = position + WORD;
        let payload_end = payload_start + payload_len as usize;
        let checksum_len_end = payload_end + WORD;
        if bytes.len() < checksum_len_end {
            return Ok((frames, PersistentJournalEnd::Torn { at: position }));
        }
        if word(payload_end) != CHECKSUM_TEXT_LEN as u64 {
            return Err(corrupt(
                "persistent journal record has an unexpected checksum length",
            ));
        }
        let frame_end = checksum_len_end + CHECKSUM_TEXT_LEN;
        if bytes.len() < frame_end {
            return Ok((frames, PersistentJournalEnd::Torn { at: position }));
        }
        if verify_checksums {
            let text = std::str::from_utf8(&bytes[checksum_len_end..frame_end])
                .map_err(|_| corrupt("persistent journal record checksum is not text"))?;
            if !etag_matches(&bytes[payload_start..payload_end], text) {
                return Err(corrupt("persistent journal record checksum mismatch"));
            }
        }
        position = frame_end;
        frames += 1;
    }
    Ok((frames, PersistentJournalEnd::Clean))
}

/// Drop a torn final frame from a journal file, KEEPING the dropped bytes.
///
/// Returns `None` when the journal already ends cleanly (or does not exist), and otherwise
/// `(kept_bytes, dropped_bytes, preserved_path)`.
///
/// Order is the safety property. Every complete frame is checksum-verified FIRST, so damage in the
/// middle of the log is refused instead of being mistaken for the end. The dropped bytes are then
/// written to `<journal>.torn-<offset>` and synced BEFORE the journal is shortened, so a crash
/// between the two leaves the journal whole and the copy present, and a rerun finishes the job.
/// A different file already sitting at that name is refused rather than overwritten.
pub(crate) fn repair_persistent_journal_tail_file(
    path: &str,
) -> Result<Option<(u64, u64, String)>, ObjectError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(ObjectError::Corruption(format!(
                "failed to read persistent journal {path}: {err}"
            )));
        }
    };
    let (_frames, end) = scan_persistent_journal(&bytes, true)?;
    let at = match end {
        PersistentJournalEnd::Clean => return Ok(None),
        PersistentJournalEnd::Torn { at } => at,
    };
    let dropped = &bytes[at..];
    let preserved_path = format!("{path}.torn-{at}");
    let io_error = |what: &str, err: std::io::Error| {
        ObjectError::Corruption(format!("journal repair: {what} {preserved_path}: {err}"))
    };
    match std::fs::read(&preserved_path) {
        Ok(existing) if existing == dropped => {} // an earlier repair got this far
        Ok(_) => {
            return Err(ObjectError::Corruption(format!(
                "journal repair refused: {preserved_path} already exists with different bytes"
            )));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            use std::io::Write;
            let mut copy = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&preserved_path)
                .map_err(|err| io_error("could not create", err))?;
            copy.write_all(dropped)
                .map_err(|err| io_error("could not write", err))?;
            copy.sync_all()
                .map_err(|err| io_error("could not sync", err))?;
        }
        Err(err) => return Err(io_error("could not read", err)),
    }
    let journal = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|err| {
            ObjectError::Corruption(format!("journal repair: could not open {path}: {err}"))
        })?;
    journal.set_len(at as u64).map_err(|err| {
        ObjectError::Corruption(format!("journal repair: could not shorten {path}: {err}"))
    })?;
    journal.sync_all().map_err(|err| {
        ObjectError::Corruption(format!("journal repair: could not sync {path}: {err}"))
    })?;
    // This process may hold an open append handle, a "header already written" flag, or sync
    // counters for the old length; none of them is true any more.
    forget_persistent_journal_path(path);
    Ok(Some((at as u64, dropped.len() as u64, preserved_path)))
}
