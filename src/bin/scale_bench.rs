use matrixobjectstore_rs::{
    raft_backend, PageId, DataRange, GcReport, MatrixObjectStore, ObjectDeleteRequest,
    ObjectError, ObjectPutRequest, ObjectReadRequest, ObjectStoreDistributedReadiness, PutOptions,
    StoreOptions,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cmp;
use std::env;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};


/// Counts every allocation the process makes.
///
/// The box this runs on is shared, so wall-clock cannot resolve a change worth
/// less than about 2x -- two object sizes have disagreed in sign on one patch.
/// Allocated bytes and allocation counts do not move with load, so an arm that
/// removes a copy shows it here exactly.
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static FREED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

impl CountingAllocator {
    fn record(size: u64) {
        ALLOC_BYTES.fetch_add(size, Ordering::Relaxed);
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        let live = ALLOC_BYTES
            .load(Ordering::Relaxed)
            .saturating_sub(FREED_BYTES.load(Ordering::Relaxed));
        record_max(&LIVE_PEAK_BYTES, live);
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            Self::record(layout.size() as u64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                Self::record((new_size - layout.size()) as u64);
            } else {
                FREED_BYTES.fetch_add((layout.size() - new_size) as u64, Ordering::Relaxed);
                ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;

const DEFAULT_SCALE_READ_CACHE_MAX_BYTES_PER_REPLICA: u64 = 64 * 1024 * 1024;
const DEFAULT_SCALE_READ_CACHE_MAX_CACHEABLE_READ_BYTES_PER_REPLICA: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
struct Config {
    object_size: usize,
    object_count: u64,
    concurrency: u64,
    range_size: u64,
    overwrite_ratio_percent: u64,
    delete_ratio_percent: u64,
    chunk_size: u64,
    max_extent_bytes: u64,
    page_size: u64,
    chunk_read_cache_max_bytes_per_replica: u64,
    chunk_read_cache_max_cacheable_read_bytes_per_replica: u64,
    volume_count: u64,
    replica_count: usize,
    write_quorum: usize,
    object_batch_size: u64,
    mixed_duration_ms: u64,
    mixed_put_percent: u64,
    mixed_get_percent: u64,
    mixed_overwrite_percent: u64,
    mixed_delete_percent: u64,
    fault_freeze_replicas: u64,
    fault_probe_reads: u64,
    fault_quarantine_replicas: u64,
    fault_quarantine_probe_reads: u64,
    fault_drain_replicas: u64,
    fault_drain_probe_reads: u64,
    fault_drain_put_objects: u64,
    fault_capacity_replicas: u64,
    fault_capacity_probe_reads: u64,
    fault_capacity_put_objects: u64,
    required_fault_probes: u64,
    fail_on_fault_tolerance: bool,
    max_put_p99_ms: f64,
    max_range_p99_ms: f64,
    max_mixed_p99_ms: f64,
    max_recovery_snapshot_file_bytes: u64,
    min_put_mib_per_sec: f64,
    min_range_mib_per_sec: f64,
    min_mixed_mib_per_sec: f64,
    fail_on_performance_slo: bool,
    repair_fault_remove_chunks: u64,
    repair_fault_max_chunks: u64,
}

fn percent_operation_count(object_count: u64, percent: u64) -> u64 {
    let capped_percent = percent.min(100);
    if object_count == 0 || capped_percent == 0 {
        return 0;
    }
    let scaled = (object_count / 100)
        .saturating_mul(capped_percent)
        .saturating_add((object_count % 100).saturating_mul(capped_percent) / 100);
    scaled.max(1)
}

#[derive(Clone, Debug, Default)]
struct PhaseResult {
    ops: u64,
    bytes: u64,
    max_inflight: u64,
    /// Allocations made while this phase ran. The counters are process-global,
    /// so only a delta across the phase boundary is attributable to the phase.
    allocs: u64,
    alloc_bytes: u64,
    seconds: f64,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
}

#[derive(Clone, Debug, Default)]
struct MixedResult {
    put_ops: u64,
    get_ops: u64,
    overwrite_ops: u64,
    delete_ops: u64,
    bytes: u64,
    max_inflight: u64,
    seconds: f64,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
}

#[derive(Clone, Debug, Default)]
struct RecoveryResult {
    enabled: bool,
    passed: bool,
    snapshot_file_bytes: u64,
    reopen_ms: f64,
    verified_objects: u64,
    verified_bytes: u64,
    sample_index: u64,
    failure: String,
}

#[derive(Clone, Debug, Default)]
struct FaultInjectionResult {
    enabled: bool,
    passed: bool,
    requested_frozen_replicas: u64,
    frozen_replicas: u64,
    probe_reads: u64,
    verified_reads: u64,
    verified_bytes: u64,
    writable_replicas_during_fault: u64,
    readable_replicas_during_fault: u64,
    failure: String,
}

#[derive(Clone, Debug, Default)]
struct RepairFaultResult {
    enabled: bool,
    passed: bool,
    requested_removed_chunks: u64,
    removed_chunks: u64,
    audit_degraded_chunks: u64,
    audit_missing_replicas: u64,
    repaired_chunks: u64,
    repaired_replicas: u64,
    unrepaired_chunks: u64,
    failed_repairs: u64,
    verified_reads: u64,
    verified_bytes: u64,
    failure: String,
}

#[derive(Clone, Debug, Default)]
struct QuarantineFaultResult {
    enabled: bool,
    passed: bool,
    requested_quarantined_replicas: u64,
    quarantined_replicas: u64,
    probe_reads: u64,
    verified_reads: u64,
    verified_bytes: u64,
    active_replicas_during_fault: u64,
    writable_replicas_during_fault: u64,
    readable_replicas_during_fault: u64,
    failure: String,
}

#[derive(Clone, Debug, Default)]
struct DrainFaultResult {
    enabled: bool,
    passed: bool,
    requested_drained_replicas: u64,
    drained_replicas: u64,
    probe_reads: u64,
    verified_reads: u64,
    verified_bytes: u64,
    put_objects: u64,
    put_bytes: u64,
    active_replicas_during_fault: u64,
    writable_replicas_during_fault: u64,
    readable_replicas_during_fault: u64,
    failure: String,
}

#[derive(Clone, Debug, Default)]
struct CapacityFaultResult {
    enabled: bool,
    passed: bool,
    requested_capacity_replicas: u64,
    capacity_limited_replicas: u64,
    probe_reads: u64,
    verified_reads: u64,
    verified_bytes: u64,
    put_objects: u64,
    put_bytes: u64,
    capacity_pressure_replicas_during_fault: u64,
    writable_replicas_during_fault: u64,
    readable_replicas_during_fault: u64,
    failure: String,
}

#[derive(Clone, Debug, Default)]
struct FaultSuiteSummary {
    required_probes: u64,
    enabled_probes: u64,
    passed_probes: u64,
    failed_probes: u64,
    coverage_passed: bool,
    coverage_status: String,
    verified_reads: u64,
    verified_bytes: u64,
    fault_write_objects: u64,
    fault_write_bytes: u64,
    affected_replica_controls: u64,
    removed_chunks: u64,
    repaired_replicas: u64,
    failure_summary: String,
}

#[derive(Clone, Debug, Default)]
struct PerformanceSloSummary {
    configured_checks: u64,
    passed_checks: u64,
    failed_checks: u64,
    passed: bool,
    status: String,
    failure_summary: String,
    put_p99_ms: f64,
    range_p99_ms: f64,
    mixed_p99_ms: f64,
    put_mib_per_sec: f64,
    range_mib_per_sec: f64,
    mixed_mib_per_sec: f64,
    recovery_snapshot_file_bytes: u64,
    max_put_p99_ms: f64,
    max_range_p99_ms: f64,
    max_mixed_p99_ms: f64,
    max_recovery_snapshot_file_bytes: u64,
    min_put_mib_per_sec: f64,
    min_range_mib_per_sec: f64,
    min_mixed_mib_per_sec: f64,
}

#[derive(Clone, Debug, Default)]
struct ReplicaMetadataJournalMetrics {
    fsync_every_records: u64,
    fsync_every_bytes: u64,
    fsync_max_unsynced_ms: u64,
    pending_fsync_records: u64,
    pending_fsync_bytes: u64,
    oldest_unsynced_ms: u64,
    record_pressure_ppm: u64,
    byte_pressure_ppm: u64,
    age_pressure_ppm: u64,
}

#[derive(Debug, Default)]
struct StoreLockStats {
    acquisitions: AtomicU64,
    total_wait_ns: AtomicU64,
    max_wait_ns: AtomicU64,
}

impl StoreLockStats {
    fn record(&self, wait_ns: u64) {
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
        self.total_wait_ns.fetch_add(wait_ns, Ordering::Relaxed);
        let mut current = self.max_wait_ns.load(Ordering::Relaxed);
        while wait_ns > current {
            match self.max_wait_ns.compare_exchange_weak(
                current,
                wait_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    fn acquisitions(&self) -> u64 {
        self.acquisitions.load(Ordering::Relaxed)
    }

    fn total_wait_ns(&self) -> u64 {
        self.total_wait_ns.load(Ordering::Relaxed)
    }

    fn max_wait_ns(&self) -> u64 {
        self.max_wait_ns.load(Ordering::Relaxed)
    }

    fn avg_wait_ns(&self) -> u64 {
        let acquisitions = self.acquisitions();
        if acquisitions == 0 {
            0
        } else {
            self.total_wait_ns() / acquisitions
        }
    }
}

fn with_store<T, F>(
    store: &Arc<Mutex<MatrixObjectStore>>,
    stats: &Arc<StoreLockStats>,
    operation: F,
) -> T
where
    F: FnOnce(&mut MatrixObjectStore) -> T,
{
    let wait_start = Instant::now();
    let mut locked = lock_recover(store);
    stats.record(wait_start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    operation(&mut locked)
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn parse_u64_arg(args: &[String], index: usize, name: &str, fallback: u64) -> Result<u64, String> {
    if args.len() <= index {
        return Ok(fallback);
    }
    args[index]
        .parse::<u64>()
        .map_err(|_| format!("{name} is not a valid integer: {}", args[index]))
}

fn env_u64_any(names: &[&str], fallback: u64) -> Result<u64, String> {
    for name in names {
        match env::var(name) {
            Ok(value) if !value.is_empty() => {
                return value
                    .parse::<u64>()
                    .map_err(|_| format!("{name} is not a valid integer: {value}"));
            }
            _ => {}
        }
    }
    Ok(fallback)
}

fn env_f64_any(names: &[&str], fallback: f64) -> Result<f64, String> {
    for name in names {
        match env::var(name) {
            Ok(value) if !value.is_empty() => {
                let parsed = value
                    .parse::<f64>()
                    .map_err(|_| format!("{name} is not a valid number: {value}"))?;
                if !parsed.is_finite() || parsed < 0.0 {
                    return Err(format!(
                        "{name} must be a finite non-negative number: {value}"
                    ));
                }
                return Ok(parsed);
            }
            _ => {}
        }
    }
    Ok(fallback)
}

fn env_bool_any(names: &[&str], fallback: bool) -> Result<bool, String> {
    for name in names {
        match env::var(name) {
            Ok(value) if !value.is_empty() => {
                return match value.trim().to_ascii_lowercase().as_str() {
                    "1" | "true" | "yes" | "on" => Ok(true),
                    "0" | "false" | "no" | "off" => Ok(false),
                    _ => Err(format!("{name} is not a valid boolean: {value}")),
                };
            }
            _ => {}
        }
    }
    Ok(fallback)
}

fn payload(bytes: usize, salt: u64) -> Vec<u8> {
    let mut out = vec![0u8; bytes];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = b'a' + (((index as u64).wrapping_add(salt)) % 26) as u8;
    }
    out
}

fn validate_payload_range(
    data: &[u8],
    offset: u64,
    salt: u64,
    context: &str,
) -> Result<(), ObjectError> {
    for (index, byte) in data.iter().enumerate() {
        let expected = b'a' + (((offset + index as u64).wrapping_add(salt)) % 26) as u8;
        if *byte != expected {
            return Err(ObjectError::Corruption(format!(
                "{context} returned corrupted byte at range offset {index}"
            )));
        }
    }
    Ok(())
}

fn percentile_ms_sorted(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let rank = percentile * ((values.len() - 1) as f64);
    values[cmp::min(rank.ceil() as usize, values.len() - 1)]
}

fn latency_percentiles_ms(values: &mut [f64]) -> (f64, f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(cmp::Ordering::Equal));
    (
        percentile_ms_sorted(values, 0.50),
        percentile_ms_sorted(values, 0.95),
        percentile_ms_sorted(values, 0.99),
        percentile_ms_sorted(values, 0.999),
    )
}

fn bounded_latency_reserve(total_ops: u64, worker_count: u64) -> usize {
    const MAX_WORKER_LATENCY_RESERVE: u64 = 1_000_000;
    let per_worker =
        total_ops.saturating_add(worker_count.saturating_sub(1)) / cmp::max(1, worker_count);
    cmp::min(per_worker, MAX_WORKER_LATENCY_RESERVE) as usize
}

/// A phase's cost per operation. Zero ops reports zero rather than dividing.
fn per_op(total: u64, ops: u64) -> u64 {
    if ops == 0 {
        0
    } else {
        total / ops
    }
}

fn run_batched_concurrent<F>(
    total_ops: u64,
    bytes_per_op: u64,
    concurrency: u64,
    batch_size: u64,
    operation: F,
) -> Result<PhaseResult, ObjectError>
where
    F: Fn(u64, u64, u64) -> Result<(), ObjectError> + Send + Sync + 'static,
{
    let allocs_at_start = ALLOC_COUNT.load(Ordering::Relaxed);
    let alloc_bytes_at_start = ALLOC_BYTES.load(Ordering::Relaxed);
    let start = Instant::now();
    let next = Arc::new(AtomicU64::new(0));
    let ok_ops = Arc::new(AtomicU64::new(0));
    let active = Arc::new(AtomicU64::new(0));
    let max_inflight = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicBool::new(false));
    let first_error = Arc::new(Mutex::new(None::<ObjectError>));
    let operation = Arc::new(operation);
    let worker_count = cmp::max(1, concurrency);
    let effective_batch_size = cmp::max(1, batch_size);
    let worker_latency_reserve = bounded_latency_reserve(total_ops, worker_count);
    let mut threads = Vec::with_capacity(worker_count as usize);

    for worker in 0..worker_count {
        let next = Arc::clone(&next);
        let ok_ops = Arc::clone(&ok_ops);
        let active = Arc::clone(&active);
        let max_inflight = Arc::clone(&max_inflight);
        let failed = Arc::clone(&failed);
        let first_error = Arc::clone(&first_error);
        let operation = Arc::clone(&operation);
        threads.push(thread::spawn(move || {
            let mut latencies = Vec::with_capacity(worker_latency_reserve);
            loop {
                if failed.load(Ordering::Acquire) {
                    break;
                }
                let start_index = next.fetch_add(effective_batch_size, Ordering::Relaxed);
                if start_index >= total_ops {
                    break;
                }
                let count = cmp::min(effective_batch_size, total_ops - start_index);
                let op_start = Instant::now();
                record_max(
                    &max_inflight,
                    active.fetch_add(count, Ordering::Relaxed) + count,
                );
                let result = operation(worker, start_index, count);
                active.fetch_sub(count, Ordering::Relaxed);
                match result {
                    Ok(()) => {
                        let per_object_ms =
                            op_start.elapsed().as_secs_f64() * 1000.0 / cmp::max(1, count) as f64;
                        latencies.extend(std::iter::repeat(per_object_ms).take(count as usize));
                        ok_ops.fetch_add(count, Ordering::Relaxed);
                    }
                    Err(err) => {
                        let already_failed = failed.swap(true, Ordering::AcqRel);
                        if !already_failed {
                            *lock_recover(&first_error) = Some(err);
                        }
                        break;
                    }
                }
            }
            latencies
        }));
    }

    let mut latencies = Vec::new();
    for thread in threads {
        let worker_latencies = thread.join().map_err(|_| {
            ObjectError::Corruption("scale benchmark batch worker thread panicked".to_string())
        })?;
        latencies.reserve(worker_latencies.len());
        latencies.extend(worker_latencies);
    }
    if let Some(err) = lock_recover(&first_error).clone() {
        return Err(err);
    }

    let seconds = start.elapsed().as_secs_f64();
    let ops = ok_ops.load(Ordering::Relaxed);
    let total_ms: f64 = latencies.iter().copied().sum();
    let avg_ms = if latencies.is_empty() {
        0.0
    } else {
        total_ms / latencies.len() as f64
    };
    let (p50_ms, p95_ms, p99_ms, p999_ms) = latency_percentiles_ms(&mut latencies);
    Ok(PhaseResult {
        allocs: ALLOC_COUNT
            .load(Ordering::Relaxed)
            .saturating_sub(allocs_at_start),
        alloc_bytes: ALLOC_BYTES
            .load(Ordering::Relaxed)
            .saturating_sub(alloc_bytes_at_start),
        ops,
        bytes: ops.saturating_mul(bytes_per_op),
        max_inflight: max_inflight.load(Ordering::Relaxed),
        seconds,
        avg_ms,
        p50_ms,
        p95_ms,
        p99_ms,
        p999_ms,
    })
}

fn qps(phase: &PhaseResult) -> f64 {
    if phase.seconds <= 0.0 {
        0.0
    } else {
        phase.ops as f64 / phase.seconds
    }
}

fn record_max(max_value: &AtomicU64, observed: u64) {
    let mut current = max_value.load(Ordering::Relaxed);
    while observed > current {
        match max_value.compare_exchange_weak(
            current,
            observed,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

fn mib_per_second(phase: &PhaseResult) -> f64 {
    if phase.seconds <= 0.0 {
        0.0
    } else {
        (phase.bytes as f64 / (1024.0 * 1024.0)) / phase.seconds
    }
}

fn mixed_qps(mixed: &MixedResult) -> f64 {
    if mixed.seconds <= 0.0 {
        0.0
    } else {
        (mixed.put_ops + mixed.get_ops + mixed.overwrite_ops + mixed.delete_ops) as f64
            / mixed.seconds
    }
}

fn mixed_mib_per_second(mixed: &MixedResult) -> f64 {
    if mixed.seconds <= 0.0 {
        0.0
    } else {
        (mixed.bytes as f64 / (1024.0 * 1024.0)) / mixed.seconds
    }
}

fn ratio_ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        0
    } else {
        ((u128::from(numerator) * 1_000_000) / u128::from(denominator)).min(u128::from(u64::MAX))
            as u64
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len().saturating_add(2));
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                let _ = std::fmt::Write::write_fmt(&mut out, format_args!("\\u{:04x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn json_f64(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else {
        "0".to_string()
    }
}

fn json_optional_string(value: Option<&str>) -> String {
    match value {
        Some(value) if !value.is_empty() => json_escape(value),
        _ => "null".to_string(),
    }
}

#[derive(Debug)]
struct GitProvenance {
    repo_root: Option<PathBuf>,
    git_commit: Option<String>,
    git_commit_short: Option<String>,
    git_branch: Option<String>,
    git_describe: Option<String>,
    git_tracked_dirty: bool,
    git_tracked_dirty_count: u64,
    git_untracked_count: u64,
}

impl GitProvenance {
    fn detect() -> Self {
        let repo_root = detect_repo_root();
        let Some(repo_root) = repo_root else {
            return Self {
                repo_root: None,
                git_commit: None,
                git_commit_short: None,
                git_branch: None,
                git_describe: None,
                git_tracked_dirty: true,
                git_tracked_dirty_count: 0,
                git_untracked_count: 0,
            };
        };
        let tracked_status = run_git(
            &repo_root,
            &["status", "--porcelain", "--untracked-files=no"],
        )
        .unwrap_or_default();
        let untracked_dirs = run_git(
            &repo_root,
            &["ls-files", "--others", "--exclude-standard", "--directory"],
        )
        .unwrap_or_default();
        let git_tracked_dirty_count = count_nonempty_lines(&tracked_status);
        Self {
            git_commit: run_git(&repo_root, &["rev-parse", "HEAD"]),
            git_commit_short: run_git(&repo_root, &["rev-parse", "--short=8", "HEAD"]),
            git_branch: run_git(&repo_root, &["branch", "--show-current"]),
            git_describe: run_git(&repo_root, &["describe", "--always", "--dirty", "--broken"]),
            repo_root: Some(repo_root),
            git_tracked_dirty: git_tracked_dirty_count > 0,
            git_tracked_dirty_count,
            git_untracked_count: count_nonempty_lines(&untracked_dirs),
        }
    }

    fn to_json(&self, generated_at_epoch_seconds: f64) -> String {
        format!(
            concat!(
                "{{\"generated_at_epoch_seconds\":{},",
                "\"repo_root\":{},",
                "\"git_commit\":{},",
                "\"git_commit_short\":{},",
                "\"git_branch\":{},",
                "\"git_describe\":{},",
                "\"git_tracked_dirty\":{},",
                "\"git_tracked_dirty_count\":{},",
                "\"git_untracked_count\":{}}}"
            ),
            json_f64(generated_at_epoch_seconds),
            json_optional_string(self.repo_root.as_ref().and_then(|path| path.to_str())),
            json_optional_string(self.git_commit.as_deref()),
            json_optional_string(self.git_commit_short.as_deref()),
            json_optional_string(self.git_branch.as_deref()),
            json_optional_string(self.git_describe.as_deref()),
            self.git_tracked_dirty,
            self.git_tracked_dirty_count,
            self.git_untracked_count,
        )
    }
}

fn detect_repo_root() -> Option<PathBuf> {
    if let Ok(root) = env::var("MATRIX_OBJECTSTORE_REPO_ROOT") {
        let candidate = PathBuf::from(root);
        if candidate.join(".git").exists() {
            return Some(candidate);
        }
    }
    let mut current = env::current_dir().ok()?;
    loop {
        if current.join(".git").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn run_git(repo_root: &Path, args: &[&str]) -> Option<String> {
    let output = process::Command::new("git")
        .arg("-c")
        .arg("core.autocrlf=true")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn count_nonempty_lines(value: &str) -> u64 {
    value.lines().filter(|line| !line.trim().is_empty()).count() as u64
}

fn recovery_snapshot_path() -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!(
        "matrixobjectstore-rust-scale-recovery-{}-{}.snapshot",
        process::id(),
        now_ms()
    ));
    path
}

fn run_snapshot_recovery_probe(
    store: &Arc<Mutex<MatrixObjectStore>>,
    lock_stats: &Arc<StoreLockStats>,
    options: &StoreOptions,
    snapshot_path: &Path,
    cfg: &Config,
    sample_index: u64,
    effective_range: u64,
) -> Result<RecoveryResult, ObjectError> {
    if sample_index >= cfg.object_count {
        return Ok(RecoveryResult {
            enabled: true,
            passed: false,
            failure: "no surviving baseline object was available to sample".to_string(),
            ..RecoveryResult::default()
        });
    }
    with_store(store, lock_stats, |locked| {
        locked.flush_persistent_snapshot()
    })?;
    let snapshot_file_bytes = std::fs::metadata(snapshot_path)
        .map_err(|err| {
            ObjectError::Corruption(format!(
                "failed to stat recovery snapshot {}: {}",
                snapshot_path.display(),
                err
            ))
        })?
        .len();
    let reopen_start = Instant::now();
    let reopened = MatrixObjectStore::new(options.clone())?;
    let reopen_ms = reopen_start.elapsed().as_secs_f64() * 1000.0;
    let offset = if cfg.object_size as u64 > effective_range {
        (sample_index.saturating_mul(4096)) % ((cfg.object_size as u64) - effective_range)
    } else {
        0
    };
    let got = reopened.get_object_range(
        "bench",
        &format!("object-{sample_index}"),
        offset,
        effective_range,
    )?;
    if got.data.len() != effective_range as usize {
        return Err(ObjectError::Corruption(
            "recovery range read returned unexpected byte count".to_string(),
        ));
    }
    validate_payload_range(&got.data, offset, 0, "snapshot recovery range read")?;
    Ok(RecoveryResult {
        enabled: true,
        passed: true,
        snapshot_file_bytes,
        reopen_ms,
        verified_objects: 1,
        verified_bytes: effective_range,
        sample_index,
        failure: String::new(),
    })
}

fn run_replica_freeze_fault_probe(
    store: &Arc<Mutex<MatrixObjectStore>>,
    lock_stats: &Arc<StoreLockStats>,
    cfg: &Config,
    effective_range: u64,
) -> Result<FaultInjectionResult, ObjectError> {
    if cfg.fault_freeze_replicas == 0 {
        return Ok(FaultInjectionResult {
            passed: true,
            ..FaultInjectionResult::default()
        });
    }
    let mut result = FaultInjectionResult {
        enabled: true,
        requested_frozen_replicas: cfg.fault_freeze_replicas,
        probe_reads: cfg.fault_probe_reads,
        ..FaultInjectionResult::default()
    };
    if cfg.fault_probe_reads == 0 {
        result.failure = "fault probe reads must be non-zero".to_string();
        return Ok(result);
    }
    let safe_freeze_limit = (cfg.replica_count as u64).saturating_sub(cfg.write_quorum as u64);
    let freeze_count = cfg.fault_freeze_replicas.min(safe_freeze_limit);
    if freeze_count == 0 {
        result.failure = format!(
            "no spare replica can be frozen without violating write quorum {}/{}",
            cfg.write_quorum, cfg.replica_count
        );
        return Ok(result);
    }

    let mut frozen = Vec::with_capacity(freeze_count as usize);
    for replica_index in 0..freeze_count as usize {
        match with_store(store, lock_stats, |locked| {
            locked.set_page_replica_frozen(replica_index, true)
        }) {
            Ok(()) => {
                frozen.push(replica_index);
                result.frozen_replicas = result.frozen_replicas.saturating_add(1);
            }
            Err(err) => {
                result.failure = format!("failed to freeze replica {replica_index}: {err}");
                break;
            }
        }
    }

    if result.failure.is_empty() {
        let readiness = with_store(store, lock_stats, |locked| {
            locked.distributed_readiness(0, now_ms(), 0, 0, 1, 64, 0)
        })?;
        result.writable_replicas_during_fault = readiness.storage_writable_replicas;
        result.readable_replicas_during_fault = readiness.storage_readable_replicas;
        for probe in 0..cfg.fault_probe_reads {
            let index = probe % cfg.object_count;
            let offset = if cfg.object_size as u64 > effective_range {
                (probe.saturating_mul(4096)) % ((cfg.object_size as u64) - effective_range)
            } else {
                0
            };
            let probe_result = match with_store(store, lock_stats, |locked| {
                locked.get_object_range(
                    "bench",
                    &format!("object-{index}"),
                    offset,
                    effective_range,
                )
            }) {
                Ok(got) if got.data.len() == effective_range as usize => {
                    validate_payload_range(&got.data, offset, 0, "replica freeze fault range read")
                }
                Ok(_) => Err(ObjectError::Corruption(
                    "replica freeze fault range read returned unexpected byte count".to_string(),
                )),
                Err(err) => Err(err),
            };
            if let Err(err) = probe_result {
                result.failure = format!("fault probe read {probe} failed: {err}");
                break;
            }
            result.verified_reads = result.verified_reads.saturating_add(1);
            result.verified_bytes = result.verified_bytes.saturating_add(effective_range);
        }
    }

    for replica_index in frozen {
        if let Err(err) = with_store(store, lock_stats, |locked| {
            locked.set_page_replica_frozen(replica_index, false)
        }) {
            if result.failure.is_empty() {
                result.failure = format!("failed to thaw replica {replica_index}: {err}");
            }
        }
    }
    result.passed = result.failure.is_empty()
        && result.frozen_replicas == freeze_count
        && result.verified_reads == cfg.fault_probe_reads;
    Ok(result)
}

fn run_replica_quarantine_fault_probe(
    store: &Arc<Mutex<MatrixObjectStore>>,
    lock_stats: &Arc<StoreLockStats>,
    cfg: &Config,
    effective_range: u64,
) -> Result<QuarantineFaultResult, ObjectError> {
    if cfg.fault_quarantine_replicas == 0 {
        return Ok(QuarantineFaultResult {
            passed: true,
            ..QuarantineFaultResult::default()
        });
    }
    let mut result = QuarantineFaultResult {
        enabled: true,
        requested_quarantined_replicas: cfg.fault_quarantine_replicas,
        probe_reads: cfg.fault_quarantine_probe_reads,
        ..QuarantineFaultResult::default()
    };
    if cfg.fault_quarantine_probe_reads == 0 {
        result.failure = "quarantine fault probe reads must be non-zero".to_string();
        return Ok(result);
    }
    let safe_quarantine_limit = (cfg.replica_count as u64).saturating_sub(cfg.write_quorum as u64);
    let quarantine_count = cfg.fault_quarantine_replicas.min(safe_quarantine_limit);
    if quarantine_count == 0 {
        result.failure = format!(
            "no spare replica can be quarantined without violating write quorum {}/{}",
            cfg.write_quorum, cfg.replica_count
        );
        return Ok(result);
    }

    let mut quarantined = Vec::with_capacity(quarantine_count as usize);
    for replica_index in 0..quarantine_count as usize {
        match with_store(store, lock_stats, |locked| {
            locked.set_page_replica_quarantine(replica_index, true)
        }) {
            Ok(()) => {
                quarantined.push(replica_index);
                result.quarantined_replicas = result.quarantined_replicas.saturating_add(1);
            }
            Err(err) => {
                result.failure = format!("failed to quarantine replica {replica_index}: {err}");
                break;
            }
        }
    }

    if result.failure.is_empty() {
        let readiness = with_store(store, lock_stats, |locked| {
            locked.distributed_readiness(0, now_ms(), 0, 0, 1, 64, 0)
        })?;
        result.active_replicas_during_fault = readiness.storage_active_replicas;
        result.writable_replicas_during_fault = readiness.storage_writable_replicas;
        result.readable_replicas_during_fault = readiness.storage_readable_replicas;
        for probe in 0..cfg.fault_quarantine_probe_reads {
            let index = probe % cfg.object_count;
            let offset = if cfg.object_size as u64 > effective_range {
                (probe.saturating_mul(4096)) % ((cfg.object_size as u64) - effective_range)
            } else {
                0
            };
            let probe_result = match with_store(store, lock_stats, |locked| {
                locked.get_object_range(
                    "bench",
                    &format!("object-{index}"),
                    offset,
                    effective_range,
                )
            }) {
                Ok(got) if got.data.len() == effective_range as usize => validate_payload_range(
                    &got.data,
                    offset,
                    0,
                    "replica quarantine fault range read",
                ),
                Ok(_) => Err(ObjectError::Corruption(
                    "replica quarantine fault range read returned unexpected byte count"
                        .to_string(),
                )),
                Err(err) => Err(err),
            };
            if let Err(err) = probe_result {
                result.failure = format!("quarantine fault probe read {probe} failed: {err}");
                break;
            }
            result.verified_reads = result.verified_reads.saturating_add(1);
            result.verified_bytes = result.verified_bytes.saturating_add(effective_range);
        }
    }

    for replica_index in quarantined {
        if let Err(err) = with_store(store, lock_stats, |locked| {
            locked.set_page_replica_quarantine(replica_index, false)
        }) {
            if result.failure.is_empty() {
                result.failure =
                    format!("failed to restore quarantined replica {replica_index}: {err}");
            }
        }
    }
    result.passed = result.failure.is_empty()
        && result.quarantined_replicas == quarantine_count
        && result.verified_reads == cfg.fault_quarantine_probe_reads;
    Ok(result)
}

fn run_replica_drain_fault_probe(
    store: &Arc<Mutex<MatrixObjectStore>>,
    lock_stats: &Arc<StoreLockStats>,
    cfg: &Config,
    effective_range: u64,
) -> Result<DrainFaultResult, ObjectError> {
    if cfg.fault_drain_replicas == 0 {
        return Ok(DrainFaultResult {
            passed: true,
            ..DrainFaultResult::default()
        });
    }
    let mut result = DrainFaultResult {
        enabled: true,
        requested_drained_replicas: cfg.fault_drain_replicas,
        probe_reads: cfg.fault_drain_probe_reads,
        ..DrainFaultResult::default()
    };
    if cfg.fault_drain_probe_reads == 0 {
        result.failure = "drain fault probe reads must be non-zero".to_string();
        return Ok(result);
    }
    let safe_drain_limit = (cfg.replica_count as u64).saturating_sub(cfg.write_quorum as u64);
    let drain_count = cfg.fault_drain_replicas.min(safe_drain_limit);
    if drain_count == 0 {
        result.failure = format!(
            "no spare replica can be drained without violating write quorum {}/{}",
            cfg.write_quorum, cfg.replica_count
        );
        return Ok(result);
    }

    let mut drained = Vec::with_capacity(drain_count as usize);
    for replica_index in 0..drain_count as usize {
        match with_store(store, lock_stats, |locked| {
            locked.configure_chunk_replica_service_mode(replica_index, None, Some(true), None)
        }) {
            Ok(()) => {
                drained.push(replica_index);
                result.drained_replicas = result.drained_replicas.saturating_add(1);
            }
            Err(err) => {
                result.failure = format!("failed to drain replica {replica_index}: {err}");
                break;
            }
        }
    }

    if result.failure.is_empty() {
        let readiness = with_store(store, lock_stats, |locked| {
            locked.distributed_readiness(0, now_ms(), 0, 0, 1, 64, 0)
        })?;
        result.active_replicas_during_fault = readiness.storage_active_replicas;
        result.writable_replicas_during_fault = readiness.storage_writable_replicas;
        result.readable_replicas_during_fault = readiness.storage_readable_replicas;

        for probe in 0..cfg.fault_drain_probe_reads {
            let index = probe % cfg.object_count;
            let offset = if cfg.object_size as u64 > effective_range {
                (probe.saturating_mul(4096)) % ((cfg.object_size as u64) - effective_range)
            } else {
                0
            };
            let probe_result = match with_store(store, lock_stats, |locked| {
                locked.get_object_range(
                    "bench",
                    &format!("object-{index}"),
                    offset,
                    effective_range,
                )
            }) {
                Ok(got) if got.data.len() == effective_range as usize => {
                    validate_payload_range(&got.data, offset, 0, "replica drain fault range read")
                }
                Ok(_) => Err(ObjectError::Corruption(
                    "replica drain fault range read returned unexpected byte count".to_string(),
                )),
                Err(err) => Err(err),
            };
            if let Err(err) = probe_result {
                result.failure = format!("drain fault probe read {probe} failed: {err}");
                break;
            }
            result.verified_reads = result.verified_reads.saturating_add(1);
            result.verified_bytes = result.verified_bytes.saturating_add(effective_range);
        }
    }

    if result.failure.is_empty() {
        for put_index in 0..cfg.fault_drain_put_objects {
            let salt = 17_000 + put_index;
            let key = format!("__fault/drain-object-{put_index}");
            let put_payload = payload(cfg.object_size, salt);
            let put_result = with_store(store, lock_stats, |locked| {
                locked.put_object("bench", &key, put_payload, "application/octet-stream")
            });
            if let Err(err) = put_result {
                result.failure = format!("drain fault put {put_index} failed: {err}");
                break;
            }
            result.put_objects = result.put_objects.saturating_add(1);
            result.put_bytes = result.put_bytes.saturating_add(cfg.object_size as u64);

            let verify = match with_store(store, lock_stats, |locked| {
                locked.get_object_range("bench", &key, 0, effective_range)
            }) {
                Ok(got) if got.data.len() == effective_range as usize => {
                    validate_payload_range(&got.data, 0, salt, "replica drain fault put readback")
                }
                Ok(_) => Err(ObjectError::Corruption(
                    "replica drain fault put readback returned unexpected byte count".to_string(),
                )),
                Err(err) => Err(err),
            };
            if let Err(err) = verify {
                result.failure = format!("drain fault put readback {put_index} failed: {err}");
                break;
            }
        }
    }

    for replica_index in drained {
        if let Err(err) = with_store(store, lock_stats, |locked| {
            locked.configure_chunk_replica_service_mode(replica_index, None, Some(false), None)
        }) {
            if result.failure.is_empty() {
                result.failure =
                    format!("failed to restore drained replica {replica_index}: {err}");
            }
        }
    }
    result.passed = result.failure.is_empty()
        && result.drained_replicas == drain_count
        && result.verified_reads == cfg.fault_drain_probe_reads
        && result.put_objects == cfg.fault_drain_put_objects;
    Ok(result)
}

fn run_replica_capacity_fault_probe(
    store: &Arc<Mutex<MatrixObjectStore>>,
    lock_stats: &Arc<StoreLockStats>,
    cfg: &Config,
    effective_range: u64,
) -> Result<CapacityFaultResult, ObjectError> {
    if cfg.fault_capacity_replicas == 0 {
        return Ok(CapacityFaultResult {
            passed: true,
            ..CapacityFaultResult::default()
        });
    }
    let mut result = CapacityFaultResult {
        enabled: true,
        requested_capacity_replicas: cfg.fault_capacity_replicas,
        probe_reads: cfg.fault_capacity_probe_reads,
        ..CapacityFaultResult::default()
    };
    if cfg.fault_capacity_probe_reads == 0 {
        result.failure = "capacity fault probe reads must be non-zero".to_string();
        return Ok(result);
    }
    let safe_capacity_limit = (cfg.replica_count as u64).saturating_sub(cfg.write_quorum as u64);
    let capacity_count = cfg.fault_capacity_replicas.min(safe_capacity_limit);
    if capacity_count == 0 {
        result.failure = format!(
            "no spare replica can be capacity-limited without violating write quorum {}/{}",
            cfg.write_quorum, cfg.replica_count
        );
        return Ok(result);
    }

    let original_capacities = with_store(store, lock_stats, |locked| {
        locked.chunk_replica_capacity_bytes()
    });
    let physical_bytes = with_store(store, lock_stats, |locked| {
        locked.chunk_replica_physical_bytes()
    });
    let mut limited = Vec::with_capacity(capacity_count as usize);
    for replica_index in 0..capacity_count as usize {
        let current_physical = physical_bytes
            .get(replica_index)
            .copied()
            .unwrap_or_default();
        let full_capacity = current_physical.max(1);
        match with_store(store, lock_stats, |locked| {
            locked.configure_chunk_replica_capacity(replica_index, full_capacity)
        }) {
            Ok(()) => {
                limited.push(replica_index);
                result.capacity_limited_replicas =
                    result.capacity_limited_replicas.saturating_add(1);
            }
            Err(err) => {
                result.failure = format!("failed to capacity-limit replica {replica_index}: {err}");
                break;
            }
        }
    }

    if result.failure.is_empty() {
        let readiness = with_store(store, lock_stats, |locked| {
            locked.distributed_readiness(0, now_ms(), 1, 0, 1, 64, 0)
        })?;
        result.capacity_pressure_replicas_during_fault =
            readiness.storage_capacity_pressure_replicas;
        result.writable_replicas_during_fault = readiness.storage_writable_replicas;
        result.readable_replicas_during_fault = readiness.storage_readable_replicas;

        for probe in 0..cfg.fault_capacity_probe_reads {
            let index = probe % cfg.object_count;
            let offset = if cfg.object_size as u64 > effective_range {
                (probe.saturating_mul(4096)) % ((cfg.object_size as u64) - effective_range)
            } else {
                0
            };
            let probe_result = match with_store(store, lock_stats, |locked| {
                locked.get_object_range(
                    "bench",
                    &format!("object-{index}"),
                    offset,
                    effective_range,
                )
            }) {
                Ok(got) if got.data.len() == effective_range as usize => validate_payload_range(
                    &got.data,
                    offset,
                    0,
                    "replica capacity fault range read",
                ),
                Ok(_) => Err(ObjectError::Corruption(
                    "replica capacity fault range read returned unexpected byte count".to_string(),
                )),
                Err(err) => Err(err),
            };
            if let Err(err) = probe_result {
                result.failure = format!("capacity fault probe read {probe} failed: {err}");
                break;
            }
            result.verified_reads = result.verified_reads.saturating_add(1);
            result.verified_bytes = result.verified_bytes.saturating_add(effective_range);
        }
    }

    if result.failure.is_empty() {
        for put_index in 0..cfg.fault_capacity_put_objects {
            let salt = 23_000 + put_index;
            let key = format!("__fault/capacity-object-{put_index}");
            let put_payload = payload(cfg.object_size, salt);
            let put_result = with_store(store, lock_stats, |locked| {
                locked.put_object("bench", &key, put_payload, "application/octet-stream")
            });
            if let Err(err) = put_result {
                result.failure = format!("capacity fault put {put_index} failed: {err}");
                break;
            }
            result.put_objects = result.put_objects.saturating_add(1);
            result.put_bytes = result.put_bytes.saturating_add(cfg.object_size as u64);

            let verify = match with_store(store, lock_stats, |locked| {
                locked.get_object_range("bench", &key, 0, effective_range)
            }) {
                Ok(got) if got.data.len() == effective_range as usize => validate_payload_range(
                    &got.data,
                    0,
                    salt,
                    "replica capacity fault put readback",
                ),
                Ok(_) => Err(ObjectError::Corruption(
                    "replica capacity fault put readback returned unexpected byte count"
                        .to_string(),
                )),
                Err(err) => Err(err),
            };
            if let Err(err) = verify {
                result.failure = format!("capacity fault put readback {put_index} failed: {err}");
                break;
            }
        }
    }

    for replica_index in limited {
        let restored_capacity = original_capacities
            .get(replica_index)
            .copied()
            .unwrap_or_default();
        if let Err(err) = with_store(store, lock_stats, |locked| {
            locked.configure_chunk_replica_capacity(replica_index, restored_capacity)
        }) {
            if result.failure.is_empty() {
                result.failure =
                    format!("failed to restore capacity for replica {replica_index}: {err}");
            }
        }
    }
    result.passed = result.failure.is_empty()
        && result.capacity_limited_replicas == capacity_count
        && result.verified_reads == cfg.fault_capacity_probe_reads
        && result.put_objects == cfg.fault_capacity_put_objects
        && result.capacity_pressure_replicas_during_fault >= capacity_count;
    Ok(result)
}

fn run_replica_repair_fault_probe(
    store: &Arc<Mutex<MatrixObjectStore>>,
    lock_stats: &Arc<StoreLockStats>,
    cfg: &Config,
    effective_range: u64,
) -> Result<RepairFaultResult, ObjectError> {
    if cfg.repair_fault_remove_chunks == 0 {
        return Ok(RepairFaultResult {
            passed: true,
            ..RepairFaultResult::default()
        });
    }
    let mut result = RepairFaultResult {
        enabled: true,
        requested_removed_chunks: cfg.repair_fault_remove_chunks,
        ..RepairFaultResult::default()
    };
    let mut target_chunks = Vec::<PageId>::new();
    for index in 0..cfg.object_count {
        let metadata = with_store(store, lock_stats, |locked| {
            locked.head_object("bench", &format!("object-{index}"))
        })?;
        for extent in metadata.extents {
            let page_id = PageId {
                chunk_id: extent.chunk_id,
                page_index: extent.offset / cfg.page_size,
            };
            if !target_chunks.contains(&page_id) {
                target_chunks.push(page_id);
            }
            if target_chunks.len() as u64 >= cfg.repair_fault_remove_chunks {
                break;
            }
        }
        if target_chunks.len() as u64 >= cfg.repair_fault_remove_chunks {
            break;
        }
    }
    let receipt_limit = cfg.repair_fault_remove_chunks.min(usize::MAX as u64) as usize;
    let write_receipts = with_store(store, lock_stats, |locked| {
        locked.list_page_write_receipts(None, receipt_limit, None)
    })?;
    if target_chunks.is_empty() && write_receipts.receipts.is_empty() {
        let batch_receipts = with_store(store, lock_stats, |locked| {
            locked.list_chunk_batch_write_receipts(0, None, receipt_limit, None)
        })?;
        for receipt in batch_receipts.receipts {
            if !target_chunks.contains(&receipt.page_id) {
                target_chunks.push(receipt.page_id);
            }
            if target_chunks.len() as u64 >= cfg.repair_fault_remove_chunks {
                break;
            }
        }
    } else if target_chunks.is_empty() {
        for receipt in write_receipts.receipts {
            let page_index = if cfg.page_size == 0 {
                0
            } else {
                receipt.offset / cfg.page_size
            };
            let page_id = PageId {
                chunk_id: receipt.chunk_id,
                page_index,
            };
            if !target_chunks.contains(&page_id) {
                target_chunks.push(page_id);
            }
            if target_chunks.len() as u64 >= cfg.repair_fault_remove_chunks {
                break;
            }
        }
    }
    if target_chunks.is_empty() {
        let allocation_receipts = with_store(store, lock_stats, |locked| {
            locked.list_page_allocation_receipts(None, receipt_limit, None)
        })?;
        for receipt in allocation_receipts.receipts {
            if receipt.released {
                continue;
            }
            let page_id = PageId {
                chunk_id: receipt.allocation.chunk_id,
                page_index: receipt.allocation.offset / cfg.page_size,
            };
            if !target_chunks.contains(&page_id) {
                target_chunks.push(page_id);
            }
            if target_chunks.len() as u64 >= cfg.repair_fault_remove_chunks {
                break;
            }
        }
    }
    if target_chunks.is_empty() {
        let batch_allocation_receipts = with_store(store, lock_stats, |locked| {
            locked.list_page_batch_allocation_receipts(None, receipt_limit, None)
        })?;
        for receipt in batch_allocation_receipts.receipts {
            for allocation in receipt.allocations {
                let page_id = PageId {
                    chunk_id: allocation.chunk_id,
                    page_index: allocation.offset / cfg.page_size,
                };
                if !target_chunks.contains(&page_id) {
                    target_chunks.push(page_id);
                }
                if target_chunks.len() as u64 >= cfg.repair_fault_remove_chunks {
                    break;
                }
            }
            if target_chunks.len() as u64 >= cfg.repair_fault_remove_chunks {
                break;
            }
        }
    }
    if target_chunks.is_empty() {
        result.failure = "no target chunks could be derived for repair fault probe".to_string();
        return Ok(result);
    }

    for page_id in &target_chunks {
        match with_store(store, lock_stats, |locked| {
            locked.remove_direct_replica_chunk_for_repair(0, page_id)
        }) {
            Ok(()) => result.removed_chunks = result.removed_chunks.saturating_add(1),
            Err(err) => {
                result.failure = format!("failed to remove replica chunk for repair: {err}");
                break;
            }
        }
    }
    if !result.failure.is_empty() {
        return Ok(result);
    }

    let max_chunks = cfg
        .repair_fault_max_chunks
        .max(result.removed_chunks)
        .max(1)
        .min(usize::MAX as u64) as usize;
    let max_payload_bytes = cfg
        .page_size
        .saturating_mul(max_chunks as u64)
        .max(effective_range);
    let audit = with_store(store, lock_stats, |locked| {
        locked.audit_page_replicas(None, max_chunks, max_payload_bytes)
    })?;
    result.audit_degraded_chunks = audit.degraded_chunks;
    result.audit_missing_replicas = audit.missing_replicas;
    let repair = with_store(store, lock_stats, |locked| {
        locked.repair_audited_page_replicas(None, max_chunks, max_payload_bytes)
    })?;
    result.repaired_chunks = repair.repaired_chunks;
    result.repaired_replicas = repair.stats.repaired_replicas;
    result.unrepaired_chunks = repair.unrepaired_chunks;
    result.failed_repairs = repair.stats.failed_repairs;

    for probe in 0..result.removed_chunks {
        let index = probe % cfg.object_count;
        let offset = if cfg.object_size as u64 > effective_range {
            (probe.saturating_mul(4096)) % ((cfg.object_size as u64) - effective_range)
        } else {
            0
        };
        let probe_result = match with_store(store, lock_stats, |locked| {
            locked.get_object_range("bench", &format!("object-{index}"), offset, effective_range)
        }) {
            Ok(got) if got.data.len() == effective_range as usize => {
                validate_payload_range(&got.data, offset, 0, "replica repair fault range read")
            }
            Ok(_) => Err(ObjectError::Corruption(
                "replica repair fault range read returned unexpected byte count".to_string(),
            )),
            Err(err) => Err(err),
        };
        if let Err(err) = probe_result {
            result.failure = format!("repair fault verification read {probe} failed: {err}");
            break;
        }
        result.verified_reads = result.verified_reads.saturating_add(1);
        result.verified_bytes = result.verified_bytes.saturating_add(effective_range);
    }

    result.passed = result.failure.is_empty()
        && result.removed_chunks > 0
        && result.audit_degraded_chunks > 0
        && result.audit_missing_replicas >= result.removed_chunks
        && result.repaired_replicas >= result.removed_chunks
        && result.failed_repairs == 0
        && result.unrepaired_chunks == 0
        && result.verified_reads == result.removed_chunks;
    if !result.passed && result.failure.is_empty() {
        result.failure =
            "repair fault probe did not observe expected degraded-and-repaired chunk counts"
                .to_string();
    }
    Ok(result)
}

fn phase_json(phase: &PhaseResult) -> String {
    format!(
        "{{\"ops\":{},\"bytes\":{},\"max_inflight\":{},\"seconds\":{},\"avg_ms\":{},\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{},\"p999_ms\":{},\"ops_per_sec\":{},\"mib_per_sec\":{}}}",
        phase.ops,
        phase.bytes,
        phase.max_inflight,
        json_f64(phase.seconds),
        json_f64(phase.avg_ms),
        json_f64(phase.p50_ms),
        json_f64(phase.p95_ms),
        json_f64(phase.p99_ms),
        json_f64(phase.p999_ms),
        json_f64(qps(phase)),
        json_f64(mib_per_second(phase)),
    )
}

fn phase_evidence_json(phase: &PhaseResult) -> String {
    format!(
        "{{\"qps\":{},\"mbps\":{},\"ops\":{},\"bytes\":{},\"max_inflight\":{},\"seconds\":{},\"avg_ms\":{},\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{},\"p999_ms\":{}}}",
        json_f64(qps(phase)),
        json_f64(mib_per_second(phase)),
        phase.ops,
        phase.bytes,
        phase.max_inflight,
        json_f64(phase.seconds),
        json_f64(phase.avg_ms),
        json_f64(phase.p50_ms),
        json_f64(phase.p95_ms),
        json_f64(phase.p99_ms),
        json_f64(phase.p999_ms),
    )
}

fn mixed_json(mixed: &MixedResult) -> String {
    format!(
        "{{\"ops\":{},\"bytes\":{},\"max_inflight\":{},\"seconds\":{},\"avg_ms\":{},\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{},\"p999_ms\":{},\"ops_per_sec\":{},\"mib_per_sec\":{},\"put_ops\":{},\"get_ops\":{},\"overwrite_ops\":{},\"delete_ops\":{}}}",
        mixed.put_ops + mixed.get_ops + mixed.overwrite_ops + mixed.delete_ops,
        mixed.bytes,
        mixed.max_inflight,
        json_f64(mixed.seconds),
        json_f64(mixed.avg_ms),
        json_f64(mixed.p50_ms),
        json_f64(mixed.p95_ms),
        json_f64(mixed.p99_ms),
        json_f64(mixed.p999_ms),
        json_f64(mixed_qps(mixed)),
        json_f64(mixed_mib_per_second(mixed)),
        mixed.put_ops,
        mixed.get_ops,
        mixed.overwrite_ops,
        mixed.delete_ops,
    )
}

fn mixed_evidence_json(mixed: &MixedResult) -> String {
    format!(
        "{{\"qps\":{},\"mbps\":{},\"ops\":{},\"bytes\":{},\"max_inflight\":{},\"seconds\":{},\"avg_ms\":{},\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{},\"p999_ms\":{},\"put_ops\":{},\"get_ops\":{},\"overwrite_ops\":{},\"delete_ops\":{}}}",
        json_f64(mixed_qps(mixed)),
        json_f64(mixed_mib_per_second(mixed)),
        mixed.put_ops + mixed.get_ops + mixed.overwrite_ops + mixed.delete_ops,
        mixed.bytes,
        mixed.max_inflight,
        json_f64(mixed.seconds),
        json_f64(mixed.avg_ms),
        json_f64(mixed.p50_ms),
        json_f64(mixed.p95_ms),
        json_f64(mixed.p99_ms),
        json_f64(mixed.p999_ms),
        mixed.put_ops,
        mixed.get_ops,
        mixed.overwrite_ops,
        mixed.delete_ops,
    )
}

fn recovery_json(recovery: &RecoveryResult) -> String {
    format!(
        "{{\"enabled\":{},\"passed\":{},\"snapshot_file_bytes\":{},\"reopen_ms\":{},\"verified_objects\":{},\"verified_bytes\":{},\"sample_index\":{},\"failure\":{}}}",
        recovery.enabled,
        recovery.passed,
        recovery.snapshot_file_bytes,
        json_f64(recovery.reopen_ms),
        recovery.verified_objects,
        recovery.verified_bytes,
        recovery.sample_index,
        json_escape(&recovery.failure),
    )
}

fn fault_injection_json(fault: &FaultInjectionResult) -> String {
    format!(
        "{{\"enabled\":{},\"passed\":{},\"requested_frozen_replicas\":{},\"frozen_replicas\":{},\"probe_reads\":{},\"verified_reads\":{},\"verified_bytes\":{},\"writable_replicas_during_fault\":{},\"readable_replicas_during_fault\":{},\"failure\":{}}}",
        fault.enabled,
        fault.passed,
        fault.requested_frozen_replicas,
        fault.frozen_replicas,
        fault.probe_reads,
        fault.verified_reads,
        fault.verified_bytes,
        fault.writable_replicas_during_fault,
        fault.readable_replicas_during_fault,
        json_escape(&fault.failure),
    )
}

fn repair_fault_json(repair_fault: &RepairFaultResult) -> String {
    format!(
        "{{\"enabled\":{},\"passed\":{},\"requested_removed_chunks\":{},\"removed_chunks\":{},\"audit_degraded_chunks\":{},\"audit_missing_replicas\":{},\"repaired_chunks\":{},\"repaired_replicas\":{},\"unrepaired_chunks\":{},\"failed_repairs\":{},\"verified_reads\":{},\"verified_bytes\":{},\"failure\":{}}}",
        repair_fault.enabled,
        repair_fault.passed,
        repair_fault.requested_removed_chunks,
        repair_fault.removed_chunks,
        repair_fault.audit_degraded_chunks,
        repair_fault.audit_missing_replicas,
        repair_fault.repaired_chunks,
        repair_fault.repaired_replicas,
        repair_fault.unrepaired_chunks,
        repair_fault.failed_repairs,
        repair_fault.verified_reads,
        repair_fault.verified_bytes,
        json_escape(&repair_fault.failure),
    )
}

fn quarantine_fault_json(quarantine_fault: &QuarantineFaultResult) -> String {
    format!(
        "{{\"enabled\":{},\"passed\":{},\"requested_quarantined_replicas\":{},\"quarantined_replicas\":{},\"probe_reads\":{},\"verified_reads\":{},\"verified_bytes\":{},\"writable_replicas_during_fault\":{},\"readable_replicas_during_fault\":{},\"failure\":{}}}",
        quarantine_fault.enabled,
        quarantine_fault.passed,
        quarantine_fault.requested_quarantined_replicas,
        quarantine_fault.quarantined_replicas,
        quarantine_fault.probe_reads,
        quarantine_fault.verified_reads,
        quarantine_fault.verified_bytes,
        quarantine_fault.writable_replicas_during_fault,
        quarantine_fault.readable_replicas_during_fault,
        json_escape(&quarantine_fault.failure),
    )
}

fn drain_fault_json(drain_fault: &DrainFaultResult) -> String {
    format!(
        "{{\"enabled\":{},\"passed\":{},\"requested_drained_replicas\":{},\"drained_replicas\":{},\"probe_reads\":{},\"verified_reads\":{},\"verified_bytes\":{},\"put_objects\":{},\"put_bytes\":{},\"active_replicas_during_fault\":{},\"writable_replicas_during_fault\":{},\"readable_replicas_during_fault\":{},\"failure\":{}}}",
        drain_fault.enabled,
        drain_fault.passed,
        drain_fault.requested_drained_replicas,
        drain_fault.drained_replicas,
        drain_fault.probe_reads,
        drain_fault.verified_reads,
        drain_fault.verified_bytes,
        drain_fault.put_objects,
        drain_fault.put_bytes,
        drain_fault.active_replicas_during_fault,
        drain_fault.writable_replicas_during_fault,
        drain_fault.readable_replicas_during_fault,
        json_escape(&drain_fault.failure),
    )
}

fn capacity_fault_json(capacity_fault: &CapacityFaultResult) -> String {
    format!(
        "{{\"enabled\":{},\"passed\":{},\"requested_capacity_replicas\":{},\"capacity_limited_replicas\":{},\"probe_reads\":{},\"verified_reads\":{},\"verified_bytes\":{},\"put_objects\":{},\"put_bytes\":{},\"capacity_pressure_replicas_during_fault\":{},\"writable_replicas_during_fault\":{},\"readable_replicas_during_fault\":{},\"failure\":{}}}",
        capacity_fault.enabled,
        capacity_fault.passed,
        capacity_fault.requested_capacity_replicas,
        capacity_fault.capacity_limited_replicas,
        capacity_fault.probe_reads,
        capacity_fault.verified_reads,
        capacity_fault.verified_bytes,
        capacity_fault.put_objects,
        capacity_fault.put_bytes,
        capacity_fault.capacity_pressure_replicas_during_fault,
        capacity_fault.writable_replicas_during_fault,
        capacity_fault.readable_replicas_during_fault,
        json_escape(&capacity_fault.failure),
    )
}

fn fault_suite_summary(
    required_fault_probes: u64,
    fault_injection: &FaultInjectionResult,
    quarantine_fault: &QuarantineFaultResult,
    drain_fault: &DrainFaultResult,
    capacity_fault: &CapacityFaultResult,
    repair_fault: &RepairFaultResult,
) -> FaultSuiteSummary {
    let mut summary = FaultSuiteSummary {
        required_probes: required_fault_probes,
        ..FaultSuiteSummary::default()
    };
    let mut failures = Vec::new();

    let mut add_probe = |name: &str, enabled: bool, passed: bool, failure: &str| {
        if !enabled {
            return;
        }
        summary.enabled_probes = summary.enabled_probes.saturating_add(1);
        if passed {
            summary.passed_probes = summary.passed_probes.saturating_add(1);
        } else {
            summary.failed_probes = summary.failed_probes.saturating_add(1);
            failures.push(format!("{name}:{failure}"));
        }
    };

    add_probe(
        "freeze",
        fault_injection.enabled,
        fault_injection.passed,
        &fault_injection.failure,
    );
    add_probe(
        "quarantine",
        quarantine_fault.enabled,
        quarantine_fault.passed,
        &quarantine_fault.failure,
    );
    add_probe(
        "drain",
        drain_fault.enabled,
        drain_fault.passed,
        &drain_fault.failure,
    );
    add_probe(
        "capacity",
        capacity_fault.enabled,
        capacity_fault.passed,
        &capacity_fault.failure,
    );
    add_probe(
        "repair",
        repair_fault.enabled,
        repair_fault.passed,
        &repair_fault.failure,
    );

    summary.verified_reads = fault_injection
        .verified_reads
        .saturating_add(quarantine_fault.verified_reads)
        .saturating_add(drain_fault.verified_reads)
        .saturating_add(capacity_fault.verified_reads)
        .saturating_add(repair_fault.verified_reads);
    summary.verified_bytes = fault_injection
        .verified_bytes
        .saturating_add(quarantine_fault.verified_bytes)
        .saturating_add(drain_fault.verified_bytes)
        .saturating_add(capacity_fault.verified_bytes)
        .saturating_add(repair_fault.verified_bytes);
    summary.fault_write_objects = drain_fault
        .put_objects
        .saturating_add(capacity_fault.put_objects);
    summary.fault_write_bytes = drain_fault
        .put_bytes
        .saturating_add(capacity_fault.put_bytes);
    summary.affected_replica_controls = fault_injection
        .frozen_replicas
        .saturating_add(quarantine_fault.quarantined_replicas)
        .saturating_add(drain_fault.drained_replicas)
        .saturating_add(capacity_fault.capacity_limited_replicas);
    summary.removed_chunks = repair_fault.removed_chunks;
    summary.repaired_replicas = repair_fault.repaired_replicas;
    summary.failure_summary = failures.join(",");
    summary.coverage_passed = summary.failed_probes == 0
        && summary.enabled_probes >= required_fault_probes
        && summary.passed_probes == summary.enabled_probes;
    summary.coverage_status = if summary.failed_probes != 0 {
        "failed".to_string()
    } else if summary.enabled_probes < required_fault_probes {
        "insufficient".to_string()
    } else if summary.enabled_probes == 0 {
        "disabled".to_string()
    } else {
        "complete".to_string()
    };
    summary
}

fn fault_suite_json(summary: &FaultSuiteSummary) -> String {
    format!(
        "{{\"required_probes\":{},\"enabled_probes\":{},\"passed_probes\":{},\"failed_probes\":{},\"coverage_passed\":{},\"coverage_status\":{},\"verified_reads\":{},\"verified_bytes\":{},\"fault_write_objects\":{},\"fault_write_bytes\":{},\"affected_replica_controls\":{},\"removed_chunks\":{},\"repaired_replicas\":{},\"failure_summary\":{}}}",
        summary.required_probes,
        summary.enabled_probes,
        summary.passed_probes,
        summary.failed_probes,
        summary.coverage_passed,
        json_escape(&summary.coverage_status),
        summary.verified_reads,
        summary.verified_bytes,
        summary.fault_write_objects,
        summary.fault_write_bytes,
        summary.affected_replica_controls,
        summary.removed_chunks,
        summary.repaired_replicas,
        json_escape(&summary.failure_summary),
    )
}

fn performance_slo_summary(
    cfg: &Config,
    put_phase: &PhaseResult,
    range_phase: &PhaseResult,
    mixed_phase: &MixedResult,
    recovery: &RecoveryResult,
) -> PerformanceSloSummary {
    let mut summary = PerformanceSloSummary {
        put_p99_ms: put_phase.p99_ms,
        range_p99_ms: range_phase.p99_ms,
        mixed_p99_ms: mixed_phase.p99_ms,
        put_mib_per_sec: mib_per_second(put_phase),
        range_mib_per_sec: mib_per_second(range_phase),
        mixed_mib_per_sec: mixed_mib_per_second(mixed_phase),
        recovery_snapshot_file_bytes: recovery.snapshot_file_bytes,
        max_put_p99_ms: cfg.max_put_p99_ms,
        max_range_p99_ms: cfg.max_range_p99_ms,
        max_mixed_p99_ms: cfg.max_mixed_p99_ms,
        max_recovery_snapshot_file_bytes: cfg.max_recovery_snapshot_file_bytes,
        min_put_mib_per_sec: cfg.min_put_mib_per_sec,
        min_range_mib_per_sec: cfg.min_range_mib_per_sec,
        min_mixed_mib_per_sec: cfg.min_mixed_mib_per_sec,
        ..PerformanceSloSummary::default()
    };
    let mut failures = Vec::new();

    let mut check_max = |name: &str, actual: f64, threshold: f64| {
        if threshold <= 0.0 {
            return;
        }
        summary.configured_checks = summary.configured_checks.saturating_add(1);
        if actual <= threshold {
            summary.passed_checks = summary.passed_checks.saturating_add(1);
        } else {
            summary.failed_checks = summary.failed_checks.saturating_add(1);
            failures.push(format!("{name}:{actual}>{threshold}"));
        }
    };
    check_max("put_p99_ms", summary.put_p99_ms, cfg.max_put_p99_ms);
    check_max("range_p99_ms", summary.range_p99_ms, cfg.max_range_p99_ms);
    check_max("mixed_p99_ms", summary.mixed_p99_ms, cfg.max_mixed_p99_ms);
    if cfg.max_recovery_snapshot_file_bytes > 0 {
        summary.configured_checks = summary.configured_checks.saturating_add(1);
        if recovery.snapshot_file_bytes <= cfg.max_recovery_snapshot_file_bytes {
            summary.passed_checks = summary.passed_checks.saturating_add(1);
        } else {
            summary.failed_checks = summary.failed_checks.saturating_add(1);
            failures.push(format!(
                "recovery_snapshot_file_bytes:{}>{}",
                recovery.snapshot_file_bytes, cfg.max_recovery_snapshot_file_bytes
            ));
        }
    }

    let mut check_min = |name: &str, actual: f64, threshold: f64| {
        if threshold <= 0.0 {
            return;
        }
        summary.configured_checks = summary.configured_checks.saturating_add(1);
        if actual >= threshold {
            summary.passed_checks = summary.passed_checks.saturating_add(1);
        } else {
            summary.failed_checks = summary.failed_checks.saturating_add(1);
            failures.push(format!("{name}:{actual}<{threshold}"));
        }
    };
    check_min(
        "put_mib_per_sec",
        summary.put_mib_per_sec,
        cfg.min_put_mib_per_sec,
    );
    check_min(
        "range_mib_per_sec",
        summary.range_mib_per_sec,
        cfg.min_range_mib_per_sec,
    );
    check_min(
        "mixed_mib_per_sec",
        summary.mixed_mib_per_sec,
        cfg.min_mixed_mib_per_sec,
    );

    summary.passed = summary.failed_checks == 0;
    summary.status = if summary.configured_checks == 0 {
        "disabled".to_string()
    } else if summary.failed_checks == 0 {
        "complete".to_string()
    } else {
        "failed".to_string()
    };
    summary.failure_summary = failures.join(",");
    summary
}

fn performance_slo_json(summary: &PerformanceSloSummary) -> String {
    format!(
        "{{\"configured_checks\":{},\"passed_checks\":{},\"failed_checks\":{},\"passed\":{},\"status\":{},\"failure_summary\":{},\"put_p99_ms\":{},\"range_p99_ms\":{},\"mixed_p99_ms\":{},\"put_mib_per_sec\":{},\"range_mib_per_sec\":{},\"mixed_mib_per_sec\":{},\"recovery_snapshot_file_bytes\":{},\"max_put_p99_ms\":{},\"max_range_p99_ms\":{},\"max_mixed_p99_ms\":{},\"max_recovery_snapshot_file_bytes\":{},\"min_put_mib_per_sec\":{},\"min_range_mib_per_sec\":{},\"min_mixed_mib_per_sec\":{}}}",
        summary.configured_checks,
        summary.passed_checks,
        summary.failed_checks,
        summary.passed,
        json_escape(&summary.status),
        json_escape(&summary.failure_summary),
        json_f64(summary.put_p99_ms),
        json_f64(summary.range_p99_ms),
        json_f64(summary.mixed_p99_ms),
        json_f64(summary.put_mib_per_sec),
        json_f64(summary.range_mib_per_sec),
        json_f64(summary.mixed_mib_per_sec),
        summary.recovery_snapshot_file_bytes,
        json_f64(summary.max_put_p99_ms),
        json_f64(summary.max_range_p99_ms),
        json_f64(summary.max_mixed_p99_ms),
        summary.max_recovery_snapshot_file_bytes,
        json_f64(summary.min_put_mib_per_sec),
        json_f64(summary.min_range_mib_per_sec),
        json_f64(summary.min_mixed_mib_per_sec),
    )
}

fn append_event(out: &mut String, event: &str, fields: &str) {
    let timestamp_ms = now_ms();
    out.push_str("{\"timestamp_ms\":");
    out.push_str(&timestamp_ms.to_string());
    out.push_str(",\"event\":");
    out.push_str(&json_escape(event));
    if !fields.is_empty() {
        out.push(',');
        out.push_str(fields);
    }
    out.push_str("}\n");
}

fn render_scale_event_log(
    cfg: &Config,
    put_phase: &PhaseResult,
    range_phase: &PhaseResult,
    overwrite_phase: &PhaseResult,
    delete_phase: &PhaseResult,
    mixed_phase: &MixedResult,
    gc: &GcReport,
    readiness: &ObjectStoreDistributedReadiness,
    configured_control_plane_mode: &str,
    benchmark_mode: &str,
    snapshot_bytes: u64,
    snapshot_compact_metadata_bytes: u64,
    snapshot_payload_bytes: u64,
    lock_stats: &StoreLockStats,
    benchmark_wall_ns: u64,
    store_lock_wait_wall_ppm: u64,
    recovery: &RecoveryResult,
    fault_injection: &FaultInjectionResult,
    quarantine_fault: &QuarantineFaultResult,
    drain_fault: &DrainFaultResult,
    capacity_fault: &CapacityFaultResult,
    repair_fault: &RepairFaultResult,
    fault_tolerance_passed: bool,
    fault_tolerance_reason: &str,
) -> String {
    let mut out = String::new();
    let storage_read_cache_lookup_count = readiness
        .storage_read_cache_hits
        .saturating_add(readiness.storage_read_cache_misses);
    let storage_read_cache_hit_ratio_ppm = ratio_ppm(
        readiness.storage_read_cache_hits,
        storage_read_cache_lookup_count,
    );
    let storage_completed_events = readiness
        .storage_physical_read_count
        .saturating_add(readiness.storage_physical_write_count)
        .saturating_add(readiness.storage_physical_discard_ranges);
    let storage_completed_bytes = readiness
        .storage_physical_write_bytes
        .saturating_add(readiness.storage_physical_discard_bytes);
    let storage_last_local_replica_read_repair_failure_status =
        failure_status(readiness.storage_local_replica_read_repair_failures);
    let storage_last_ec_reconstruction_failure_status =
        failure_status(readiness.storage_ec_reconstruction_failures);
    let storage_last_capacity_rejection_status =
        failure_status(readiness.storage_capacity_rejected_writes);
    let storage_last_mutation_admission_rejection_status =
        failure_status(readiness.storage_service_mode_rejected_mutations);
    let storage_fault_count = readiness.storage_fault_count();
    let storage_completion_ratio_ppm = ratio_ppm(
        storage_completed_events,
        storage_completed_events.saturating_add(storage_fault_count),
    );
    let storage_fault_status = readiness.storage_fault_status();
    let storage_fault_reason = readiness.storage_fault_reason();
    let chunk_metadata_journal = chunk_metadata_journal_metrics(readiness);
    let fault_suite = fault_suite_summary(
        cfg.required_fault_probes,
        fault_injection,
        quarantine_fault,
        drain_fault,
        capacity_fault,
        repair_fault,
    );
    let performance_slo =
        performance_slo_summary(cfg, put_phase, range_phase, mixed_phase, recovery);
    append_event(
        &mut out,
        "benchmark_config",
        &format!(
            "\"benchmark\":\"matrixobjectstore_rust_scale\",\"mode\":{},\"rust_raft_rs_linked\":{},\"object_size\":{},\"object_count\":{},\"concurrency\":{},\"range_size\":{},\"segment_size\":{},\"max_extent_bytes\":{},\"chunk_size\":{},\"chunk_read_cache_max_bytes_per_replica\":{},\"chunk_read_cache_max_cacheable_read_bytes_per_replica\":{},\"volumes\":{},\"replicas\":{},\"write_quorum\":{},\"object_batch_size\":{},\"batch_mode\":{},\"mixed_duration_ms\":{},\"fault_freeze_replicas\":{},\"fault_probe_reads\":{},\"fault_quarantine_replicas\":{},\"fault_quarantine_probe_reads\":{},\"fault_drain_replicas\":{},\"fault_drain_probe_reads\":{},\"fault_drain_put_objects\":{},\"fault_capacity_replicas\":{},\"fault_capacity_probe_reads\":{},\"fault_capacity_put_objects\":{},\"required_fault_probes\":{},\"fail_on_fault_tolerance\":{},\"max_put_p99_ms\":{},\"max_range_p99_ms\":{},\"max_mixed_p99_ms\":{},\"max_recovery_snapshot_file_bytes\":{},\"min_put_mib_per_sec\":{},\"min_range_mib_per_sec\":{},\"min_mixed_mib_per_sec\":{},\"fail_on_performance_slo\":{},\"repair_fault_remove_chunks\":{},\"repair_fault_max_chunks\":{},\"configured_control_plane_mode\":{}",
            json_escape(benchmark_mode),
            raft_backend::raft_rs_linked(),
            cfg.object_size,
            cfg.object_count,
            cfg.concurrency,
            cfg.range_size,
            cfg.chunk_size,
            cfg.max_extent_bytes,
            cfg.page_size,
            cfg.chunk_read_cache_max_bytes_per_replica,
            cfg.chunk_read_cache_max_cacheable_read_bytes_per_replica,
            cfg.volume_count,
            cfg.replica_count,
            cfg.write_quorum,
            cfg.object_batch_size,
            cfg.object_batch_size > 1,
            cfg.mixed_duration_ms,
            cfg.fault_freeze_replicas,
            cfg.fault_probe_reads,
            cfg.fault_quarantine_replicas,
            cfg.fault_quarantine_probe_reads,
            cfg.fault_drain_replicas,
            cfg.fault_drain_probe_reads,
            cfg.fault_drain_put_objects,
            cfg.fault_capacity_replicas,
            cfg.fault_capacity_probe_reads,
            cfg.fault_capacity_put_objects,
            cfg.required_fault_probes,
            cfg.fail_on_fault_tolerance,
            json_f64(cfg.max_put_p99_ms),
            json_f64(cfg.max_range_p99_ms),
            json_f64(cfg.max_mixed_p99_ms),
            cfg.max_recovery_snapshot_file_bytes,
            json_f64(cfg.min_put_mib_per_sec),
            json_f64(cfg.min_range_mib_per_sec),
            json_f64(cfg.min_mixed_mib_per_sec),
            cfg.fail_on_performance_slo,
            cfg.repair_fault_remove_chunks,
            cfg.repair_fault_max_chunks,
            json_escape(configured_control_plane_mode),
        ),
    );
    append_event(
        &mut out,
        "put_phase_completed",
        &format!("\"phase\":{}", phase_json(put_phase)),
    );
    append_event(
        &mut out,
        "range_phase_completed",
        &format!("\"phase\":{}", phase_json(range_phase)),
    );
    append_event(
        &mut out,
        "overwrite_phase_completed",
        &format!("\"phase\":{}", phase_json(overwrite_phase)),
    );
    append_event(
        &mut out,
        "delete_phase_completed",
        &format!("\"phase\":{}", phase_json(delete_phase)),
    );
    append_event(
        &mut out,
        "mixed_phase_completed",
        &format!("\"phase\":{}", mixed_json(mixed_phase)),
    );
    append_event(
        &mut out,
        "gc_pressure_reported",
        &format!(
            "\"live_bytes\":{},\"stale_bytes\":{},\"discarded_bytes\":{},\"logical_write_amplification_ppm\":{},\"space_amplification_ppm\":{},\"discard_candidate_bytes\":{},\"pending_discard_range_count\":{},\"merged_discard_range_count\":{},\"compaction_candidate_segments\":{}",
            gc.live_bytes,
            gc.stale_bytes,
            gc.discarded_bytes,
            gc.logical_write_amplification_ppm,
            gc.space_amplification_ppm,
            gc.discard_candidate_bytes,
            gc.pending_discard_range_count,
            gc.merged_discard_range_count,
            gc.compaction_candidate_count,
        ),
    );
    append_event(
        &mut out,
        "chunk_metadata_journal_reported",
        &format!(
            "\"fsync_every_records\":{},\"fsync_every_bytes\":{},\"fsync_max_unsynced_ms\":{},\"pending_fsync_records\":{},\"pending_fsync_bytes\":{},\"oldest_unsynced_ms\":{},\"record_pressure_ppm\":{},\"byte_pressure_ppm\":{},\"age_pressure_ppm\":{}",
            chunk_metadata_journal.fsync_every_records,
            chunk_metadata_journal.fsync_every_bytes,
            chunk_metadata_journal.fsync_max_unsynced_ms,
            chunk_metadata_journal.pending_fsync_records,
            chunk_metadata_journal.pending_fsync_bytes,
            chunk_metadata_journal.oldest_unsynced_ms,
            chunk_metadata_journal.record_pressure_ppm,
            chunk_metadata_journal.byte_pressure_ppm,
            chunk_metadata_journal.age_pressure_ppm,
        ),
    );
    append_event(
        &mut out,
        "snapshot_recovery_checked",
        &format!("\"recovery\":{}", recovery_json(recovery)),
    );
    append_event(
        &mut out,
        "replica_freeze_fault_checked",
        &format!(
            "\"fault_injection\":{}",
            fault_injection_json(fault_injection)
        ),
    );
    append_event(
        &mut out,
        "replica_quarantine_fault_checked",
        &format!(
            "\"quarantine_fault\":{}",
            quarantine_fault_json(quarantine_fault)
        ),
    );
    append_event(
        &mut out,
        "replica_drain_fault_checked",
        &format!("\"drain_fault\":{}", drain_fault_json(drain_fault)),
    );
    append_event(
        &mut out,
        "replica_capacity_fault_checked",
        &format!("\"capacity_fault\":{}", capacity_fault_json(capacity_fault)),
    );
    append_event(
        &mut out,
        "replica_repair_fault_checked",
        &format!("\"repair_fault\":{}", repair_fault_json(repair_fault)),
    );
    append_event(
        &mut out,
        "fault_suite_reported",
        &format!("\"fault_suite\":{}", fault_suite_json(&fault_suite)),
    );
    append_event(
        &mut out,
        "performance_slo_checked",
        &format!(
            "\"performance_slo\":{}",
            performance_slo_json(&performance_slo)
        ),
    );
    append_event(
        &mut out,
        "readiness_checked",
        &format!(
            "\"ready\":{},\"reason\":{},\"configured_control_plane_mode\":{},\"control_plane_mode\":{},\"rootserver_required\":{},\"control_checksum_format\":{},\"control_checksum\":{},\"distributed_readiness_checksum\":{},\"metadata_state_fingerprint\":{},\"root_state_fingerprint\":{},\"block_state_fingerprint\":{},\"chunk_state_fingerprint\":{},\"root_read_ready\":{},\"root_metadata_read_ready\":{},\"root_metadata_write_ready\":{},\"root_blockserver_read_ready\":{},\"root_blockserver_write_ready\":{},\"root_chunkserver_read_ready\":{},\"root_chunkserver_write_ready\":{},\"root_readiness_gate_read_ready\":{},\"root_readiness_gate_write_ready\":{},\"root_service_health\":{},\"root_service_health_reason_count\":{},\"root_service_health_reasons\":{},\"root_service_fault_tolerant\":{},\"root_snapshot_fault_count\":{},\"storage_active_replicas\":{},\"storage_writable_replicas\":{},\"storage_readable_replicas\":{},\"storage_physical_read_count\":{},\"storage_coalesced_read_count\":{},\"storage_physical_write_count\":{},\"storage_physical_write_bytes\":{},\"storage_physical_discard_ranges\":{},\"storage_physical_discard_bytes\":{},\"storage_completed_events\":{},\"storage_completed_bytes\":{},\"storage_completion_ratio_ppm\":{},\"storage_allocation_receipt_count\":{},\"storage_batch_allocation_receipt_count\":{},\"storage_release_allocation_receipt_count\":{},\"storage_write_receipt_count\":{},\"storage_discard_receipt_count\":{},\"storage_batch_write_receipt_count\":{},\"storage_batch_discard_receipt_count\":{},\"storage_read_cache_bytes\":{},\"storage_read_cache_entries\":{},\"storage_read_cache_max_bytes\":{},\"storage_read_cache_max_cacheable_read_bytes\":{},\"storage_read_cache_lookup_count\":{},\"storage_read_cache_hit_ratio_ppm\":{},\"storage_read_cache_evictions\":{},\"storage_read_cache_pressure\":{},\"storage_read_cache_uncacheable_read_pages\":{},\"storage_read_cache_uncacheable_read_bytes\":{},\"storage_read_cache_corrupt_pages\":{},\"storage_read_cache_corrupt_bytes\":{},\"storage_capacity_rejected_writes\":{},\"storage_last_capacity_rejection_status\":{},\"storage_service_mode_rejected_mutations\":{},\"storage_last_mutation_admission_rejection_status\":{},\"storage_quarantined_replicas\":{},\"storage_frozen_replicas\":{},\"storage_draining_replicas\":{},\"storage_capacity_pressure_replicas\":{},\"storage_trim_flow_limited_replicas\":{},\"storage_trim_flow_available_ops\":{},\"storage_trim_flow_throttled_ops\":{},\"storage_total_capacity_bytes\":{},\"storage_total_capacity_free_bytes\":{},\"storage_total_fragmented_chunk_count\":{},\"storage_total_fragmented_discarded_bytes\":{},\"storage_total_trailing_trim_bytes\":{},\"root_blockserver_health_ok_count\":{},\"root_blockserver_health_degraded_count\":{},\"root_blockserver_health_unavailable_count\":{},\"root_blockserver_health_unknown_count\":{},\"root_blockserver_not_fault_tolerant_count\":{},\"root_blockserver_total_health_reason_count\":{},\"root_blockserver_total_write_quorum_deficit\":{},\"root_blockserver_total_read_quorum_deficit\":{},\"root_blockserver_total_write_quorum_margin\":{},\"root_blockserver_total_read_quorum_margin\":{},\"root_chunkserver_health_ok_count\":{},\"root_chunkserver_health_degraded_count\":{},\"root_chunkserver_health_unavailable_count\":{},\"root_chunkserver_health_unknown_count\":{},\"root_chunkserver_not_fault_tolerant_count\":{},\"root_chunkserver_total_health_reason_count\":{},\"root_blockserver_first_health_reasons\":{},\"root_chunkserver_first_health_reasons\":{},\"batch_write_input_ranges\":{},\"batch_write_chunk_ranges\":{},\"chunk_batch_write_requests\":{},\"chunk_batch_write_ranges\":{},\"chunk_batch_write_fallbacks\":{},\"fresh_chunk_batch_write_requests\":{},\"fresh_chunk_batch_write_ranges\":{},\"fresh_chunk_batch_write_fallbacks\":{},\"batch_read_input_ranges\":{},\"batch_read_chunk_ranges\":{},\"primary_batch_read_requests\":{},\"primary_batch_read_chunk_ranges\":{},\"primary_batch_read_fallbacks\":{},\"primary_batch_read_repair_probe_limit\":{},\"batch_discard_input_ranges\":{},\"batch_discard_merged_ranges\":{},\"batch_discard_chunk_ranges\":{},\"chunk_batch_discard_requests\":{},\"chunk_batch_discard_ranges\":{},\"chunk_batch_discard_fallbacks\":{},\"batch_write_failures\":{},\"batch_read_failures\":{},\"batch_discard_failures\":{},\"chunk_reclaim_failures\":{},\"last_chunk_reclaim_failure_status\":{},\"storage_fault_count\":{},\"storage_fault_status\":{},\"storage_partial_reclaim_chunks\":{},\"storage_partial_reclaim_bytes\":{},\"storage_partial_reclaim_failures\":{},\"storage_last_partial_reclaim_failure_status\":{},\"storage_local_replica_read_repairs\":{},\"storage_local_replica_read_repair_failures\":{},\"storage_last_local_replica_read_repair_failure_status\":{},\"storage_cumulative_repair_checked_chunks\":{},\"storage_cumulative_repair_degraded_chunks\":{},\"storage_cumulative_repaired_replicas\":{},\"storage_cumulative_failed_repairs\":{},\"storage_compressed_chunk_count\":{},\"storage_compression_saved_bytes\":{},\"storage_ec_reconstructed_reads\":{},\"storage_ec_reconstruction_failures\":{},\"storage_last_ec_reconstruction_failure_status\":{}",
            readiness.ready,
            json_escape(&readiness.reason),
            json_escape(configured_control_plane_mode),
            json_escape(&readiness.control_plane_mode),
            readiness.rootserver_required,
            json_escape(&readiness.control_checksum_format),
            json_escape(&readiness.control_checksum),
            json_escape(&readiness.distributed_readiness_checksum),
            json_escape(&readiness.metadata_state_fingerprint),
            json_escape(&readiness.root_state_fingerprint),
            json_escape(&readiness.page_state_fingerprint),
            json_escape(&readiness.chunk_state_fingerprint),
            readiness.root_read_ready,
            readiness.root_metadata_read_ready,
            readiness.root_metadata_write_ready,
            readiness.root_pageserver_read_ready,
            readiness.root_pageserver_write_ready,
            readiness.root_replicaserver_read_ready,
            readiness.root_replicaserver_write_ready,
            readiness.root_readiness_gate_read_ready,
            readiness.root_readiness_gate_write_ready,
            json_escape(&readiness.root_service_health),
            readiness.root_service_health_reason_count,
            json_escape(&readiness.root_service_health_reasons),
            readiness.root_service_fault_tolerant,
            readiness.root_snapshot_fault_count,
            readiness.storage_active_replicas,
            readiness.storage_writable_replicas,
            readiness.storage_readable_replicas,
            readiness.storage_physical_read_count,
            readiness.storage_coalesced_read_count,
            readiness.storage_physical_write_count,
            readiness.storage_physical_write_bytes,
            readiness.storage_physical_discard_ranges,
            readiness.storage_physical_discard_bytes,
            storage_completed_events,
            storage_completed_bytes,
            storage_completion_ratio_ppm,
            readiness.storage_allocation_receipt_count,
            readiness.storage_batch_allocation_receipt_count,
            readiness.storage_release_allocation_receipt_count,
            readiness.storage_write_receipt_count,
            readiness.storage_discard_receipt_count,
            readiness.storage_batch_write_receipt_count,
            readiness.storage_batch_discard_receipt_count,
            readiness.storage_read_cache_bytes,
            readiness.storage_read_cache_entries,
            readiness.storage_read_cache_max_bytes,
            readiness.storage_read_cache_max_cacheable_read_bytes,
            storage_read_cache_lookup_count,
            storage_read_cache_hit_ratio_ppm,
            readiness.storage_read_cache_evictions,
            readiness.storage_read_cache_pressure,
            readiness.storage_read_cache_uncacheable_read_pages,
            readiness.storage_read_cache_uncacheable_read_bytes,
            readiness.storage_read_cache_corrupt_pages,
            readiness.storage_read_cache_corrupt_bytes,
            readiness.storage_capacity_rejected_writes,
            json_escape(storage_last_capacity_rejection_status),
            readiness.storage_service_mode_rejected_mutations,
            json_escape(storage_last_mutation_admission_rejection_status),
            readiness.storage_quarantined_replicas,
            readiness.storage_frozen_replicas,
            readiness.storage_draining_replicas,
            readiness.storage_capacity_pressure_replicas,
            readiness.storage_trim_flow_limited_replicas,
            readiness.storage_trim_flow_available_ops,
            readiness.storage_trim_flow_throttled_ops,
            readiness.storage_total_capacity_bytes,
            readiness.storage_total_capacity_free_bytes,
            readiness.storage_total_fragmented_chunk_count,
            readiness.storage_total_fragmented_discarded_bytes,
            readiness.storage_total_trailing_trim_bytes,
            readiness.root_pageserver_health_ok_count,
            readiness.root_pageserver_health_degraded_count,
            readiness.root_pageserver_health_unavailable_count,
            readiness.root_pageserver_health_unknown_count,
            readiness.root_pageserver_not_fault_tolerant_count,
            readiness.root_pageserver_total_health_reason_count,
            readiness.root_pageserver_total_write_quorum_deficit,
            readiness.root_pageserver_total_read_quorum_deficit,
            readiness.root_pageserver_total_write_quorum_margin,
            readiness.root_pageserver_total_read_quorum_margin,
            readiness.root_replicaserver_health_ok_count,
            readiness.root_replicaserver_health_degraded_count,
            readiness.root_replicaserver_health_unavailable_count,
            readiness.root_replicaserver_health_unknown_count,
            readiness.root_replicaserver_not_fault_tolerant_count,
            readiness.root_replicaserver_total_health_reason_count,
            json_escape(&readiness.root_pageserver_first_health_reasons),
            json_escape(&readiness.root_replicaserver_first_health_reasons),
            readiness.batch_write_input_ranges,
            readiness.batch_write_chunk_ranges,
            readiness.chunk_batch_write_requests,
            readiness.chunk_batch_write_ranges,
            readiness.chunk_batch_write_fallbacks,
            readiness.fresh_chunk_batch_write_requests,
            readiness.fresh_chunk_batch_write_ranges,
            readiness.fresh_chunk_batch_write_fallbacks,
            readiness.batch_read_input_ranges,
            readiness.batch_read_chunk_ranges,
            readiness.primary_batch_read_requests,
            readiness.primary_batch_read_chunk_ranges,
            readiness.primary_batch_read_fallbacks,
            readiness.primary_batch_read_repair_probe_limit,
            readiness.batch_discard_input_ranges,
            readiness.batch_discard_merged_ranges,
            readiness.batch_discard_chunk_ranges,
            readiness.chunk_batch_discard_requests,
            readiness.chunk_batch_discard_ranges,
            readiness.chunk_batch_discard_fallbacks,
            readiness.batch_write_failures,
            readiness.batch_read_failures,
            readiness.batch_discard_failures,
            readiness.chunk_reclaim_failures,
            json_escape(&readiness.last_chunk_reclaim_failure_status),
            storage_fault_count,
            json_escape(&storage_fault_status),
            readiness.storage_partial_reclaim_chunks,
            readiness.storage_partial_reclaim_bytes,
            readiness.storage_partial_reclaim_failures,
            json_escape(&readiness.storage_last_partial_reclaim_failure_status),
            readiness.storage_local_replica_read_repairs,
            readiness.storage_local_replica_read_repair_failures,
            json_escape(storage_last_local_replica_read_repair_failure_status),
            readiness.storage_cumulative_repair_checked_chunks,
            readiness.storage_cumulative_repair_degraded_chunks,
            readiness.storage_cumulative_repaired_replicas,
            readiness.storage_cumulative_failed_repairs,
            readiness.storage_compressed_chunk_count,
            readiness.storage_compression_saved_bytes,
            readiness.storage_ec_reconstructed_reads,
            readiness.storage_ec_reconstruction_failures,
            json_escape(storage_last_ec_reconstruction_failure_status),
        ),
    );
    append_event(
        &mut out,
        "snapshot_footprint_reported",
        &format!(
            "\"snapshot_bytes\":{},\"snapshot_compact_metadata_bytes\":{},\"snapshot_payload_bytes\":{},\"payload_elided_for_benchmark\":true",
            snapshot_bytes, snapshot_compact_metadata_bytes, snapshot_payload_bytes,
        ),
    );
    append_event(
        &mut out,
        "storage_fault_aggregate_reported",
        &format!(
            "\"storage_fault_count\":{},\"storage_fault_status\":{},\"storage_fault_reason\":{},\"storage_fault_reason_count\":{},\"batch_write_failures\":{},\"last_batch_write_failure_status\":{},\"batch_read_failures\":{},\"last_batch_read_failure_status\":{},\"batch_discard_failures\":{},\"last_batch_discard_failure_status\":{},\"chunk_reclaim_failures\":{},\"last_chunk_reclaim_failure_status\":{},\"storage_partial_reclaim_failures\":{},\"storage_last_partial_reclaim_failure_status\":{},\"storage_local_replica_read_repair_failures\":{},\"storage_last_local_replica_read_repair_failure_status\":{},\"storage_cumulative_failed_repairs\":{},\"storage_ec_reconstruction_failures\":{},\"storage_last_ec_reconstruction_failure_status\":{},\"storage_read_cache_corrupt_pages\":{},\"storage_quarantined_replicas\":{},\"storage_frozen_replicas\":{},\"storage_draining_replicas\":{},\"storage_capacity_pressure_replicas\":{},\"storage_trim_flow_limited_replicas\":{},\"storage_capacity_rejected_writes\":{},\"storage_last_capacity_rejection_status\":{},\"storage_service_mode_rejected_mutations\":{},\"storage_last_mutation_admission_rejection_status\":{},\"storage_read_cache_pressure\":{}",
            storage_fault_count,
            json_escape(storage_fault_status),
            json_escape(&storage_fault_reason),
            readiness.storage_fault_reason_count(),
            readiness.batch_write_failures,
            json_escape(&readiness.last_batch_write_failure_status),
            readiness.batch_read_failures,
            json_escape(&readiness.last_batch_read_failure_status),
            readiness.batch_discard_failures,
            json_escape(&readiness.last_batch_discard_failure_status),
            readiness.chunk_reclaim_failures,
            json_escape(&readiness.last_chunk_reclaim_failure_status),
            readiness.storage_partial_reclaim_failures,
            json_escape(&readiness.storage_last_partial_reclaim_failure_status),
            readiness.storage_local_replica_read_repair_failures,
            json_escape(storage_last_local_replica_read_repair_failure_status),
            readiness.storage_cumulative_failed_repairs,
            readiness.storage_ec_reconstruction_failures,
            json_escape(storage_last_ec_reconstruction_failure_status),
            readiness.storage_read_cache_corrupt_pages,
            readiness.storage_quarantined_replicas,
            readiness.storage_frozen_replicas,
            readiness.storage_draining_replicas,
            readiness.storage_capacity_pressure_replicas,
            readiness.storage_trim_flow_limited_replicas,
            readiness.storage_capacity_rejected_writes,
            json_escape(storage_last_capacity_rejection_status),
            readiness.storage_service_mode_rejected_mutations,
            json_escape(storage_last_mutation_admission_rejection_status),
            readiness.storage_read_cache_pressure,
        ),
    );
    append_event(
        &mut out,
        "object_timing_reported",
        &format!(
            "\"benchmark_wall_ns\":{},\"put_object_successes\":{},\"put_object_avg_total_ms\":{},\"put_object_p95_total_ms\":{},\"put_object_p99_total_ms\":{},\"put_object_p999_total_ms\":{},\"store_lock_acquisitions\":{},\"store_lock_avg_wait_ns\":{},\"store_lock_max_wait_ns\":{},\"store_lock_wait_wall_ppm\":{}",
            benchmark_wall_ns,
            put_phase.ops,
            json_f64(put_phase.avg_ms),
            json_f64(put_phase.p95_ms),
            json_f64(put_phase.p99_ms),
            json_f64(put_phase.p999_ms),
            lock_stats.acquisitions(),
            lock_stats.avg_wait_ns(),
            lock_stats.max_wait_ns(),
            store_lock_wait_wall_ppm,
        ),
    );
    append_event(
        &mut out,
        "lock_contention_reported",
        &format!(
            "\"benchmark_wall_ns\":{},\"store_lock_acquisitions\":{},\"store_lock_total_wait_ns\":{},\"store_lock_avg_wait_ns\":{},\"store_lock_max_wait_ns\":{},\"store_lock_wait_wall_ppm\":{}",
            benchmark_wall_ns,
            lock_stats.acquisitions(),
            lock_stats.total_wait_ns(),
            lock_stats.avg_wait_ns(),
            lock_stats.max_wait_ns(),
            store_lock_wait_wall_ppm,
        ),
    );
    append_event(
        &mut out,
        "fault_tolerance_checked",
        &format!(
            "\"passed\":{},\"reason\":{},\"reason_count\":{}",
            fault_tolerance_passed,
            json_escape(fault_tolerance_reason),
            reason_count(fault_tolerance_reason),
        ),
    );
    out
}

fn benchmark_fault_tolerance(
    readiness: &ObjectStoreDistributedReadiness,
    recovery: &RecoveryResult,
    fault_injection: &FaultInjectionResult,
    quarantine_fault: &QuarantineFaultResult,
    drain_fault: &DrainFaultResult,
    capacity_fault: &CapacityFaultResult,
    repair_fault: &RepairFaultResult,
    required_fault_probes: u64,
    write_quorum: usize,
) -> (bool, String) {
    let required_write_quorum = write_quorum as u64;
    let mut reasons = Vec::new();
    let with_detail = |reason: &str, detail: &str| {
        if detail.is_empty() {
            reason.to_string()
        } else {
            format!("{reason}:{}", normalize_reason_detail(detail))
        }
    };
    let fault_suite = fault_suite_summary(
        required_fault_probes,
        fault_injection,
        quarantine_fault,
        drain_fault,
        capacity_fault,
        repair_fault,
    );
    if !readiness.ready {
        reasons.push(format!("readiness_not_ready:{}", readiness.reason));
    }
    if !recovery.passed {
        reasons.push(format!("recovery_failed:{}", recovery.failure));
    }
    if fault_injection.enabled && !fault_injection.passed {
        reasons.push(format!(
            "fault_injection_failed:{}",
            fault_injection.failure
        ));
    }
    if quarantine_fault.enabled && !quarantine_fault.passed {
        reasons.push(format!(
            "quarantine_fault_failed:{}",
            quarantine_fault.failure
        ));
    }
    if drain_fault.enabled && !drain_fault.passed {
        reasons.push(format!("drain_fault_failed:{}", drain_fault.failure));
    }
    if capacity_fault.enabled && !capacity_fault.passed {
        reasons.push(format!("capacity_fault_failed:{}", capacity_fault.failure));
    }
    if repair_fault.enabled && !repair_fault.passed {
        reasons.push(format!("repair_fault_failed:{}", repair_fault.failure));
    }
    if !fault_suite.coverage_passed && required_fault_probes > 0 {
        reasons.push(format!(
            "fault_suite_coverage:{}:{}/{}",
            fault_suite.coverage_status, fault_suite.passed_probes, fault_suite.required_probes
        ));
    }
    if required_write_quorum == 0 {
        reasons.push("write_quorum_invalid:0".to_string());
    }
    if readiness.storage_writable_replicas < required_write_quorum {
        reasons.push(format!(
            "write_quorum_unavailable:{}/{}",
            readiness.storage_writable_replicas, required_write_quorum
        ));
    }
    if readiness.storage_readable_replicas == 0 {
        reasons.push("read_quorum_unavailable:0".to_string());
    }
    if readiness.storage_partial_reclaim_failures != 0 {
        reasons.push(format!(
            "storage_partial_reclaim_failures:{}:{}",
            readiness.storage_partial_reclaim_failures,
            readiness.storage_last_partial_reclaim_failure_status
        ));
    }
    if readiness.batch_write_failures != 0 {
        reasons.push(format!(
            "batch_write_failures:{}",
            readiness.batch_write_failures
        ));
    }
    if readiness.last_batch_write_failure_status != "ok" {
        reasons.push(format!(
            "batch_write:{}",
            readiness.last_batch_write_failure_status
        ));
    }
    if readiness.batch_read_failures != 0 {
        reasons.push(format!(
            "batch_read_failures:{}",
            readiness.batch_read_failures
        ));
    }
    if readiness.last_batch_read_failure_status != "ok" {
        reasons.push(format!(
            "batch_read:{}",
            readiness.last_batch_read_failure_status
        ));
    }
    if readiness.batch_discard_failures != 0 {
        reasons.push(format!(
            "batch_discard_failures:{}",
            readiness.batch_discard_failures
        ));
    }
    if readiness.last_batch_discard_failure_status != "ok" {
        reasons.push(format!(
            "batch_discard:{}",
            readiness.last_batch_discard_failure_status
        ));
    }
    if readiness.chunk_reclaim_failures != 0 {
        reasons.push(format!(
            "chunk_reclaim_failures:{}",
            readiness.chunk_reclaim_failures
        ));
    }
    if readiness.last_chunk_reclaim_failure_status != "ok" {
        reasons.push(format!(
            "chunk_reclaim:{}",
            readiness.last_chunk_reclaim_failure_status
        ));
    }
    if readiness.storage_local_replica_read_repair_failures != 0 {
        reasons.push(format!(
            "storage_local_replica_read_repair_failures:{}",
            readiness.storage_local_replica_read_repair_failures
        ));
    }
    if readiness.storage_cumulative_failed_repairs != 0 {
        reasons.push(format!(
            "storage_cumulative_failed_repairs:{}",
            readiness.storage_cumulative_failed_repairs
        ));
    }
    if readiness.storage_cumulative_repair_degraded_chunks != 0 {
        reasons.push(format!(
            "storage_cumulative_repair_degraded_chunks:{}",
            readiness.storage_cumulative_repair_degraded_chunks
        ));
    }
    if readiness.storage_ec_reconstruction_failures != 0 {
        reasons.push(format!(
            "storage_ec_reconstruction_failures:{}",
            readiness.storage_ec_reconstruction_failures
        ));
    }
    if readiness.storage_read_cache_corrupt_pages != 0
        || readiness.storage_read_cache_corrupt_bytes != 0
    {
        reasons.push(format!(
            "storage_read_cache_corrupt:{}/{}",
            readiness.storage_read_cache_corrupt_pages, readiness.storage_read_cache_corrupt_bytes
        ));
    }
    if readiness.storage_quarantined_replicas != 0 {
        reasons.push(format!(
            "storage_quarantined_replicas:{}",
            readiness.storage_quarantined_replicas
        ));
    }
    if readiness.storage_frozen_replicas != 0 {
        reasons.push(format!(
            "storage_frozen_replicas:{}",
            readiness.storage_frozen_replicas
        ));
    }
    if readiness.storage_draining_replicas != 0 {
        reasons.push(format!(
            "storage_draining_replicas:{}",
            readiness.storage_draining_replicas
        ));
    }
    if readiness.storage_capacity_pressure_replicas != 0 {
        reasons.push(format!(
            "storage_capacity_pressure_replicas:{}",
            readiness.storage_capacity_pressure_replicas
        ));
    }
    if readiness.storage_trim_flow_limited_replicas != 0 {
        reasons.push(format!(
            "storage_trim_flow_limited_replicas:{}",
            readiness.storage_trim_flow_limited_replicas
        ));
    }
    if readiness.storage_trim_flow_throttled_ops != 0 {
        reasons.push(format!(
            "storage_trim_flow_throttled_ops:{}",
            readiness.storage_trim_flow_throttled_ops
        ));
    }
    if readiness.storage_total_capacity_bytes != 0
        && readiness.storage_total_capacity_free_bytes == 0
    {
        reasons.push(format!(
            "storage_capacity_exhausted:{}/{}",
            readiness.storage_total_capacity_free_bytes, readiness.storage_total_capacity_bytes
        ));
    }
    if readiness.rootserver_required && !readiness.root_read_ready {
        reasons.push("root_read_not_ready".to_string());
    }
    if readiness.rootserver_required && !readiness.root_service_fault_tolerant {
        reasons.push("root_not_fault_tolerant".to_string());
    }
    if readiness.rootserver_required && readiness.root_service_health == "unavailable" {
        reasons.push(format!(
            "root_service_unavailable:{}",
            readiness.root_service_health_reasons
        ));
    }
    if readiness.rootserver_required && readiness.root_service_health == "degraded" {
        reasons.push(format!(
            "root_service_degraded:{}",
            readiness.root_service_health_reasons
        ));
    }
    if readiness.rootserver_required && readiness.root_snapshot_fault_count != 0 {
        reasons.push(format!(
            "root_snapshot_faults:{}",
            readiness.root_snapshot_fault_count
        ));
    }
    if readiness.root_pageserver_not_fault_tolerant_count != 0 {
        reasons.push(with_detail(
            &format!(
                "root_blockserver_not_fault_tolerant:{}",
                readiness.root_pageserver_not_fault_tolerant_count
            ),
            &readiness.root_pageserver_first_health_reasons,
        ));
    }
    if readiness.root_pageserver_health_degraded_count != 0 {
        reasons.push(with_detail(
            &format!(
                "root_blockserver_degraded:{}",
                readiness.root_pageserver_health_degraded_count
            ),
            &readiness.root_pageserver_first_health_reasons,
        ));
    }
    if readiness.root_pageserver_health_unavailable_count != 0 {
        reasons.push(with_detail(
            &format!(
                "root_blockserver_unavailable:{}",
                readiness.root_pageserver_health_unavailable_count
            ),
            &readiness.root_pageserver_first_health_reasons,
        ));
    }
    if readiness.root_pageserver_health_unknown_count != 0 {
        reasons.push(with_detail(
            &format!(
                "root_blockserver_health_unknown:{}",
                readiness.root_pageserver_health_unknown_count
            ),
            &readiness.root_pageserver_first_health_reasons,
        ));
    }
    if readiness.root_replicaserver_not_fault_tolerant_count != 0 {
        reasons.push(with_detail(
            &format!(
                "root_chunkserver_not_fault_tolerant:{}",
                readiness.root_replicaserver_not_fault_tolerant_count
            ),
            &readiness.root_replicaserver_first_health_reasons,
        ));
    }
    if readiness.root_replicaserver_health_degraded_count != 0 {
        reasons.push(with_detail(
            &format!(
                "root_chunkserver_degraded:{}",
                readiness.root_replicaserver_health_degraded_count
            ),
            &readiness.root_replicaserver_first_health_reasons,
        ));
    }
    if readiness.root_replicaserver_health_unavailable_count != 0 {
        reasons.push(with_detail(
            &format!(
                "root_chunkserver_unavailable:{}",
                readiness.root_replicaserver_health_unavailable_count
            ),
            &readiness.root_replicaserver_first_health_reasons,
        ));
    }
    if readiness.root_replicaserver_health_unknown_count != 0 {
        reasons.push(format!(
            "root_chunkserver_health_unknown:{}",
            readiness.root_replicaserver_health_unknown_count
        ));
    }
    if readiness.root_pageserver_total_write_quorum_deficit != 0 {
        reasons.push(format!(
            "root_blockserver_write_quorum_deficit:{}",
            readiness.root_pageserver_total_write_quorum_deficit
        ));
    }
    if readiness.root_pageserver_total_read_quorum_deficit != 0 {
        reasons.push(format!(
            "root_blockserver_read_quorum_deficit:{}",
            readiness.root_pageserver_total_read_quorum_deficit
        ));
    }
    if reasons.is_empty() {
        (true, "ok".to_string())
    } else {
        (
            false,
            reasons
                .into_iter()
                .map(|reason| normalize_reason_detail(&reason))
                .collect::<Vec<_>>()
                .join(";"),
        )
    }
}

fn normalize_reason_detail(detail: &str) -> String {
    detail
        .chars()
        .map(|ch| match ch {
            ';' | '\n' | '\r' | '\t' => '|',
            _ => ch,
        })
        .collect()
}

fn reason_count(reason: &str) -> usize {
    if reason.is_empty() || reason == "ok" {
        0
    } else {
        reason.split(';').filter(|entry| !entry.is_empty()).count()
    }
}

fn failure_status(failures: u64) -> &'static str {
    if failures == 0 {
        "ok"
    } else {
        "failure"
    }
}

fn chunk_metadata_journal_metrics(
    readiness: &ObjectStoreDistributedReadiness,
) -> ReplicaMetadataJournalMetrics {
    let fsync_every_records = readiness
        .root_pageserver_max_chunk_metadata_checkpoint_interval
        .max(1);
    let pending_fsync_records = readiness.root_pageserver_total_chunk_metadata_dirty_mutations;
    ReplicaMetadataJournalMetrics {
        fsync_every_records,
        fsync_every_bytes: 0,
        fsync_max_unsynced_ms: 0,
        pending_fsync_records,
        pending_fsync_bytes: 0,
        oldest_unsynced_ms: 0,
        record_pressure_ppm: ratio_ppm(pending_fsync_records, fsync_every_records),
        byte_pressure_ppm: 0,
        age_pressure_ppm: 0,
    }
}

fn replicaserver_readiness_json(readiness: &ObjectStoreDistributedReadiness) -> String {
    format!(
        concat!(
            "{{",
            "\"active\":{},\"writable\":{},\"placement_eligible\":{},",
            "\"placement_ineligible\":{},\"draining\":{},\"capacity_floor_blocked\":{},",
            "\"frozen\":{},\"read_only\":{},\"raft_unwritable\":{},",
            "\"total_reported_free_bytes\":{},\"min_free_bytes\":{},",
            "\"control_proof_count\":{},\"control_read_ready_count\":{},",
            "\"control_write_ready_count\":{},\"control_proof_checksum\":{},",
            "\"control_min_raft_commit_index\":{},\"control_max_raft_commit_index\":{},",
            "\"coverage_status\":{},\"health_ok\":{},\"health_degraded\":{},",
            "\"health_unavailable\":{},\"health_unknown\":{},",
            "\"not_fault_tolerant\":{},\"total_health_reason_count\":{},",
            "\"first_health_reasons\":{}",
            "}}"
        ),
        readiness.root_active_replicaserver_count,
        readiness.root_writable_replicaserver_count,
        readiness.root_placement_eligible_replicaserver_count,
        readiness.root_placement_ineligible_replicaserver_count,
        readiness.root_draining_replicaserver_count,
        readiness.root_capacity_floor_blocked_replicaserver_count,
        readiness.root_frozen_replicaserver_count,
        readiness.root_read_only_replicaserver_count,
        readiness.root_raft_unwritable_replicaserver_count,
        readiness.root_replicaserver_total_reported_free_bytes,
        readiness.root_replicaserver_min_free_bytes,
        readiness.root_replicaserver_control_proof_count,
        readiness.root_replicaserver_control_read_ready_count,
        readiness.root_replicaserver_control_write_ready_count,
        json_escape(&readiness.root_replicaserver_control_proof_checksum),
        readiness.root_replicaserver_control_min_raft_commit_index,
        readiness.root_replicaserver_control_max_raft_commit_index,
        json_escape(&readiness.root_replicaserver_coverage_status),
        readiness.root_replicaserver_health_ok_count,
        readiness.root_replicaserver_health_degraded_count,
        readiness.root_replicaserver_health_unavailable_count,
        readiness.root_replicaserver_health_unknown_count,
        readiness.root_replicaserver_not_fault_tolerant_count,
        readiness.root_replicaserver_total_health_reason_count,
        json_escape(&readiness.root_replicaserver_first_health_reasons),
    )
}

fn render_scale_json_report(
    cfg: &Config,
    put_phase: &PhaseResult,
    range_phase: &PhaseResult,
    overwrite_phase: &PhaseResult,
    delete_phase: &PhaseResult,
    mixed_phase: &MixedResult,
    gc: &GcReport,
    readiness: &ObjectStoreDistributedReadiness,
    configured_control_plane_mode: &str,
    snapshot_bytes: u64,
    snapshot_compact_metadata_bytes: u64,
    snapshot_payload_bytes: u64,
    lock_stats: &StoreLockStats,
    benchmark_wall_ns: u64,
    store_lock_wait_wall_ppm: u64,
    recovery: &RecoveryResult,
    fault_injection: &FaultInjectionResult,
    quarantine_fault: &QuarantineFaultResult,
    drain_fault: &DrainFaultResult,
    capacity_fault: &CapacityFaultResult,
    repair_fault: &RepairFaultResult,
) -> String {
    let (fault_tolerance_passed, fault_tolerance_reason) = benchmark_fault_tolerance(
        readiness,
        recovery,
        fault_injection,
        quarantine_fault,
        drain_fault,
        capacity_fault,
        repair_fault,
        cfg.required_fault_probes,
        cfg.write_quorum,
    );
    let storage_read_cache_lookup_count = readiness
        .storage_read_cache_hits
        .saturating_add(readiness.storage_read_cache_misses);
    let storage_read_cache_hit_ratio_ppm = ratio_ppm(
        readiness.storage_read_cache_hits,
        storage_read_cache_lookup_count,
    );
    let storage_completed_events = readiness
        .storage_physical_read_count
        .saturating_add(readiness.storage_physical_write_count)
        .saturating_add(readiness.storage_physical_discard_ranges);
    let storage_completed_bytes = readiness
        .storage_physical_write_bytes
        .saturating_add(readiness.storage_physical_discard_bytes);
    let storage_last_capacity_rejection_status =
        failure_status(readiness.storage_capacity_rejected_writes);
    let storage_last_mutation_admission_rejection_status =
        failure_status(readiness.storage_service_mode_rejected_mutations);
    let storage_last_local_replica_read_repair_failure_status =
        failure_status(readiness.storage_local_replica_read_repair_failures);
    let storage_last_ec_reconstruction_failure_status =
        failure_status(readiness.storage_ec_reconstruction_failures);
    let storage_fault_count = readiness.storage_fault_count();
    let storage_completion_ratio_ppm = ratio_ppm(
        storage_completed_events,
        storage_completed_events.saturating_add(storage_fault_count),
    );
    let storage_fault_status = readiness.storage_fault_status();
    let storage_fault_reason = readiness.storage_fault_reason();
    let chunk_metadata_journal = chunk_metadata_journal_metrics(readiness);
    let fault_suite = fault_suite_summary(
        cfg.required_fault_probes,
        fault_injection,
        quarantine_fault,
        drain_fault,
        capacity_fault,
        repair_fault,
    );
    let performance_slo =
        performance_slo_summary(cfg, put_phase, range_phase, mixed_phase, recovery);
    let generated_at_epoch_seconds = now_ms() as f64 / 1000.0;
    let provenance = GitProvenance::detect().to_json(generated_at_epoch_seconds);
    let mut report = format!(
        concat!(
            "{{\n",
            "  \"benchmark\": \"matrixobjectstore_rust_scale\",\n",
            "  \"rust_raft_rs_linked\": {},\n",
            "  \"provenance\": {},\n",
            "  \"lock_model\": {{\"store_lock_model\": \"exclusive_mutex\", \"store_sync_model\": \"send_not_sync\", \"benchmark_wall_ns\": {}, \"store_lock_acquisitions\": {}, \"store_lock_total_wait_ns\": {}, \"store_lock_avg_wait_ns\": {}, \"store_lock_max_wait_ns\": {}, \"store_lock_wait_wall_ppm\": {}}},\n",
            "  \"store_lock\": {{\"model\": \"exclusive_mutex\", \"acquisitions\": {}, \"total_wait_ns\": {}, \"avg_wait_ns\": {}, \"max_wait_ns\": {}, \"wait_wall_ppm\": {}}},\n",
            "  \"store_sync_model\": \"send_not_sync\",\n",
            "  \"benchmark_wall_ns\": {},\n",
            "  \"config\": {{\"object_size\": {}, \"object_count\": {}, \"concurrency\": {}, \"range_size\": {}, \"overwrite_ratio_percent\": {}, \"delete_ratio_percent\": {}, \"segment_size\": {}, \"max_extent_bytes\": {}, \"chunk_size\": {}, \"chunk_read_cache_max_bytes_per_replica\": {}, \"chunk_read_cache_max_cacheable_read_bytes_per_replica\": {}, \"volumes\": {}, \"replicas\": {}, \"write_quorum\": {}, \"object_batch_size\": {}, \"batch_mode\": {}, \"mixed_duration_ms\": {}, \"mixed_put_percent\": {}, \"mixed_get_percent\": {}, \"mixed_overwrite_percent\": {}, \"mixed_delete_percent\": {}, \"fault_freeze_replicas\": {}, \"fault_probe_reads\": {}, \"fault_quarantine_replicas\": {}, \"fault_quarantine_probe_reads\": {}, \"fault_drain_replicas\": {}, \"fault_drain_probe_reads\": {}, \"fault_drain_put_objects\": {}, \"fault_capacity_replicas\": {}, \"fault_capacity_probe_reads\": {}, \"fault_capacity_put_objects\": {}, \"required_fault_probes\": {}, \"fail_on_fault_tolerance\": {}, \"repair_fault_remove_chunks\": {}, \"repair_fault_max_chunks\": {}}},\n",
            "  \"workload\": {{\"object_size_bytes\": {}, \"object_count\": {}, \"clients\": {}, \"range_size_bytes\": {}, \"volumes\": {}, \"replicas\": {}, \"write_quorum\": {}, \"mixed_duration_ms\": {}, \"segment_size_bytes\": {}, \"max_extent_bytes\": {}, \"chunk_size_bytes\": {}, \"chunk_read_cache_max_bytes_per_replica\": {}, \"chunk_read_cache_max_cacheable_read_bytes_per_replica\": {}, \"object_batch_size\": {}, \"batch_mode\": {}, \"fault_freeze_replicas\": {}, \"fault_probe_reads\": {}, \"fault_quarantine_replicas\": {}, \"fault_quarantine_probe_reads\": {}, \"fault_drain_replicas\": {}, \"fault_drain_probe_reads\": {}, \"fault_drain_put_objects\": {}, \"fault_capacity_replicas\": {}, \"fault_capacity_probe_reads\": {}, \"fault_capacity_put_objects\": {}, \"required_fault_probes\": {}, \"fail_on_fault_tolerance\": {}, \"repair_fault_remove_chunks\": {}, \"repair_fault_max_chunks\": {}}},\n",
            "  \"put\": {},\n",
            "  \"get_range\": {},\n",
            "  \"overwrite\": {},\n",
            "  \"delete\": {},\n",
            "  \"mixed\": {},\n",
            "  \"phases\": {{\"put\": {}, \"range\": {}, \"overwrite\": {}, \"delete\": {}, \"mixed\": {}}},\n",
            "  \"gc\": {{\"live_bytes\": {}, \"stale_bytes\": {}, \"discarded_bytes\": {}, \"logical_write_amplification_ppm\": {}, \"space_amplification_ppm\": {}, \"discard_candidate_bytes\": {}, \"pending_discard_range_count\": {}, \"merged_discard_range_count\": {}, \"compaction_candidate_segments\": {}}},\n",
            "  \"cache\": {{\"read_cache_hit_ratio_ppm\": {}, \"read_cache_hits\": {}, \"read_cache_lookup_count\": {}, \"read_cache_misses\": {}, \"read_cache_uncacheable_read_pages\": {}, \"read_cache_uncacheable_read_bytes\": {}}},\n",
            "  \"readiness\": {{\"ready\": {}, \"reason\": {}, \"segment_count\": {}, \"chunk_count\": {}, \"metadata_commit_index\": {}, \"metadata_state_fingerprint\": {}, \"root_state_fingerprint\": {}, \"block_state_fingerprint\": {}, \"block_state_fingerprint_format\": {}, \"block_state_fingerprint_bytes\": {}, \"chunk_state_fingerprint\": {}, \"chunk_state_fingerprint_format\": {}, \"chunk_state_fingerprint_bytes\": {}, \"distributed_readiness_checksum\": {}, \"control_checksum_format\": {}, \"control_checksum\": {}, \"configured_control_plane_mode\": {}, \"effective_control_plane_mode\": {}, \"rootserver_required\": {}, \"root_read_ready\": {}, \"root_metadata_read_ready\": {}, \"root_metadata_write_ready\": {}, \"root_blockserver_read_ready\": {}, \"root_blockserver_write_ready\": {}, \"root_chunkserver_read_ready\": {}, \"root_chunkserver_write_ready\": {}, \"root_readiness_gate_read_ready\": {}, \"root_readiness_gate_write_ready\": {}, \"root_service_health\": {}, \"root_service_health_reason_count\": {}, \"root_service_health_reasons\": {}, \"root_service_fault_tolerant\": {}, \"root_snapshot_fault_count\": {}, \"root_blockserver_health_ok_count\": {}, \"root_blockserver_health_degraded_count\": {}, \"root_blockserver_health_unavailable_count\": {}, \"root_blockserver_health_unknown_count\": {}, \"root_blockserver_not_fault_tolerant_count\": {}, \"root_blockserver_total_health_reason_count\": {}, \"root_blockserver_total_write_quorum\": {}, \"root_blockserver_total_read_quorum\": {}, \"root_blockserver_total_write_quorum_deficit\": {}, \"root_blockserver_total_read_quorum_deficit\": {}, \"root_blockserver_total_write_quorum_margin\": {}, \"root_blockserver_total_read_quorum_margin\": {}, \"root_blockserver_first_health_reasons\": {}, \"root_chunkserver_health_ok_count\": {}, \"root_chunkserver_health_degraded_count\": {}, \"root_chunkserver_health_unavailable_count\": {}, \"root_chunkserver_health_unknown_count\": {}, \"root_chunkserver_not_fault_tolerant_count\": {}, \"root_chunkserver_total_health_reason_count\": {}, \"root_chunkserver_first_health_reasons\": {}, \"metadata_coordinator_backend\": {}, \"metadata_coordinator_preferred_backend\": {}, \"metadata_coordinator_production_ready\": {}, \"metadata_coordinator_external_dependency\": {}, \"storage_replica_count\": {}, \"storage_active_replicas\": {}, \"storage_readable_replicas\": {}, \"storage_writable_replicas\": {}, \"chunk_active_replicas\": {}, \"chunk_readable_replicas\": {}, \"chunk_writable_replicas\": {}}},\n",
            "  \"control_plane\": {{\"configured_mode\": {}, \"effective_mode\": {}, \"rootserver_required\": {}, \"rust_raft_rs_linked\": {}, \"metadata_coordinator_backend\": {}, \"metadata_coordinator_preferred_backend\": {}, \"metadata_coordinator_production_ready\": {}, \"metadata_coordinator_external_dependency\": {}}},\n",
            "  \"state_proofs\": {{\"metadata_state_fingerprint\": {}, \"root_state_fingerprint\": {}, \"block_state_fingerprint\": {}, \"chunk_state_fingerprint\": {}, \"distributed_readiness_checksum\": {}, \"control_checksum\": {}}},\n",
            "  \"distribution\": {{\"storage_active_replicas\": {}, \"storage_readable_replicas\": {}, \"storage_writable_replicas\": {}, \"chunk_active_replicas\": {}, \"chunk_readable_replicas\": {}, \"chunk_writable_replicas\": {}, \"root_blockserver_total_chunk_endpoint_count\": {}, \"root_blockserver_total_writable_chunk_endpoint_count\": {}, \"root_blockserver_total_raft_writable_chunk_endpoint_count\": {}, \"root_blockserver_total_raft_readable_chunk_endpoint_count\": {}, \"root_blockserver_health_ok_count\": {}, \"root_blockserver_health_degraded_count\": {}, \"root_blockserver_health_unavailable_count\": {}, \"root_blockserver_health_unknown_count\": {}, \"root_blockserver_not_fault_tolerant_count\": {}, \"root_blockserver_total_health_reason_count\": {}, \"root_blockserver_total_write_quorum_deficit\": {}, \"root_blockserver_total_read_quorum_deficit\": {}, \"root_blockserver_total_write_quorum_margin\": {}, \"root_blockserver_total_read_quorum_margin\": {}, \"root_chunkserver_health_ok_count\": {}, \"root_chunkserver_health_degraded_count\": {}, \"root_chunkserver_health_unavailable_count\": {}, \"root_chunkserver_health_unknown_count\": {}, \"root_chunkserver_not_fault_tolerant_count\": {}, \"root_chunkserver_total_health_reason_count\": {}, \"metaserver_route_cache_lookup_count\": {}, \"metaserver_route_cache_hit_ratio_ppm\": {}, \"metaserver_route_cache_miss_ratio_ppm\": {}, \"metaserver_route_cache_remaining_capacity\": {}}},\n",
            "  \"chunkserver_readiness\": {},\n",
            "  \"storage\": {{\"segment_count\": {}, \"chunk_count\": {}, \"chunk_metadata_journal_fsync_every_records\": {}, \"chunk_metadata_journal_fsync_every_bytes\": {}, \"chunk_metadata_journal_fsync_max_unsynced_ms\": {}, \"chunk_metadata_journal_pending_fsync_records\": {}, \"chunk_metadata_journal_pending_fsync_bytes\": {}, \"chunk_metadata_journal_oldest_unsynced_ms\": {}, \"chunk_metadata_journal_record_pressure_ppm\": {}, \"chunk_metadata_journal_byte_pressure_ppm\": {}, \"chunk_metadata_journal_age_pressure_ppm\": {}, \"live_bytes\": {}, \"stale_bytes\": {}, \"discarded_bytes\": {}, \"logical_write_amplification_ppm\": {}, \"space_amplification_ppm\": {}, \"discard_candidate_bytes\": {}, \"pending_discard_range_count\": {}, \"merged_discard_range_count\": {}, \"compaction_candidate_segments\": {}, \"storage_physical_read_count\": {}, \"storage_coalesced_read_count\": {}, \"storage_physical_write_count\": {}, \"storage_physical_write_bytes\": {}, \"storage_physical_discard_ranges\": {}, \"storage_physical_discard_bytes\": {}, \"completed_events\": {}, \"completed_bytes\": {}, \"completion_ratio_ppm\": {}, \"storage_allocation_receipt_count\": {}, \"storage_batch_allocation_receipt_count\": {}, \"storage_release_allocation_receipt_count\": {}, \"storage_write_receipt_count\": {}, \"storage_discard_receipt_count\": {}, \"storage_batch_write_receipt_count\": {}, \"storage_batch_discard_receipt_count\": {}, \"batch_write_input_ranges\": {}, \"batch_write_chunk_ranges\": {}, \"batch_write_failures\": {}, \"last_batch_write_failure_status\": {}, \"chunk_batch_write_requests\": {}, \"chunk_batch_write_ranges\": {}, \"chunk_batch_write_fallbacks\": {}, \"fresh_chunk_batch_write_requests\": {}, \"fresh_chunk_batch_write_ranges\": {}, \"fresh_chunk_batch_write_fallbacks\": {}, \"batch_read_input_ranges\": {}, \"batch_read_chunk_ranges\": {}, \"batch_read_failures\": {}, \"last_batch_read_failure_status\": {}, \"primary_batch_read_requests\": {}, \"primary_batch_read_chunk_ranges\": {}, \"primary_batch_read_fallbacks\": {}, \"primary_batch_read_repair_probe_limit\": {}, \"batch_discard_input_ranges\": {}, \"batch_discard_merged_ranges\": {}, \"batch_discard_chunk_ranges\": {}, \"batch_discard_failures\": {}, \"last_batch_discard_failure_status\": {}, \"chunk_batch_discard_requests\": {}, \"chunk_batch_discard_ranges\": {}, \"chunk_batch_discard_fallbacks\": {}, \"discard_admission_requests\": {}, \"discard_admission_ranges\": {}, \"chunk_reclaim_requests\": {}, \"chunk_reclaim_rewritten_chunks\": {}, \"chunk_reclaim_failures\": {}, \"last_chunk_reclaim_failure_status\": {}, \"storage_read_cache_bytes\": {}, \"storage_read_cache_entries\": {}, \"storage_read_cache_hits\": {}, \"storage_read_cache_misses\": {}, \"storage_read_cache_lookup_count\": {}, \"storage_read_cache_hit_ratio_ppm\": {}, \"storage_read_cache_uncacheable_read_pages\": {}, \"storage_read_cache_uncacheable_read_bytes\": {}, \"storage_read_cache_evictions\": {}, \"storage_local_replica_read_repairs\": {}, \"storage_local_replica_read_repair_failures\": {}, \"last_local_replica_read_repair_failure_status\": {}, \"storage_cumulative_repair_checked_chunks\": {}, \"storage_cumulative_repair_degraded_chunks\": {}, \"storage_cumulative_repaired_replicas\": {}, \"storage_cumulative_failed_repairs\": {}, \"storage_compressed_chunk_count\": {}, \"storage_compression_saved_bytes\": {}, \"storage_ec_reconstructed_reads\": {}, \"storage_ec_reconstruction_failures\": {}, \"last_ec_reconstruction_failure_status\": {}, \"storage_read_cache_pressure\": {}, \"storage_read_cache_corrupt_pages\": {}, \"storage_read_cache_corrupt_bytes\": {}, \"storage_capacity_rejected_writes\": {}, \"last_capacity_rejection_status\": {}, \"storage_service_mode_rejected_mutations\": {}, \"last_mutation_admission_rejection_status\": {}, \"storage_quarantined_replicas\": {}, \"storage_frozen_replicas\": {}, \"storage_draining_replicas\": {}, \"storage_capacity_pressure_replicas\": {}, \"storage_trim_flow_limited_replicas\": {}, \"storage_trim_flow_available_ops\": {}, \"storage_trim_flow_throttled_ops\": {}, \"storage_total_capacity_bytes\": {}, \"storage_total_capacity_free_bytes\": {}, \"storage_total_reclaimable_bytes\": {}, \"storage_total_fragmented_chunk_count\": {}, \"storage_total_fragmented_discarded_bytes\": {}, \"storage_total_trailing_trim_bytes\": {}, \"storage_partial_reclaim_chunks\": {}, \"storage_partial_reclaim_bytes\": {}, \"storage_partial_reclaim_failures\": {}, \"storage_last_partial_reclaim_failure_status\": {}, \"root_blockserver_total_chunk_endpoint_count\": {}, \"root_blockserver_total_writable_chunk_endpoint_count\": {}, \"root_blockserver_total_healthy_chunk_endpoint_count\": {}, \"root_blockserver_total_raft_writable_chunk_endpoint_count\": {}, \"root_blockserver_total_raft_readable_chunk_endpoint_count\": {}, \"block_read_parallelism\": {}, \"block_write_parallelism\": {}, \"block_batch_read_parallelism\": {}, \"block_batch_write_parallelism\": {}, \"block_batch_discard_parallelism\": {}, \"block_repair_parallelism\": {}, \"block_chunk_fanout_parallelism\": {}, \"effective_chunk_fanout_parallelism\": {}, \"local_replica_store_parallelism\": {}, \"effective_local_replica_store_parallelism\": {}, \"physical_write_bytes\": {}, \"physical_discard_bytes\": {}}},\n",
            "  \"snapshot\": {{\"bytes\": {}, \"bytes_mode\": \"estimated_compact_metadata_plus_payload\", \"compact_metadata_bytes\": {}, \"payload_bytes\": {}, \"payload_elided_for_benchmark\": true}},\n",
            "  \"recovery\": {},\n",
            "  \"fault_injection\": {},\n",
            "  \"storage_faults\": {{\"storage_fault_count\": {}, \"storage_fault_status\": {}, \"storage_fault_reason\": {}, \"storage_fault_reason_count\": {}, \"batch_write_failures\": {}, \"last_batch_write_failure_status\": {}, \"batch_read_failures\": {}, \"last_batch_read_failure_status\": {}, \"batch_discard_failures\": {}, \"last_batch_discard_failure_status\": {}, \"chunk_reclaim_failures\": {}, \"last_chunk_reclaim_failure_status\": {}, \"storage_partial_reclaim_failures\": {}, \"storage_last_partial_reclaim_failure_status\": {}, \"storage_local_replica_read_repair_failures\": {}, \"storage_last_local_replica_read_repair_failure_status\": {}, \"storage_cumulative_failed_repairs\": {}, \"storage_ec_reconstruction_failures\": {}, \"storage_last_ec_reconstruction_failure_status\": {}, \"storage_read_cache_corrupt_pages\": {}, \"storage_quarantined_replicas\": {}, \"storage_frozen_replicas\": {}, \"storage_draining_replicas\": {}, \"storage_capacity_pressure_replicas\": {}, \"storage_trim_flow_limited_replicas\": {}, \"storage_capacity_rejected_writes\": {}, \"storage_last_capacity_rejection_status\": {}, \"storage_service_mode_rejected_mutations\": {}, \"storage_last_mutation_admission_rejection_status\": {}, \"storage_read_cache_pressure\": {}}},\n",
            "  \"fault_suite\": {},\n",
            "  \"quarantine_fault\": {},\n",
            "  \"drain_fault\": {},\n",
            "  \"capacity_fault\": {},\n",
            "  \"repair_fault\": {},\n",
            "  \"fault_tolerance\": {{\"passed\": {}, \"reason\": {}, \"reason_count\": {}}},\n",
            "  \"errors\": 0\n",
            "}}\n"
        ),
        raft_backend::raft_rs_linked(),
        provenance,
        benchmark_wall_ns,
        lock_stats.acquisitions(),
        lock_stats.total_wait_ns(),
        lock_stats.avg_wait_ns(),
        lock_stats.max_wait_ns(),
        store_lock_wait_wall_ppm,
        lock_stats.acquisitions(),
        lock_stats.total_wait_ns(),
        lock_stats.avg_wait_ns(),
        lock_stats.max_wait_ns(),
        store_lock_wait_wall_ppm,
        benchmark_wall_ns,
        cfg.object_size,
        cfg.object_count,
        cfg.concurrency,
        cfg.range_size,
        cfg.overwrite_ratio_percent.min(100),
        cfg.delete_ratio_percent.min(100),
        cfg.chunk_size,
        cfg.max_extent_bytes,
        cfg.page_size,
        cfg.chunk_read_cache_max_bytes_per_replica,
        cfg.chunk_read_cache_max_cacheable_read_bytes_per_replica,
        cfg.volume_count,
        cfg.replica_count,
        cfg.write_quorum,
        cfg.object_batch_size,
        cfg.object_batch_size > 1,
        cfg.mixed_duration_ms,
        cfg.mixed_put_percent,
        cfg.mixed_get_percent,
        cfg.mixed_overwrite_percent,
        cfg.mixed_delete_percent,
        cfg.fault_freeze_replicas,
        cfg.fault_probe_reads,
        cfg.fault_quarantine_replicas,
        cfg.fault_quarantine_probe_reads,
        cfg.fault_drain_replicas,
        cfg.fault_drain_probe_reads,
        cfg.fault_drain_put_objects,
        cfg.fault_capacity_replicas,
        cfg.fault_capacity_probe_reads,
        cfg.fault_capacity_put_objects,
        cfg.required_fault_probes,
        cfg.fail_on_fault_tolerance,
        cfg.repair_fault_remove_chunks,
        cfg.repair_fault_max_chunks,
        cfg.object_size,
        cfg.object_count,
        cfg.concurrency,
        cfg.range_size,
        cfg.volume_count,
        cfg.replica_count,
        cfg.write_quorum,
        cfg.mixed_duration_ms,
        cfg.chunk_size,
        cfg.max_extent_bytes,
        cfg.page_size,
        cfg.chunk_read_cache_max_bytes_per_replica,
        cfg.chunk_read_cache_max_cacheable_read_bytes_per_replica,
        cfg.object_batch_size,
        cfg.object_batch_size > 1,
        cfg.fault_freeze_replicas,
        cfg.fault_probe_reads,
        cfg.fault_quarantine_replicas,
        cfg.fault_quarantine_probe_reads,
        cfg.fault_drain_replicas,
        cfg.fault_drain_probe_reads,
        cfg.fault_drain_put_objects,
        cfg.fault_capacity_replicas,
        cfg.fault_capacity_probe_reads,
        cfg.fault_capacity_put_objects,
        cfg.required_fault_probes,
        cfg.fail_on_fault_tolerance,
        cfg.repair_fault_remove_chunks,
        cfg.repair_fault_max_chunks,
        phase_evidence_json(put_phase),
        phase_evidence_json(range_phase),
        phase_evidence_json(overwrite_phase),
        phase_evidence_json(delete_phase),
        mixed_evidence_json(mixed_phase),
        phase_json(put_phase),
        phase_json(range_phase),
        phase_json(overwrite_phase),
        phase_json(delete_phase),
        mixed_json(mixed_phase),
        gc.live_bytes,
        gc.stale_bytes,
        gc.discarded_bytes,
        gc.logical_write_amplification_ppm,
        gc.space_amplification_ppm,
        gc.discard_candidate_bytes,
        gc.pending_discard_range_count,
        gc.merged_discard_range_count,
        gc.compaction_candidate_count,
        storage_read_cache_hit_ratio_ppm,
        readiness.storage_read_cache_hits,
        storage_read_cache_lookup_count,
        readiness.storage_read_cache_misses,
        readiness.storage_read_cache_uncacheable_read_pages,
        readiness.storage_read_cache_uncacheable_read_bytes,
        readiness.ready,
        json_escape(&readiness.reason),
        readiness.chunk_count,
        readiness.page_count,
        readiness.metadata_commit_index,
        json_escape(&readiness.metadata_state_fingerprint),
        json_escape(&readiness.root_state_fingerprint),
        json_escape(&readiness.page_state_fingerprint),
        json_escape(&readiness.page_state_fingerprint_format),
        readiness.page_state_fingerprint_bytes,
        json_escape(&readiness.chunk_state_fingerprint),
        json_escape(&readiness.chunk_state_fingerprint_format),
        readiness.chunk_state_fingerprint_bytes,
        json_escape(&readiness.distributed_readiness_checksum),
        json_escape(&readiness.control_checksum_format),
        json_escape(&readiness.control_checksum),
        json_escape(configured_control_plane_mode),
        json_escape(&readiness.control_plane_mode),
        readiness.rootserver_required,
        readiness.root_read_ready,
        readiness.root_metadata_read_ready,
        readiness.root_metadata_write_ready,
        readiness.root_pageserver_read_ready,
        readiness.root_pageserver_write_ready,
        readiness.root_replicaserver_read_ready,
        readiness.root_replicaserver_write_ready,
        readiness.root_readiness_gate_read_ready,
        readiness.root_readiness_gate_write_ready,
        json_escape(&readiness.root_service_health),
        readiness.root_service_health_reason_count,
        json_escape(&readiness.root_service_health_reasons),
        readiness.root_service_fault_tolerant,
        readiness.root_snapshot_fault_count,
        readiness.root_pageserver_health_ok_count,
        readiness.root_pageserver_health_degraded_count,
        readiness.root_pageserver_health_unavailable_count,
        readiness.root_pageserver_health_unknown_count,
        readiness.root_pageserver_not_fault_tolerant_count,
        readiness.root_pageserver_total_health_reason_count,
        readiness.root_pageserver_total_write_quorum,
        readiness.root_pageserver_total_read_quorum,
        readiness.root_pageserver_total_write_quorum_deficit,
        readiness.root_pageserver_total_read_quorum_deficit,
        readiness.root_pageserver_total_write_quorum_margin,
        readiness.root_pageserver_total_read_quorum_margin,
        json_escape(&readiness.root_pageserver_first_health_reasons),
        readiness.root_replicaserver_health_ok_count,
        readiness.root_replicaserver_health_degraded_count,
        readiness.root_replicaserver_health_unavailable_count,
        readiness.root_replicaserver_health_unknown_count,
        readiness.root_replicaserver_not_fault_tolerant_count,
        readiness.root_replicaserver_total_health_reason_count,
        json_escape(&readiness.root_replicaserver_first_health_reasons),
        json_escape(&readiness.metadata_coordinator_backend),
        json_escape(&readiness.metadata_coordinator_preferred_backend),
        readiness.metadata_coordinator_production_ready,
        json_escape(&readiness.metadata_coordinator_external_dependency),
        readiness.storage_replica_count,
        readiness.storage_active_replicas,
        readiness.storage_readable_replicas,
        readiness.storage_writable_replicas,
        readiness.page.active_replicas,
        readiness.page.readable_replicas,
        readiness.page.writable_replicas,
        json_escape(configured_control_plane_mode),
        json_escape(&readiness.control_plane_mode),
        readiness.rootserver_required,
        raft_backend::raft_rs_linked(),
        json_escape(&readiness.metadata_coordinator_backend),
        json_escape(&readiness.metadata_coordinator_preferred_backend),
        readiness.metadata_coordinator_production_ready,
        json_escape(&readiness.metadata_coordinator_external_dependency),
        json_escape(&readiness.metadata_state_fingerprint),
        json_escape(&readiness.root_state_fingerprint),
        json_escape(&readiness.page_state_fingerprint),
        json_escape(&readiness.chunk_state_fingerprint),
        json_escape(&readiness.distributed_readiness_checksum),
        json_escape(&readiness.control_checksum),
        readiness.storage_active_replicas,
        readiness.storage_readable_replicas,
        readiness.storage_writable_replicas,
        readiness.page.active_replicas,
        readiness.page.readable_replicas,
        readiness.page.writable_replicas,
        readiness.root_pageserver_total_chunk_endpoint_count,
        readiness.root_pageserver_total_writable_chunk_endpoint_count,
        readiness.root_pageserver_total_raft_writable_chunk_endpoint_count,
        readiness.root_pageserver_total_raft_readable_chunk_endpoint_count,
        readiness.root_pageserver_health_ok_count,
        readiness.root_pageserver_health_degraded_count,
        readiness.root_pageserver_health_unavailable_count,
        readiness.root_pageserver_health_unknown_count,
        readiness.root_pageserver_not_fault_tolerant_count,
        readiness.root_pageserver_total_health_reason_count,
        readiness.root_pageserver_total_write_quorum_deficit,
        readiness.root_pageserver_total_read_quorum_deficit,
        readiness.root_pageserver_total_write_quorum_margin,
        readiness.root_pageserver_total_read_quorum_margin,
        readiness.root_replicaserver_health_ok_count,
        readiness.root_replicaserver_health_degraded_count,
        readiness.root_replicaserver_health_unavailable_count,
        readiness.root_replicaserver_health_unknown_count,
        readiness.root_replicaserver_not_fault_tolerant_count,
        readiness.root_replicaserver_total_health_reason_count,
        readiness.metaserver_route_cache_lookup_count,
        readiness.metaserver_route_cache_hit_ratio_ppm,
        readiness.metaserver_route_cache_miss_ratio_ppm,
        readiness.metaserver_route_cache_remaining_capacity,
        replicaserver_readiness_json(readiness),
        readiness.chunk_count,
        readiness.page_count,
        chunk_metadata_journal.fsync_every_records,
        chunk_metadata_journal.fsync_every_bytes,
        chunk_metadata_journal.fsync_max_unsynced_ms,
        chunk_metadata_journal.pending_fsync_records,
        chunk_metadata_journal.pending_fsync_bytes,
        chunk_metadata_journal.oldest_unsynced_ms,
        chunk_metadata_journal.record_pressure_ppm,
        chunk_metadata_journal.byte_pressure_ppm,
        chunk_metadata_journal.age_pressure_ppm,
        gc.live_bytes,
        gc.stale_bytes,
        gc.discarded_bytes,
        gc.logical_write_amplification_ppm,
        gc.space_amplification_ppm,
        gc.discard_candidate_bytes,
        gc.pending_discard_range_count,
        gc.merged_discard_range_count,
        gc.compaction_candidate_count,
        readiness.storage_physical_read_count,
        readiness.storage_coalesced_read_count,
        readiness.storage_physical_write_count,
        readiness.storage_physical_write_bytes,
        readiness.storage_physical_discard_ranges,
        readiness.storage_physical_discard_bytes,
        storage_completed_events,
        storage_completed_bytes,
        storage_completion_ratio_ppm,
        readiness.storage_allocation_receipt_count,
        readiness.storage_batch_allocation_receipt_count,
        readiness.storage_release_allocation_receipt_count,
        readiness.storage_write_receipt_count,
        readiness.storage_discard_receipt_count,
        readiness.storage_batch_write_receipt_count,
        readiness.storage_batch_discard_receipt_count,
        readiness.batch_write_input_ranges,
        readiness.batch_write_chunk_ranges,
        readiness.batch_write_failures,
        json_escape(&readiness.last_batch_write_failure_status),
        readiness.chunk_batch_write_requests,
        readiness.chunk_batch_write_ranges,
        readiness.chunk_batch_write_fallbacks,
        readiness.fresh_chunk_batch_write_requests,
        readiness.fresh_chunk_batch_write_ranges,
        readiness.fresh_chunk_batch_write_fallbacks,
        readiness.batch_read_input_ranges,
        readiness.batch_read_chunk_ranges,
        readiness.batch_read_failures,
        json_escape(&readiness.last_batch_read_failure_status),
        readiness.primary_batch_read_requests,
        readiness.primary_batch_read_chunk_ranges,
        readiness.primary_batch_read_fallbacks,
        readiness.primary_batch_read_repair_probe_limit,
        readiness.batch_discard_input_ranges,
        readiness.batch_discard_merged_ranges,
        readiness.batch_discard_chunk_ranges,
        readiness.batch_discard_failures,
        json_escape(&readiness.last_batch_discard_failure_status),
        readiness.chunk_batch_discard_requests,
        readiness.chunk_batch_discard_ranges,
        readiness.chunk_batch_discard_fallbacks,
        readiness.discard_admission_requests,
        readiness.discard_admission_ranges,
        readiness.chunk_reclaim_requests,
        readiness.chunk_reclaim_rewritten_chunks,
        readiness.chunk_reclaim_failures,
        json_escape(&readiness.last_chunk_reclaim_failure_status),
        readiness.storage_read_cache_bytes,
        readiness.storage_read_cache_entries,
        readiness.storage_read_cache_hits,
        readiness.storage_read_cache_misses,
        storage_read_cache_lookup_count,
        storage_read_cache_hit_ratio_ppm,
        readiness.storage_read_cache_uncacheable_read_pages,
        readiness.storage_read_cache_uncacheable_read_bytes,
        readiness.storage_read_cache_evictions,
        readiness.storage_local_replica_read_repairs,
        readiness.storage_local_replica_read_repair_failures,
        json_escape(storage_last_local_replica_read_repair_failure_status),
        readiness.storage_cumulative_repair_checked_chunks,
        readiness.storage_cumulative_repair_degraded_chunks,
        readiness.storage_cumulative_repaired_replicas,
        readiness.storage_cumulative_failed_repairs,
        readiness.storage_compressed_chunk_count,
        readiness.storage_compression_saved_bytes,
        readiness.storage_ec_reconstructed_reads,
        readiness.storage_ec_reconstruction_failures,
        json_escape(storage_last_ec_reconstruction_failure_status),
        readiness.storage_read_cache_pressure,
        readiness.storage_read_cache_corrupt_pages,
        readiness.storage_read_cache_corrupt_bytes,
        readiness.storage_capacity_rejected_writes,
        json_escape(storage_last_capacity_rejection_status),
        readiness.storage_service_mode_rejected_mutations,
        json_escape(storage_last_mutation_admission_rejection_status),
        readiness.storage_quarantined_replicas,
        readiness.storage_frozen_replicas,
        readiness.storage_draining_replicas,
        readiness.storage_capacity_pressure_replicas,
        readiness.storage_trim_flow_limited_replicas,
        readiness.storage_trim_flow_available_ops,
        readiness.storage_trim_flow_throttled_ops,
        readiness.storage_total_capacity_bytes,
        readiness.storage_total_capacity_free_bytes,
        readiness.storage_total_reclaimable_bytes,
        readiness.storage_total_fragmented_chunk_count,
        readiness.storage_total_fragmented_discarded_bytes,
        readiness.storage_total_trailing_trim_bytes,
        readiness.storage_partial_reclaim_chunks,
        readiness.storage_partial_reclaim_bytes,
        readiness.storage_partial_reclaim_failures,
        json_escape(&readiness.storage_last_partial_reclaim_failure_status),
        readiness.root_pageserver_total_chunk_endpoint_count,
        readiness.root_pageserver_total_writable_chunk_endpoint_count,
        readiness.root_pageserver_total_healthy_chunk_endpoint_count,
        readiness.root_pageserver_total_raft_writable_chunk_endpoint_count,
        readiness.root_pageserver_total_raft_readable_chunk_endpoint_count,
        readiness.storage_page_read_parallelism,
        readiness.storage_page_write_parallelism,
        readiness.storage_page_batch_read_parallelism,
        readiness.storage_page_batch_write_parallelism,
        readiness.storage_page_batch_discard_parallelism,
        readiness.storage_page_repair_parallelism,
        readiness.storage_page_chunk_fanout_parallelism,
        readiness.storage_effective_chunk_fanout_parallelism,
        readiness.storage_local_replica_store_parallelism,
        readiness.storage_effective_local_replica_store_parallelism,
        readiness.storage_physical_write_bytes,
        readiness.storage_physical_discard_bytes,
        snapshot_bytes,
        snapshot_compact_metadata_bytes,
        snapshot_payload_bytes,
        recovery_json(recovery),
        fault_injection_json(fault_injection),
        storage_fault_count,
        json_escape(storage_fault_status),
        json_escape(&storage_fault_reason),
        readiness.storage_fault_reason_count(),
        readiness.batch_write_failures,
        json_escape(&readiness.last_batch_write_failure_status),
        readiness.batch_read_failures,
        json_escape(&readiness.last_batch_read_failure_status),
        readiness.batch_discard_failures,
        json_escape(&readiness.last_batch_discard_failure_status),
        readiness.chunk_reclaim_failures,
        json_escape(&readiness.last_chunk_reclaim_failure_status),
        readiness.storage_partial_reclaim_failures,
        json_escape(&readiness.storage_last_partial_reclaim_failure_status),
        readiness.storage_local_replica_read_repair_failures,
        json_escape(storage_last_local_replica_read_repair_failure_status),
        readiness.storage_cumulative_failed_repairs,
        readiness.storage_ec_reconstruction_failures,
        json_escape(storage_last_ec_reconstruction_failure_status),
        readiness.storage_read_cache_corrupt_pages,
        readiness.storage_quarantined_replicas,
        readiness.storage_frozen_replicas,
        readiness.storage_draining_replicas,
        readiness.storage_capacity_pressure_replicas,
        readiness.storage_trim_flow_limited_replicas,
        readiness.storage_capacity_rejected_writes,
        json_escape(storage_last_capacity_rejection_status),
        readiness.storage_service_mode_rejected_mutations,
        json_escape(storage_last_mutation_admission_rejection_status),
        readiness.storage_read_cache_pressure,
        fault_suite_json(&fault_suite),
        quarantine_fault_json(quarantine_fault),
        drain_fault_json(drain_fault),
        capacity_fault_json(capacity_fault),
        repair_fault_json(repair_fault),
        fault_tolerance_passed,
        json_escape(&fault_tolerance_reason),
        reason_count(&fault_tolerance_reason),
    );
    if let Some(offset) = report.rfind("  \"errors\": 0") {
        report.insert_str(
            offset,
            &format!(
                "  \"performance_slo\": {},\n",
                performance_slo_json(&performance_slo)
            ),
        );
    }
    report
}

fn run() -> Result<(), ObjectError> {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        eprintln!(
            "usage: scale_bench [object_size] [object_count] [concurrency] [range_size]\n\
             env: MATRIX_OBJECTSTORE_RUST_OVERWRITE_PERCENT, MATRIX_OBJECTSTORE_RUST_DELETE_PERCENT,\n\
                  MATRIX_OBJECTSTORE_RUST_SLAB_SIZE or MATRIX_OBJECTSTORE_SLAB_SIZE,\n\
                  MATRIX_OBJECTSTORE_RUST_MAX_EXTENT_BYTES or MATRIX_OBJECTSTORE_MAX_EXTENT_BYTES,\n\
                  MATRIX_OBJECTSTORE_RUST_CHUNK_SIZE or MATRIX_OBJECTSTORE_CHUNK_SIZE,\n\
                  MATRIX_OBJECTSTORE_RUST_READ_CACHE_MAX_BYTES or MATRIX_OBJECTSTORE_READ_CACHE_MAX_BYTES,\n\
                  MATRIX_OBJECTSTORE_RUST_READ_CACHE_MAX_CACHEABLE_BYTES or MATRIX_OBJECTSTORE_READ_CACHE_MAX_CACHEABLE_BYTES,\n\
                  MATRIX_OBJECTSTORE_RUST_VOLUMES or MATRIX_OBJECTSTORE_VOLUMES,\n\
                  MATRIX_OBJECTSTORE_RUST_REPLICAS or MATRIX_OBJECTSTORE_REPLICAS,\n\
                  MATRIX_OBJECTSTORE_RUST_WRITE_QUORUM or MATRIX_OBJECTSTORE_WRITE_QUORUM,\n\
                  MATRIX_OBJECTSTORE_RUST_OBJECT_BATCH_SIZE or MATRIX_OBJECTSTORE_OBJECT_BATCH_SIZE,\n\
                  MATRIX_OBJECTSTORE_RUST_MIXED_DURATION_MS, MATRIX_OBJECTSTORE_RUST_MIXED_PUT_PERCENT,\n\
                  MATRIX_OBJECTSTORE_RUST_MIXED_GET_PERCENT, MATRIX_OBJECTSTORE_RUST_MIXED_OVERWRITE_PERCENT,\n\
                  MATRIX_OBJECTSTORE_RUST_MIXED_DELETE_PERCENT,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_FREEZE_REPLICAS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_PROBE_READS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_QUARANTINE_REPLICAS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_QUARANTINE_PROBE_READS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_DRAIN_REPLICAS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_DRAIN_PROBE_READS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_DRAIN_PUT_OBJECTS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_CAPACITY_REPLICAS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_CAPACITY_PROBE_READS,\n\
                  MATRIX_OBJECTSTORE_RUST_FAULT_CAPACITY_PUT_OBJECTS,\n\
                  MATRIX_OBJECTSTORE_RUST_REQUIRED_FAULT_PROBES,\n\
                  MATRIX_OBJECTSTORE_RUST_FAIL_ON_FAULT_TOLERANCE,\n\
                  MATRIX_OBJECTSTORE_RUST_MAX_PUT_P99_MS, MATRIX_OBJECTSTORE_RUST_MAX_RANGE_P99_MS,\n\
                  MATRIX_OBJECTSTORE_RUST_MAX_MIXED_P99_MS,\n\
                  MATRIX_OBJECTSTORE_RUST_MIN_PUT_MIB_PER_SEC,\n\
                  MATRIX_OBJECTSTORE_RUST_MIN_RANGE_MIB_PER_SEC,\n\
                  MATRIX_OBJECTSTORE_RUST_MIN_MIXED_MIB_PER_SEC,\n\
                  MATRIX_OBJECTSTORE_RUST_FAIL_ON_PERFORMANCE_SLO,\n\
                  MATRIX_OBJECTSTORE_RUST_REPAIR_FAULT_REMOVE_CHUNKS,\n\
                  MATRIX_OBJECTSTORE_RUST_REPAIR_FAULT_MAX_CHUNKS,\n\
                  MATRIX_OBJECTSTORE_RUST_SCALE_JSON_OUT,\n\
                  MATRIX_OBJECTSTORE_RUST_SCALE_EVENT_LOG_OUT\n\
             compact aliases: MATRIXOBJECTSTORE_* equivalents are accepted for workload and storage knobs"
        );
        return Ok(());
    }

    let object_size = parse_u64_arg(&args, 1, "object_size", 1024 * 1024)
        .map_err(ObjectError::InvalidArgument)? as usize;
    let object_count =
        parse_u64_arg(&args, 2, "object_count", 1024).map_err(ObjectError::InvalidArgument)?;
    let concurrency =
        parse_u64_arg(&args, 3, "concurrency", 32).map_err(ObjectError::InvalidArgument)?;
    let range_size =
        parse_u64_arg(&args, 4, "range_size", 64 * 1024).map_err(ObjectError::InvalidArgument)?;
    let cfg = Config {
        object_size,
        object_count,
        concurrency,
        range_size,
        overwrite_ratio_percent: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_OVERWRITE_PERCENT",
                "MATRIX_OBJECTSTORE_OVERWRITE_PERCENT",
                "MATRIXOBJECTSTORE_RUST_OVERWRITE_PERCENT",
                "MATRIXOBJECTSTORE_OVERWRITE_PERCENT",
            ],
            10,
        )
        .map_err(ObjectError::InvalidArgument)?,
        delete_ratio_percent: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_DELETE_PERCENT",
                "MATRIX_OBJECTSTORE_DELETE_PERCENT",
                "MATRIXOBJECTSTORE_RUST_DELETE_PERCENT",
                "MATRIXOBJECTSTORE_DELETE_PERCENT",
            ],
            5,
        )
        .map_err(ObjectError::InvalidArgument)?,
        chunk_size: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_SEGMENT_SIZE",
                "MATRIX_OBJECTSTORE_SEGMENT_SIZE",
                "MATRIXOBJECTSTORE_RUST_SEGMENT_SIZE",
                "MATRIXOBJECTSTORE_SEGMENT_SIZE",
            ],
            64 * 1024 * 1024,
        )
        .map_err(ObjectError::InvalidArgument)?,
        max_extent_bytes: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MAX_EXTENT_BYTES",
                "MATRIX_OBJECTSTORE_MAX_EXTENT_BYTES",
                "MATRIXOBJECTSTORE_RUST_MAX_EXTENT_BYTES",
                "MATRIXOBJECTSTORE_MAX_EXTENT_BYTES",
            ],
            4 * 1024 * 1024,
        )
        .map_err(ObjectError::InvalidArgument)?,
        page_size: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_CHUNK_SIZE",
                "MATRIX_OBJECTSTORE_CHUNK_SIZE",
                "MATRIXOBJECTSTORE_RUST_CHUNK_SIZE",
                "MATRIXOBJECTSTORE_CHUNK_SIZE",
            ],
            1024 * 1024,
        )
        .map_err(ObjectError::InvalidArgument)?,
        chunk_read_cache_max_bytes_per_replica: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_READ_CACHE_MAX_BYTES",
                "MATRIX_OBJECTSTORE_READ_CACHE_MAX_BYTES",
                "MATRIX_OBJECTSTORE_RUST_CHUNK_READ_CACHE_MAX_BYTES",
                "MATRIX_OBJECTSTORE_CHUNK_READ_CACHE_MAX_BYTES",
                "MATRIXOBJECTSTORE_RUST_READ_CACHE_MAX_BYTES",
                "MATRIXOBJECTSTORE_READ_CACHE_MAX_BYTES",
                "MATRIXOBJECTSTORE_RUST_CHUNK_READ_CACHE_MAX_BYTES",
                "MATRIXOBJECTSTORE_CHUNK_READ_CACHE_MAX_BYTES",
            ],
            DEFAULT_SCALE_READ_CACHE_MAX_BYTES_PER_REPLICA,
        )
        .map_err(ObjectError::InvalidArgument)?,
        chunk_read_cache_max_cacheable_read_bytes_per_replica: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_READ_CACHE_MAX_CACHEABLE_BYTES",
                "MATRIX_OBJECTSTORE_READ_CACHE_MAX_CACHEABLE_BYTES",
                "MATRIX_OBJECTSTORE_RUST_CHUNK_READ_CACHE_MAX_CACHEABLE_BYTES",
                "MATRIX_OBJECTSTORE_CHUNK_READ_CACHE_MAX_CACHEABLE_BYTES",
                "MATRIXOBJECTSTORE_RUST_READ_CACHE_MAX_CACHEABLE_BYTES",
                "MATRIXOBJECTSTORE_READ_CACHE_MAX_CACHEABLE_BYTES",
                "MATRIXOBJECTSTORE_RUST_CHUNK_READ_CACHE_MAX_CACHEABLE_BYTES",
                "MATRIXOBJECTSTORE_CHUNK_READ_CACHE_MAX_CACHEABLE_BYTES",
            ],
            DEFAULT_SCALE_READ_CACHE_MAX_CACHEABLE_READ_BYTES_PER_REPLICA,
        )
        .map_err(ObjectError::InvalidArgument)?,
        volume_count: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_VOLUMES",
                "MATRIX_OBJECTSTORE_VOLUMES",
                "MATRIXOBJECTSTORE_RUST_VOLUMES",
                "MATRIXOBJECTSTORE_VOLUMES",
            ],
            4,
        )
        .map_err(ObjectError::InvalidArgument)?,
        replica_count: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_REPLICAS",
                "MATRIX_OBJECTSTORE_REPLICAS",
                "MATRIXOBJECTSTORE_RUST_REPLICAS",
                "MATRIXOBJECTSTORE_REPLICAS",
            ],
            3,
        )
        .map_err(ObjectError::InvalidArgument)? as usize,
        write_quorum: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_WRITE_QUORUM",
                "MATRIX_OBJECTSTORE_WRITE_QUORUM",
                "MATRIXOBJECTSTORE_RUST_WRITE_QUORUM",
                "MATRIXOBJECTSTORE_WRITE_QUORUM",
            ],
            2,
        )
        .map_err(ObjectError::InvalidArgument)? as usize,
        object_batch_size: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_OBJECT_BATCH_SIZE",
                "MATRIX_OBJECTSTORE_OBJECT_BATCH_SIZE",
                "MATRIXOBJECTSTORE_RUST_OBJECT_BATCH_SIZE",
                "MATRIXOBJECTSTORE_OBJECT_BATCH_SIZE",
            ],
            1,
        )
        .map_err(ObjectError::InvalidArgument)?,
        mixed_duration_ms: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MIXED_DURATION_MS",
                "MATRIX_OBJECTSTORE_MIXED_DURATION_MS",
                "MATRIXOBJECTSTORE_RUST_MIXED_DURATION_MS",
                "MATRIXOBJECTSTORE_MIXED_DURATION_MS",
            ],
            0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        mixed_put_percent: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MIXED_PUT_PERCENT",
                "MATRIX_OBJECTSTORE_MIXED_PUT_PERCENT",
                "MATRIXOBJECTSTORE_RUST_MIXED_PUT_PERCENT",
                "MATRIXOBJECTSTORE_MIXED_PUT_PERCENT",
            ],
            30,
        )
        .map_err(ObjectError::InvalidArgument)?
        .min(100),
        mixed_get_percent: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MIXED_GET_PERCENT",
                "MATRIX_OBJECTSTORE_MIXED_GET_PERCENT",
                "MATRIXOBJECTSTORE_RUST_MIXED_GET_PERCENT",
                "MATRIXOBJECTSTORE_MIXED_GET_PERCENT",
            ],
            50,
        )
        .map_err(ObjectError::InvalidArgument)?
        .min(100),
        mixed_overwrite_percent: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MIXED_OVERWRITE_PERCENT",
                "MATRIX_OBJECTSTORE_MIXED_OVERWRITE_PERCENT",
                "MATRIXOBJECTSTORE_RUST_MIXED_OVERWRITE_PERCENT",
                "MATRIXOBJECTSTORE_MIXED_OVERWRITE_PERCENT",
            ],
            15,
        )
        .map_err(ObjectError::InvalidArgument)?
        .min(100),
        mixed_delete_percent: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MIXED_DELETE_PERCENT",
                "MATRIX_OBJECTSTORE_MIXED_DELETE_PERCENT",
                "MATRIXOBJECTSTORE_RUST_MIXED_DELETE_PERCENT",
                "MATRIXOBJECTSTORE_MIXED_DELETE_PERCENT",
            ],
            5,
        )
        .map_err(ObjectError::InvalidArgument)?
        .min(100),
        fault_freeze_replicas: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_FREEZE_REPLICAS",
                "MATRIX_OBJECTSTORE_FAULT_FREEZE_REPLICAS",
                "MATRIXOBJECTSTORE_RUST_FAULT_FREEZE_REPLICAS",
                "MATRIXOBJECTSTORE_FAULT_FREEZE_REPLICAS",
            ],
            0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_probe_reads: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_PROBE_READS",
                "MATRIX_OBJECTSTORE_FAULT_PROBE_READS",
                "MATRIXOBJECTSTORE_RUST_FAULT_PROBE_READS",
                "MATRIXOBJECTSTORE_FAULT_PROBE_READS",
            ],
            8,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_quarantine_replicas: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_QUARANTINE_REPLICAS",
                "MATRIX_OBJECTSTORE_FAULT_QUARANTINE_REPLICAS",
                "MATRIXOBJECTSTORE_RUST_FAULT_QUARANTINE_REPLICAS",
                "MATRIXOBJECTSTORE_FAULT_QUARANTINE_REPLICAS",
            ],
            0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_quarantine_probe_reads: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_QUARANTINE_PROBE_READS",
                "MATRIX_OBJECTSTORE_FAULT_QUARANTINE_PROBE_READS",
                "MATRIXOBJECTSTORE_RUST_FAULT_QUARANTINE_PROBE_READS",
                "MATRIXOBJECTSTORE_FAULT_QUARANTINE_PROBE_READS",
            ],
            8,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_drain_replicas: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_DRAIN_REPLICAS",
                "MATRIX_OBJECTSTORE_FAULT_DRAIN_REPLICAS",
                "MATRIXOBJECTSTORE_RUST_FAULT_DRAIN_REPLICAS",
                "MATRIXOBJECTSTORE_FAULT_DRAIN_REPLICAS",
            ],
            0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_drain_probe_reads: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_DRAIN_PROBE_READS",
                "MATRIX_OBJECTSTORE_FAULT_DRAIN_PROBE_READS",
                "MATRIXOBJECTSTORE_RUST_FAULT_DRAIN_PROBE_READS",
                "MATRIXOBJECTSTORE_FAULT_DRAIN_PROBE_READS",
            ],
            8,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_drain_put_objects: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_DRAIN_PUT_OBJECTS",
                "MATRIX_OBJECTSTORE_FAULT_DRAIN_PUT_OBJECTS",
                "MATRIXOBJECTSTORE_RUST_FAULT_DRAIN_PUT_OBJECTS",
                "MATRIXOBJECTSTORE_FAULT_DRAIN_PUT_OBJECTS",
            ],
            1,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_capacity_replicas: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_CAPACITY_REPLICAS",
                "MATRIX_OBJECTSTORE_FAULT_CAPACITY_REPLICAS",
                "MATRIXOBJECTSTORE_RUST_FAULT_CAPACITY_REPLICAS",
                "MATRIXOBJECTSTORE_FAULT_CAPACITY_REPLICAS",
            ],
            0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_capacity_probe_reads: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_CAPACITY_PROBE_READS",
                "MATRIX_OBJECTSTORE_FAULT_CAPACITY_PROBE_READS",
                "MATRIXOBJECTSTORE_RUST_FAULT_CAPACITY_PROBE_READS",
                "MATRIXOBJECTSTORE_FAULT_CAPACITY_PROBE_READS",
            ],
            8,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fault_capacity_put_objects: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAULT_CAPACITY_PUT_OBJECTS",
                "MATRIX_OBJECTSTORE_FAULT_CAPACITY_PUT_OBJECTS",
                "MATRIXOBJECTSTORE_RUST_FAULT_CAPACITY_PUT_OBJECTS",
                "MATRIXOBJECTSTORE_FAULT_CAPACITY_PUT_OBJECTS",
            ],
            1,
        )
        .map_err(ObjectError::InvalidArgument)?,
        required_fault_probes: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_REQUIRED_FAULT_PROBES",
                "MATRIX_OBJECTSTORE_REQUIRED_FAULT_PROBES",
                "MATRIXOBJECTSTORE_RUST_REQUIRED_FAULT_PROBES",
                "MATRIXOBJECTSTORE_REQUIRED_FAULT_PROBES",
            ],
            0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fail_on_fault_tolerance: env_bool_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAIL_ON_FAULT_TOLERANCE",
                "MATRIX_OBJECTSTORE_FAIL_ON_FAULT_TOLERANCE",
                "MATRIXOBJECTSTORE_RUST_FAIL_ON_FAULT_TOLERANCE",
                "MATRIXOBJECTSTORE_FAIL_ON_FAULT_TOLERANCE",
            ],
            false,
        )
        .map_err(ObjectError::InvalidArgument)?,
        max_put_p99_ms: env_f64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MAX_PUT_P99_MS",
                "MATRIX_OBJECTSTORE_MAX_PUT_P99_MS",
                "MATRIXOBJECTSTORE_RUST_MAX_PUT_P99_MS",
                "MATRIXOBJECTSTORE_MAX_PUT_P99_MS",
            ],
            0.0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        max_range_p99_ms: env_f64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MAX_RANGE_P99_MS",
                "MATRIX_OBJECTSTORE_MAX_RANGE_P99_MS",
                "MATRIXOBJECTSTORE_RUST_MAX_RANGE_P99_MS",
                "MATRIXOBJECTSTORE_MAX_RANGE_P99_MS",
            ],
            0.0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        max_mixed_p99_ms: env_f64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MAX_MIXED_P99_MS",
                "MATRIX_OBJECTSTORE_MAX_MIXED_P99_MS",
                "MATRIXOBJECTSTORE_RUST_MAX_MIXED_P99_MS",
                "MATRIXOBJECTSTORE_MAX_MIXED_P99_MS",
            ],
            0.0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        max_recovery_snapshot_file_bytes: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MAX_RECOVERY_SNAPSHOT_FILE_BYTES",
                "MATRIX_OBJECTSTORE_MAX_RECOVERY_SNAPSHOT_FILE_BYTES",
                "MATRIXOBJECTSTORE_RUST_MAX_RECOVERY_SNAPSHOT_FILE_BYTES",
                "MATRIXOBJECTSTORE_MAX_RECOVERY_SNAPSHOT_FILE_BYTES",
            ],
            0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        min_put_mib_per_sec: env_f64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MIN_PUT_MIB_PER_SEC",
                "MATRIX_OBJECTSTORE_MIN_PUT_MIB_PER_SEC",
                "MATRIXOBJECTSTORE_RUST_MIN_PUT_MIB_PER_SEC",
                "MATRIXOBJECTSTORE_MIN_PUT_MIB_PER_SEC",
            ],
            0.0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        min_range_mib_per_sec: env_f64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MIN_RANGE_MIB_PER_SEC",
                "MATRIX_OBJECTSTORE_MIN_RANGE_MIB_PER_SEC",
                "MATRIXOBJECTSTORE_RUST_MIN_RANGE_MIB_PER_SEC",
                "MATRIXOBJECTSTORE_MIN_RANGE_MIB_PER_SEC",
            ],
            0.0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        min_mixed_mib_per_sec: env_f64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_MIN_MIXED_MIB_PER_SEC",
                "MATRIX_OBJECTSTORE_MIN_MIXED_MIB_PER_SEC",
                "MATRIXOBJECTSTORE_RUST_MIN_MIXED_MIB_PER_SEC",
                "MATRIXOBJECTSTORE_MIN_MIXED_MIB_PER_SEC",
            ],
            0.0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        fail_on_performance_slo: env_bool_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_FAIL_ON_PERFORMANCE_SLO",
                "MATRIX_OBJECTSTORE_FAIL_ON_PERFORMANCE_SLO",
                "MATRIXOBJECTSTORE_RUST_FAIL_ON_PERFORMANCE_SLO",
                "MATRIXOBJECTSTORE_FAIL_ON_PERFORMANCE_SLO",
            ],
            false,
        )
        .map_err(ObjectError::InvalidArgument)?,
        repair_fault_remove_chunks: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_REPAIR_FAULT_REMOVE_CHUNKS",
                "MATRIX_OBJECTSTORE_REPAIR_FAULT_REMOVE_CHUNKS",
                "MATRIXOBJECTSTORE_RUST_REPAIR_FAULT_REMOVE_CHUNKS",
                "MATRIXOBJECTSTORE_REPAIR_FAULT_REMOVE_CHUNKS",
            ],
            0,
        )
        .map_err(ObjectError::InvalidArgument)?,
        repair_fault_max_chunks: env_u64_any(
            &[
                "MATRIX_OBJECTSTORE_RUST_REPAIR_FAULT_MAX_CHUNKS",
                "MATRIX_OBJECTSTORE_REPAIR_FAULT_MAX_CHUNKS",
                "MATRIXOBJECTSTORE_RUST_REPAIR_FAULT_MAX_CHUNKS",
                "MATRIXOBJECTSTORE_REPAIR_FAULT_MAX_CHUNKS",
            ],
            64,
        )
        .map_err(ObjectError::InvalidArgument)?,
    };
    let benchmark_mode = env::var("MATRIX_OBJECTSTORE_BENCH_MODE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "direct".to_string());
    if cfg.object_size == 0 || cfg.object_count == 0 || cfg.concurrency == 0 || cfg.range_size == 0
    {
        return Err(ObjectError::InvalidArgument(
            "object_size, object_count, concurrency, and range_size must be non-zero".to_string(),
        ));
    }
    if cfg.page_size == 0 {
        return Err(ObjectError::InvalidArgument(
            "chunk size must be non-zero".to_string(),
        ));
    }
    let mixed_total_percent = cfg.mixed_put_percent
        + cfg.mixed_get_percent
        + cfg.mixed_overwrite_percent
        + cfg.mixed_delete_percent;
    if cfg.mixed_duration_ms > 0 && mixed_total_percent == 0 {
        return Err(ObjectError::InvalidArgument(
            "mixed workload percentages must not all be zero".to_string(),
        ));
    }
    if mixed_total_percent > 100 {
        return Err(ObjectError::InvalidArgument(
            "mixed workload percentages must sum to at most 100".to_string(),
        ));
    }

    let benchmark_start = Instant::now();
    let recovery_snapshot_path = recovery_snapshot_path();
    let options = StoreOptions {
        chunk_size: cfg.chunk_size,
        max_extent_bytes: cfg.max_extent_bytes,
        page_size: cfg.page_size,
        volume_count: cfg.volume_count,
        replica_count: cfg.replica_count,
        write_quorum: cfg.write_quorum,
        persistent_snapshot_path: recovery_snapshot_path.to_string_lossy().into_owned(),
        ..StoreOptions::default()
    };
    let mut store = MatrixObjectStore::new(options.clone())?;
    store.configure_chunk_read_cache_with_stream_bypass(
        cfg.chunk_read_cache_max_bytes_per_replica,
        cfg.chunk_read_cache_max_cacheable_read_bytes_per_replica,
    )?;
    store.create_bucket("bench")?;
    let store = Arc::new(Mutex::new(store));
    let lock_stats = Arc::new(StoreLockStats::default());

    let put_payload = Arc::new(payload(cfg.object_size, 0));
    let put_store = Arc::clone(&store);
    let put_lock_stats = Arc::clone(&lock_stats);
    let put_payload_for_workers = Arc::clone(&put_payload);
    if cfg.object_batch_size == 0 {
        return Err(ObjectError::InvalidArgument(
            "object batch size must be non-zero".to_string(),
        ));
    }

    let put_phase = run_batched_concurrent(
        cfg.object_count,
        cfg.object_size as u64,
        cfg.concurrency,
        cfg.object_batch_size,
        move |_worker, start_index, count| {
            if count == 1 {
                with_store(&put_store, &put_lock_stats, |locked| {
                    locked.put_object(
                        "bench",
                        &format!("object-{start_index}"),
                        (*put_payload_for_workers).clone(),
                        "application/octet-stream",
                    )
                })?;
                return Ok(());
            }
            let requests = (0..count)
                .map(|offset| ObjectPutRequest {
                    bucket: "bench".to_string(),
                    key: format!("object-{}", start_index + offset),
                    data: (*put_payload_for_workers).clone(),
                    content_type: "application/octet-stream".to_string(),
                    options: PutOptions::default(),
                })
                .collect::<Vec<_>>();
            let report = with_store(&put_store, &put_lock_stats, |locked| {
                locked.batch_put_objects(requests, count as usize, cfg.object_size as u64 * count)
            })?;
            if report.put_objects != count || report.failed_objects != 0 {
                return Err(ObjectError::Corruption(format!(
                    "batch put committed {}/{} objects with {} failures",
                    report.put_objects, count, report.failed_objects
                )));
            }
            Ok(())
        },
    )?;

    let effective_range = cmp::min(cfg.range_size, cfg.object_size as u64);
    let fault_injection =
        run_replica_freeze_fault_probe(&store, &lock_stats, &cfg, effective_range)?;
    let quarantine_fault =
        run_replica_quarantine_fault_probe(&store, &lock_stats, &cfg, effective_range)?;
    let drain_fault = run_replica_drain_fault_probe(&store, &lock_stats, &cfg, effective_range)?;
    let capacity_fault =
        run_replica_capacity_fault_probe(&store, &lock_stats, &cfg, effective_range)?;
    let repair_fault = run_replica_repair_fault_probe(&store, &lock_stats, &cfg, effective_range)?;

    let range_store = Arc::clone(&store);
    let range_lock_stats = Arc::clone(&lock_stats);
    let range_phase = run_batched_concurrent(
        cfg.object_count,
        effective_range,
        cfg.concurrency,
        cfg.object_batch_size,
        move |_worker, start_index, count| {
            if count == 1 {
                let offset = if cfg.object_size as u64 > effective_range {
                    (start_index.saturating_mul(4096))
                        % ((cfg.object_size as u64) - effective_range)
                } else {
                    0
                };
                let got = with_store(&range_store, &range_lock_stats, |locked| {
                    locked.get_object_range(
                        "bench",
                        &format!("object-{start_index}"),
                        offset,
                        effective_range,
                    )
                })?;
                if got.data.len() != effective_range as usize {
                    return Err(ObjectError::Corruption(
                        "range read returned unexpected byte count".to_string(),
                    ));
                }
                validate_payload_range(&got.data, offset, 0, "range read")?;
                return Ok(());
            }
            let requests = (0..count)
                .map(|offset_index| {
                    let index = start_index + offset_index;
                    let offset = if cfg.object_size as u64 > effective_range {
                        (index.saturating_mul(4096)) % ((cfg.object_size as u64) - effective_range)
                    } else {
                        0
                    };
                    ObjectReadRequest {
                        bucket: "bench".to_string(),
                        key: format!("object-{index}"),
                        range: Some(DataRange {
                            offset,
                            length: effective_range,
                        }),
                        version: None,
                    }
                })
                .collect::<Vec<_>>();
            let report = with_store(&range_store, &range_lock_stats, |locked| {
                locked.batch_get_objects(requests, count as usize, effective_range * count)
            })?;
            if report.successful_objects != count || report.failed_objects != 0 {
                return Err(ObjectError::Corruption(format!(
                    "batch range read returned {}/{} objects with {} failures",
                    report.successful_objects, count, report.failed_objects
                )));
            }
            for item in &report.items {
                let object = item.object.as_ref().ok_or_else(|| {
                    ObjectError::Corruption("batch range read missing object payload".to_string())
                })?;
                if object.data.len() != effective_range as usize {
                    return Err(ObjectError::Corruption(
                        "batch range read returned unexpected byte count".to_string(),
                    ));
                }
                let offset = item
                    .request
                    .range
                    .as_ref()
                    .map(|range| range.offset)
                    .unwrap_or(0);
                validate_payload_range(&object.data, offset, 0, "batch range read")?;
            }
            Ok(())
        },
    )?;

    let overwrite_count = percent_operation_count(cfg.object_count, cfg.overwrite_ratio_percent);
    let overwrite_store = Arc::clone(&store);
    let overwrite_lock_stats = Arc::clone(&lock_stats);
    let overwrite_payload = Arc::new(payload(cfg.object_size, 17));
    let overwrite_payload_for_workers = Arc::clone(&overwrite_payload);
    let overwrite_phase = run_batched_concurrent(
        overwrite_count,
        cfg.object_size as u64,
        cfg.concurrency,
        cfg.object_batch_size,
        move |_worker, start_index, count| {
            if count == 1 {
                with_store(&overwrite_store, &overwrite_lock_stats, |locked| {
                    locked.put_object(
                        "bench",
                        &format!("object-{start_index}"),
                        (*overwrite_payload_for_workers).clone(),
                        "application/octet-stream",
                    )
                })?;
                return Ok(());
            }
            let requests = (0..count)
                .map(|offset| ObjectPutRequest {
                    bucket: "bench".to_string(),
                    key: format!("object-{}", start_index + offset),
                    data: (*overwrite_payload_for_workers).clone(),
                    content_type: "application/octet-stream".to_string(),
                    options: PutOptions::default(),
                })
                .collect::<Vec<_>>();
            let report = with_store(&overwrite_store, &overwrite_lock_stats, |locked| {
                locked.batch_put_objects(requests, count as usize, cfg.object_size as u64 * count)
            })?;
            if report.put_objects != count || report.failed_objects != 0 {
                return Err(ObjectError::Corruption(format!(
                    "batch overwrite committed {}/{} objects with {} failures",
                    report.put_objects, count, report.failed_objects
                )));
            }
            Ok(())
        },
    )?;

    let delete_count = percent_operation_count(cfg.object_count, cfg.delete_ratio_percent);
    let recovery_sample_index = cmp::max(overwrite_count, delete_count);
    let delete_store = Arc::clone(&store);
    let delete_lock_stats = Arc::clone(&lock_stats);
    let delete_phase = run_batched_concurrent(
        delete_count,
        0,
        cfg.concurrency,
        cfg.object_batch_size,
        move |_worker, start_index, count| {
            if count == 1 {
                with_store(&delete_store, &delete_lock_stats, |locked| {
                    locked.delete_object("bench", &format!("object-{start_index}"))
                })?;
                return Ok(());
            }
            let requests = (0..count)
                .map(|offset| ObjectDeleteRequest {
                    bucket: "bench".to_string(),
                    key: format!("object-{}", start_index + offset),
                    if_match_etag: None,
                    version: None,
                })
                .collect::<Vec<_>>();
            let report = with_store(&delete_store, &delete_lock_stats, |locked| {
                locked.batch_delete_objects(requests, count as usize, false, now_ms())
            })?;
            if report.deleted_objects != count || report.failed_objects != 0 {
                return Err(ObjectError::Corruption(format!(
                    "batch delete removed {}/{} objects with {} failures",
                    report.deleted_objects, count, report.failed_objects
                )));
            }
            Ok(())
        },
    )?;

    let mixed_phase = if cfg.mixed_duration_ms == 0 {
        MixedResult::default()
    } else {
        let start = Instant::now();
        let deadline = start + Duration::from_millis(cfg.mixed_duration_ms);
        let next = Arc::new(AtomicU64::new(4));
        let active = Arc::new(AtomicU64::new(0));
        let max_inflight = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicBool::new(false));
        let first_error = Arc::new(Mutex::new(None::<ObjectError>));
        let worker_count = cmp::max(1, cfg.concurrency);
        let protected_prefix_count = cmp::max(delete_count, overwrite_count);
        let live_count = cmp::max(
            1,
            if cfg.object_count > protected_prefix_count {
                cfg.object_count - protected_prefix_count
            } else {
                cfg.object_count
            },
        );
        let read_count = cmp::max(1, live_count / 2);
        let overwrite_count = cmp::max(1, live_count - read_count);
        let overwrite_offset = protected_prefix_count + read_count;
        let mut threads = Vec::with_capacity(worker_count as usize);
        let counters = Arc::new(Mutex::new(vec![[0u64; 5]; worker_count as usize]));
        let mut seed_latencies = Vec::with_capacity(4);
        let mixed_worker_latency_reserve = bounded_latency_reserve(cfg.object_count, worker_count);
        let mut record_seed = |counter_index: usize,
                               bytes: u64,
                               op_start: Instant,
                               result: Result<(), ObjectError>| {
            result?;
            record_max(&max_inflight, 1);
            let mut locked = lock_recover(&counters);
            locked[0][counter_index] += 1;
            locked[0][4] += bytes;
            seed_latencies.push(op_start.elapsed().as_secs_f64() * 1000.0);
            Ok::<(), ObjectError>(())
        };
        let op_start = Instant::now();
        record_seed(
            0,
            cfg.object_size as u64,
            op_start,
            with_store(&store, &lock_stats, |locked| {
                locked
                    .put_object(
                        "bench",
                        "mixed-object-0",
                        payload(cfg.object_size, 0),
                        "application/octet-stream",
                    )
                    .map(|_| ())
            }),
        )?;
        let op_start = Instant::now();
        let seed_range = match with_store(&store, &lock_stats, |locked| {
            locked.get_object_range(
                "bench",
                &format!("object-{protected_prefix_count}"),
                0,
                effective_range,
            )
        }) {
            Ok(got) if got.data.len() == effective_range as usize => {
                validate_payload_range(&got.data, 0, 0, "mixed seed range read")
            }
            Ok(_) => Err(ObjectError::Corruption(
                "mixed seed range read returned unexpected byte count".to_string(),
            )),
            Err(err) => Err(err),
        };
        record_seed(1, effective_range, op_start, seed_range)?;
        let op_start = Instant::now();
        record_seed(
            2,
            cfg.object_size as u64,
            op_start,
            with_store(&store, &lock_stats, |locked| {
                locked
                    .put_object(
                        "bench",
                        &format!("object-{overwrite_offset}"),
                        payload(cfg.object_size, 17),
                        "application/octet-stream",
                    )
                    .map(|_| ())
            }),
        )?;
        let op_start = Instant::now();
        let seed_delete = with_store(&store, &lock_stats, |locked| {
            match locked.put_object(
                "bench",
                "mixed-delete-3",
                payload(cfg.object_size, 34),
                "application/octet-stream",
            ) {
                Ok(_) => locked.delete_object("bench", "mixed-delete-3").map(|_| ()),
                Err(err) => Err(err),
            }
        });
        record_seed(3, cfg.object_size as u64, op_start, seed_delete)?;
        for worker in 0..worker_count {
            let cfg_for_worker = cfg.clone();
            let store = Arc::clone(&store);
            let lock_stats = Arc::clone(&lock_stats);
            let next = Arc::clone(&next);
            let active = Arc::clone(&active);
            let max_inflight = Arc::clone(&max_inflight);
            let failed = Arc::clone(&failed);
            let first_error = Arc::clone(&first_error);
            let counters = Arc::clone(&counters);
            threads.push(thread::spawn(move || {
                let mut latencies = Vec::with_capacity(mixed_worker_latency_reserve);
                while !failed.load(Ordering::Acquire) && Instant::now() < deadline {
                    let seq = next.fetch_add(1, Ordering::Relaxed);
                    let selector = seq.wrapping_mul(1_103_515_245).wrapping_add(12_345) % 100;
                    let op_start = Instant::now();
                    record_max(&max_inflight, active.fetch_add(1, Ordering::Relaxed) + 1);
                    let result = if selector < cfg_for_worker.mixed_put_percent {
                        with_store(&store, &lock_stats, |locked| {
                            locked
                                .put_object(
                                    "bench",
                                    &format!("mixed-object-{seq}"),
                                    payload(cfg_for_worker.object_size, seq),
                                    "application/octet-stream",
                                )
                                .map(|_| ())
                        })
                    } else if selector
                        < cfg_for_worker.mixed_put_percent + cfg_for_worker.mixed_get_percent
                    {
                        let index = protected_prefix_count + (seq % read_count);
                        let offset = if cfg_for_worker.object_size as u64 > effective_range {
                            (seq.saturating_mul(4096))
                                % ((cfg_for_worker.object_size as u64) - effective_range)
                        } else {
                            0
                        };
                        match with_store(&store, &lock_stats, |locked| {
                            locked.get_object_range(
                                "bench",
                                &format!("object-{index}"),
                                offset,
                                effective_range,
                            )
                        }) {
                            Ok(got) if got.data.len() == effective_range as usize => {
                                validate_payload_range(&got.data, offset, 0, "mixed range read")
                            }
                            Ok(_) => Err(ObjectError::Corruption(
                                "mixed range read returned unexpected byte count".to_string(),
                            )),
                            Err(err) => Err(err),
                        }
                    } else if selector
                        < cfg_for_worker.mixed_put_percent
                            + cfg_for_worker.mixed_get_percent
                            + cfg_for_worker.mixed_overwrite_percent
                    {
                        let index = overwrite_offset + (seq % overwrite_count);
                        with_store(&store, &lock_stats, |locked| {
                            locked
                                .put_object(
                                    "bench",
                                    &format!("object-{index}"),
                                    payload(cfg_for_worker.object_size, seq + 17),
                                    "application/octet-stream",
                                )
                                .map(|_| ())
                        })
                    } else {
                        let key = format!("mixed-delete-{seq}");
                        with_store(&store, &lock_stats, |locked| {
                            match locked.put_object(
                                "bench",
                                &key,
                                payload(cfg_for_worker.object_size, seq + 31),
                                "application/octet-stream",
                            ) {
                                Ok(_) => locked.delete_object("bench", &key).map(|_| ()),
                                Err(err) => Err(err),
                            }
                        })
                    };
                    active.fetch_sub(1, Ordering::Relaxed);
                    if let Err(err) = result {
                        let already_failed = failed.swap(true, Ordering::AcqRel);
                        if !already_failed {
                            *lock_recover(&first_error) = Some(err);
                        }
                        break;
                    }
                    let mut locked = lock_recover(&counters);
                    let local = &mut locked[worker as usize];
                    if selector < cfg_for_worker.mixed_put_percent {
                        local[0] += 1;
                        local[4] += cfg_for_worker.object_size as u64;
                    } else if selector
                        < cfg_for_worker.mixed_put_percent + cfg_for_worker.mixed_get_percent
                    {
                        local[1] += 1;
                        local[4] += effective_range;
                    } else if selector
                        < cfg_for_worker.mixed_put_percent
                            + cfg_for_worker.mixed_get_percent
                            + cfg_for_worker.mixed_overwrite_percent
                    {
                        local[2] += 1;
                        local[4] += cfg_for_worker.object_size as u64;
                    } else {
                        local[3] += 1;
                        local[4] += cfg_for_worker.object_size as u64;
                    }
                    latencies.push(op_start.elapsed().as_secs_f64() * 1000.0);
                }
                latencies
            }));
        }
        let mut latencies = seed_latencies;
        for thread in threads {
            let worker_latencies = thread.join().map_err(|_| {
                ObjectError::Corruption("mixed scale benchmark worker thread panicked".to_string())
            })?;
            latencies.reserve(worker_latencies.len());
            latencies.extend(worker_latencies);
        }
        if let Some(err) = lock_recover(&first_error).clone() {
            return Err(err);
        }
        let total_ms: f64 = latencies.iter().copied().sum();
        let (p50_ms, p95_ms, p99_ms, p999_ms) = latency_percentiles_ms(&mut latencies);
        let mut mixed = MixedResult {
            seconds: start.elapsed().as_secs_f64(),
            max_inflight: max_inflight.load(Ordering::Relaxed),
            avg_ms: if latencies.is_empty() {
                0.0
            } else {
                total_ms / latencies.len() as f64
            },
            p50_ms,
            p95_ms,
            p99_ms,
            p999_ms,
            ..MixedResult::default()
        };
        for counter in lock_recover(&counters).iter() {
            mixed.put_ops += counter[0];
            mixed.get_ops += counter[1];
            mixed.overwrite_ops += counter[2];
            mixed.delete_ops += counter[3];
            mixed.bytes += counter[4];
        }
        mixed
    };

    let recovery = run_snapshot_recovery_probe(
        &store,
        &lock_stats,
        &options,
        &recovery_snapshot_path,
        &cfg,
        recovery_sample_index,
        effective_range,
    )?;
    let _ = std::fs::remove_file(&recovery_snapshot_path);

    let (
        gc,
        readiness,
        configured_control_plane_mode,
        snapshot_bytes,
        snapshot_compact_metadata_bytes,
        snapshot_payload_bytes,
    ) = with_store(&store, &lock_stats, |locked| {
        // Reclamation exists in the library but nothing here drives it, so the
        // audit below otherwise reports the store still holding every superseded
        // write. Running it first separates real footprint from garbage that has
        // simply not been collected. Off by default so existing numbers stand.
        if std::env::var("MATRIX_OBJECTSTORE_RUST_RECLAIM_BEFORE_REPORT")
            .map(|value| value != "0" && !value.is_empty())
            .unwrap_or(false)
        {
            // The short form passes no trim budget, and trimming is what actually
            // shrinks a chunk buffer, so use the form that takes one.
            let reclaim = locked.reclaim_chunk_replicas_with_limits(
                usize::MAX,
                u64::MAX,
                1,
                0,
                usize::MAX,
                u64::MAX,
            );
            println!("reclaim_scanned_chunks={}", reclaim.scanned_chunks);
            println!("reclaim_candidate_chunks={}", reclaim.candidate_chunks);
            println!("reclaim_reclaimed_chunks={}", reclaim.reclaimed_chunks);
            println!(
                "reclaim_reclaimed_discard_bytes={}",
                reclaim.reclaimed_discard_bytes
            );
            println!("reclaim_rewritten_bytes={}", reclaim.rewritten_bytes);
            println!("reclaim_trimmed_bytes={}", reclaim.trimmed_bytes);
            println!(
                "reclaim_remaining_discard_bytes={}",
                reclaim.remaining_discard_bytes
            );
        }
        let gc = locked.gc_report();
        let readiness = locked.distributed_readiness(0, now_ms(), 0, 0, 1, 64, 0)?;
        let configured_control_plane_mode =
            locked.configured_control_plane_mode().as_str().to_string();
        let snapshot_compact_metadata_bytes = locked.compact_metadata_snapshot_footprint_bytes();
        let snapshot_payload_bytes = locked.snapshot_payload_footprint_bytes();
        let snapshot_bytes = snapshot_compact_metadata_bytes.saturating_add(snapshot_payload_bytes);
        Ok::<_, ObjectError>((
            gc,
            readiness,
            configured_control_plane_mode,
            snapshot_bytes,
            snapshot_compact_metadata_bytes,
            snapshot_payload_bytes,
        ))
    })?;
    let benchmark_wall_ns = benchmark_start
        .elapsed()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64;
    let store_lock_wait_wall_ppm = ratio_ppm(lock_stats.total_wait_ns(), benchmark_wall_ns);
    let chunk_metadata_journal = chunk_metadata_journal_metrics(&readiness);
    let fault_suite = fault_suite_summary(
        cfg.required_fault_probes,
        &fault_injection,
        &quarantine_fault,
        &drain_fault,
        &capacity_fault,
        &repair_fault,
    );
    let performance_slo =
        performance_slo_summary(&cfg, &put_phase, &range_phase, &mixed_phase, &recovery);

    println!("benchmark=matrixobjectstore_rust_scale");
    println!("mode={benchmark_mode}");
    println!("rust_raft_rs_linked={}", raft_backend::raft_rs_linked());
    println!("store_lock_model=exclusive_mutex");
    println!("store_sync_model=send_not_sync");
    println!("benchmark_wall_ns={benchmark_wall_ns}");
    println!("store_lock_acquisitions={}", lock_stats.acquisitions());
    println!("store_lock_total_wait_ns={}", lock_stats.total_wait_ns());
    println!("store_lock_avg_wait_ns={}", lock_stats.avg_wait_ns());
    println!("store_lock_max_wait_ns={}", lock_stats.max_wait_ns());
    println!("store_lock_wait_wall_ppm={store_lock_wait_wall_ppm}");
    println!("object_size={}", cfg.object_size);
    println!("object_count={}", cfg.object_count);
    println!("concurrency={}", cfg.concurrency);
    println!("range_size={}", cfg.range_size);
    println!(
        "overwrite_ratio_percent={}",
        cfg.overwrite_ratio_percent.min(100)
    );
    println!("delete_ratio_percent={}", cfg.delete_ratio_percent.min(100));
    println!("chunk_size={}", cfg.chunk_size);
    println!("max_extent_bytes={}", cfg.max_extent_bytes);
    println!("page_size={}", cfg.page_size);
    println!(
        "chunk_read_cache_max_bytes_per_replica={}",
        cfg.chunk_read_cache_max_bytes_per_replica
    );
    println!(
        "chunk_read_cache_max_cacheable_read_bytes_per_replica={}",
        cfg.chunk_read_cache_max_cacheable_read_bytes_per_replica
    );
    println!("volumes={}", cfg.volume_count);
    println!("replicas={}", cfg.replica_count);
    println!("write_quorum={}", cfg.write_quorum);
    println!("object_batch_size={}", cfg.object_batch_size);
    println!("batch_mode={}", cfg.object_batch_size > 1);
    println!(
        "alloc.total_bytes={}",
        ALLOC_BYTES.load(Ordering::Relaxed)
    );
    println!(
        "alloc.total_count={}",
        ALLOC_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "alloc.live_peak_bytes={}",
        LIVE_PEAK_BYTES.load(Ordering::Relaxed)
    );
    println!("put_ops={}", put_phase.ops);
    println!("put_allocs={}", put_phase.allocs);
    println!("put_alloc_bytes={}", put_phase.alloc_bytes);
    println!(
        "put_allocs_per_op={}",
        per_op(put_phase.allocs, put_phase.ops)
    );
    println!(
        "put_alloc_bytes_per_op={}",
        per_op(put_phase.alloc_bytes, put_phase.ops)
    );
    println!("put_bytes={}", put_phase.bytes);
    println!("put_max_inflight={}", put_phase.max_inflight);
    println!("put_seconds={}", put_phase.seconds);
    println!("put_mib_per_sec={}", mib_per_second(&put_phase));
    println!("put_ops_per_sec={}", qps(&put_phase));
    println!("put_avg_ms={}", put_phase.avg_ms);
    println!("put_p50_ms={}", put_phase.p50_ms);
    println!("put_p95_ms={}", put_phase.p95_ms);
    println!("put_p99_ms={}", put_phase.p99_ms);
    println!("put_p999_ms={}", put_phase.p999_ms);
    println!("range_ops={}", range_phase.ops);
    println!("range_allocs={}", range_phase.allocs);
    println!("range_alloc_bytes={}", range_phase.alloc_bytes);
    println!(
        "range_allocs_per_op={}",
        per_op(range_phase.allocs, range_phase.ops)
    );
    println!(
        "range_alloc_bytes_per_op={}",
        per_op(range_phase.alloc_bytes, range_phase.ops)
    );
    println!("range_bytes={}", range_phase.bytes);
    println!("range_max_inflight={}", range_phase.max_inflight);
    println!("range_seconds={}", range_phase.seconds);
    println!("range_mib_per_sec={}", mib_per_second(&range_phase));
    println!("range_ops_per_sec={}", qps(&range_phase));
    println!("range_avg_ms={}", range_phase.avg_ms);
    println!("range_p50_ms={}", range_phase.p50_ms);
    println!("range_p95_ms={}", range_phase.p95_ms);
    println!("range_p99_ms={}", range_phase.p99_ms);
    println!("range_p999_ms={}", range_phase.p999_ms);
    println!("overwrite_ops={}", overwrite_phase.ops);
    println!("overwrite_allocs={}", overwrite_phase.allocs);
    println!("overwrite_alloc_bytes={}", overwrite_phase.alloc_bytes);
    println!(
        "overwrite_allocs_per_op={}",
        per_op(overwrite_phase.allocs, overwrite_phase.ops)
    );
    println!(
        "overwrite_alloc_bytes_per_op={}",
        per_op(overwrite_phase.alloc_bytes, overwrite_phase.ops)
    );
    println!("overwrite_max_inflight={}", overwrite_phase.max_inflight);
    println!("overwrite_seconds={}", overwrite_phase.seconds);
    println!("overwrite_ops_per_sec={}", qps(&overwrite_phase));
    println!("delete_ops={}", delete_phase.ops);
    println!("delete_allocs={}", delete_phase.allocs);
    println!("delete_alloc_bytes={}", delete_phase.alloc_bytes);
    println!(
        "delete_allocs_per_op={}",
        per_op(delete_phase.allocs, delete_phase.ops)
    );
    println!(
        "delete_alloc_bytes_per_op={}",
        per_op(delete_phase.alloc_bytes, delete_phase.ops)
    );
    println!("delete_max_inflight={}", delete_phase.max_inflight);
    println!("delete_seconds={}", delete_phase.seconds);
    println!("delete_ops_per_sec={}", qps(&delete_phase));
    println!("mixed_duration_ms={}", cfg.mixed_duration_ms);
    println!("mixed_put_percent={}", cfg.mixed_put_percent);
    println!("mixed_get_percent={}", cfg.mixed_get_percent);
    println!("mixed_overwrite_percent={}", cfg.mixed_overwrite_percent);
    println!("mixed_delete_percent={}", cfg.mixed_delete_percent);
    println!("fault_freeze_replicas={}", cfg.fault_freeze_replicas);
    println!("fault_probe_reads={}", cfg.fault_probe_reads);
    println!(
        "fault_quarantine_replicas={}",
        cfg.fault_quarantine_replicas
    );
    println!(
        "fault_quarantine_probe_reads={}",
        cfg.fault_quarantine_probe_reads
    );
    println!("fault_drain_replicas={}", cfg.fault_drain_replicas);
    println!("fault_drain_probe_reads={}", cfg.fault_drain_probe_reads);
    println!("fault_drain_put_objects={}", cfg.fault_drain_put_objects);
    println!("fault_capacity_replicas={}", cfg.fault_capacity_replicas);
    println!(
        "fault_capacity_probe_reads={}",
        cfg.fault_capacity_probe_reads
    );
    println!(
        "fault_capacity_put_objects={}",
        cfg.fault_capacity_put_objects
    );
    println!("required_fault_probes={}", cfg.required_fault_probes);
    println!("fail_on_fault_tolerance={}", cfg.fail_on_fault_tolerance);
    println!("max_put_p99_ms={}", cfg.max_put_p99_ms);
    println!("max_range_p99_ms={}", cfg.max_range_p99_ms);
    println!("max_mixed_p99_ms={}", cfg.max_mixed_p99_ms);
    println!(
        "max_recovery_snapshot_file_bytes={}",
        cfg.max_recovery_snapshot_file_bytes
    );
    println!("min_put_mib_per_sec={}", cfg.min_put_mib_per_sec);
    println!("min_range_mib_per_sec={}", cfg.min_range_mib_per_sec);
    println!("min_mixed_mib_per_sec={}", cfg.min_mixed_mib_per_sec);
    println!("fail_on_performance_slo={}", cfg.fail_on_performance_slo);
    println!(
        "repair_fault_remove_chunks={}",
        cfg.repair_fault_remove_chunks
    );
    println!("repair_fault_max_chunks={}", cfg.repair_fault_max_chunks);
    println!("fault_injection_enabled={}", fault_injection.enabled);
    println!("fault_injection_passed={}", fault_injection.passed);
    println!(
        "fault_injection_requested_frozen_replicas={}",
        fault_injection.requested_frozen_replicas
    );
    println!(
        "fault_injection_frozen_replicas={}",
        fault_injection.frozen_replicas
    );
    println!(
        "fault_injection_verified_reads={}",
        fault_injection.verified_reads
    );
    println!(
        "fault_injection_verified_bytes={}",
        fault_injection.verified_bytes
    );
    println!(
        "fault_injection_writable_replicas_during_fault={}",
        fault_injection.writable_replicas_during_fault
    );
    println!(
        "fault_injection_readable_replicas_during_fault={}",
        fault_injection.readable_replicas_during_fault
    );
    println!("fault_injection_failure={}", fault_injection.failure);
    println!("quarantine_fault_enabled={}", quarantine_fault.enabled);
    println!("quarantine_fault_passed={}", quarantine_fault.passed);
    println!(
        "quarantine_fault_requested_quarantined_replicas={}",
        quarantine_fault.requested_quarantined_replicas
    );
    println!(
        "quarantine_fault_quarantined_replicas={}",
        quarantine_fault.quarantined_replicas
    );
    println!(
        "quarantine_fault_probe_reads={}",
        quarantine_fault.probe_reads
    );
    println!(
        "quarantine_fault_verified_reads={}",
        quarantine_fault.verified_reads
    );
    println!(
        "quarantine_fault_verified_bytes={}",
        quarantine_fault.verified_bytes
    );
    println!(
        "quarantine_fault_active_replicas_during_fault={}",
        quarantine_fault.active_replicas_during_fault
    );
    println!(
        "quarantine_fault_writable_replicas_during_fault={}",
        quarantine_fault.writable_replicas_during_fault
    );
    println!(
        "quarantine_fault_readable_replicas_during_fault={}",
        quarantine_fault.readable_replicas_during_fault
    );
    println!("quarantine_fault_failure={}", quarantine_fault.failure);
    println!("drain_fault_enabled={}", drain_fault.enabled);
    println!("drain_fault_passed={}", drain_fault.passed);
    println!(
        "drain_fault_requested_drained_replicas={}",
        drain_fault.requested_drained_replicas
    );
    println!(
        "drain_fault_drained_replicas={}",
        drain_fault.drained_replicas
    );
    println!("drain_fault_verified_reads={}", drain_fault.verified_reads);
    println!("drain_fault_verified_bytes={}", drain_fault.verified_bytes);
    println!("drain_fault_put_objects={}", drain_fault.put_objects);
    println!("drain_fault_put_bytes={}", drain_fault.put_bytes);
    println!(
        "drain_fault_active_replicas_during_fault={}",
        drain_fault.active_replicas_during_fault
    );
    println!(
        "drain_fault_writable_replicas_during_fault={}",
        drain_fault.writable_replicas_during_fault
    );
    println!(
        "drain_fault_readable_replicas_during_fault={}",
        drain_fault.readable_replicas_during_fault
    );
    println!("drain_fault_failure={}", drain_fault.failure);
    println!("capacity_fault_enabled={}", capacity_fault.enabled);
    println!("capacity_fault_passed={}", capacity_fault.passed);
    println!(
        "capacity_fault_requested_capacity_replicas={}",
        capacity_fault.requested_capacity_replicas
    );
    println!(
        "capacity_fault_capacity_limited_replicas={}",
        capacity_fault.capacity_limited_replicas
    );
    println!(
        "capacity_fault_verified_reads={}",
        capacity_fault.verified_reads
    );
    println!(
        "capacity_fault_verified_bytes={}",
        capacity_fault.verified_bytes
    );
    println!("capacity_fault_put_objects={}", capacity_fault.put_objects);
    println!("capacity_fault_put_bytes={}", capacity_fault.put_bytes);
    println!(
        "capacity_fault_capacity_pressure_replicas_during_fault={}",
        capacity_fault.capacity_pressure_replicas_during_fault
    );
    println!(
        "capacity_fault_writable_replicas_during_fault={}",
        capacity_fault.writable_replicas_during_fault
    );
    println!(
        "capacity_fault_readable_replicas_during_fault={}",
        capacity_fault.readable_replicas_during_fault
    );
    println!("capacity_fault_failure={}", capacity_fault.failure);
    println!("repair_fault_enabled={}", repair_fault.enabled);
    println!("repair_fault_passed={}", repair_fault.passed);
    println!(
        "repair_fault_requested_removed_chunks={}",
        repair_fault.requested_removed_chunks
    );
    println!(
        "repair_fault_removed_chunks={}",
        repair_fault.removed_chunks
    );
    println!(
        "repair_fault_audit_degraded_chunks={}",
        repair_fault.audit_degraded_chunks
    );
    println!(
        "repair_fault_audit_missing_replicas={}",
        repair_fault.audit_missing_replicas
    );
    println!(
        "repair_fault_repaired_chunks={}",
        repair_fault.repaired_chunks
    );
    println!(
        "repair_fault_repaired_replicas={}",
        repair_fault.repaired_replicas
    );
    println!(
        "repair_fault_unrepaired_chunks={}",
        repair_fault.unrepaired_chunks
    );
    println!(
        "repair_fault_failed_repairs={}",
        repair_fault.failed_repairs
    );
    println!(
        "repair_fault_verified_reads={}",
        repair_fault.verified_reads
    );
    println!(
        "repair_fault_verified_bytes={}",
        repair_fault.verified_bytes
    );
    println!("repair_fault_failure={}", repair_fault.failure);
    println!("fault_suite_enabled_probes={}", fault_suite.enabled_probes);
    println!("fault_suite_passed_probes={}", fault_suite.passed_probes);
    println!("fault_suite_failed_probes={}", fault_suite.failed_probes);
    println!(
        "fault_suite_required_probes={}",
        fault_suite.required_probes
    );
    println!(
        "fault_suite_coverage_passed={}",
        fault_suite.coverage_passed
    );
    println!(
        "fault_suite_coverage_status={}",
        fault_suite.coverage_status
    );
    println!("fault_suite_verified_reads={}", fault_suite.verified_reads);
    println!("fault_suite_verified_bytes={}", fault_suite.verified_bytes);
    println!(
        "fault_suite_fault_write_objects={}",
        fault_suite.fault_write_objects
    );
    println!(
        "fault_suite_fault_write_bytes={}",
        fault_suite.fault_write_bytes
    );
    println!(
        "fault_suite_affected_replica_controls={}",
        fault_suite.affected_replica_controls
    );
    println!("fault_suite_removed_chunks={}", fault_suite.removed_chunks);
    println!(
        "fault_suite_repaired_replicas={}",
        fault_suite.repaired_replicas
    );
    println!(
        "fault_suite_failure_summary={}",
        fault_suite.failure_summary
    );
    println!(
        "performance_slo_configured_checks={}",
        performance_slo.configured_checks
    );
    println!(
        "performance_slo_passed_checks={}",
        performance_slo.passed_checks
    );
    println!(
        "performance_slo_failed_checks={}",
        performance_slo.failed_checks
    );
    println!("performance_slo_passed={}", performance_slo.passed);
    println!("performance_slo_status={}", performance_slo.status);
    println!(
        "performance_slo_failure_summary={}",
        performance_slo.failure_summary
    );
    println!(
        "performance_slo_recovery_snapshot_file_bytes={}",
        performance_slo.recovery_snapshot_file_bytes
    );
    println!(
        "performance_slo_max_recovery_snapshot_file_bytes={}",
        performance_slo.max_recovery_snapshot_file_bytes
    );
    println!("mixed_seconds={}", mixed_phase.seconds);
    println!(
        "mixed_ops={}",
        mixed_phase.put_ops
            + mixed_phase.get_ops
            + mixed_phase.overwrite_ops
            + mixed_phase.delete_ops
    );
    println!("mixed_bytes={}", mixed_phase.bytes);
    println!("mixed_max_inflight={}", mixed_phase.max_inflight);
    println!("mixed_ops_per_sec={}", mixed_qps(&mixed_phase));
    println!("mixed_mib_per_sec={}", mixed_mib_per_second(&mixed_phase));
    println!("mixed_avg_ms={}", mixed_phase.avg_ms);
    println!("mixed_p50_ms={}", mixed_phase.p50_ms);
    println!("mixed_p95_ms={}", mixed_phase.p95_ms);
    println!("mixed_p99_ms={}", mixed_phase.p99_ms);
    println!("mixed_p999_ms={}", mixed_phase.p999_ms);
    println!("mixed_put_ops={}", mixed_phase.put_ops);
    println!("mixed_get_ops={}", mixed_phase.get_ops);
    println!("mixed_overwrite_ops={}", mixed_phase.overwrite_ops);
    println!("mixed_delete_ops={}", mixed_phase.delete_ops);
    println!("live_bytes={}", gc.live_bytes);
    println!("stale_bytes={}", gc.stale_bytes);
    println!("discarded_bytes={}", gc.discarded_bytes);
    println!(
        "logical_write_amplification_ppm={}",
        gc.logical_write_amplification_ppm
    );
    println!("space_amplification_ppm={}", gc.space_amplification_ppm);
    for (key, value) in store.lock().unwrap().footprint_audit() {
        println!("audit.{key}={value}");
    }
    println!("discard_candidate_bytes={}", gc.discard_candidate_bytes);
    println!(
        "pending_discard_range_count={}",
        gc.pending_discard_range_count
    );
    println!(
        "merged_discard_range_count={}",
        gc.merged_discard_range_count
    );
    println!(
        "compaction_candidate_segments={}",
        gc.compaction_candidate_count
    );
    println!("segment_count={}", readiness.chunk_count);
    println!("chunk_count={}", readiness.page_count);
    println!(
        "chunk_metadata_journal_fsync_every_records={}",
        chunk_metadata_journal.fsync_every_records
    );
    println!(
        "chunk_metadata_journal_fsync_every_bytes={}",
        chunk_metadata_journal.fsync_every_bytes
    );
    println!(
        "chunk_metadata_journal_fsync_max_unsynced_ms={}",
        chunk_metadata_journal.fsync_max_unsynced_ms
    );
    println!(
        "chunk_metadata_journal_pending_fsync_records={}",
        chunk_metadata_journal.pending_fsync_records
    );
    println!(
        "chunk_metadata_journal_pending_fsync_bytes={}",
        chunk_metadata_journal.pending_fsync_bytes
    );
    println!(
        "chunk_metadata_journal_oldest_unsynced_ms={}",
        chunk_metadata_journal.oldest_unsynced_ms
    );
    println!(
        "chunk_metadata_journal_record_pressure_ppm={}",
        chunk_metadata_journal.record_pressure_ppm
    );
    println!(
        "chunk_metadata_journal_byte_pressure_ppm={}",
        chunk_metadata_journal.byte_pressure_ppm
    );
    println!(
        "chunk_metadata_journal_age_pressure_ppm={}",
        chunk_metadata_journal.age_pressure_ppm
    );
    println!("readiness_ready={}", readiness.ready);
    println!("readiness_reason={}", readiness.reason);
    println!("metadata_commit_index={}", readiness.metadata_commit_index);
    println!(
        "metadata_state_fingerprint={}",
        readiness.metadata_state_fingerprint
    );
    println!(
        "root_state_fingerprint={}",
        readiness.root_state_fingerprint
    );
    println!(
        "block_state_fingerprint={}",
        readiness.page_state_fingerprint
    );
    println!(
        "block_state_fingerprint_format={}",
        readiness.page_state_fingerprint_format
    );
    println!(
        "block_state_fingerprint_bytes={}",
        readiness.page_state_fingerprint_bytes
    );
    println!(
        "chunk_state_fingerprint={}",
        readiness.chunk_state_fingerprint
    );
    println!(
        "chunk_state_fingerprint_format={}",
        readiness.chunk_state_fingerprint_format
    );
    println!(
        "chunk_state_fingerprint_bytes={}",
        readiness.chunk_state_fingerprint_bytes
    );
    println!(
        "distributed_readiness_checksum={}",
        readiness.distributed_readiness_checksum
    );
    println!(
        "control_checksum_format={}",
        readiness.control_checksum_format
    );
    println!("control_checksum={}", readiness.control_checksum);
    println!(
        "configured_control_plane_mode={}",
        configured_control_plane_mode
    );
    println!(
        "effective_control_plane_mode={}",
        readiness.control_plane_mode
    );
    println!("rootserver_required={}", readiness.rootserver_required);
    println!("root_read_ready={}", readiness.root_read_ready);
    println!(
        "root_metadata_read_ready={}",
        readiness.root_metadata_read_ready
    );
    println!(
        "root_metadata_write_ready={}",
        readiness.root_metadata_write_ready
    );
    println!(
        "root_blockserver_read_ready={}",
        readiness.root_pageserver_read_ready
    );
    println!(
        "root_blockserver_write_ready={}",
        readiness.root_pageserver_write_ready
    );
    println!(
        "root_chunkserver_read_ready={}",
        readiness.root_replicaserver_read_ready
    );
    println!(
        "root_chunkserver_write_ready={}",
        readiness.root_replicaserver_write_ready
    );
    println!(
        "root_readiness_gate_read_ready={}",
        readiness.root_readiness_gate_read_ready
    );
    println!(
        "root_readiness_gate_write_ready={}",
        readiness.root_readiness_gate_write_ready
    );
    println!("root_service_health={}", readiness.root_service_health);
    println!(
        "root_service_health_reason_count={}",
        readiness.root_service_health_reason_count
    );
    println!(
        "root_service_health_reasons={}",
        readiness.root_service_health_reasons
    );
    println!(
        "root_service_fault_tolerant={}",
        readiness.root_service_fault_tolerant
    );
    println!(
        "root_snapshot_fault_count={}",
        readiness.root_snapshot_fault_count
    );
    println!(
        "root_blockserver_health_ok_count={}",
        readiness.root_pageserver_health_ok_count
    );
    println!(
        "root_blockserver_health_degraded_count={}",
        readiness.root_pageserver_health_degraded_count
    );
    println!(
        "root_blockserver_health_unavailable_count={}",
        readiness.root_pageserver_health_unavailable_count
    );
    println!(
        "root_blockserver_health_unknown_count={}",
        readiness.root_pageserver_health_unknown_count
    );
    println!(
        "root_blockserver_not_fault_tolerant_count={}",
        readiness.root_pageserver_not_fault_tolerant_count
    );
    println!(
        "root_blockserver_total_health_reason_count={}",
        readiness.root_pageserver_total_health_reason_count
    );
    println!(
        "root_blockserver_total_write_quorum={}",
        readiness.root_pageserver_total_write_quorum
    );
    println!(
        "root_blockserver_total_read_quorum={}",
        readiness.root_pageserver_total_read_quorum
    );
    println!(
        "root_blockserver_total_write_quorum_deficit={}",
        readiness.root_pageserver_total_write_quorum_deficit
    );
    println!(
        "root_blockserver_total_read_quorum_deficit={}",
        readiness.root_pageserver_total_read_quorum_deficit
    );
    println!(
        "root_blockserver_total_write_quorum_margin={}",
        readiness.root_pageserver_total_write_quorum_margin
    );
    println!(
        "root_blockserver_total_read_quorum_margin={}",
        readiness.root_pageserver_total_read_quorum_margin
    );
    println!(
        "root_blockserver_first_health_reasons={}",
        readiness.root_pageserver_first_health_reasons
    );
    println!(
        "root_active_chunkserver_count={}",
        readiness.root_active_replicaserver_count
    );
    println!(
        "root_writable_chunkserver_count={}",
        readiness.root_writable_replicaserver_count
    );
    println!(
        "root_placement_eligible_chunkserver_count={}",
        readiness.root_placement_eligible_replicaserver_count
    );
    println!(
        "root_placement_ineligible_chunkserver_count={}",
        readiness.root_placement_ineligible_replicaserver_count
    );
    println!(
        "root_draining_chunkserver_count={}",
        readiness.root_draining_replicaserver_count
    );
    println!(
        "root_capacity_floor_blocked_chunkserver_count={}",
        readiness.root_capacity_floor_blocked_replicaserver_count
    );
    println!(
        "root_frozen_chunkserver_count={}",
        readiness.root_frozen_replicaserver_count
    );
    println!(
        "root_read_only_chunkserver_count={}",
        readiness.root_read_only_replicaserver_count
    );
    println!(
        "root_raft_unwritable_chunkserver_count={}",
        readiness.root_raft_unwritable_replicaserver_count
    );
    println!(
        "root_chunkserver_total_reported_free_bytes={}",
        readiness.root_replicaserver_total_reported_free_bytes
    );
    println!(
        "root_chunkserver_min_free_bytes={}",
        readiness.root_replicaserver_min_free_bytes
    );
    println!(
        "root_chunkserver_control_proof_count={}",
        readiness.root_replicaserver_control_proof_count
    );
    println!(
        "root_chunkserver_control_read_ready_count={}",
        readiness.root_replicaserver_control_read_ready_count
    );
    println!(
        "root_chunkserver_control_write_ready_count={}",
        readiness.root_replicaserver_control_write_ready_count
    );
    println!(
        "root_chunkserver_control_proof_checksum={}",
        readiness.root_replicaserver_control_proof_checksum
    );
    println!(
        "root_chunkserver_control_min_raft_commit_index={}",
        readiness.root_replicaserver_control_min_raft_commit_index
    );
    println!(
        "root_chunkserver_control_max_raft_commit_index={}",
        readiness.root_replicaserver_control_max_raft_commit_index
    );
    println!(
        "root_chunkserver_coverage_status={}",
        readiness.root_replicaserver_coverage_status
    );
    println!(
        "root_chunkserver_health_ok_count={}",
        readiness.root_replicaserver_health_ok_count
    );
    println!(
        "root_chunkserver_health_degraded_count={}",
        readiness.root_replicaserver_health_degraded_count
    );
    println!(
        "root_chunkserver_health_unavailable_count={}",
        readiness.root_replicaserver_health_unavailable_count
    );
    println!(
        "root_chunkserver_health_unknown_count={}",
        readiness.root_replicaserver_health_unknown_count
    );
    println!(
        "root_chunkserver_not_fault_tolerant_count={}",
        readiness.root_replicaserver_not_fault_tolerant_count
    );
    println!(
        "root_chunkserver_total_health_reason_count={}",
        readiness.root_replicaserver_total_health_reason_count
    );
    println!(
        "root_chunkserver_first_health_reasons={}",
        readiness.root_replicaserver_first_health_reasons
    );
    println!(
        "metadata_coordinator_backend={}",
        readiness.metadata_coordinator_backend
    );
    println!(
        "metadata_coordinator_preferred_backend={}",
        readiness.metadata_coordinator_preferred_backend
    );
    println!(
        "metadata_coordinator_production_ready={}",
        readiness.metadata_coordinator_production_ready
    );
    println!(
        "metadata_coordinator_external_dependency={}",
        readiness.metadata_coordinator_external_dependency
    );
    println!("storage_replica_count={}", readiness.storage_replica_count);
    println!(
        "storage_active_replicas={}",
        readiness.storage_active_replicas
    );
    println!(
        "storage_readable_replicas={}",
        readiness.storage_readable_replicas
    );
    println!(
        "storage_writable_replicas={}",
        readiness.storage_writable_replicas
    );
    println!(
        "storage_quarantined_replicas={}",
        readiness.storage_quarantined_replicas
    );
    println!(
        "storage_frozen_replicas={}",
        readiness.storage_frozen_replicas
    );
    println!(
        "storage_draining_replicas={}",
        readiness.storage_draining_replicas
    );
    println!(
        "storage_capacity_pressure_replicas={}",
        readiness.storage_capacity_pressure_replicas
    );
    println!(
        "storage_trim_flow_limited_replicas={}",
        readiness.storage_trim_flow_limited_replicas
    );
    println!(
        "storage_trim_flow_available_ops={}",
        readiness.storage_trim_flow_available_ops
    );
    println!(
        "storage_trim_flow_throttled_ops={}",
        readiness.storage_trim_flow_throttled_ops
    );
    println!(
        "storage_total_capacity_bytes={}",
        readiness.storage_total_capacity_bytes
    );
    println!(
        "storage_total_capacity_free_bytes={}",
        readiness.storage_total_capacity_free_bytes
    );
    println!(
        "storage_total_reclaimable_bytes={}",
        readiness.storage_total_reclaimable_bytes
    );
    println!(
        "storage_total_fragmented_chunk_count={}",
        readiness.storage_total_fragmented_chunk_count
    );
    println!(
        "storage_total_fragmented_discarded_bytes={}",
        readiness.storage_total_fragmented_discarded_bytes
    );
    println!(
        "storage_total_trailing_trim_bytes={}",
        readiness.storage_total_trailing_trim_bytes
    );
    println!(
        "storage_partial_reclaim_chunks={}",
        readiness.storage_partial_reclaim_chunks
    );
    println!(
        "storage_partial_reclaim_bytes={}",
        readiness.storage_partial_reclaim_bytes
    );
    println!("chunk_active_replicas={}", readiness.page.active_replicas);
    println!(
        "chunk_readable_replicas={}",
        readiness.page.readable_replicas
    );
    println!(
        "chunk_writable_replicas={}",
        readiness.page.writable_replicas
    );
    println!(
        "root_blockserver_total_chunk_endpoint_count={}",
        readiness.root_pageserver_total_chunk_endpoint_count
    );
    println!(
        "root_blockserver_total_writable_chunk_endpoint_count={}",
        readiness.root_pageserver_total_writable_chunk_endpoint_count
    );
    println!(
        "root_blockserver_total_healthy_chunk_endpoint_count={}",
        readiness.root_pageserver_total_healthy_chunk_endpoint_count
    );
    println!(
        "root_blockserver_total_raft_writable_chunk_endpoint_count={}",
        readiness.root_pageserver_total_raft_writable_chunk_endpoint_count
    );
    println!(
        "root_blockserver_total_raft_readable_chunk_endpoint_count={}",
        readiness.root_pageserver_total_raft_readable_chunk_endpoint_count
    );
    println!(
        "metaserver_route_cache_lookup_count={}",
        readiness.metaserver_route_cache_lookup_count
    );
    println!(
        "metaserver_route_cache_hits={}",
        readiness.metaserver_route_cache_hits
    );
    println!(
        "metaserver_route_cache_misses={}",
        readiness.metaserver_route_cache_misses
    );
    println!(
        "metaserver_route_cache_hit_ratio_ppm={}",
        readiness.metaserver_route_cache_hit_ratio_ppm
    );
    println!(
        "metaserver_route_cache_miss_ratio_ppm={}",
        readiness.metaserver_route_cache_miss_ratio_ppm
    );
    println!(
        "metaserver_route_cache_remaining_capacity={}",
        readiness.metaserver_route_cache_remaining_capacity
    );
    println!(
        "metaserver_route_cache_entries={}",
        readiness.metaserver_route_cache_entries
    );
    println!(
        "metaserver_route_cache_capacity={}",
        readiness.metaserver_route_cache_capacity
    );
    println!(
        "metaserver_route_cache_ttl_ms={}",
        readiness.metaserver_route_cache_ttl_ms
    );
    println!(
        "metaserver_route_cache_capacity_evictions={}",
        readiness.metaserver_route_cache_capacity_evictions
    );
    println!(
        "metaserver_route_cache_pressure={}",
        readiness.metaserver_route_cache_pressure
    );
    println!(
        "metaserver_route_cache_fresh_entries={}",
        readiness.metaserver_route_cache_fresh_entries
    );
    println!(
        "metaserver_route_cache_stale_entries={}",
        readiness.metaserver_route_cache_stale_entries
    );
    println!(
        "metaserver_route_cache_expired_entries={}",
        readiness.metaserver_route_cache_expired_entries
    );
    println!(
        "metaserver_route_cache_stale_topology_entries={}",
        readiness.metaserver_route_cache_stale_topology_entries
    );
    println!(
        "metaserver_route_cache_stale_session_entries={}",
        readiness.metaserver_route_cache_stale_session_entries
    );
    println!(
        "metaserver_route_cache_fresh={}",
        readiness.metaserver_route_cache_fresh
    );
    println!(
        "storage_block_read_parallelism={}",
        readiness.storage_page_read_parallelism
    );
    println!(
        "storage_block_write_parallelism={}",
        readiness.storage_page_write_parallelism
    );
    println!(
        "storage_block_batch_read_parallelism={}",
        readiness.storage_page_batch_read_parallelism
    );
    println!(
        "storage_block_batch_write_parallelism={}",
        readiness.storage_page_batch_write_parallelism
    );
    println!(
        "storage_block_batch_discard_parallelism={}",
        readiness.storage_page_batch_discard_parallelism
    );
    println!(
        "storage_block_repair_parallelism={}",
        readiness.storage_page_repair_parallelism
    );
    println!(
        "storage_block_chunk_fanout_parallelism={}",
        readiness.storage_page_chunk_fanout_parallelism
    );
    println!(
        "storage_effective_chunk_fanout_parallelism={}",
        readiness.storage_effective_chunk_fanout_parallelism
    );
    println!(
        "storage_local_replica_store_parallelism={}",
        readiness.storage_local_replica_store_parallelism
    );
    println!(
        "storage_effective_local_replica_store_parallelism={}",
        readiness.storage_effective_local_replica_store_parallelism
    );
    println!(
        "storage_physical_write_bytes={}",
        readiness.storage_physical_write_bytes
    );
    println!(
        "storage_physical_discard_bytes={}",
        readiness.storage_physical_discard_bytes
    );
    println!(
        "storage_physical_read_count={}",
        readiness.storage_physical_read_count
    );
    println!(
        "storage_coalesced_read_count={}",
        readiness.storage_coalesced_read_count
    );
    println!(
        "storage_physical_write_count={}",
        readiness.storage_physical_write_count
    );
    println!(
        "storage_physical_discard_ranges={}",
        readiness.storage_physical_discard_ranges
    );
    let storage_completed_events = readiness
        .storage_physical_read_count
        .saturating_add(readiness.storage_physical_write_count)
        .saturating_add(readiness.storage_physical_discard_ranges);
    let storage_completed_bytes = readiness
        .storage_physical_write_bytes
        .saturating_add(readiness.storage_physical_discard_bytes);
    let storage_completion_ratio_ppm = ratio_ppm(
        storage_completed_events,
        storage_completed_events.saturating_add(readiness.storage_fault_count()),
    );
    println!("storage_completed_events={}", storage_completed_events);
    println!("storage_completed_bytes={}", storage_completed_bytes);
    println!(
        "storage_completion_ratio_ppm={}",
        storage_completion_ratio_ppm
    );
    println!(
        "storage_allocation_receipt_count={}",
        readiness.storage_allocation_receipt_count
    );
    println!(
        "storage_batch_allocation_receipt_count={}",
        readiness.storage_batch_allocation_receipt_count
    );
    println!(
        "storage_release_allocation_receipt_count={}",
        readiness.storage_release_allocation_receipt_count
    );
    println!(
        "storage_write_receipt_count={}",
        readiness.storage_write_receipt_count
    );
    println!(
        "storage_discard_receipt_count={}",
        readiness.storage_discard_receipt_count
    );
    println!(
        "storage_batch_write_receipt_count={}",
        readiness.storage_batch_write_receipt_count
    );
    println!(
        "storage_batch_discard_receipt_count={}",
        readiness.storage_batch_discard_receipt_count
    );
    println!(
        "batch_write_input_ranges={}",
        readiness.batch_write_input_ranges
    );
    println!(
        "batch_write_chunk_ranges={}",
        readiness.batch_write_chunk_ranges
    );
    println!("batch_write_failures={}", readiness.batch_write_failures);
    println!(
        "last_batch_write_failure_status={}",
        readiness.last_batch_write_failure_status
    );
    println!(
        "chunk_batch_write_requests={}",
        readiness.chunk_batch_write_requests
    );
    println!(
        "chunk_batch_write_ranges={}",
        readiness.chunk_batch_write_ranges
    );
    println!(
        "chunk_batch_write_fallbacks={}",
        readiness.chunk_batch_write_fallbacks
    );
    println!(
        "fresh_chunk_batch_write_requests={}",
        readiness.fresh_chunk_batch_write_requests
    );
    println!(
        "fresh_chunk_batch_write_ranges={}",
        readiness.fresh_chunk_batch_write_ranges
    );
    println!(
        "fresh_chunk_batch_write_fallbacks={}",
        readiness.fresh_chunk_batch_write_fallbacks
    );
    println!(
        "batch_read_input_ranges={}",
        readiness.batch_read_input_ranges
    );
    println!(
        "batch_read_chunk_ranges={}",
        readiness.batch_read_chunk_ranges
    );
    println!("batch_read_failures={}", readiness.batch_read_failures);
    println!(
        "last_batch_read_failure_status={}",
        readiness.last_batch_read_failure_status
    );
    println!(
        "primary_batch_read_requests={}",
        readiness.primary_batch_read_requests
    );
    println!(
        "primary_batch_read_chunk_ranges={}",
        readiness.primary_batch_read_chunk_ranges
    );
    println!(
        "primary_batch_read_fallbacks={}",
        readiness.primary_batch_read_fallbacks
    );
    println!(
        "primary_batch_read_repair_probe_limit={}",
        readiness.primary_batch_read_repair_probe_limit
    );
    println!(
        "batch_discard_input_ranges={}",
        readiness.batch_discard_input_ranges
    );
    println!(
        "batch_discard_merged_ranges={}",
        readiness.batch_discard_merged_ranges
    );
    println!(
        "batch_discard_chunk_ranges={}",
        readiness.batch_discard_chunk_ranges
    );
    println!(
        "batch_discard_failures={}",
        readiness.batch_discard_failures
    );
    println!(
        "last_batch_discard_failure_status={}",
        readiness.last_batch_discard_failure_status
    );
    println!(
        "chunk_batch_discard_requests={}",
        readiness.chunk_batch_discard_requests
    );
    println!(
        "chunk_batch_discard_ranges={}",
        readiness.chunk_batch_discard_ranges
    );
    println!(
        "chunk_batch_discard_fallbacks={}",
        readiness.chunk_batch_discard_fallbacks
    );
    println!(
        "discard_admission_requests={}",
        readiness.discard_admission_requests
    );
    println!(
        "discard_admission_ranges={}",
        readiness.discard_admission_ranges
    );
    println!(
        "chunk_reclaim_requests={}",
        readiness.chunk_reclaim_requests
    );
    println!(
        "chunk_reclaim_rewritten_chunks={}",
        readiness.chunk_reclaim_rewritten_chunks
    );
    println!(
        "chunk_reclaim_failures={}",
        readiness.chunk_reclaim_failures
    );
    println!(
        "last_chunk_reclaim_failure_status={}",
        readiness.last_chunk_reclaim_failure_status
    );
    println!(
        "storage_read_cache_bytes={}",
        readiness.storage_read_cache_bytes
    );
    println!(
        "storage_read_cache_entries={}",
        readiness.storage_read_cache_entries
    );
    println!(
        "storage_read_cache_hits={}",
        readiness.storage_read_cache_hits
    );
    println!(
        "storage_read_cache_misses={}",
        readiness.storage_read_cache_misses
    );
    println!(
        "storage_read_cache_uncacheable_read_pages={}",
        readiness.storage_read_cache_uncacheable_read_pages
    );
    println!(
        "storage_read_cache_uncacheable_read_bytes={}",
        readiness.storage_read_cache_uncacheable_read_bytes
    );
    let storage_read_cache_lookup_count = readiness
        .storage_read_cache_hits
        .saturating_add(readiness.storage_read_cache_misses);
    println!("storage_read_cache_lookup_count={storage_read_cache_lookup_count}");
    println!(
        "storage_read_cache_hit_ratio_ppm={}",
        ratio_ppm(
            readiness.storage_read_cache_hits,
            storage_read_cache_lookup_count
        )
    );
    println!(
        "storage_read_cache_evictions={}",
        readiness.storage_read_cache_evictions
    );
    println!(
        "storage_local_replica_read_repairs={}",
        readiness.storage_local_replica_read_repairs
    );
    println!(
        "storage_local_replica_read_repair_failures={}",
        readiness.storage_local_replica_read_repair_failures
    );
    println!(
        "storage_last_local_replica_read_repair_failure_status={}",
        failure_status(readiness.storage_local_replica_read_repair_failures)
    );
    println!(
        "storage_cumulative_repair_checked_chunks={}",
        readiness.storage_cumulative_repair_checked_chunks
    );
    println!(
        "storage_cumulative_repair_degraded_chunks={}",
        readiness.storage_cumulative_repair_degraded_chunks
    );
    println!(
        "storage_cumulative_repaired_replicas={}",
        readiness.storage_cumulative_repaired_replicas
    );
    println!(
        "storage_cumulative_failed_repairs={}",
        readiness.storage_cumulative_failed_repairs
    );
    println!(
        "storage_compressed_chunk_count={}",
        readiness.storage_compressed_chunk_count
    );
    println!(
        "storage_compression_saved_bytes={}",
        readiness.storage_compression_saved_bytes
    );
    println!(
        "storage_ec_reconstructed_reads={}",
        readiness.storage_ec_reconstructed_reads
    );
    println!(
        "storage_ec_reconstruction_failures={}",
        readiness.storage_ec_reconstruction_failures
    );
    println!(
        "storage_last_ec_reconstruction_failure_status={}",
        failure_status(readiness.storage_ec_reconstruction_failures)
    );
    println!(
        "storage_read_cache_pressure={}",
        readiness.storage_read_cache_pressure
    );
    println!(
        "storage_read_cache_corrupt_pages={}",
        readiness.storage_read_cache_corrupt_pages
    );
    println!(
        "storage_read_cache_corrupt_bytes={}",
        readiness.storage_read_cache_corrupt_bytes
    );
    println!(
        "storage_capacity_rejected_writes={}",
        readiness.storage_capacity_rejected_writes
    );
    println!(
        "storage_last_capacity_rejection_status={}",
        failure_status(readiness.storage_capacity_rejected_writes)
    );
    println!(
        "storage_service_mode_rejected_mutations={}",
        readiness.storage_service_mode_rejected_mutations
    );
    println!(
        "storage_last_mutation_admission_rejection_status={}",
        failure_status(readiness.storage_service_mode_rejected_mutations)
    );
    println!(
        "storage_partial_reclaim_failures={}",
        readiness.storage_partial_reclaim_failures
    );
    println!(
        "storage_last_partial_reclaim_failure_status={}",
        readiness.storage_last_partial_reclaim_failure_status
    );
    println!("storage_fault_count={}", readiness.storage_fault_count());
    println!("storage_fault_status={}", readiness.storage_fault_status());
    let storage_fault_reason = readiness.storage_fault_reason();
    println!("storage_fault_reason={storage_fault_reason}");
    println!(
        "storage_fault_reason_count={}",
        readiness.storage_fault_reason_count()
    );
    println!("snapshot_bytes={snapshot_bytes}");
    println!("snapshot_bytes_mode=estimated_compact_metadata_plus_payload");
    println!("snapshot_compact_metadata_bytes={snapshot_compact_metadata_bytes}");
    println!("snapshot_payload_bytes={snapshot_payload_bytes}");
    println!("snapshot_payload_elided_for_benchmark=true");
    println!("recovery_enabled={}", recovery.enabled);
    println!("recovery_passed={}", recovery.passed);
    println!(
        "recovery_snapshot_file_bytes={}",
        recovery.snapshot_file_bytes
    );
    println!("recovery_reopen_ms={}", recovery.reopen_ms);
    println!("recovery_verified_objects={}", recovery.verified_objects);
    println!("recovery_verified_bytes={}", recovery.verified_bytes);
    println!("recovery_sample_index={}", recovery.sample_index);
    println!("recovery_failure={}", recovery.failure);
    let (benchmark_fault_tolerance_passed, benchmark_fault_tolerance_reason) =
        benchmark_fault_tolerance(
            &readiness,
            &recovery,
            &fault_injection,
            &quarantine_fault,
            &drain_fault,
            &capacity_fault,
            &repair_fault,
            cfg.required_fault_probes,
            cfg.write_quorum,
        );
    println!(
        "benchmark_fault_tolerance_passed={}",
        benchmark_fault_tolerance_passed
    );
    println!(
        "benchmark_fault_tolerance_reason={}",
        benchmark_fault_tolerance_reason
    );
    println!(
        "benchmark_fault_tolerance_reason_count={}",
        reason_count(&benchmark_fault_tolerance_reason)
    );
    let fail_for_fault_tolerance = cfg.fail_on_fault_tolerance && !benchmark_fault_tolerance_passed;
    let fail_for_performance_slo = cfg.fail_on_performance_slo && !performance_slo.passed;
    let benchmark_exit_status = match (fail_for_fault_tolerance, fail_for_performance_slo) {
        (true, true) => "fault_tolerance_and_performance_slo_failed",
        (true, false) => "fault_tolerance_failed",
        (false, true) => "performance_slo_failed",
        (false, false) => "ok",
    };
    println!("benchmark_exit_status={}", benchmark_exit_status);
    println!(
        "errors={}",
        if fail_for_fault_tolerance || fail_for_performance_slo {
            1
        } else {
            0
        }
    );
    if let Ok(path) = env::var("MATRIX_OBJECTSTORE_RUST_SCALE_JSON_OUT") {
        if !path.is_empty() {
            let report = render_scale_json_report(
                &cfg,
                &put_phase,
                &range_phase,
                &overwrite_phase,
                &delete_phase,
                &mixed_phase,
                &gc,
                &readiness,
                &configured_control_plane_mode,
                snapshot_bytes,
                snapshot_compact_metadata_bytes,
                snapshot_payload_bytes,
                &lock_stats,
                benchmark_wall_ns,
                store_lock_wait_wall_ppm,
                &recovery,
                &fault_injection,
                &quarantine_fault,
                &drain_fault,
                &capacity_fault,
                &repair_fault,
            );
            std::fs::write(&path, report).map_err(|err| {
                ObjectError::InvalidArgument(format!(
                    "failed to write scale benchmark JSON report {path}: {err}"
                ))
            })?;
            println!("json_report_path={path}");
        }
    }
    if let Ok(path) = env::var("MATRIX_OBJECTSTORE_RUST_SCALE_EVENT_LOG_OUT") {
        if !path.is_empty() {
            let event_log = render_scale_event_log(
                &cfg,
                &put_phase,
                &range_phase,
                &overwrite_phase,
                &delete_phase,
                &mixed_phase,
                &gc,
                &readiness,
                &configured_control_plane_mode,
                &benchmark_mode,
                snapshot_bytes,
                snapshot_compact_metadata_bytes,
                snapshot_payload_bytes,
                &lock_stats,
                benchmark_wall_ns,
                store_lock_wait_wall_ppm,
                &recovery,
                &fault_injection,
                &quarantine_fault,
                &drain_fault,
                &capacity_fault,
                &repair_fault,
                benchmark_fault_tolerance_passed,
                &benchmark_fault_tolerance_reason,
            );
            std::fs::write(&path, event_log).map_err(|err| {
                ObjectError::InvalidArgument(format!(
                    "failed to write scale benchmark event log {path}: {err}"
                ))
            })?;
            println!("event_log_path={path}");
        }
    }
    if fail_for_fault_tolerance || fail_for_performance_slo {
        let mut failures = Vec::new();
        if fail_for_fault_tolerance {
            failures.push(format!(
                "fault tolerance failed: {benchmark_fault_tolerance_reason}"
            ));
        }
        if fail_for_performance_slo {
            failures.push(format!(
                "performance SLO failed: {}",
                performance_slo.failure_summary
            ));
        }
        return Err(ObjectError::PreconditionFailed(format!(
            "scale benchmark {}",
            failures.join("; ")
        )));
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err:?}");
        process::exit(1);
    }
}
