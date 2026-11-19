#!/usr/bin/env bash
# Builds once and runs the log-object end-to-end measurement and its page-path baseline.
# It does nothing unless the GO file exists: the owner of the box creates it when the box is quiet.
#   touch /root/cgm_work/e2e_go
#   bash scripts/log_object_e2e/run.sh
# Settings (environment): E2E_DIR (state, default /root/e2e_run), E2E_BYTES, E2E_APPEND_BYTES,
# E2E_INTERVAL_BYTES, E2E_MODE=closed|open, E2E_RATE_PER_S, E2E_META_RESTART_AT,
# E2E_CHUNK_RESTART_AT, E2E_BASELINE=1 (also run the baseline; default on), E2E_TARGET_DIR.
set -eu
GO_FILE="${E2E_GO_FILE:-/root/cgm_work/e2e_go}"
if [ ! -e "$GO_FILE" ]; then
  echo "run: $GO_FILE does not exist; not building, not running"
  exit 3
fi
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
export E2E_DIR="${E2E_DIR:-/root/e2e_run}"
export TMPDIR="$E2E_DIR/tmp"
export E2E_RESULTS_DIR="$E2E_DIR/results"
CARGO_TARGET_DIR="${E2E_TARGET_DIR:-$E2E_DIR/target}"
export CARGO_TARGET_DIR
mkdir -p "$TMPDIR" "$E2E_RESULTS_DIR"

bash "$HERE/preflight.sh"

stamp="$(date +%Y%m%dT%H%M%S)"
(cd "$REPO" && nice -n 19 ionice -c 3 cargo +stable build --release --all-features -j 1 --test network_metadata_consensus) \
  2>&1 | tee "$E2E_RESULTS_DIR/build_$stamp.log"
# the test binary: the build above produced it; run it by name so nothing is compiled twice
TEST_BIN="$(ls -t "$CARGO_TARGET_DIR"/release/deps/network_metadata_consensus-* 2>/dev/null | grep -v '\.d$' | head -n 1)"
if [ -z "$TEST_BIN" ]; then echo "run: test binary not found"; exit 4; fi
echo "run: test binary $TEST_BIN"

run_test() {
  # $1 = test name, remaining = environment assignments
  name="$1"; shift
  env "$@" E2E_RESULTS="$E2E_RESULTS_DIR/results_$stamp.txt" \
    nice -n 19 ionice -c 3 "$TEST_BIN" --ignored --exact "log_object_end_to_end::$name" --nocapture --test-threads=1 \
    2>&1 | tee -a "$E2E_RESULTS_DIR/run_$stamp.log"
}

run_test measure_log_object_growth_end_to_end E2E_RUN_LABEL=log
if [ "${E2E_BASELINE:-1}" = "1" ]; then
  # the page path at one MiB appends: ends at the extent limit near 4 GiB
  run_test measure_page_path_growth_baseline E2E_BASELINE_APPEND_BYTES=1048576
  # the page path at the log object's append size: shows where the same appender is refused
  run_test measure_page_path_growth_baseline E2E_BASELINE_APPEND_BYTES="${E2E_APPEND_BYTES:-65536}"
fi
python3 "$HERE/table.py" "$E2E_RESULTS_DIR/results_$stamp.txt" | tee "$E2E_RESULTS_DIR/table_$stamp.md"
echo "run: results in $E2E_RESULTS_DIR (results_$stamp.txt, table_$stamp.md)"
