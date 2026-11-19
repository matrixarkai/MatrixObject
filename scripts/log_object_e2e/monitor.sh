#!/usr/bin/env bash
# Samples load, swap and memory pressure every INTERVAL seconds (default 15) and labels each
# sample quiet or busy. Quiet means load1 < 12 and swap used < 2 GB. Runs until killed.
#   E2E_MONITOR_LOG   where samples go (default /root/cgm_work/e2e_monitor.log)
set -u
LOG="${E2E_MONITOR_LOG:-/root/cgm_work/e2e_monitor.log}"
INTERVAL="${INTERVAL:-15}"
mkdir -p "$(dirname "$LOG")"
while true; do
  load1=$(awk '{print $1}' /proc/loadavg)
  swap_total=$(awk '/^SwapTotal:/ {print $2}' /proc/meminfo)
  swap_free=$(awk '/^SwapFree:/ {print $2}' /proc/meminfo)
  swap_used=$((swap_total - swap_free))
  mem_avail=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  psi=$(awk '/^some/ {print $2}' /proc/pressure/memory 2>/dev/null || true)
  psi=${psi:-na}
  quiet=busy
  if awk -v l="$load1" 'BEGIN {exit !(l < 12)}' && [ "$swap_used" -lt $((2 * 1024 * 1024)) ]; then
    quiet=quiet
  fi
  echo "$(date +%Y-%m-%dT%H:%M:%S) load1=$load1 swap_used_kb=$swap_used mem_avail_kb=$mem_avail psi_mem_some_avg10=$psi state=$quiet" >> "$LOG"
  sleep "$INTERVAL"
done
