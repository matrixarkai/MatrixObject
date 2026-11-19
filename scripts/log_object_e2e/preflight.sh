#!/usr/bin/env bash
# Preflight for the log-object end-to-end measurement. Reads only; starts nothing, builds nothing.
# Exit 0 when the machine can host the run, 1 with the reasons printed otherwise.
#   E2E_DIR       where the run keeps its state (default /root/e2e_run); needs >= 40 GB free
#   MIN_MEM_GB    MemAvailable floor (default 6)
#   MIN_DISK_GB   free disk floor at E2E_DIR (default 40)
set -u
E2E_DIR="${E2E_DIR:-/root/e2e_run}"
MIN_MEM_GB="${MIN_MEM_GB:-6}"
MIN_DISK_GB="${MIN_DISK_GB:-40}"
bad=0
say() { echo "preflight: $*"; }
fail() { say "FAIL $*"; bad=1; }

mem_kb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
if [ "${mem_kb:-0}" -lt $((MIN_MEM_GB * 1024 * 1024)) ]; then
  fail "MemAvailable ${mem_kb} kB is below ${MIN_MEM_GB} GB"
else
  say "ok MemAvailable ${mem_kb} kB"
fi

mkdir -p "$E2E_DIR" 2>/dev/null
disk_kb=$(df -Pk "$E2E_DIR" 2>/dev/null | awk 'NR==2 {print $4}')
if [ "${disk_kb:-0}" -lt $((MIN_DISK_GB * 1024 * 1024)) ]; then
  fail "free disk ${disk_kb:-0} kB at $E2E_DIR is below ${MIN_DISK_GB} GB"
else
  say "ok free disk ${disk_kb} kB at $E2E_DIR"
fi

# The test binds kernel-chosen loopback ports. Report whether the kernel's range can hand out the
# ports the measurement cluster of the other session uses, and whether any of them is taken now.
range=$(cat /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null || echo "32768 60999")
lo=${range% *}; hi=${range#* }
if [ "$lo" -le 23030 ] && [ "$hi" -ge 22000 ]; then
  say "warn ephemeral range $lo-$hi overlaps 22000-22099 / 23020-23030; a kernel-chosen port could land there if that cluster is not up yet"
else
  say "ok ephemeral range $lo-$hi does not overlap 22000-22099 or 23020-23030"
fi
listening=$( (ss -ltn 2>/dev/null || true) | awk 'NR>1 {n=split($4,a,":"); print a[n]}' | sort -un )
for p in $listening; do
  if { [ "$p" -ge 22000 ] && [ "$p" -le 22099 ]; } || { [ "$p" -ge 23020 ] && [ "$p" -le 23030 ]; }; then
    say "note port $p is listening (another session's cluster); this run does not use it"
  fi
done

# No scale measurement may be running: it owns the box while it runs.
if pgrep -f '/root/qps_scale/nscale' >/dev/null 2>&1; then
  fail "a process from /root/qps_scale/nscale is running"
fi
for marker in /root/qps_scale/gate /root/qps_scale/nscale.running /root/qps_scale/RUNNING; do
  if [ -e "$marker" ]; then fail "gate marker present: $marker"; fi
done
if pgrep -f 'cargo|rustc' >/dev/null 2>&1; then
  say "warn a cargo/rustc process is running now"
fi

load1=$(awk '{print $1}' /proc/loadavg)
say "info load1=$load1"
if [ "$bad" -eq 0 ]; then say "PASS"; else say "REFUSED"; fi
exit "$bad"
