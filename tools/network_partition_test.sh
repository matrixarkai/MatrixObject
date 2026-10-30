#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
set -euo pipefail
cd "$(dirname "$0")/.."
for tool in cargo python3 unshare ip iptables; do command -v "$tool" >/dev/null; done
binary="$(cargo test --release --locked --all-features --test network_chunk_consensus --no-run --message-format=json |
python3 -c '
import json, sys
paths = set()
for line in sys.stdin:
    record = json.loads(line)
    if record.get("reason") == "compiler-message":
        print(record["message"].get("rendered", ""), file=sys.stderr, end="")
    if record.get("reason") == "compiler-artifact" and record.get("target", {}).get("name") == "network_chunk_consensus" and record.get("profile", {}).get("test"):
        if record.get("executable"):
            paths.add(record["executable"])
if len(paths) != 1:
    sys.exit("expected exactly one compiled network test executable")
print(paths.pop())
')"
privileged=()
if (( EUID != 0 )); then privileged=(sudo --); fi
"${privileged[@]}" unshare --net -- bash -c '
set -euo pipefail
ip link set dev lo up
"$1" coupled_groups::live_peer_partitions_preserve_objects_and_recover --exact --ignored --nocapture --test-threads=1
"$1" coupled_groups::authenticated_live_peer_partitions_preserve_objects_and_recover --exact --ignored --nocapture --test-threads=1
exec "$1" coupled_groups::authenticated_api_and_peer_partitions_preserve_objects --exact --ignored --nocapture --test-threads=1
' bash "$binary"
