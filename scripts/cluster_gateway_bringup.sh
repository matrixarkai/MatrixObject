#!/usr/bin/env bash
# Bring up a local MatrixObject cluster as separate processes, with the S3 gateway in front.
#
#   metadata server(s)  ->  three chunk servers (one standalone replicated group)  ->  gateway
#
# usage: cluster_gateway_bringup.sh up|down|status
#
# environment:
#   BIN        directory holding metaserver, chunkserver, object_server and the star_catalog
#              example (default $CARGO_TARGET_DIR/release, examples under release/examples)
#   RUN        run directory for state, journals, logs and pids   (default /root/clustergw_run)
#   BASE_PORT  first of a block of 24 loopback ports               (default 19400)
#   META       single | raft3 : one metadata server, or a three-voter quorum (default single)
#   BUCKET     bucket the gateway reports as ready                 (default temporalstore)
#   THREADS    gateway accept threads                              (default 8)
#   GATEWAY_ENV   extra NAME=VALUE pairs for the gateway, space separated
#   CHECKPOINT_BYTES  MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES for raft3 voters (default unset)
#   CHUNK_HEARTBEAT_MS chunk server heartbeat interval (default 3000, the product default; a shorter value
#                     is only for quick tests and needs HEARTBEAT_TTL_MS of at least three intervals)
#   HEARTBEAT_TTL_MS  how long a chunk server stays healthy after its last heartbeat (default 15000, five
#                     of the default 3000 ms intervals: a TTL under two intervals expires a node on one
#                     late heartbeat and placement then refuses every write)
#
# Only processes this script started are signalled: their pids are recorded in $RUN/pids and
# `down` reads that file. Nothing is matched by name.
set -u

BIN=${BIN:-${CARGO_TARGET_DIR:-/root/clustergw_target}/release}
RUN=${RUN:-/root/clustergw_run}
BASE=${BASE_PORT:-19400}
META=${META:-single}
BUCKET=${BUCKET:-temporalstore}
THREADS=${THREADS:-8}
CAPACITY=${CAPACITY_BYTES:-4294967296}
MAX_PAGES=${MAX_PAGES:-65536}
PIDS="$RUN/pids"

fail() { echo "STOP: $*" >&2; exit 1; }

# a chunk server that is not heard from for HEARTBEAT_TTL_MS is placed out of every group: with a TTL under
# three heartbeat intervals one heartbeat skipped under load takes a node out until the next one lands
HB_INTERVAL_MS=${CHUNK_HEARTBEAT_MS:-3000}
TTL_MS=${HEARTBEAT_TTL_MS:-15000}
[ "$TTL_MS" -ge $((3 * HB_INTERVAL_MS)) ] || { echo "STOP: HEARTBEAT_TTL_MS=$TTL_MS is under three chunk heartbeat intervals ($HB_INTERVAL_MS ms)" >&2; exit 1; }

port_free() { ! ss -lnt 2>/dev/null | awk '{print $4}' | grep -q ":$1\$"; }

# wait for "listening on" in a log, up to N seconds
wait_line() {
  local log=$1 secs=$2 i
  for ((i = 0; i < secs * 10; i++)); do
    grep -q "listening on" "$log" 2>/dev/null && return 0
    sleep 0.1
  done
  return 1
}

spawn() { # name logfile -- env assignments and command follow
  local name=$1 log=$2; shift 2
  setsid env "$@" > "$log" 2>&1 < /dev/null &
  echo "$name $!" >> "$PIDS"
}

down() {
  [ -f "$PIDS" ] || { echo "no $PIDS: nothing started by this script"; return 0; }
  tac "$PIDS" | while read -r name pid; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
      for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
      kill -0 "$pid" 2>/dev/null && { echo "$name ($pid) ignored SIGTERM, sending SIGKILL"; kill -9 "$pid" 2>/dev/null; }
      echo "stopped $name ($pid)"
    fi
  done
  rm -f "$PIDS"
}

status() {
  [ -f "$PIDS" ] || { echo "not running"; return 1; }
  while read -r name pid; do
    if kill -0 "$pid" 2>/dev/null; then echo "$name $pid alive"; else echo "$name $pid DEAD"; fi
  done < "$PIDS"
  curl -s -m 3 "http://127.0.0.1:$((BASE + 10))/healthz" || echo "gateway: no answer"
}

up() {
  [ -f "$PIDS" ] && fail "$PIDS exists: run 'down' first (or remove it if those processes are gone)"
  for b in metaserver chunkserver object_server examples/star_catalog; do
    [ -x "$BIN/$b" ] || fail "missing $BIN/$b"
  done
  for p in $(seq "$BASE" $((BASE + 23))); do port_free "$p" || fail "port $p is in use; choose another BASE_PORT"; done
  mkdir -p "$RUN" || fail "cannot create $RUN"
  : > "$PIDS"
  trap 'echo "bring-up failed; stopping what was started"; down; exit 1' ERR
  set -E

  local META_PORTS CHUNK_PORTS CHUNK_EPS MEMBERS i
  CHUNK_PORTS=("$((BASE + 1))" "$((BASE + 2))" "$((BASE + 3))")
  CHUNK_EPS="127.0.0.1:${CHUNK_PORTS[0]},127.0.0.1:${CHUNK_PORTS[1]},127.0.0.1:${CHUNK_PORTS[2]}"
  MEMBERS="chunk-1,chunk-2,chunk-3"

  # the group catalog every metadata voter loads
  "$BIN/examples/star_catalog" "$RUN/catalog.bin" group-a "$CAPACITY" "$MAX_PAGES" "$CHUNK_EPS" "$MEMBERS" \
    || fail "catalog could not be written"

  # --- metadata
  local META_EPS=""
  if [ "$META" = single ]; then
    mkdir -p "$RUN/meta1"
    spawn meta1 "$RUN/meta1.log" \
      MATRIXOBJECT_METASERVER_ADDR="127.0.0.1:$((BASE))" \
      MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE="$RUN/catalog.bin" \
      MATRIXOBJECT_METASERVER_STATE_DIR="$RUN/meta1" \
      MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS="${HEARTBEAT_TTL_MS:-15000}" \
      "$BIN/metaserver"
    wait_line "$RUN/meta1.log" 20 || fail "metaserver did not report listening; see $RUN/meta1.log"
    META_EPS="127.0.0.1:$BASE"
  elif [ "$META" = raft3 ]; then
    # api ports BASE+4..6, peer ports BASE+7..9
    local PEERS="" a p CKPT_ENV=()
    [ -n "${CHECKPOINT_BYTES:-}" ] && CKPT_ENV=(MATRIXOBJECT_METASERVER_RAFT_CHECKPOINT_BYTES="$CHECKPOINT_BYTES")
    for i in 1 2 3; do PEERS="$PEERS${PEERS:+,}$i@127.0.0.1:$((BASE + 6 + i))"; done
    for i in 1 2 3; do
      mkdir -p "$RUN/meta$i"
      spawn "meta$i" "$RUN/meta$i.log" \
        MATRIXOBJECT_METASERVER_ADDR="127.0.0.1:$((BASE + 3 + i))" \
        MATRIXOBJECT_METASERVER_RAFT_PEERS="$PEERS" \
        MATRIXOBJECT_METASERVER_RAFT_NODE_ID="$i" \
        MATRIXOBJECT_METASERVER_RAFT_STATE_DIR="$RUN/meta$i" \
        MATRIXOBJECT_METASERVER_RAFT_TIMEOUT_MS=3000         "${CKPT_ENV[@]}" \
        MATRIXOBJECT_METASERVER_CHUNK_GROUPS_FILE="$RUN/catalog.bin" \
        MATRIXOBJECT_METASERVER_HEARTBEAT_TTL_MS="${HEARTBEAT_TTL_MS:-15000}" \
        "$BIN/metaserver"
      META_EPS="$META_EPS${META_EPS:+,}127.0.0.1:$((BASE + 3 + i))"
    done
    for i in 1 2 3; do wait_line "$RUN/meta$i.log" 20 || fail "metaserver $i did not report listening"; done
  else
    fail "META must be single or raft3"
  fi

  # --- chunk servers: a durable journal each, heartbeating to the metadata endpoints
  for i in 1 2 3; do
    mkdir -p "$RUN/chunk$i"
    spawn "chunk$i" "$RUN/chunk$i.log" \
      MATRIXOBJECT_REPLICA_ADDR="127.0.0.1:${CHUNK_PORTS[$((i - 1))]}" \
      MATRIXOBJECT_REPLICA_JOURNAL="$RUN/chunk$i/journal.wal" \
      MATRIXOBJECT_REPLICA_LOCAL_RECOVERY=peers \
      MATRIXOBJECT_REPLICA_CAPACITY_BYTES="$CAPACITY" \
      MATRIXOBJECT_REPLICA_MAX_PAGES="$MAX_PAGES" \
      MATRIXOBJECT_CHUNK_NODE_ID="chunk-$i" \
      MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS="$META_EPS" \
      MATRIXOBJECT_CHUNK_HEARTBEAT_MS="$HB_INTERVAL_MS" \
      "$BIN/chunkserver"
  done
  for i in 1 2 3; do wait_line "$RUN/chunk$i.log" 20 || fail "chunkserver $i did not report listening; see $RUN/chunk$i.log"; done

  # --- gateway
  local GROUP_ENV=()
  [ "$META" = raft3 ] && GROUP_ENV=(MATRIXOBJECT_CLUSTER_META_GROUP=1)
  spawn gateway "$RUN/gateway.log" \
    MATRIXOBJECT_HTTP_ADDR="127.0.0.1:$((BASE + 10))" \
    MATRIXOBJECT_BUCKET="$BUCKET" \
    MATRIXOBJECT_THREADS="$THREADS" \
    MATRIXOBJECT_CLUSTER_META="$META_EPS" \
    "${GROUP_ENV[@]}"     ${GATEWAY_ENV:-} \
    "$BIN/object_server"
  wait_line "$RUN/gateway.log" 20 || fail "gateway did not report listening; see $RUN/gateway.log"

  # readiness is a request that crosses the whole stack, not a log line
  local ok=0
  for _ in $(seq 1 100); do
    code=$(curl -s -m 3 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$((BASE + 10))/healthz")
    [ "$code" = 200 ] && { ok=1; break; }
    sleep 0.2
  done
  [ "$ok" = 1 ] || fail "gateway /healthz never answered 200; see $RUN/gateway.log"
  # Verify the stored bucket before probing object writes. Placement may still
  # be waiting for the first chunk heartbeats after the HTTP listener is ready.
  local bucket_ready=0 bucket_code=000
  for _ in $(seq 1 100); do
    code=$(curl -s -m 10 -o "$RUN/bucket-create-response.txt" -w '%{http_code}' -X PUT -H 'Content-Length: 0' \
      "http://127.0.0.1:$((BASE + 10))/$BUCKET")
    bucket_code=$(curl -s -I -m 3 -o /dev/null -w '%{http_code}' \
      "http://127.0.0.1:$((BASE + 10))/$BUCKET")
    [ "$code" = 200 ] && [ "$bucket_code" = 200 ] && { bucket_ready=1; break; }
    sleep 0.3
  done
  [ "$bucket_ready" = 1 ] || fail "bucket did not become ready (create $code, head $bucket_code); see $RUN/bucket-create-response.txt"

  # the first write proves placement is online (chunk heartbeats reached the metadata plane)
  for _ in $(seq 1 100); do
    code=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -X PUT --data-binary ready \
      "http://127.0.0.1:$((BASE + 10))/$BUCKET/_bringup/ready")
    [ "$code" = 200 ] && { ok=2; break; }
    sleep 0.3
  done
  [ "$ok" = 2 ] || fail "a probe write never succeeded (last status $code); see $RUN/gateway.log and $RUN/meta1.log"
  trap - ERR
  echo "cluster up: gateway http://127.0.0.1:$((BASE + 10))  meta=$META_EPS  chunks=$CHUNK_EPS  run=$RUN"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 up|down|status" >&2; exit 2 ;;
esac
