#!/usr/bin/env bash
# bench_one.sh KIND TAG OPS SHARD   (KIND = gateway | door)
# Runs the TemporalStore harness once against a FRESH target and records latency JSON plus
# the bytes the target's durable files grew by.
set -u
KIND=$1; TAG=$2; OPS=$3; SHARD=$4
W=/root/scratch_clustergw
OUT=$W/out/$TAG; rm -rf "$OUT"; mkdir -p "$OUT"
HARNESS=/root/scratch_8b90/ab/scale_harness_final
DOOR=/root/scratch_8b90/ab/door_span
WT=/root/wt_clustergw
dsz() { du -sb "$@" 2>/dev/null | awk '{s+=$1} END{print s+0}'; }

if [ "$KIND" = gateway ]; then
  export BIN=/root/clustergw_target/release RUN=/root/clustergw_run BASE_PORT=19400
  rm -rf "$RUN"
  bash $WT/scripts/cluster_gateway_bringup.sh up > "$OUT/up.log" 2>&1 || { cat "$OUT/up.log"; exit 1; }
  PORT=19410
  DUR="$RUN/chunk1 $RUN/chunk2 $RUN/chunk3"; MET="$RUN/meta1"
  cleanup() { bash $WT/scripts/cluster_gateway_bringup.sh down > /dev/null 2>&1; }
else
  PORT=19450; D=$W/door_run; rm -rf $D; mkdir -p $D
  setsid env MATRIXOBJECT_HTTP_ADDR=127.0.0.1:$PORT MATRIXOBJECT_BUCKET=temporalstore MATRIXOBJECT_THREADS=8 \
    MATRIXOBJECT_JOURNAL_PATH=$D/store.jnl $DOOR > $OUT/door.log 2>&1 < /dev/null &
  DPID=$!
  for _ in $(seq 1 100); do
    curl -s -m 2 "http://127.0.0.1:$PORT/temporalstore?list-type=2" | grep -q ListBucketResult && break; sleep 0.3
  done
  DUR="$D"; MET=""
  cleanup() { kill $DPID 2>/dev/null; wait $DPID 2>/dev/null; }
fi
trap cleanup EXIT

B0=$(dsz $DUR); M0=$(dsz $MET)
S=$(date +%s%N)
timeout 3000 $HARNESS --compare-shared-store true --shared-store-uri "matrixobject://127.0.0.1:$PORT" \
  --shared-store-ops "$OPS" --shared-store-flush-every 25 --nodes 3 --shard-id "$SHARD" \
  > $OUT/h.json 2> $OUT/h.err
RC=$?
E=$(( ($(date +%s%N)-S)/1000000 ))
B1=$(dsz $DUR); M1=$(dsz $MET)
echo "kind=$KIND tag=$TAG ops=$OPS rc=$RC wall_ms=$E data_bytes=$((B1-B0)) meta_bytes=$((M1-M0))" | tee $OUT/summary.txt
tail -3 $OUT/h.err | cut -c1-300
