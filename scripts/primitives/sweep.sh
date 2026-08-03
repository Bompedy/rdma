#!/bin/bash
# Sweep primitive benchmarks over a fixed set of (threads, clients-per-thread) pairs.
#
# Usage: ./scripts/primitives/sweep.sh [SWEEP_PAIRS] [PRIMITIVES] [NUM_OPS] [NUM_LOCKS] [ZIPF_SKEW]
#   SWEEP_PAIRS   space-separated list of threads:cpt pairs
#                 default: "1:1 1:2 1:4 1:8 1:16 2:16 4:16 8:16"
#   PRIMITIVES    space-separated list of primitive names
#                 default: "synra_tas synra_faa mu_faa"
#   NUM_OPS       total operations per run
#                 default: 100000
#   NUM_LOCKS     number of comparison locks (default: 1)
#   ZIPF_SKEW     lock-selection skew (default: 0.0)
#
# Output: results/sweep_<timestamp>/sweep.csv
#         one row per (primitive, threads, cpt) run

set -euo pipefail

SWEEP_PAIRS="${1:-1:1 1:2 1:4 1:8 1:16 2:16 4:16 8:16}"
PRIMITIVES="${2:-synra_tas synra_faa mu_faa}"
NUM_OPS="${3:-100000}"
NUM_LOCKS="${4:-1}"
ZIPF_SKEW="${5:-0.0}"

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

CLIENT_MACHINES=$(awk '
    /^\[clients\]/{inside=1; next}
    /^\[/{inside=0}
    inside && /=/ && !/^#/ {count++}
    END {print count+0}
' cluster.toml)
if [[ "$CLIENT_MACHINES" -lt 1 ]]; then
    echo "ERROR: no client machines configured" >&2
    exit 1
fi

SWEEP_ID="sweep_$(date +%Y%m%d_%H%M%S)"
SWEEP_DIR="results/$SWEEP_ID"
mkdir -p "$SWEEP_DIR"
CSV="$SWEEP_DIR/sweep.csv"
echo "primitive,total_clients,threads,cpt,locks,zipf_skew,ops,p50,p90,p99,p999,goodput" > "$CSV"

echo "=== Sweep $SWEEP_ID ==="
echo "Pairs:      $SWEEP_PAIRS"
echo "Primitives: $PRIMITIVES"
echo "Num ops:    $NUM_OPS"
echo "Locks:      $NUM_LOCKS"
echo "Zipf skew:  $ZIPF_SKEW"
echo "Client machines: $CLIENT_MACHINES"
echo "CSV:        $CSV"
echo

echo "=== Deploying once ==="
make deploy

for prim in $PRIMITIVES; do
    for pair in $SWEEP_PAIRS; do
        requested_threads="${pair%%:*}"
        requested_cpt="${pair##*:}"
        total=$((requested_threads * requested_cpt))
        threads="$requested_threads"
        cpt="$requested_cpt"
        total_worker_threads="$threads"

        if [[ "$prim" == "rdma_cas" || "$prim" == "shiftlock" ]]; then
            max_local=$(((total + CLIENT_MACHINES - 1) / CLIENT_MACHINES))
            threads=$(((max_local + 15) / 16))
            cpt=16
            total_worker_threads=0
            base_clients=$((total / CLIENT_MACHINES))
            extra_machines=$((total % CLIENT_MACHINES))
            for ((machine = 0; machine < CLIENT_MACHINES; machine++)); do
                machine_clients=$base_clients
                if ((machine < extra_machines)); then
                    machine_clients=$((machine_clients + 1))
                fi
                if ((machine_clients > 0)); then
                    total_worker_threads=$((total_worker_threads + (machine_clients + 15) / 16))
                fi
            done
        fi

        echo "=== $prim  total_clients=$total worker_threads=$total_worker_threads "\
             "transport_threads/machine=$threads ==="

        make run BENCH="PRIMITIVE=$prim NUM_THREADS=$threads CLIENTS_PER_THREAD=$cpt TOTAL_CLIENTS=$total NUM_OPS=$NUM_OPS NUM_LOCKS=$NUM_LOCKS ZIPF_SKEW=$ZIPF_SKEW"

        # Find the most recent non-sweep run directory.
        latest_run=$(ls -t results/ | grep -v '^sweep_' | head -1)
        if [[ -z "$latest_run" ]]; then
            echo "ERROR: no run directory found" >&2
            continue
        fi

        # Distributed comparisons get goodput from node0's global clock.
        # Latency remains per-client-machine; record client0's distribution.
        if [[ "$prim" == "rdma_cas" || "$prim" == "shiftlock" ]]; then
            goodput_log=$(grep -l "GLOBAL COMPARISON RESULTS" "results/$latest_run"/rdma-node-*.log 2>/dev/null | head -1)
            latency_log=$(grep -l "LATENCY (Microseconds)" "results/$latest_run"/rdma-node-*.log 2>/dev/null | head -1)
        else
            goodput_log=$(grep -l "BENCHMARK RESULTS" "results/$latest_run"/rdma-node-*.log 2>/dev/null | head -1)
            latency_log="$goodput_log"
        fi
        if [[ -z "$goodput_log" || -z "$latency_log" ]]; then
            echo "ERROR: no benchmark output found in $latest_run" >&2
            continue
        fi

        p50=$(grep  'P50 (Med):' "$latency_log" | awk '{print $3}')
        p90=$(grep  'P90:'       "$latency_log" | awk '{print $2}')
        p99=$(grep  'P99:'       "$latency_log" | awk '{print $2}')
        p999=$(grep 'P99.9:'     "$latency_log" | awk '{print $2}')
        goodput=$(grep 'Goodput:' "$goodput_log" | awk '{print $2}')

        echo "  -> p50=$p50 p90=$p90 p99=$p99 p999=$p999 goodput=$goodput"
        echo "$prim,$total,$total_worker_threads,$cpt,$NUM_LOCKS,$ZIPF_SKEW,$NUM_OPS,$p50,$p90,$p99,$p999,$goodput" >> "$CSV"
    done
done

echo
echo "=== Sweep complete ==="
echo "CSV: $CSV"

if python3 scripts/primitives/plot.py "$CSV" 2>/dev/null; then
    echo "Plots: $(dirname "$CSV")/plots/"
else
    echo "Skipping plots (matplotlib/pandas not installed)."
    echo "Install with: pip3 install matplotlib pandas"
    echo "Then run: ./scripts/primitives/plot.py \"$CSV\""
fi
