#!/bin/bash
# Sweep primitive benchmarks over a fixed set of (threads, clients-per-thread) pairs.
#
# Usage: ./scripts/primitives/sweep.sh [SWEEP_PAIRS] [PRIMITIVES] [NUM_OPS]
#   SWEEP_PAIRS   space-separated list of threads:cpt pairs
#                 default: "1:1 1:2 1:4 1:8 1:16 2:16 4:16 8:16"
#   PRIMITIVES    space-separated list of primitive names
#                 default: "synra_tas synra_faa mu_faa"
#   NUM_OPS       total operations per run
#                 default: 100000
#
# Output: results/sweep_<timestamp>/sweep.csv
#         one row per (primitive, threads, cpt) run

set -euo pipefail

SWEEP_PAIRS="${1:-1:1 1:2 1:4 1:8 1:16 2:16 4:16 8:16}"
PRIMITIVES="${2:-synra_tas synra_faa mu_faa}"
NUM_OPS="${3:-100000}"

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

SWEEP_ID="sweep_$(date +%Y%m%d_%H%M%S)"
SWEEP_DIR="results/$SWEEP_ID"
mkdir -p "$SWEEP_DIR"
CSV="$SWEEP_DIR/sweep.csv"
echo "primitive,total_clients,threads,cpt,ops,p50,p90,p99,p999,throughput" > "$CSV"

echo "=== Sweep $SWEEP_ID ==="
echo "Pairs:      $SWEEP_PAIRS"
echo "Primitives: $PRIMITIVES"
echo "Num ops:    $NUM_OPS"
echo "CSV:        $CSV"
echo

echo "=== Deploying once ==="
make deploy

for prim in $PRIMITIVES; do
    for pair in $SWEEP_PAIRS; do
        threads="${pair%%:*}"
        cpt="${pair##*:}"
        total=$((threads * cpt))
        echo "=== $prim  threads=$threads cpt=$cpt  (total=$total) ==="

        make run BENCH="PRIMITIVE=$prim NUM_THREADS=$threads CLIENTS_PER_THREAD=$cpt NUM_OPS=$NUM_OPS"

        # Find the most recent non-sweep run directory.
        latest_run=$(ls -t results/ | grep -v '^sweep_' | head -1)
        if [[ -z "$latest_run" ]]; then
            echo "ERROR: no run directory found" >&2
            continue
        fi

        # The node that prints BENCHMARK RESULTS is the one that ran clients.
        # For synra_faa every node prints; for mu_faa/synra_tas only the last node prints.
        log=$(grep -l "BENCHMARK RESULTS" "results/$latest_run"/rdma-node-*.log 2>/dev/null | head -1)
        if [[ -z "$log" ]]; then
            echo "ERROR: no benchmark output found in $latest_run" >&2
            continue
        fi

        p50=$(grep  'P50 (Med):' "$log" | awk '{print $3}')
        p90=$(grep  'P90:'       "$log" | awk '{print $2}')
        p99=$(grep  'P99:'       "$log" | awk '{print $2}')
        p999=$(grep 'P99.9:'     "$log" | awk '{print $2}')
        tput=$(grep 'Throughput:' "$log" | awk '{print $2}')

        echo "  -> p50=$p50 p90=$p90 p99=$p99 p999=$p999 tput=$tput"
        echo "$prim,$total,$threads,$cpt,$NUM_OPS,$p50,$p90,$p99,$p999,$tput" >> "$CSV"
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
