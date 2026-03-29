#!/bin/bash

# Run all 6 experiments with phase-separated metrics

# Kill any leftover rdma processes before starting
echo "Cleaning up any leftover rdma processes..."
for host in apt128 apt132 apt095 apt104 apt112 apt121; do
    ssh stevie98@${host}.apt.emulab.net "sudo pkill -9 rdma || true" > /dev/null 2>&1 &
done
wait
echo "Cleanup complete."

EXPERIMENTS=(
    "1000:Experiment 1"
    "5000:Experiment 2"
    "10000:Experiment 3"
    "30000:Experiment 4"
    "50000:Experiment 5"
    "100000:Experiment 6"
    "200000:Experiment 7"
    "500000:Experiment 8"
)

for exp in "${EXPERIMENTS[@]}"; do
    NUM_OPS="${exp%%:*}"
    NAME="${exp##*:}"

    echo "========================================="
    echo "Running $NAME: $NUM_OPS ops"
    echo "========================================="

    # Update NUM_OPS in common.h
    sed -i '' "s/constexpr size_t NUM_OPS = [0-9]*;.*/constexpr size_t NUM_OPS = $NUM_OPS;  \/\/ $NAME/" include/rdma/common.h

    # Commit and push
    git add -f include/rdma/common.h
    git commit -m "$NAME: $NUM_OPS ops"
    git push

    # Rebuild all nodes in parallel (servers + client)
    echo "Rebuilding all nodes..."
    for host in apt128 apt132 apt095 apt104 apt112 apt121; do
        ssh stevie98@${host}.apt.emulab.net "cd /local/rdma && git checkout yingjianw/wip && git pull --rebase origin yingjianw/wip && cd build && make -j" > "/tmp/build_${host}.log" 2>&1 &
    done
    wait  # Wait for all rebuilds to complete

    # Check if any builds failed
    build_failed=false
    for host in apt128 apt132 apt095 apt104 apt112 apt121; do
        if ! grep -q "Built target rdma" "/tmp/build_${host}.log"; then
            echo "❌ Build FAILED on ${host}! Check /tmp/build_${host}.log"
            build_failed=true
        fi
    done
    if [ "$build_failed" = true ]; then
        echo "Aborting due to build failures"
        exit 1
    fi
    echo "✓ All nodes rebuilt successfully"

    # Kill all RDMA processes (servers + client)
    echo "Stopping all RDMA processes..."
    for host in apt128 apt132 apt095 apt104 apt112 apt121; do
        ssh stevie98@${host}.apt.emulab.net "sudo pkill -9 rdma || true" &
    done
    wait

    # Verify all RDMA processes are stopped
    echo "Verifying all processes stopped..."
    for attempt in {1..10}; do
        all_stopped=true
        for host in apt128 apt132 apt095 apt104 apt112 apt121; do
            if ssh stevie98@${host}.apt.emulab.net "pgrep -x rdma > /dev/null 2>&1"; then
                all_stopped=false
                break
            fi
        done
        if [ "$all_stopped" = true ]; then
            echo "✓ All processes stopped"
            break
        fi
        echo "  Waiting for processes to stop (attempt $attempt/10)..."
        sleep 1
    done

    # Start servers
    echo "Starting servers..."
    for i in 0 1 2 3 4; do
        case $i in
            0) host=apt128 ;;
            1) host=apt132 ;;
            2) host=apt095 ;;
            3) host=apt104 ;;
            4) host=apt112 ;;
        esac
        ssh stevie98@${host}.apt.emulab.net "cd /local/rdma/build && sudo bash -c 'NODE_ID=$i IS_CLIENT=0 nohup ./rdma > server_$i.log 2>&1 < /dev/null &'" > /dev/null 2>&1
    done

    sleep 5
    echo "✓ Servers started"

    # Run client on apt121 (dedicated client node)
    ssh stevie98@apt121.apt.emulab.net "cd /local/rdma/build && sudo IS_CLIENT=1 MACHINE_ID=0 timeout 300 ./rdma 2>&1" > "/tmp/exp_${NUM_OPS}.log"

    echo ""
    echo "========================================="
    echo "Results for $NAME:"
    echo "========================================="
    echo ""
    echo "--- Phase Throughput & Latency ---"
    grep -A 20 "PHASE THROUGHPUT" "/tmp/exp_${NUM_OPS}.log" | head -25
    echo ""
    echo "--- Overall Benchmark Summary ---"
    grep -A 20 "RDMA LOCK BENCHMARK RESULTS" "/tmp/exp_${NUM_OPS}.log" | head -25
    echo ""

done

echo "All experiments completed!"
