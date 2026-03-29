#!/bin/bash
# Run Synra watch benchmark

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/cluster_info.sh"
SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"

echo "========================================="
echo "Running Synra Watch Benchmark"
echo "========================================="
echo ""

# Update strategy in main.cpp
echo "Setting strategy to 'watch'..."
ssh $SSH_OPTS "${NODES[0]}" "cd ~/rdma && sed -i 's/constexpr const char\* STRATEGY = \".*\";/constexpr const char* STRATEGY = \"watch\";/' src/main.cpp"

# Recompile on all nodes
echo "Recompiling on all nodes..."
for i in $(seq 0 $((NUM_NODES-1))); do
    node="${NODES[$i]}"
    ssh $SSH_OPTS "$node" "cd ~/rdma/build && make -j > /dev/null 2>&1" &
done
wait
echo "✓ Recompiled"
echo ""

# Start server processes in background (auto-detect NODE_ID like existing run.sh)
echo "Starting $NUM_NODES server processes..."
for i in $(seq 0 $((NUM_NODES-1))); do
    node="${NODES[$i]}"
    echo "  Starting server on Node$i ($node)..."
    ssh $SSH_OPTS "$node" "bash -c 'RAW_ID=\$(ip -4 addr show ibp8s0 | grep inet | awk \"{print \\\$2}\" | cut -d. -f4 | cut -d/ -f1); NODE_ID=\$((RAW_ID - 1)); cd ~/rdma/build && sudo NODE_ID=\$NODE_ID IS_CLIENT=0 nohup ./rdma > server_\${NODE_ID}.log 2>&1 < /dev/null &'" > /dev/null 2>&1
done
sleep 5
echo "✓ Servers started"
echo ""

# Run client on Node0 (using sudo)
echo "Running client on Node0..."
echo "(This will take ~30 seconds...)"
echo ""
ssh $SSH_OPTS "${NODES[0]}" "cd ~/rdma/build && sudo IS_CLIENT=1 MACHINE_ID=0 ./rdma" | tee synra_watch_results.txt

echo ""
echo "✓ Benchmark complete!"
echo ""

# Kill server processes
echo "Stopping servers..."
for i in $(seq 0 $((NUM_NODES-1))); do
    node="${NODES[$i]}"
    ssh $SSH_OPTS "$node" "sudo pkill -f './rdma' || true" &
done
wait
echo "✓ Servers stopped"
echo ""

echo "Results saved to: synra_watch_results.txt"
echo ""
