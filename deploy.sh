#!/bin/bash
# Automated deployment script for N-node Emulab cluster

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
NODES_FILE="$SCRIPT_DIR/nodes.txt"
SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"

echo "========================================="
echo "RDMA Watch Benchmark - Emulab Deployment"
echo "========================================="
echo ""

# Read nodes from file (skip comments and empty lines)
NODES=()
while IFS= read -r line; do
    # Skip comments and empty lines
    [[ "$line" =~ ^#.*$ ]] && continue
    [[ -z "$line" ]] && continue
    NODES+=("$line")
done < "$NODES_FILE"
NUM_NODES=${#NODES[@]}

if [ $NUM_NODES -eq 0 ]; then
    echo "Error: No nodes found in $NODES_FILE"
    exit 1
fi

echo "Found $NUM_NODES nodes:"
for i in "${!NODES[@]}"; do
    echo "  Node$i: ${NODES[$i]}"
done
echo ""

echo "Step 1: Getting internal IPs from nodes..."
declare -a IPS
for i in "${!NODES[@]}"; do
    node="${NODES[$i]}"
    echo "  Fetching IP from $node..."
    ip=$(ssh $SSH_OPTS "$node" "hostname -I | awk '{print \$1}'")
    IPS+=("$ip")
    echo "    -> $ip"
done
echo ""

echo "Step 2: Updating include/rdma/common.h..."
# Backup original
cp include/rdma/common.h include/rdma/common.h.backup

# Simple sed replacements for each node IP
for i in "${!IPS[@]}"; do
    node_num=$((i+1))
    if [ $i -lt ${#IPS[@]} ]; then
        sed -i.tmp "s|\"10.0.0.$node_num\",  // Server $node_num - UPDATE THIS|\"${IPS[$i]}\",  // Node$i ${NODES[$i]#*@}|g" include/rdma/common.h
    fi
done
sed -i.tmp "s|\"10.0.0.1\",  // Run client on Server 1 - UPDATE THIS|\"${IPS[0]}\",  // Client on Node0|g" include/rdma/common.h
rm -f include/rdma/common.h.tmp

echo "✓ Configuration updated with $NUM_NODES nodes!"
echo "  IPs: ${IPS[*]}"
echo ""

echo "Step 3: Copying code to all nodes..."
for node in "${NODES[@]}"; do
    echo "  Copying to $node..."
    ssh $SSH_OPTS "$node" "rm -rf ~/rdma" 2>/dev/null || true
    scp $SSH_OPTS -r -q "$SCRIPT_DIR" "$node":~/ 2>&1 | grep -v "Permission denied" || true
done
echo "✓ Code deployed to all nodes!"
echo ""

echo "Step 4: Installing dependencies on all nodes..."
for node in "${NODES[@]}"; do
    echo "  Installing on $node..."
    ssh $SSH_OPTS "$node" "sudo DEBIAN_FRONTEND=noninteractive apt update -qq && sudo DEBIAN_FRONTEND=noninteractive apt install -y -qq build-essential cmake libibverbs-dev librdmacm-dev pkg-config 2>&1 | grep -v 'debconf: unable'" &
done
wait
echo "✓ Dependencies installed!"
echo ""

echo "Step 5: Compiling on all nodes..."
for node in "${NODES[@]}"; do
    echo "  Compiling on $node..."
    ssh $SSH_OPTS "$node" "cd ~/rdma && rm -rf build && mkdir build && cd build && cmake .. > /dev/null 2>&1 && make -j > /dev/null 2>&1" &
done
wait
echo "✓ Compiled on all nodes!"
echo ""

# Save cluster info for run scripts
cat > "$SCRIPT_DIR/cluster_info.sh" <<EOF
#!/bin/bash
# Auto-generated cluster configuration
NUM_NODES=$NUM_NODES
NODES=(${NODES[@]@Q})
IPS=(${IPS[@]@Q})
EOF

echo "========================================="
echo "Deployment Complete!"
echo "========================================="
echo ""
echo "Cluster configuration:"
echo "  Nodes: $NUM_NODES"
echo "  IPs: ${IPS[*]}"
echo ""
echo "Next steps:"
echo "  1. Run Synra watch:  ./run_synra_watch.sh"
echo "  2. Run Mu watch:     ./run_mu_watch.sh"
echo ""
