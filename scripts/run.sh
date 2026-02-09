#!/bin/bash

# 1. Extract the last digit (e.g., 6)
NODE_ID=$(ifconfig enp8s0d1 | grep 'inet ' | awk '{print $2}' | cut -d'.' -f4)

if [ -z "$RAW_ID" ]; then
    echo "Error: Could not find IP for enp8s0d1"
    exit 1
fi

echo "Detected Raw ID: $NODE_ID | Setting NODE_ID to: $NODE_ID"

# 3. Build and Run
mkdir -p /local/rdma/build
cd /local/rdma/build || exit
cd ..
git pull

# Standard build dance
mkdir -p build && cd build
cmake ..
make -j

# 4. Execute with the calculated NODE_ID
env NODE_ID=$NODE_ID ./rdma