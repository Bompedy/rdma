#!/bin/bash

# 0. Get IS_CLIENT from the first argument (default to 0 if not provided)
IS_CLIENT=${1:-0}

# 1. Extract the last digit
RAW_ID=$(ifconfig enp8s0d1 | grep 'inet ' | awk '{print $2}' | cut -d'.' -f4)

if [ -z "$RAW_ID" ]; then
    echo "Error: Could not find IP for enp8s0d1"
    exit 1
fi

# 2. Subtract 1 for the NODE_ID
NODE_ID=$((RAW_ID - 1))

echo "Detected Raw ID: $RAW_ID | Setting NODE_ID to: $NODE_ID | IS_CLIENT: $IS_CLIENT"

# 3. Build logic
cd /local/rdma || exit
git pull
mkdir -p build && cd build
rm -f CMakeCache.txt
cmake -DCMAKE_C_COMPILER=/usr/bin/clang-22 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-22 ..

# 4. Compile
make -j

# 5. Execute
# We pass both NODE_ID and IS_CLIENT to the binary
sudo NODE_ID=$NODE_ID IS_CLIENT=$IS_CLIENT ./rdma