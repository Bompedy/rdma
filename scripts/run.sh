#!/bin/bash

# 1. Extract the last digit (e.g., 6)
RAW_ID=$(ifconfig enp8s0d1 | grep 'inet ' | sed 's/addr://' | awk '{print $2}' | cut -d'.' -f4)

if [ -z "$RAW_ID" ]; then
    echo "Error: Could not find IP for enp8s0d1"
    exit 1
fi

# 2. Subtract 1 for the NODE_ID (e.g., 6 becomes 5)
NODE_ID=$((RAW_ID - 1))

echo "Detected Raw ID: $RAW_ID | Setting NODE_ID to: $NODE_ID"

# 3. Build logic
cd /local/rdma || exit
git pull

# Ensure build dir exists and enter it
mkdir -p build && cd build

# Clear old cache to force Clang detection
rm -f CMakeCache.txt

# Run CMake using the Clang-22 binaries
# We use the full path /usr/bin/ to be 100% safe
cmake -DCMAKE_C_COMPILER=/usr/bin/clang-22 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-22 ..

# 4. Compile
make -j

# 5. Execute
# We pass the calculated NODE_ID to your C++ program
env NODE_ID=$NODE_ID ./rdma