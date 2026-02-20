#!/bin/bash

# 1. Update and Install RDMA/InfiniBand packages
sudo apt update
sudo apt install -y ibverbs-providers libibverbs1 ibutils ibverbs-utils \
    rdmacm-utils perftest libibverbs-dev librdmacm-dev infiniband-diags \
    ninja-build cmake pkg-config build-essential opensm  # Added opensm just in case

# 2. Install Clang
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 22
rm llvm.sh

# 3. Load Kernel Modules & Setup Hugepages
# Added rdma_cm and ib_umad which are critical for mlx4
sudo modprobe ib_uverbs ib_ipoib rdma_ucm rdma_cm ib_umad
sudo sysctl -w vm.nr_hugepages=1024 # Increased for better RDMA performance

# 4. Extract Node ID
NODE_ID=$(ip -4 addr show enp8s0d1 | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | cut -d'.' -f4)

if [ -z "$NODE_ID" ]; then
    echo "Error: Could not find IP for enp8s0d1"
    exit 1
fi

TARGET_IP="192.168.1.$NODE_ID"
echo "Assigning IP: $TARGET_IP to ibp8s0"

# 5. Configure the InfiniBand interface (The Fixes)
if [ -d "/sys/class/net/ibp8s0" ]; then
    # IMPORTANT: Flush existing IP and bring down to reset the GID table
    sudo ip addr flush dev ibp8s0
    sudo ip link set ibp8s0 down

    # Change mode to 'connected' while link is down
    echo connected | sudo tee /sys/class/net/ibp8s0/mode

    # Bring up with Hardware-supported MTU (4096)
    # 65520 was causing your RDMA CM to fail/reject
    sudo ip link set ibp8s0 mtu 4096
    sudo ip link set ibp8s0 up

    # Assign IP using 'ip' command instead of deprecated 'ifconfig'
    sudo ip addr add $TARGET_IP/24 dev ibp8s0

    # Restart the RDMA name daemon to refresh GID cache
    sudo systemctl restart rdma-ndd 2>/dev/null || true
else
    echo "Error: Interface ibp8s0 not found."
    exit 1
fi

# Link clang
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-22 100
sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-22 100

echo "Done! Node configured as $TARGET_IP with MTU 4096 in Connected Mode"