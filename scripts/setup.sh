#!/bin/bash

# 1. Update and Install RDMA/InfiniBand packages
sudo apt update
sudo apt install -y ibverbs-providers libibverbs1 ibutils ibverbs-utils \
    rdmacm-utils perftest libibverbs-dev librdmacm-dev infiniband-diags \
    ninja-build cmake pkg-config build-essential

# 2. Install Clang 23 via the LLVM automatic repository script
## This avoids building from source and handles the apt keys for you
#wget https://apt.llvm.org/llvm.sh
#chmod +x llvm.sh
#sudo ./llvm.sh 22
#rm llvm.sh

sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"

# 3. Load Kernel Modules
sudo modprobe ib_uverbs ib_ipoib rdma_ucm
sudo modprobe -r ib_ipoib && sudo modprobe ib_ipoib
sudo sysctl -w vm.nr_hugepages=512

# 4. Extract the last digit of the IP from enp8s0d1
NODE_ID=$(ifconfig enp8s0d1 | grep 'inet ' | awk '{print $2}' | cut -d'.' -f4)

if [ -z "$NODE_ID" ]; then
    echo "Error: Could not find IP for enp8s0d1"
    exit 1
fi

TARGET_IP="192.168.1.$NODE_ID"
echo "Assigning IP: $TARGET_IP to ibp8s0"

# 5. Configure the InfiniBand interface
# Ensure the device exists before writing to sysfs
if [ -d "/sys/class/net/ibp8s0" ]; then
    echo connected | sudo tee /sys/class/net/ibp8s0/mode
    sudo ifconfig ibp8s0 $TARGET_IP netmask 255.255.255.0 mtu 65520 up
else
    echo "Error: Interface ibp8s0 not found."
fi

# Link clang
#sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-22 100
#sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-22 100

LLVM_VER=$(ls /usr/bin/clang-[0-9]* | grep -oP '\d+' | sort -rn | head -n1)

echo "Detected LLVM version $LLVM_VER. Linking..."

sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-$LLVM_VER 100
sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-$LLVM_VER 100

echo "Done! Node configured as $TARGET_IP"
