#!/bin/bash
# Idempotent CloudLab node provisioning for RDMA benchmarks.
# Runs ON the remote node. Called by: make setup
#
# Usage: setup.sh <ib_interface> <ib_ip> <ib_netmask> <ib_mtu>

set -euo pipefail

IB_INTERFACE="${1:?Usage: setup.sh <ib_interface> <ib_ip> <ib_netmask> <ib_mtu> <user>}"
IB_IP="${2:?}"
IB_NETMASK="${3:?}"
IB_MTU="${4:?}"
RDMA_USER="${5:?}"

log() { echo "[setup] $*"; }

# ── 1. Packages ───────────────────────────────────────────────────────────────

REQUIRED_PKGS=(
    ibverbs-providers libibverbs1 ibutils ibverbs-utils
    rdmacm-utils perftest libibverbs-dev librdmacm-dev infiniband-diags
    build-essential pkg-config
)

missing=()
for pkg in "${REQUIRED_PKGS[@]}"; do
    if ! dpkg -s "$pkg" &>/dev/null; then
        missing+=("$pkg")
    fi
done

if [ ${#missing[@]} -gt 0 ]; then
    log "Installing missing packages: ${missing[*]}"
    apt-get update -qq
    apt-get install -y -qq "${missing[@]}"
else
    log "All packages already installed"
fi

# ── 2. Clang ──────────────────────────────────────────────────────────────────

if command -v clang &>/dev/null; then
    CLANG_VER=$(clang --version | head -1 | grep -oP '\d+' | head -1)
    log "Clang $CLANG_VER already installed"
else
    log "Installing Clang via LLVM apt script..."
    bash -c "$(wget -qO- https://apt.llvm.org/llvm.sh)"

    LLVM_VER=$(ls /usr/bin/clang-[0-9]* 2>/dev/null | grep -oP '\d+' | sort -rn | head -n1)
    if [ -n "$LLVM_VER" ]; then
        update-alternatives --install /usr/bin/clang clang /usr/bin/clang-$LLVM_VER 100
        update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-$LLVM_VER 100
        log "Linked clang/clang++ to version $LLVM_VER"
    fi
fi

# ── 3. Kernel modules ────────────────────────────────────────────────────────

for mod in ib_uverbs ib_ipoib rdma_ucm; do
    if ! lsmod | grep -q "^$mod"; then
        log "Loading kernel module: $mod"
        modprobe "$mod"
    fi
done

# ── 4. Hugepages ──────────────────────────────────────────────────────────────

CURRENT_HUGEPAGES=$(sysctl -n vm.nr_hugepages)
if [ "$CURRENT_HUGEPAGES" -lt 2048 ]; then
    log "Setting hugepages: $CURRENT_HUGEPAGES -> 2048"
    sysctl -w vm.nr_hugepages=2048
else
    log "Hugepages already at $CURRENT_HUGEPAGES"
fi

# ── 5. CPU governors ─────────────────────────────────────────────────────────

GOV_FILES=(/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor)
if [ -e "${GOV_FILES[0]}" ]; then
    for f in "${GOV_FILES[@]}"; do
        echo performance > "$f"
    done
    log "CPU governors set to performance"
else
    log "CPU governor files not found, skipping"
fi

# ── 6. InfiniBand interface ──────────────────────────────────────────────────

if [ -d "/sys/class/net/$IB_INTERFACE" ]; then
    CURRENT_IP=$(ip -4 addr show "$IB_INTERFACE" 2>/dev/null | grep -oP 'inet \K[\d.]+' || true)
    if [ "$CURRENT_IP" != "$IB_IP" ]; then
        log "Configuring $IB_INTERFACE: $IB_IP (was: ${CURRENT_IP:-none})"
        echo connected > "/sys/class/net/$IB_INTERFACE/mode"
        ifconfig "$IB_INTERFACE" "$IB_IP" netmask "$IB_NETMASK" mtu "$IB_MTU" up
    else
        log "$IB_INTERFACE already configured as $IB_IP"
    fi
else
    log "WARNING: Interface $IB_INTERFACE not found"
fi

# ── 7. ulimits ───────────────────────────────────────────────────────────────

LIMITS_LINE="* - nofile 65536"
if ! grep -qF "$LIMITS_LINE" /etc/security/limits.conf; then
    log "Adding nofile ulimit to /etc/security/limits.conf"
    echo "$LIMITS_LINE" >> /etc/security/limits.conf
else
    log "ulimit already configured"
fi

# ── 8. Working directory ─────────────────────────────────────────────────────

mkdir -p /local/rdma
chown "$RDMA_USER" /local/rdma
log "Done"
