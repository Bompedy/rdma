#pragma once

// Shared MU RPC structs and mutation-log encoding helpers.

#include "rdma/common.h"

#include <cstddef>
#include <cstdint>

enum class MuRpcOp : uint8_t {
    Lock = 1,
    Unlock = 2,
    WatchRegister = 3,    // Register as watcher (returns watcher slot)
    WatchNotify = 4,      // Notify all watchers (E2E operation)
};

enum class MuRpcStatus : uint8_t {
    Ok = 0,
    InvalidUnlock = 1,
    QueueFull = 2,
    InternalError = 3,
};

struct MuRequest {
    uint8_t op;
    uint8_t reserved0;
    uint16_t client_id;
    uint32_t lock_id;
    uint32_t req_id;
    // For MU, granted_slot refers to the global mutation-log slot that granted
    // the lock, not a per-lock log position.
    uint32_t granted_slot;
};

struct MuResponse {
    uint8_t op;
    uint8_t status;
    uint16_t client_id;
    uint32_t lock_id;
    uint32_t req_id;
    uint32_t granted_slot;
};

static_assert(sizeof(MuRequest) == 16);
static_assert(sizeof(MuResponse) == 16);

constexpr uint64_t MU_LOG_OP_SHIFT = 63;
constexpr uint64_t MU_LOG_LOCK_ID_SHIFT = 48;
constexpr uint64_t MU_LOG_CLIENT_ID_SHIFT = 32;
constexpr uint64_t MU_LOG_REQ_ID_SHIFT = 0;
constexpr uint64_t MU_LOG_LOCK_ID_MASK = 0x7FFFULL;
constexpr uint64_t MU_LOG_CLIENT_ID_MASK = 0xFFFFULL;
constexpr uint64_t MU_LOG_REQ_ID_MASK = 0xFFFFFFFFULL;

inline uint64_t mu_make_log_entry(
    const MuRpcOp op,
    const uint32_t lock_id,
    const uint16_t client_id,
    const uint32_t req_id
) {
    // Global MU mutation log entry: op kind, lock id, client id, req id.
    return (static_cast<uint64_t>(op == MuRpcOp::Unlock) << MU_LOG_OP_SHIFT)
         | ((static_cast<uint64_t>(lock_id) & MU_LOG_LOCK_ID_MASK) << MU_LOG_LOCK_ID_SHIFT)
         | ((static_cast<uint64_t>(client_id) & MU_LOG_CLIENT_ID_MASK) << MU_LOG_CLIENT_ID_SHIFT)
         | ((static_cast<uint64_t>(req_id) & MU_LOG_REQ_ID_MASK) << MU_LOG_REQ_ID_SHIFT);
}

inline MuRpcOp mu_log_entry_op(const uint64_t entry) {
    return ((entry >> MU_LOG_OP_SHIFT) & 0x1ULL) != 0 ? MuRpcOp::Unlock : MuRpcOp::Lock;
}

inline uint32_t mu_log_entry_lock_id(const uint64_t entry) {
    return static_cast<uint32_t>((entry >> MU_LOG_LOCK_ID_SHIFT) & MU_LOG_LOCK_ID_MASK);
}

inline uint16_t mu_log_entry_client_id(const uint64_t entry) {
    return static_cast<uint16_t>((entry >> MU_LOG_CLIENT_ID_SHIFT) & MU_LOG_CLIENT_ID_MASK);
}

inline uint32_t mu_log_entry_req_id(const uint64_t entry) {
    return static_cast<uint32_t>((entry >> MU_LOG_REQ_ID_SHIFT) & MU_LOG_REQ_ID_MASK);
}

inline void mu_write_commit_index(uint8_t* lock_base, const uint64_t commit_index) {
    *reinterpret_cast<uint64_t*>(lock_base) = commit_index;
}

inline uint64_t mu_read_commit_index(const uint8_t* lock_base) {
    return *reinterpret_cast<const uint64_t*>(lock_base);
}

inline uint8_t* mu_lock_base(uint8_t* buf, const uint32_t lock_id) {
    return buf + lock_base_offset(lock_id);
}

inline const uint8_t* mu_lock_base(const uint8_t* buf, const uint32_t lock_id) {
    return buf + lock_base_offset(lock_id);
}

constexpr size_t MU_SERVER_RECV_RING = 2048;
constexpr size_t MU_MAX_PENDING_PER_LOCK = 1024;
constexpr size_t MU_MAX_APPEND_INFLIGHT_PER_LOCK = 64;
