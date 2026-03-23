#pragma once

// Shared benchmark configuration, memory layout, allocation helpers, and
// low-level RDMA utilities used across all remaining pipelines.

#include <array>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <pthread.h>
#include <sys/mman.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

// ─── Cluster config ───

inline const std::vector<std::string> CLUSTER_NODES = {
    "192.168.1.1",
    "192.168.1.2",
    "192.168.1.3",
    //"192.168.1.4",
    //"192.168.1.5",
    // "192.168.1.6",
    // "192.168.1.7"
};

// change these two variables together
inline const std::vector<std::string> CLIENT_NODES = {
      "192.168.1.4",
   // "192.168.1.5",
    //"192.168.1.6",
    // "192.168.1.7",
    // "192.168.1.8",
    // "192.168.1.9",
    // "192.168.1.10",
};

constexpr size_t TOTAL_CLIENT_MACHINES = 1;
//

inline const size_t QUORUM = (CLUSTER_NODES.size() / 2) + 1;
inline const size_t SUPER_QUORUM = (3 * CLUSTER_NODES.size() + 3) / 4;

// ─── RDMA constants ───

constexpr uint16_t RDMA_PORT = 6969;
constexpr size_t ENTRY_SIZE = 8;
constexpr size_t QP_DEPTH = 2048;
constexpr size_t MAX_INLINE_DEPTH = 64;
constexpr size_t MAX_REPLICAS = 10;
constexpr uint8_t RDMA_RESPONDER_RESOURCES = 16;
constexpr uint8_t RDMA_INITIATOR_DEPTH = 16;

// ─── Benchmark / workload config ───
// These knobs define the workload shape shared across all pipelines.

constexpr size_t NUM_OPS = 500000;
constexpr size_t NUM_CLIENTS_PER_MACHINE = 4;
constexpr size_t TOTAL_CLIENTS = NUM_CLIENTS_PER_MACHINE * TOTAL_CLIENT_MACHINES;
constexpr size_t NUM_OPS_PER_CLIENT = NUM_OPS / TOTAL_CLIENTS;
constexpr size_t NUM_TOTAL_OPS = NUM_OPS_PER_CLIENT * TOTAL_CLIENTS;
constexpr size_t MAX_LOCKS = 1;

// Prototype recovery scope: one lock whose frontier starts on node 0 and moves
// to node 1 after a hardcoded failure trigger.
constexpr uint32_t RECOVERY_TARGET_LOCK = 0;
constexpr uint32_t RECOVERY_FAILED_NODE = 0;
constexpr uint32_t RECOVERY_COORD_NODE = 1;
constexpr uint32_t RECOVERY_REPLACEMENT_NODE = 1;
constexpr uint32_t RECOVERY_TRIGGER_MS = 2000;
constexpr uint32_t RECOVERY_NUM_ROUNDS = 1000;
constexpr uint32_t RECOVERY_DETECTION_DELAY_MS = 0;
constexpr uint32_t RECOVERY_RESET_QUIESCE_MS = 10;
constexpr uint32_t RECOVERY_ROUND_GAP_MS = 10;
constexpr uint32_t RECOVERY_EXPERIMENT_DONE_GRACE_MS = 250;
constexpr size_t RECOVERY_CTRL_RECV_RING = 8;
constexpr size_t RECOVERY_CTRL_SEND_RING = 32;

// ─── CAS config ───
// Wrapped per-lock replicated log plus owner-node control word.

constexpr size_t CAS_ACTIVE_WINDOW = 9;
constexpr size_t CAS_CQ_BATCH = 32;
constexpr double CAS_ZIPF_SKEW = 0.5;
constexpr bool CAS_SHARD_OWNER = true;
constexpr size_t CAS_LOG_CAPACITY = TOTAL_CLIENTS * CAS_ACTIVE_WINDOW * 4;
constexpr bool CAS_RELEASE_CONTROL_USE_CAS = false;
constexpr bool CAS_RELEASE_LOG_USE_CAS = true;

// ─── Simple CAS config ───
// One-sided non-replicated flag lock used as a non-fault-tolerant baseline.

constexpr size_t SIMPLE_CAS_ACTIVE_WINDOW = 16;
constexpr bool SIMPLE_CAS_SHARD_OWNER = true;
constexpr size_t SIMPLE_CAS_CQ_BATCH = 32;
constexpr double SIMPLE_CAS_ZIPF_SKEW = 0.0;
constexpr bool SIMPLE_CAS_RELEASE_USE_CAS = false;

// ─── Ticket FAA config ───
// Ticket register on the owner node plus wrapped per-lock replicated log.

constexpr size_t TICKET_FAA_ACTIVE_WINDOW = 8;
constexpr bool TICKET_FAA_SHARD_OWNER = true;
constexpr size_t TICKET_FAA_LOG_CAPACITY = TOTAL_CLIENTS * TICKET_FAA_ACTIVE_WINDOW * 4;
constexpr size_t TICKET_FAA_CQ_BATCH = 32;
constexpr double TICKET_FAA_ZIPF_SKEW = 0.5;
constexpr uint32_t TICKET_FAA_REPLICATE_RETRY_SPIN = 64;
constexpr bool TICKET_FAA_RELEASE_LOG_USE_CAS = true;
constexpr uint32_t TICKET_FAA_RELEASE_TURN_MODE = 0; // 0=write, 1=cas, 2=faa
constexpr uint32_t TICKET_FAA_TURN_SPIN_VERY_NEAR = 0;
constexpr uint32_t TICKET_FAA_TURN_SPIN_NEAR = 0;
constexpr uint32_t TICKET_FAA_TURN_SPIN_MID = 0;
constexpr uint32_t TICKET_FAA_TURN_SPIN_FAR = 0;

// ─── MU config ───
// Global append-only mutation log with in-memory per-lock state on the leader.

constexpr size_t MU_ACTIVE_WINDOW = 16;
constexpr size_t MU_CQ_BATCH = 32;
constexpr uint32_t MU_CLIENT_SEND_SIGNAL_EVERY = 64;
constexpr uint32_t MU_SERVER_SEND_SIGNAL_EVERY = 128;
constexpr double MU_ZIPF_SKEW = 0.5;
constexpr bool MU_DEBUG = false;
constexpr bool MU_REPL_SIGNAL_QUORUM_ONLY = true;
constexpr size_t MU_GLOBAL_LOG_CAPACITY = NUM_OPS * 2;

// ─── Lock table layout ───
// The physical server layout is shared even though pipelines use it differently.

// Shared physical per-lock log allocation. Wrapped pipelines only use a bounded
// subset of each per-lock log, while MU reinterprets the full log space as one
// global append-only mutation stream.
constexpr size_t MAX_LOG_PER_LOCK = ((NUM_OPS + MAX_LOCKS - 1) / MAX_LOCKS) * 4;
constexpr size_t LOCK_HEADER_SIZE = 16;
constexpr size_t LOCK_LOG_SIZE = MAX_LOG_PER_LOCK * ENTRY_SIZE;
constexpr size_t LOCK_REGION_SIZE = LOCK_HEADER_SIZE + LOCK_LOG_SIZE;
constexpr size_t LOCK_TABLE_SIZE = LOCK_REGION_SIZE * MAX_LOCKS;

// ─── Buffer sizing ───

constexpr size_t METADATA_SIZE = 4096;
constexpr size_t PAGE_SIZE = 4096;
constexpr size_t RECOVERY_REGION_ALIGN = 64;
constexpr size_t RECOVERY_FRONTIER_REGION_SIZE = 64;
constexpr size_t RECOVERY_TURN_REGION_SIZE = 64;
constexpr size_t RECOVERY_METADATA_REGION_SIZE = 64;
constexpr size_t RECOVERY_LOG_REGION_SIZE = ((MAX_LOG_PER_LOCK * ENTRY_SIZE + RECOVERY_REGION_ALIGN - 1)
    / RECOVERY_REGION_ALIGN) * RECOVERY_REGION_ALIGN;
constexpr size_t RECOVERY_PROTOTYPE_SIZE = RECOVERY_FRONTIER_REGION_SIZE
    + RECOVERY_TURN_REGION_SIZE
    + RECOVERY_LOG_REGION_SIZE
    + RECOVERY_METADATA_REGION_SIZE;

// Server: full lock table + metadata
constexpr size_t SERVER_POOL_SIZE = LOCK_TABLE_SIZE + METADATA_SIZE + RECOVERY_PROTOTYPE_SIZE;
constexpr size_t SERVER_ALIGNED_SIZE = ((SERVER_POOL_SIZE + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;

// Client: just LocalState + padding (a few KB)
constexpr size_t CLIENT_POOL_SIZE = 4096 * 2;  // 8KB — plenty for LocalState
constexpr size_t CLIENT_ALIGNED_SIZE = ((CLIENT_POOL_SIZE + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;

static_assert(LOCK_TABLE_SIZE <= SERVER_ALIGNED_SIZE, "Lock table exceeds server buffer size");
static_assert(CAS_LOG_CAPACITY <= MAX_LOG_PER_LOCK, "CAS log capacity exceeds allocated per-lock log size");
static_assert(TICKET_FAA_LOG_CAPACITY <= MAX_LOG_PER_LOCK, "Ticket FAA log capacity exceeds allocated per-lock log size");
static_assert(MU_GLOBAL_LOG_CAPACITY <= MAX_LOCKS * MAX_LOG_PER_LOCK, "MU global log exceeds allocated total log size");

// ─── Per-lock offset helpers ───

inline constexpr size_t lock_base_offset(const uint32_t lock_id) {
    return lock_id * LOCK_REGION_SIZE;
}

inline constexpr size_t lock_control_offset(const uint32_t lock_id) {
    return lock_base_offset(lock_id);
}

inline constexpr size_t lock_turn_offset(const uint32_t lock_id) {
    return lock_base_offset(lock_id) + 8;
}

inline constexpr size_t lock_log_slot_offset(const uint32_t lock_id, const uint64_t slot) {
    return lock_base_offset(lock_id) + LOCK_HEADER_SIZE + (slot * ENTRY_SIZE);
}

inline constexpr size_t mu_global_log_slot_offset(const uint64_t slot) {
    // MU maps a global mutation-log slot onto the shared per-lock log storage.
    return lock_log_slot_offset(
        static_cast<uint32_t>(slot / MAX_LOG_PER_LOCK),
        slot % MAX_LOG_PER_LOCK);
}

inline constexpr size_t recovery_region_base_offset() {
    return LOCK_TABLE_SIZE + METADATA_SIZE;
}

inline constexpr size_t recovery_frontier_control_offset() {
    return recovery_region_base_offset();
}

inline constexpr size_t recovery_frontier_turn_offset() {
    return recovery_frontier_control_offset() + RECOVERY_FRONTIER_REGION_SIZE;
}

inline constexpr size_t recovery_log_region_offset() {
    return recovery_frontier_turn_offset() + RECOVERY_TURN_REGION_SIZE;
}

inline constexpr size_t recovery_metadata_offset() {
    return recovery_log_region_offset() + RECOVERY_LOG_REGION_SIZE;
}

inline constexpr size_t recovery_log_slot_offset(const uint64_t slot) {
    return recovery_log_region_offset() + slot * ENTRY_SIZE;
}

inline constexpr size_t align_up(const size_t value, const size_t alignment) {
    return ((value + alignment - 1) / alignment) * alignment;
}

inline size_t huge_page_size() {
    static const size_t size = []() -> size_t {
        std::ifstream meminfo("/proc/meminfo");
        if (!meminfo) {
            throw std::runtime_error("Could not open /proc/meminfo for huge page size");
        }

        std::string line;
        while (std::getline(meminfo, line)) {
            if (line.rfind("Hugepagesize:", 0) == 0) {
                std::istringstream iss(line.substr(std::string("Hugepagesize:").size()));
                size_t value_kb = 0;
                std::string unit;
                if (!(iss >> value_kb >> unit) || unit != "kB") {
                    break;
                }
                return value_kb * 1024;
            }
        }

        throw std::runtime_error("Hugepagesize not found in /proc/meminfo");
    }();
    return size;
}

inline size_t huge_page_align(const size_t size) {
    return align_up(size, huge_page_size());
}

inline void* allocate_hugepage_buffer(const size_t requested_size) {
    const size_t aligned_size = huge_page_align(std::max(requested_size, huge_page_size()));
    void* ptr = mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ptr == MAP_FAILED) {
        throw std::runtime_error("Could not allocate hugepage RDMA buffer");
    }
    return ptr;
}

inline void free_hugepage_buffer(void* ptr, const size_t requested_size) noexcept {
    if (!ptr) return;
    const size_t aligned_size = huge_page_align(std::max(requested_size, huge_page_size()));
    munmap(ptr, aligned_size);
}

// ─── Sentinel values ───

constexpr uint64_t EMPTY_SLOT = 0xFFFFFFFFFFFFFFFF;

// ─── Connection types ───

enum class ConnType : uint8_t { FOLLOWER, CLIENT, LEADER };

struct RecoveryRegionCred {
    uintptr_t addr = 0;
    uint32_t rkey = 0;
} __attribute__((packed));

struct ConnPrivateData {
    uintptr_t addr;
    uint32_t rkey;
    uint32_t node_id;
    ConnType type;
    RecoveryRegionCred prototype_frontier;
    RecoveryRegionCred prototype_turn;
    RecoveryRegionCred prototype_log;
} __attribute__((packed));

static_assert(sizeof(ConnPrivateData) <= 56, "ConnPrivateData exceeds RDMA CM request private-data budget");

struct RemoteNode {
    rdma_cm_id* id;
    uint32_t node_id;
    uintptr_t addr;
    uint32_t rkey;
    RecoveryRegionCred prototype_frontier;
    RecoveryRegionCred prototype_turn;
    RecoveryRegionCred prototype_log;
};

struct RemoteConnection {
    uint32_t id;
    rdma_cm_id* cm_id;
    uintptr_t remote_addr;
    uint32_t rkey;
    ConnType type;
    RecoveryRegionCred prototype_frontier;
    RecoveryRegionCred prototype_turn;
    RecoveryRegionCred prototype_log;
};

enum class RecoveryMsgType : uint8_t {
    invalid = 0,
    go = 1,
    recovery_start = 2,
    recovery_switch = 3,
    replica_report = 4,
    new_creds = 5,
    recovery_done = 6,
    baseline_reset_start = 7,
    experiment_done = 8,
    client_quiesced = 9,
    replica_repaired = 10,
};

struct RecoveryControlMessage {
    RecoveryMsgType type = RecoveryMsgType::invalid;
    uint8_t reserved0[7]{};
    uint32_t epoch = 0;
    uint32_t lock_id = RECOVERY_TARGET_LOCK;
    uint32_t from_node = 0;
    uint32_t failed_node = RECOVERY_FAILED_NODE;
    uint32_t replacement_node = RECOVERY_REPLACEMENT_NODE;
    uint32_t frontier_host = RECOVERY_FAILED_NODE;
    uint64_t live_mask = 0;
    uint64_t cas_frontier = 0;
    uint64_t ticket_frontier = 0;
    uint64_t ticket_turn = 0;
    RecoveryRegionCred frontier_cred{};
    RecoveryRegionCred turn_cred{};
    std::array<RecoveryRegionCred, MAX_REPLICAS> log_creds{};
};

struct RecoveryRoute {
    uint32_t epoch = 0;
    uint32_t frontier_host = RECOVERY_FAILED_NODE;
    bool recovering = false;
    uint64_t live_mask = 0;
    RecoveryRegionCred frontier{};
    RecoveryRegionCred turn{};
    std::array<RecoveryRegionCred, MAX_REPLICAS> log_creds{};
};

struct alignas(64) LocalState {
    uint64_t frontier_values[MAX_REPLICAS];
    uint64_t cas_results[MAX_REPLICAS];
    uint64_t learn_results[MAX_REPLICAS];
    uint64_t next_frontier;
    uint64_t metadata;
    uint64_t notify_signal;
};
template <typename T, size_t Size>
class Queue {
    std::array<T, Size> buffer{};
    size_t head = 0;
    size_t tail = 0;
    size_t count = 0;

public:
    void push(T item) {
        if (count == Size) throw std::runtime_error("Queue full");
        buffer[tail] = item;
        tail = (tail + 1) % Size;
        count++;
    }

    bool pop(T& item) {
        if (count == 0) return false;
        item = buffer[head];
        head = (head + 1) % Size;
        count--;
        return true;
    }

    [[nodiscard]] size_t size() const { return count; }
};

// ─── Helpers ───

inline unsigned int get_uint_env(const std::string& name) {
    const char* val = std::getenv(name.c_str());
    if (!val || std::string(val).empty()) {
        throw std::runtime_error("Environment variable '" + name + "' is not set or empty");
    }
    return static_cast<unsigned int>(std::stoul(val));
}

inline unsigned int get_uint_env_or(const std::string& name, const unsigned int fallback) {
    const char* val = std::getenv(name.c_str());
    if (!val || std::string(val).empty()) return fallback;
    return static_cast<unsigned int>(std::stoul(val));
}

inline double get_double_env_or(const std::string& name, const double fallback) {
    const char* val = std::getenv(name.c_str());
    if (!val || std::string(val).empty()) return fallback;
    return std::stod(val);
}

inline bool is_recovery_target_lock(const uint32_t lock_id) {
    return lock_id == RECOVERY_TARGET_LOCK;
}

inline uint64_t recovery_live_mask_all_nodes() {
    uint64_t mask = 0;
    for (size_t i = 0; i < CLUSTER_NODES.size(); ++i) {
        mask |= (1ULL << i);
    }
    return mask;
}

inline bool recovery_live_mask_contains(const uint64_t mask, const uint32_t node_id) {
    return ((mask >> node_id) & 1ULL) != 0;
}

inline size_t recovery_live_node_count(const uint64_t mask) {
    size_t count = 0;
    for (size_t i = 0; i < CLUSTER_NODES.size(); ++i) {
        if (recovery_live_mask_contains(mask, static_cast<uint32_t>(i))) {
            count++;
        }
    }
    return count;
}

inline uint64_t quorum_median(std::vector<uint64_t> values) {
    if (values.empty()) {
        throw std::runtime_error("quorum_median: empty input");
    }
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

inline void* allocate_server_buffer(size_t num_locks = MAX_LOCKS) {
    void* ptr = allocate_hugepage_buffer(SERVER_ALIGNED_SIZE);
    if (!ptr) throw std::runtime_error("Could not allocate server RDMA buffer");
    std::memset(ptr, 0xFF, SERVER_ALIGNED_SIZE);

    // zero lock headers (FAA counters)
    auto* base = static_cast<char*>(ptr);
    for (size_t l = 0; l < num_locks; ++l) {
        *reinterpret_cast<uint64_t*>(base + lock_control_offset(l)) = 0;
        *reinterpret_cast<uint64_t*>(base + lock_turn_offset(l)) = 0;
    }

    *reinterpret_cast<uint64_t*>(base + recovery_frontier_control_offset()) = 0;
    *reinterpret_cast<uint64_t*>(base + recovery_frontier_turn_offset()) = 0;
    *reinterpret_cast<uint64_t*>(base + recovery_metadata_offset()) = 0;
    for (size_t slot = 0; slot < MAX_LOG_PER_LOCK; ++slot) {
        *reinterpret_cast<uint64_t*>(base + recovery_log_slot_offset(slot)) = EMPTY_SLOT;
    }

    return ptr;
}

inline void* allocate_client_buffer(size_t requested_size = CLIENT_ALIGNED_SIZE) {
    const size_t aligned_size = std::max(align_up(requested_size, PAGE_SIZE), PAGE_SIZE);
    void* ptr = allocate_hugepage_buffer(aligned_size);
    if (!ptr) throw std::runtime_error("Could not allocate client RDMA buffer");
    std::memset(ptr, 0xFF, aligned_size);
    return ptr;
}

inline void pin_thread_to_cpu(const int cpu) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        throw std::runtime_error("pthread_setaffinity_np failed");
    }
}

inline int pick_cpu_for_client(const int client_id) {
    static const int CPU_ORDER[16] = {
        0, 2, 4, 6, 8, 10, 12, 14,
        1, 3, 5, 7, 9, 11, 13, 15
    };
    return CPU_ORDER[client_id % 16];
}
