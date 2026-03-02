#pragma once

const std::vector<std::string> CLUSTER_NODES = {
    "192.168.1.1",
    "192.168.1.2",
    "192.168.1.3",
};

const size_t QUORUM = (CLUSTER_NODES.size() / 2) + 1;

inline unsigned int get_uint_env(const std::string& name) {
    const char* val = std::getenv(name.c_str());
    if (!val || std::string(val).empty()) {
        throw std::runtime_error("Environment variable '" + name + "' is not set or empty");
    }

    try {
        return static_cast<unsigned int>(std::stoul(val));
    } catch (const std::exception& e) {
        throw std::runtime_error("Environment variable '" + name + "' has invalid value: " + val);
    }
}

enum class ConnType : uint8_t { FOLLOWER, CLIENT, LEADER };
struct ConnPrivateData {
    uintptr_t addr;
    uint32_t rkey;
    uint32_t node_id;
    ConnType type;

} __attribute__((packed));

struct RemoteConnection {
    uint32_t id;
    rdma_cm_id* cm_id;
    uintptr_t remote_addr;
    uint32_t rkey;
    ConnType type;
};

constexpr uint16_t RDMA_PORT = 6969;

constexpr size_t MAX_LOG_ENTRIES = 10000000;
constexpr size_t ENTRY_SIZE = 8;
constexpr size_t TOTAL_POOL_SIZE = MAX_LOG_ENTRIES * ENTRY_SIZE;
constexpr size_t METADATA_SIZE = 4096;
constexpr size_t FINAL_POOL_SIZE = TOTAL_POOL_SIZE + METADATA_SIZE;
constexpr size_t HUGE_PAGE_SIZE = 2 * 1024 * 1024;
constexpr size_t ALIGNED_SIZE = ((FINAL_POOL_SIZE + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE) * HUGE_PAGE_SIZE;
constexpr size_t QP_DEPTH = 2048;
constexpr size_t MAX_INLINE_DEPTH = 64;
constexpr size_t CLIENT_SLOT_SIZE = 1024;


constexpr size_t NUM_OPS = 2000000;
constexpr size_t NUM_CLIENTS = 4;
constexpr size_t NUM_OPS_PER_CLIENT = NUM_OPS / NUM_CLIENTS;
constexpr size_t NUM_TOTAL_OPS = NUM_OPS_PER_CLIENT * NUM_CLIENTS;
constexpr uint64_t EMPTY_SLOT = 0xFFFFFFFFFFFFFFFF;
constexpr auto FRONTIER_OFFSET = ALIGNED_SIZE - 8;

inline void* allocate_rdma_buffer() {
    void* ptr = mmap(nullptr, ALIGNED_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ptr == MAP_FAILED) {
        std::cerr << "[memory] HugePage allocation failed, falling back to aligned_alloc\n";
        ptr = aligned_alloc(4096, ALIGNED_SIZE);
        if (!ptr) throw std::runtime_error("Critical failure: Could not allocate RDMA buffer.");
    }

    auto* slot_ptr = static_cast<uint64_t*>(ptr);
    constexpr size_t num_uint64s = ALIGNED_SIZE / sizeof(uint64_t);
    std::fill_n(slot_ptr, num_uint64s, 0xFFFFFFFFFFFFFFFF);
    return ptr;
}

template<typename T, size_t Size>
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

struct RemoteNode {
    rdma_cm_id* id;
    uintptr_t addr;
    uint32_t rkey;
};

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
        0,2,4,6,8,10,12,14,
        1,3,5,7,9,11,13,15
    };

    constexpr int NUM_LOGICAL = 16;
    return CPU_ORDER[client_id % NUM_LOGICAL];
}