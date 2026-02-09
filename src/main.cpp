#include <chrono>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <ranges>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <vector>
#include <netinet/in.h>
#include <x86intrin.h>
#include <sys/mman.h>

const std::vector<std::string> CLUSTER_NODES = {
    "192.168.1.1",
    "192.168.1.2",
    "192.168.1.3",
};

unsigned int get_node_id() {
    const char* id = std::getenv("NODE_ID");
    if (!id) throw std::runtime_error("NODE_ID not set");
    return std::stoi(id);
}

struct Peer {
    uint32_t node_id;
    rdma_cm_id* id;
    uintptr_t remote_log_base;
    uint32_t remote_rkey;
};

constexpr uint16_t RDMA_PORT = 6969;

constexpr size_t MAX_LOG_ENTRIES = 1000000;
constexpr size_t ENTRY_SIZE = 8;
constexpr size_t TOTAL_POOL_SIZE = MAX_LOG_ENTRIES * ENTRY_SIZE;
constexpr size_t COMMIT_INDEX_OFFSET = TOTAL_POOL_SIZE;
constexpr size_t METADATA_SIZE = 4096;
constexpr size_t FINAL_POOL_SIZE = TOTAL_POOL_SIZE + METADATA_SIZE;

constexpr size_t PIPE_DEPTH = 64;
constexpr size_t MAX_PEERS = 3;

alignas(64) std::array<std::array<uint32_t, PIPE_DEPTH>, MAX_PEERS> COMMIT_VALUES;
std::array<std::array<ibv_send_wr, PIPE_DEPTH>, MAX_PEERS> LOG_WRS;
std::array<std::array<ibv_sge, PIPE_DEPTH>, MAX_PEERS> LOG_SGES;
std::array<std::array<ibv_send_wr, PIPE_DEPTH>, MAX_PEERS> COMMIT_WRS;
std::array<std::array<ibv_sge, PIPE_DEPTH>, MAX_PEERS> COMMIT_SGES;

ibv_send_wr* build_propose_wr(
    const uint32_t log_index,
    const Peer& peer,
    const char* local_log,
    const ibv_mr* mr,
    ibv_send_wr* next_wr
) {
    const uint32_t slot = log_index % PIPE_DEPTH;
    ibv_send_wr& swr = LOG_WRS[peer.node_id][slot];
    ibv_sge& sge = LOG_SGES[peer.node_id][slot];

    const char* current_entry = local_log + (slot * ENTRY_SIZE);
    sge.addr = reinterpret_cast<uintptr_t>(current_entry);
    sge.length = ENTRY_SIZE;
    sge.lkey = mr->lkey;

    swr.wr_id = log_index;
    swr.opcode = IBV_WR_RDMA_WRITE;
    swr.sg_list = &sge;
    swr.num_sge = 1;
    swr.send_flags = IBV_SEND_SIGNALED;
    swr.next = next_wr;

    swr.wr.rdma.remote_addr = peer.remote_log_base + (slot * ENTRY_SIZE);
    swr.wr.rdma.rkey = peer.remote_rkey;
    return &swr;
}

void run_leader_mu(
    const unsigned int node_id,
    const std::vector<Peer>& peers, char* local_log, const ibv_mr* local_mr) {
    std::array<uint32_t, MAX_LOG_ENTRIES> acks{};
    const uint32_t majority = peers.size() - 2;

    ibv_cq* cq = peers[1].id->qp->send_cq;

    for (const auto& peer : peers) {
        if (peer.node_id == node_id || !peer.id) continue;

        ibv_send_wr* head = nullptr;
        ibv_send_wr* bad_wr;
        for (int i = PIPE_DEPTH - 1; i >= 0; --i) {
            head = build_propose_wr(i, peer, local_log, local_mr, head);
        }
        if (ibv_post_send(peer.id->qp, head, &bad_wr)) {

        }
    }

    ibv_wc wc[16];
    while (true) {
        const int n = ibv_poll_cq(cq, 16, wc);
        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) continue;
            if (const uint32_t acked_idx = wc[i].wr_id; ++acks[acked_idx] >= majority) {
                std::cout << "Got majority for: " << acked_idx << "\n";
            }
        }
    }
}

void run_follower_mu(const unsigned int node_id, const char* log_pool) {
    uint32_t last_applied = 0;
    const volatile auto* remote_commit_ptr = reinterpret_cast<const volatile uint32_t*>(log_pool + COMMIT_INDEX_OFFSET);

    while (true) {
        // if (const uint32_t current_commit = *remote_commit_ptr; current_commit > last_applied) {
        //     std::atomic_thread_fence(std::memory_order_acquire);
        //     for (uint32_t i = last_applied; i < current_commit; ++i) {
        //         const uint32_t log_idx = i % MAX_LOG_ENTRIES;
        //         const char* entry_data = log_pool + (log_idx * ENTRY_SIZE);
        //         std::cout << "Applying index: " << log_idx << "\n";
        //     }
        //
        //     last_applied = current_commit;
        //     std::atomic_thread_fence(std::memory_order_release);
        // }
    }
}

std::atomic<uint64_t> global_ops_count{0};
std::atomic<uint64_t> global_total_latency_ns{0};
std::atomic<uint64_t> last_op_latency_ns{0};


void monitor_performance() {
    auto last_time = std::chrono::steady_clock::now();
    uint64_t last_count = 0;
    uint64_t last_latency_sum = 0;

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto now = std::chrono::steady_clock::now();

        uint64_t current_count = global_ops_count.load(std::memory_order_relaxed);
        uint64_t current_latency_sum = global_total_latency_ns.load(std::memory_order_relaxed);

        uint64_t delta_ops = current_count - last_count;
        uint64_t delta_latency = current_latency_sum - last_latency_sum;

        if (delta_ops > 0) {
            double avg_us = (static_cast<double>(delta_latency) / delta_ops) / 1000.0;
            double cur_us = static_cast<double>(last_op_latency_ns.load()) / 1000.0;

            std::cout << "[Monitor] " << delta_ops << " ops/sec | "
                      << "Avg Latency: " << avg_us << " us | "
                      << "Last Op: " << cur_us << " us" << std::endl;
        }

        last_count = current_count;
        last_latency_sum = current_latency_sum;
        last_time = now;
    }
}

void run_leader_sequential(
    const unsigned int node_id,
    const std::vector<Peer>& peers,
    const char* local_log,
    const ibv_mr* local_mr
) {
    std::thread t(monitor_performance);
    t.detach();

    const uint32_t majority = peers.size() - 1;
    ibv_cq* cq = peers[1].id->qp->send_cq;
    uint32_t current_index = 0;

    while (true) {
        // --- START MEASUREMENT ---
        auto start = std::chrono::high_resolution_clock::now();

        const uint32_t slot = current_index % MAX_LOG_ENTRIES;

        for (const auto& peer : peers) {
            if (peer.node_id == node_id || !peer.id) continue;

            // Prepare WR
            ibv_send_wr& swr = LOG_WRS[peer.node_id][0];
            ibv_sge& sge = LOG_SGES[peer.node_id][0];

            const_cast<char*>(local_log + (slot * ENTRY_SIZE))[ENTRY_SIZE - 1] = 1;

            sge.addr = reinterpret_cast<uintptr_t>(local_log + (slot * ENTRY_SIZE));
            sge.length = ENTRY_SIZE;
            sge.lkey = local_mr->lkey;

            swr.wr_id = current_index;
            swr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            swr.sg_list = &sge;
            swr.num_sge = 1;
            swr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
            swr.wr.rdma.remote_addr = peer.remote_log_base + (slot * ENTRY_SIZE);
            swr.wr.rdma.rkey = peer.remote_rkey;
            swr.imm_data = htonl(current_index);

            ibv_send_wr* bad_wr;
            ibv_post_send(peer.id->qp, &swr, &bad_wr);
        }

        // Wait for hardware to confirm majority
        int acks = 0;
        while (acks < majority) {
            ibv_wc wc[16];
            const int n = ibv_poll_cq(cq, 16, wc);
            for (int i = 0; i < n; ++i) {
                if (wc[i].status == IBV_WC_SUCCESS && wc[i].wr_id == current_index) {
                    acks++;
                }
            }
        }


        // --- END MEASUREMENT ---
        auto end = std::chrono::high_resolution_clock::now();
        const uint64_t lat = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

        last_op_latency_ns.store(lat, std::memory_order_relaxed);
        global_total_latency_ns.fetch_add(lat, std::memory_order_relaxed);
        global_ops_count.fetch_add(1, std::memory_order_relaxed);

        current_index++;
    }
}

void run_follower_sequential(const unsigned int node_id, char* log_pool, ibv_cq* cq, ibv_qp* qp) {
    uint32_t current_index = 0;

    for (int i = 0; i < 512; i++) {
        ibv_recv_wr rr{};
        ibv_recv_wr* bad_rr;
        rr.sg_list = nullptr;
        rr.num_sge = 0;
        ibv_post_recv(qp, &rr, &bad_rr);
    }

    while (true) {
        ibv_wc wc{};
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n > 0) {
            if (wc.status != IBV_WC_SUCCESS) continue;
            const uint32_t received_index = be32toh(wc.imm_data);
            const uint32_t slot = received_index % MAX_LOG_ENTRIES;
            char* entry_data = log_pool + (slot * ENTRY_SIZE);

            ibv_recv_wr rr{};
            ibv_recv_wr* bad_rr;
            ibv_post_recv(qp, &rr, &bad_rr);

            current_index++;
            if (current_index % 100000 == 0) {
                std::cout << "[Follower] Processed up to: " << received_index << "\n";
            }
        }
    }
}

struct ConnPrivateData {
    uintptr_t addr;
    uint32_t rkey;
    uint32_t node_id;
} __attribute__((packed));

void run_leader(const uint32_t node_id) {
    std::cout << "[leader] starting\n";

    std::vector<Peer> peers(CLUSTER_NODES.size());
    const uint32_t expected = CLUSTER_NODES.size() - 1;
    rdma_event_channel* ec = rdma_create_event_channel();
    rdma_cm_id* listener = nullptr;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(RDMA_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (!ec) throw std::runtime_error("rdma_create_event_channel failed");
    if (rdma_create_id(ec, &listener, nullptr, RDMA_PS_TCP)) throw std::runtime_error("rdma_create_id failed");
    if (rdma_bind_addr(listener, reinterpret_cast<sockaddr*>(&addr))) throw std::runtime_error("rdma_bind_addr failed");
    if (rdma_listen(listener, 16)) throw std::runtime_error("rdma_listen failed");

    std::cout << "[leader] waiting for " << expected << " nodes\n";

    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;

    int connected = 0;
    while (connected < expected) {
        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(ec, &event)) {
            perror("rdma_get_cm_event");
            break;
        }

        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_cm_id* id = event->id;
            const auto* incoming = (ConnPrivateData*)event->param.conn.private_data;
            if (!incoming || event->param.conn.private_data_len < sizeof(ConnPrivateData)) {
                rdma_reject(event->id, nullptr, 0);
                throw std::runtime_error("Private data too small!");
            }

            const uint32_t nid = incoming->node_id;

            std::cout << "\n--- New Node Connection Data ---" << std::endl;
            std::cout << "Node ID:         " << nid << std::endl;
            std::cout << "Remote Address:  " << std::hex << "0x" << incoming->addr << std::dec << std::endl;
            std::cout << "Remote RKey:     " << "0x" << std::hex << incoming->rkey << std::dec << std::endl;
            std::cout << "Private Data Len: " << (int)event->param.conn.private_data_len << " bytes" << std::endl;
            std::cout << "---------------------------------\n" << std::endl;

            if (!pd) {
                pd = ibv_alloc_pd(id->verbs);
                if (!pd) throw std::runtime_error("ibv_alloc_pd failed");

                cq = ibv_create_cq(id->verbs, 4096, nullptr, nullptr, 0);
                if (!cq) throw std::runtime_error("ibv_create_cq failed");
            }

            ibv_qp_init_attr qp_attr{};
            qp_attr.qp_type = IBV_QPT_RC;
            qp_attr.send_cq = cq;
            qp_attr.recv_cq = cq;
            qp_attr.cap.max_send_wr = 128;
            qp_attr.cap.max_recv_wr = 128;
            qp_attr.cap.max_send_sge = 1;
            qp_attr.cap.max_recv_sge = 1;
            // qp_attr.sq_sig_all = 0;

            if (rdma_create_qp(id, pd, &qp_attr)) {
                rdma_reject(id, nullptr, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            rdma_conn_param accept{};
            if (rdma_accept(id, &accept)) {
                perror("rdma_accept");
                rdma_destroy_qp(id);
                rdma_reject(id, nullptr, 0);
                rdma_ack_cm_event(event);
                continue;
            }
            peers[nid] = Peer{nid, id, incoming->addr, incoming->rkey};

            ++connected;
            std::cout << "[leader] connected node " << nid << "\n";
        }

        rdma_ack_cm_event(event);
    }

    auto log_pool = static_cast<char*>(mmap(NULL, FINAL_POOL_SIZE,
                                        PROT_READ | PROT_WRITE,
                                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                                        -1, 0));
    if (log_pool == MAP_FAILED) {
        perror("mmap hugepages failed, falling back to standard pages");
        log_pool = static_cast<char*>(aligned_alloc(4096, FINAL_POOL_SIZE));
        if (!log_pool) throw std::runtime_error("Failed to allocate log_pool");
    }
    memset(log_pool, 0, FINAL_POOL_SIZE);

    const ibv_mr* mr = ibv_reg_mr(pd, log_pool, FINAL_POOL_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!mr) throw std::runtime_error("ibv_reg_mr failed");

    std::cout << "[leader] all nodes connected\n";
    run_leader_sequential(node_id, peers, log_pool, mr);
}


void run_follower(const unsigned int node_id) {
    std::cout << "[follower " << node_id << "] starting\n";

    rdma_event_channel* ec = rdma_create_event_channel();
    rdma_cm_id* id = nullptr;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(RDMA_PORT);
    inet_pton(AF_INET, CLUSTER_NODES[0].c_str(), &addr.sin_addr);

    if (!ec) throw std::runtime_error("rdma_create_event_channel failed");
    if (rdma_create_id(ec, &id, nullptr, RDMA_PS_TCP)) throw std::runtime_error("rdma_create_id failed");

    rdma_cm_event* event = nullptr;

    if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000)) throw std::runtime_error("rdma_resolve_addr failed");
    rdma_get_cm_event(ec, &event);
    if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        std::cerr << "ADDR_RESOLVE failed. Check if ib0 IPs match. Event: " << event->event << std::endl;
        exit(1);
    }
    rdma_ack_cm_event(event);

    if (rdma_resolve_route(id, 2000)) throw std::runtime_error("rdma_resolve_route failed");
    rdma_get_cm_event(ec, &event);
    if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        std::cerr << "ROUTE_RESOLVE failed. Is Subnet Manager (opensm) running?" << std::endl;
        exit(1);
    }
    rdma_ack_cm_event(event);

    if (!id->verbs) throw std::runtime_error("id->verbs is NULL! The RDMA device was not found.");

    ibv_pd* pd = ibv_alloc_pd(id->verbs);
    if (!pd) throw std::runtime_error("ibv_alloc_pd failed");

    ibv_cq* cq = ibv_create_cq(id->verbs, 256, nullptr, nullptr, 0);
    if (!cq) throw std::runtime_error("ibv_create_cq failed");

    ibv_qp_init_attr qp_attr{};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.cap.max_send_wr = 128;
    qp_attr.cap.max_recv_wr = 128;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = 16;

    if (rdma_create_qp(id, pd, &qp_attr)) throw std::runtime_error("rdma_create_qp failed");

    auto log_pool = static_cast<char*>(mmap(NULL, FINAL_POOL_SIZE,
                                           PROT_READ | PROT_WRITE,
                                           MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                                           -1, 0));
    if (log_pool == MAP_FAILED) {
        perror("mmap hugepages failed, falling back to standard pages");
        log_pool = static_cast<char*>(aligned_alloc(4096, FINAL_POOL_SIZE));
        if (!log_pool) throw std::runtime_error("Failed to allocate log_pool");
    }
    memset(log_pool, 0, FINAL_POOL_SIZE);

    const ibv_mr* mr = ibv_reg_mr(pd, log_pool, FINAL_POOL_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) throw std::runtime_error("ibv_reg_mr failed");

    ConnPrivateData my_info{};
    my_info.addr = reinterpret_cast<uintptr_t>(log_pool);
    my_info.rkey = mr->rkey;
    my_info.node_id = static_cast<uint32_t>(node_id);

    rdma_conn_param param{};
    param.private_data = &my_info;
    param.private_data_len = sizeof(my_info);
    param.responder_resources = 1;
    param.initiator_depth = 1;

    if (rdma_connect(id, &param)) {
        perror("rdma_connect");
        std::exit(1);
    }

    if (rdma_get_cm_event(ec, &event)) {
        perror("rdma_get_cm_event(connect)");
        std::exit(1);
    }

    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        std::cerr << "Connection NOT established. Event: " << event->event << std::endl;
        rdma_ack_cm_event(event);
        exit(1);
    }

    std::cout << "[follower " << node_id << "] Connected and Established!\n";
    rdma_ack_cm_event(event);

    run_follower_sequential(node_id, log_pool, id->recv_cq, id->qp);
}

int main() {
    try {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(1, &cpuset); // Pin to Core 1
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        const unsigned int node_id = get_node_id();
        if (node_id == 0) run_leader(node_id);
        else run_follower(node_id);
    }
    catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
