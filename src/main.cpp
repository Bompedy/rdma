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

const std::vector<std::string> CLUSTER_NODES = {
    "10.10.1.1",
    "10.10.1.2",
    "10.10.1.3",
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
constexpr size_t ENTRY_SIZE = 1024;
constexpr size_t TOTAL_POOL_SIZE = MAX_LOG_ENTRIES * ENTRY_SIZE;

void run_leader_mu(unsigned int node_id, const std::vector<Peer>& peers, char* local_log, const ibv_mr* local_mr) {
    uint32_t log_index = 0;
    std::array<uint32_t, MAX_LOG_ENTRIES> acks{};
    const uint32_t majority = peers.size() - 2;
    const uint32_t commit_index = 0;
    constexpr uint32_t PIPE_DEPTH = 64;
    uint32_t active_pipes = 0;

    ibv_cq* cq = peers[1].id->qp->send_cq;

    for (int i = 0; i < PIPE_DEPTH; i++) {
        char* current_entry = local_log + (log_index * ENTRY_SIZE);
        for (const auto& peer : peers) {
            if (!peer.id || peer.node_id == node_id) continue;

            ibv_sge sge{};
            sge.addr   = reinterpret_cast<uintptr_t>(current_entry);
            sge.length = ENTRY_SIZE;
            sge.lkey   = local_mr->lkey;

            ibv_send_wr swr{}, *bad = nullptr;
            swr.wr_id      = log_index;
            swr.opcode     = IBV_WR_RDMA_WRITE;
            swr.sg_list    = &sge;
            swr.num_sge    = 1;
            swr.send_flags = IBV_SEND_SIGNALED;

            swr.wr.rdma.remote_addr = peer.remote_log_base + (log_index * ENTRY_SIZE);
            swr.wr.rdma.rkey        = peer.remote_rkey;

            if (ibv_post_send(peer.id->qp, &swr, &bad)) {
                std::cerr << "Post send failed!\n";
            }
        }

        log_index = (log_index + 1) % MAX_LOG_ENTRIES;
    }

    ibv_wc wc[16];
    while (true) {
        const int n = ibv_poll_cq(cq, 16, wc);
        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) continue;
            if (const uint32_t acked_idx = wc[i].wr_id; ++acks[acked_idx] >= majority) {
                std::cout << "Got majority for: " << acked_idx << "\n";

                for (const auto& peer : peers) {
                    if (!peer.id || peer.node_id == node_id) continue;

                    char* status_byte_local = local_log + (acked_idx * ENTRY_SIZE) + (ENTRY_SIZE - 1);
                    *status_byte_local = 1;

                    ibv_sge sge_commit{reinterpret_cast<uintptr_t>(status_byte_local), 1, local_mr->lkey};

                    ibv_send_wr swr_commit{}, *bad = nullptr;
                    swr_commit.opcode = IBV_WR_RDMA_WRITE;
                    swr_commit.sg_list = &sge_commit;
                    swr_commit.num_sge = 1;
                    swr_commit.send_flags = 0;

                    // Target ONLY the status byte offset
                    swr_commit.wr.rdma.remote_addr = peer.remote_log_base + (acked_idx * ENTRY_SIZE) + (ENTRY_SIZE - 1);
                    swr_commit.wr.rdma.rkey = peer.remote_rkey;

                    ibv_post_send(peer.id->qp, &swr_commit, &bad);
                }
            }
        }
    }
}

void run_follower_mu(const unsigned int node_id, const rdma_cm_id* id, const char* log_pool, const ibv_mr* mr) {
    uint32_t log_index = 0;
    while (true) {
        const auto status_ptr = const_cast<volatile char*>(log_pool + (log_index * ENTRY_SIZE) + (ENTRY_SIZE - 1));
        while (*status_ptr == 0) {}

        std::atomic_thread_fence(std::memory_order_acquire);
        const char* entry_data = log_pool + (log_index * ENTRY_SIZE);
        std::cout << node_id << " " << "applied log index - " << log_index << "\n";
        std::atomic_thread_fence(std::memory_order_release);
        *status_ptr = 0;

        log_index = (log_index + 1) % MAX_LOG_ENTRIES;
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

    const auto log_pool = static_cast<char*>(aligned_alloc(4096, TOTAL_POOL_SIZE));
    if (!log_pool) throw std::runtime_error("Failed to allocate log_pool");
    memset(log_pool, 0, TOTAL_POOL_SIZE);

    const ibv_mr* mr = ibv_reg_mr(pd, log_pool, TOTAL_POOL_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!mr) throw std::runtime_error("ibv_reg_mr failed");

    std::cout << "[leader] all nodes connected\n";
    run_leader_mu(node_id, peers, log_pool, mr);
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

    rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000);
    rdma_cm_event* event = nullptr;
    rdma_get_cm_event(ec, &event);
    rdma_ack_cm_event(event);
    rdma_resolve_route(id, 2000);
    rdma_get_cm_event(ec, &event);
    rdma_ack_cm_event(event);

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

    if (rdma_create_qp(id, pd, &qp_attr)) throw std::runtime_error("rdma_create_qp failed");

    const auto log_pool = static_cast<char*>(aligned_alloc(4096, TOTAL_POOL_SIZE));
    if (!log_pool) throw std::runtime_error("Failed to allocate log_pool");
    memset(log_pool, 0, TOTAL_POOL_SIZE);

    const ibv_mr* mr = ibv_reg_mr(pd, log_pool, TOTAL_POOL_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) throw std::runtime_error("ibv_reg_mr failed");

    ConnPrivateData my_info{};
    my_info.addr = reinterpret_cast<uintptr_t>(log_pool);
    my_info.rkey = mr->rkey;
    my_info.node_id = static_cast<uint32_t>(node_id);

    rdma_conn_param param{};
    param.private_data = &my_info;
    param.private_data_len = sizeof(my_info);

    if (rdma_connect(id, &param)) {
        perror("rdma_connect");
        std::exit(1);
    }

    if (rdma_get_cm_event(ec, &event)) {
        perror("rdma_get_cm_event(connect)");
        std::exit(1);
    }

    std::cout << "[follower " << node_id << "] connect event: " << event->event << "\n";
    rdma_ack_cm_event(event);
    std::cout << "[follower " << node_id << "] connected to leader\n";
    if (id->pd == nullptr) {
        std::cout << "The pd is null somehow!" << std::endl;
    }
    run_follower_mu(node_id, id, log_pool, mr);
}

int main() {
    try {
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
