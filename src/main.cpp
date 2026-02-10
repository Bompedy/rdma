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

unsigned int get_uint_env(const std::string& name) {
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

constexpr size_t MAX_LOG_ENTRIES = 1000000;
constexpr size_t ENTRY_SIZE = 8;
constexpr size_t TOTAL_POOL_SIZE = MAX_LOG_ENTRIES * ENTRY_SIZE;
// constexpr size_t COMMIT_INDEX_OFFSET = TOTAL_POOL_SIZE;
constexpr size_t METADATA_SIZE = 4096;
constexpr size_t FINAL_POOL_SIZE = TOTAL_POOL_SIZE + METADATA_SIZE;
constexpr size_t NUM_OPS = 1000;
constexpr size_t NUM_CLIENTS = 7;
constexpr size_t HUGE_PAGE_SIZE = 2 * 1024 * 1024;
constexpr size_t ALIGNED_SIZE = ((FINAL_POOL_SIZE + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE) * HUGE_PAGE_SIZE;

void* allocate_rdma_buffer() {
    void* ptr = mmap(nullptr, ALIGNED_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ptr == MAP_FAILED) {
        std::cerr << "[memory] HugePage allocation failed, falling back to aligned_alloc\n";
        ptr = aligned_alloc(4096, ALIGNED_SIZE);
        if (!ptr) throw std::runtime_error("Critical failure: Could not allocate RDMA buffer.");
    }
    memset(ptr, 0, ALIGNED_SIZE);
    return ptr;
}

void run_leader_sequential(
    const unsigned int node_id,
    const std::vector<RemoteConnection>& peers,
    const char* local_log,
    const ibv_mr* local_mr
) {
    const uint32_t majority = peers.size() - 1;
    ibv_cq* cq = peers[1].cm_id->qp->send_cq;
    uint32_t current_index = 0;

    while (true) {
        const uint32_t slot = current_index % MAX_LOG_ENTRIES;

        for (const auto& peer : peers) {
            if (peer.id == node_id || !peer.id) continue;

            ibv_send_wr swr {};
            ibv_sge sge {};

            const_cast<char*>(local_log + (slot * ENTRY_SIZE))[ENTRY_SIZE - 1] = 1;

            sge.addr = reinterpret_cast<uintptr_t>(local_log + (slot * ENTRY_SIZE));
            sge.length = ENTRY_SIZE;
            sge.lkey = local_mr->lkey;

            swr.wr_id = current_index;
            swr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            swr.sg_list = &sge;
            swr.num_sge = 1;
            swr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
            swr.wr.rdma.remote_addr = peer.remote_addr + (slot * ENTRY_SIZE);
            swr.wr.rdma.rkey = peer.rkey;
            swr.imm_data = htonl(current_index);

            ibv_send_wr* bad_wr;
            ibv_post_send(peer.cm_id->qp, &swr, &bad_wr);
        }

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


void run_clients() {
    std::vector<std::thread> workers;

    for (int i = 0; i < NUM_CLIENTS; i++) {
        workers.emplace_back([i]() {
            std::cout << "[client " << i << "] starting\n";

            rdma_event_channel* ec = rdma_create_event_channel();
            rdma_cm_id* id = nullptr;
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(RDMA_PORT);

            inet_pton(AF_INET, CLUSTER_NODES[0].c_str(), &addr.sin_addr);

            if (!ec) return;
            if (rdma_create_id(ec, &id, nullptr, RDMA_PS_TCP)) return;

            rdma_cm_event* event = nullptr;

            if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000)) return;
            rdma_get_cm_event(ec, &event);
            if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
                rdma_ack_cm_event(event);
                return;
            }
            rdma_ack_cm_event(event);

            if (rdma_resolve_route(id, 2000)) return;
            rdma_get_cm_event(ec, &event);
            if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
                rdma_ack_cm_event(event);
                return;
            }
            rdma_ack_cm_event(event);

            ibv_pd* pd = ibv_alloc_pd(id->verbs);
            ibv_cq* cq = ibv_create_cq(id->verbs, 16, nullptr, nullptr, 0);

            char* client_mem = static_cast<char*>(allocate_rdma_buffer());
            const ibv_mr* mr = ibv_reg_mr(pd, client_mem, FINAL_POOL_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
            if (!mr) throw std::runtime_error("ibv_reg_mr failed");

            ibv_qp_init_attr qp_attr{};
            qp_attr.qp_type = IBV_QPT_RC;
            qp_attr.send_cq = cq;
            qp_attr.recv_cq = cq;
            qp_attr.cap.max_send_wr = 16;
            qp_attr.cap.max_recv_wr = 16;
            qp_attr.cap.max_send_sge = 1;
            qp_attr.cap.max_recv_sge = 1;

            if (rdma_create_qp(id, pd, &qp_attr)) return;

            ConnPrivateData private_data{};
            private_data.node_id = static_cast<uint32_t>(i);
            private_data.type = ConnType::CLIENT;
            private_data.addr = reinterpret_cast<uintptr_t>(client_mem);
            private_data.rkey = mr->rkey;

            rdma_conn_param param{};
            param.private_data = &private_data;
            param.private_data_len = sizeof(private_data);
            param.responder_resources = 1;
            param.initiator_depth = 1;

            if (rdma_connect(id, &param)) return;
            if (rdma_get_cm_event(ec, &event)) return;
            if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
                rdma_ack_cm_event(event);
                return;
            }

            uintptr_t leader_pool_addr = 0;
            uint32_t leader_rkey = 0;

            if (event->param.conn.private_data &&
                event->param.conn.private_data_len >= sizeof(ConnPrivateData)) {
                auto* creds = static_cast<const ConnPrivateData*>(event->param.conn.private_data);
                leader_pool_addr = creds->addr;
                leader_rkey = creds->rkey;

                std::cout << "[client " << i << "] Leader gave me access to pool at 0x" << std::hex << leader_pool_addr << std::dec << " with rkey " << leader_rkey << "\n";
            }

            rdma_ack_cm_event(event);

            std::cout << "[client " << i << "] Connected to Leader!\n";
            

            while (true) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }
}

void run_leader(const uint32_t node_id) {
    std::cout << "[leader] starting\n";

    std::vector<RemoteConnection> peers(CLUSTER_NODES.size());
    std::vector<RemoteConnection> clients(NUM_CLIENTS);

    const uint32_t expected_followers = CLUSTER_NODES.size() - 1;

    rdma_event_channel* ec = rdma_create_event_channel();
    if (!ec) throw std::runtime_error("rdma_create_event_channel failed");

    rdma_cm_id* listener = nullptr;
    if (rdma_create_id(ec, &listener, nullptr, RDMA_PS_TCP))
        throw std::runtime_error("rdma_create_id failed");

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(RDMA_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (rdma_bind_addr(listener, reinterpret_cast<sockaddr*>(&addr)))
        throw std::runtime_error("rdma_bind_addr failed");

    if (rdma_listen(listener, 16))
        throw std::runtime_error("rdma_listen failed");

    std::cout << "[leader] waiting for " << expected_followers << " nodes and " << NUM_CLIENTS << " clients\n";

    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_mr* log_mr = nullptr;
    ibv_mr* client_mr = nullptr;

    uint32_t followers_connected = 0;
    uint32_t clients_connected = 0;

    char* log_pool = static_cast<char*>(allocate_rdma_buffer());
    char* client_pool = static_cast<char*>(allocate_rdma_buffer());

    ConnPrivateData leader_creds{};
    leader_creds.node_id = node_id;
    leader_creds.type = ConnType::LEADER;

    while (followers_connected < expected_followers || clients_connected < NUM_CLIENTS) {
        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(ec, &event)) {
            perror("rdma_get_cm_event");
            break;
        }

        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_cm_id* id = event->id;
            const auto* incoming = (ConnPrivateData*)event->param.conn.private_data;

            if (!incoming || event->param.conn.private_data_len < sizeof(ConnPrivateData)) {
                std::cerr << "[leader] Rejecting connection: Private data invalid\n";
                rdma_reject(id, nullptr, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            if (!pd) {
                pd = ibv_alloc_pd(id->verbs);
                if (!pd) throw std::runtime_error("ibv_alloc_pd failed");
                cq = ibv_create_cq(id->verbs, 8192, nullptr, nullptr, 0);
                if (!cq) throw std::runtime_error("ibv_create_cq failed");

                log_mr = ibv_reg_mr(pd, log_pool, ALIGNED_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
                client_mr = ibv_reg_mr(pd, client_pool, ALIGNED_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
                if (!log_mr || !client_mr) throw std::runtime_error("MR registration failed");

                leader_creds.addr = reinterpret_cast<uintptr_t>(client_pool);
                leader_creds.rkey = client_mr->rkey;
            }

            ibv_qp_init_attr qp_attr{};
            qp_attr.qp_type = IBV_QPT_RC;
            qp_attr.send_cq = cq;
            qp_attr.recv_cq = cq;
            qp_attr.cap.max_send_wr = 512;
            qp_attr.cap.max_recv_wr = 512;
            qp_attr.cap.max_send_sge = 1;
            qp_attr.cap.max_recv_sge = 1;
            qp_attr.cap.max_inline_data = 64;
            qp_attr.sq_sig_all = 0;

            if (rdma_create_qp(id, pd, &qp_attr)) {
                std::cerr << "[leader] QP creation failed\n";
                rdma_reject(id, nullptr, 0);
                rdma_ack_cm_event(event);
                continue;
            }


            rdma_conn_param accept_params{};
            if (incoming->type == ConnType::CLIENT) {
                accept_params.responder_resources = 1;
                accept_params.initiator_depth = 1;
                accept_params.private_data = &leader_creds;
                accept_params.private_data_len = sizeof(leader_creds);
            }
            if (rdma_accept(id, &accept_params)) {
                perror("rdma_accept");
                rdma_destroy_qp(id);
                rdma_ack_cm_event(event);
                continue;
            }

            if (incoming->type == ConnType::FOLLOWER) {
                const uint32_t nid = incoming->node_id;
                peers[nid] = RemoteConnection{nid, id, incoming->addr, incoming->rkey, incoming->type};
                followers_connected++;
                std::cout << "[leader] Connected Follower Node: " << nid << "\n";
            } else if (incoming->type == ConnType::CLIENT) {
                const uint32_t nid = incoming->node_id;
                clients[nid] = RemoteConnection{nid, id, incoming->addr, incoming->rkey, incoming->type};
                clients_connected++;
                std::cout << "[leader] Connected Client (" << clients_connected << "/" << NUM_CLIENTS << ")\n";
            }
        }

        rdma_ack_cm_event(event);
    }

    std::cout << "[leader] All nodes and clients connected. Registering log at " << std::hex << reinterpret_cast<uintptr_t>(log_pool) << std::dec << "\n";

    // Enter the main sequential replication loop
    // run_leader_sequential(node_id, peers, log_pool, mr);
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
    qp_attr.cap.max_send_wr = 2048;
    qp_attr.cap.max_recv_wr = 2048;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = 64;

    if (rdma_create_qp(id, pd, &qp_attr)) throw std::runtime_error("rdma_create_qp failed");

    char* log_pool = static_cast<char*>(allocate_rdma_buffer());
    const ibv_mr* mr = ibv_reg_mr(pd, log_pool, FINAL_POOL_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) throw std::runtime_error("ibv_reg_mr failed");

    ConnPrivateData private_data{};
    private_data.addr = reinterpret_cast<uintptr_t>(log_pool);
    private_data.rkey = mr->rkey;
    private_data.node_id = static_cast<uint32_t>(node_id);
    private_data.type = ConnType::FOLLOWER;

    rdma_conn_param param{};
    param.private_data = &private_data;
    param.private_data_len = sizeof(private_data);
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

    run_follower_sequential(node_id, log_pool, id->qp->send_cq, id->qp);
}

int main() {
    try {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(1, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        if (get_uint_env("IS_CLIENT") != 0) {
            std::cout << "Starting as a client!" << std::endl;
            run_clients();
        } else {
            const uint32_t node_id = get_uint_env("NODE_ID");
            if (node_id == 0) run_leader(node_id);
            else run_follower(node_id);
        }
    } catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
