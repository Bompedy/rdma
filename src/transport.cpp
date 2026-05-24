#include "rdma/transport.h"
#include "rdma/config.h"

#include <arpa/inet.h>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>
#include <infiniband/verbs.h>

#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0
#endif

namespace {
    struct NodeInfo {
        uint32_t node_id, num_threads;
        uint16_t lid, pad;
        uint32_t rkey;
        uint64_t addr;
        uint32_t qpns[MAX_THREADS][MAX_REPLICAS];
    };

    void send_all(const int fd, const void *buf, size_t remaining) {
        for (const auto *cursor = static_cast<const uint8_t *>(buf); remaining > 0;) {
            const auto sent = send(fd, cursor, remaining, 0);
            if (sent <= 0) throw std::runtime_error("TCP send failed");
            cursor += sent; remaining -= sent;
        }
    }

    void recv_all(const int fd, void *buf, size_t remaining) {
        for (auto *cursor = static_cast<uint8_t *>(buf); remaining > 0;) {
            const auto received = recv(fd, cursor, remaining, 0);
            if (received <= 0) throw std::runtime_error("TCP recv failed");
            cursor += received; remaining -= received;
        }
    }
} // namespace

Transport::Transport(const uint32_t node_id,
                     const std::vector<std::string> &all_ips,
                     const uint32_t num_threads,
                     const uint16_t tcp_port) {
    const auto num_nodes = static_cast<uint32_t>(all_ips.size());
    constexpr uint8_t ib_port = 1;

    if (num_threads > MAX_THREADS) throw std::runtime_error("too many threads");
    if (num_nodes > MAX_REPLICAS) throw std::runtime_error("too many nodes");

    // open device
    int device_count = 0;
    auto *device_list = ibv_get_device_list(&device_count);
    if (!device_list || device_count == 0) throw std::runtime_error("no RDMA devices");

    auto *context = ibv_open_device(device_list[0]);
    ibv_free_device_list(device_list);
    if (!context) throw std::runtime_error("ibv_open_device failed");

    auto *protection_domain = ibv_alloc_pd(context);
    if (!protection_domain) throw std::runtime_error("ibv_alloc_pd failed");

    auto *buffer = mmap(nullptr, BUF_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (buffer == MAP_FAILED) throw std::runtime_error("mmap hugepages failed");
    std::memset(buffer, 0, BUF_SIZE);

    constexpr int access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                                 IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
    auto *memory_region = ibv_reg_mr(protection_domain, buffer, BUF_SIZE, access_flags);
    if (!memory_region) throw std::runtime_error("ibv_reg_mr failed");

    // query port
    ibv_port_attr port_attr{};
    if (ibv_query_port(context, ib_port, &port_attr)) throw std::runtime_error("ibv_query_port failed");

    // create CQs + QPs → INIT
    ibv_cq *cqs[MAX_THREADS] = {};
    ibv_qp *qps[MAX_THREADS][MAX_REPLICAS] = {};

    ibv_qp_attr init_attr{};
    init_attr.qp_state = IBV_QPS_INIT;
    init_attr.port_num = ib_port;
    init_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
    constexpr int init_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    for (uint32_t thread = 0; thread < num_threads; ++thread) {
        cqs[thread] = ibv_create_cq(context, QP_DEPTH * 2, nullptr, nullptr, 0);
        if (!cqs[thread]) throw std::runtime_error("ibv_create_cq failed");

        for (uint32_t node = 0; node < num_nodes; ++node) {
            if (node == node_id) continue;

            ibv_qp_init_attr qp_init{};
            qp_init.qp_type = IBV_QPT_RC;
            qp_init.send_cq = cqs[thread];
            qp_init.recv_cq = cqs[thread];
            qp_init.cap = {QP_DEPTH, 1, 1, 1, MAX_INLINE_DATA};
            qp_init.sq_sig_all = 0;

            qps[thread][node] = ibv_create_qp(protection_domain, &qp_init);
            if (!qps[thread][node]) throw std::runtime_error("ibv_create_qp failed");
            if (ibv_modify_qp(qps[thread][node], &init_attr, init_mask))
                throw std::runtime_error("QP → INIT failed");
        }
    }

    // build local info
    NodeInfo local_info{};
    local_info.node_id = node_id;
    local_info.num_threads = num_threads;
    local_info.lid = port_attr.lid;
    local_info.rkey = memory_region->rkey;
    local_info.addr = reinterpret_cast<uint64_t>(buffer);
    for (uint32_t thread = 0; thread < num_threads; ++thread)
        for (uint32_t node = 0; node < num_nodes; ++node)
            if (node != node_id) local_info.qpns[thread][node] = qps[thread][node]->qp_num;

    // TCP exchange (node 0 = coordinator)
    std::vector<NodeInfo> all_info(num_nodes);
    all_info[node_id] = local_info;

    int tcp_fds[MAX_REPLICAS] = {};
    int listen_fd = -1;

    if (node_id == 0) {
        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        const int opt = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_port = htons(tcp_port);
        sa.sin_addr.s_addr = INADDR_ANY;
        if (bind(listen_fd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa)) < 0)
            throw std::runtime_error("TCP bind failed");
        if (listen(listen_fd, num_nodes) < 0)
            throw std::runtime_error("TCP listen failed");

        for (uint32_t i = 0; i < num_nodes - 1; ++i) {
            const auto client_fd = accept(listen_fd, nullptr, nullptr);
            if (client_fd < 0) throw std::runtime_error("TCP accept failed");
            NodeInfo info{};
            recv_all(client_fd, &info, sizeof(info));
            all_info[info.node_id] = info;
            tcp_fds[info.node_id] = client_fd;
            fprintf(stderr, "[Transport 0] Got node %u\n", info.node_id);
        }

        for (uint32_t node = 1; node < num_nodes; ++node)
            send_all(tcp_fds[node], all_info.data(), sizeof(NodeInfo) * num_nodes);
    } else {
        auto connect_fd = -1;
        for (int attempt = 0; attempt < 60; ++attempt) {
            connect_fd = socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in sa{};
            sa.sin_family = AF_INET;
            sa.sin_port = htons(tcp_port);
            inet_pton(AF_INET, all_ips[0].c_str(), &sa.sin_addr);
            if (connect(connect_fd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa)) == 0) break;
            close(connect_fd);
            connect_fd = -1;
            usleep(500000);
        }
        if (connect_fd < 0) throw std::runtime_error("connect to node 0 failed");

        send_all(connect_fd, &local_info, sizeof(local_info));
        recv_all(connect_fd, all_info.data(), sizeof(NodeInfo) * num_nodes);
        tcp_fds[0] = connect_fd;
    }

    // QPs → RTR → RTS
    ibv_qp_attr rtr_attr{};
    rtr_attr.qp_state = IBV_QPS_RTR;
    rtr_attr.path_mtu = IBV_MTU_4096;
    rtr_attr.rq_psn = 0;
    rtr_attr.max_dest_rd_atomic = RESPONDER_RESOURCES;
    rtr_attr.min_rnr_timer = 12;
    rtr_attr.ah_attr.sl = 0;
    rtr_attr.ah_attr.src_path_bits = 0;
    rtr_attr.ah_attr.port_num = ib_port;
    rtr_attr.ah_attr.is_global = 0;
    constexpr int rtr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                             IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                             IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    ibv_qp_attr rts_attr{};
    rts_attr.qp_state = IBV_QPS_RTS;
    rts_attr.sq_psn = 0;
    rts_attr.timeout = 14;
    rts_attr.retry_cnt = 7;
    rts_attr.rnr_retry = 7;
    rts_attr.max_rd_atomic = INITIATOR_DEPTH;
    constexpr int rts_mask = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                             IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;

    for (uint32_t thread = 0; thread < num_threads; ++thread) {
        for (uint32_t node = 0; node < num_nodes; ++node) {
            if (node == node_id) continue;
            rtr_attr.dest_qp_num = all_info[node].qpns[thread][node_id];
            rtr_attr.ah_attr.dlid = all_info[node].lid;
            if (ibv_modify_qp(qps[thread][node], &rtr_attr, rtr_mask)) throw std::runtime_error("QP → RTR failed");
            if (ibv_modify_qp(qps[thread][node], &rts_attr, rts_mask)) throw std::runtime_error("QP → RTS failed");
        }
    }

    // barrier: everyone done with QP setup before anyone proceeds
    uint8_t ready = 1;
    if (node_id == 0) {
        for (uint32_t node = 1; node < num_nodes; ++node) recv_all(tcp_fds[node], &ready, 1);
        for (uint32_t node = 1; node < num_nodes; ++node) send_all(tcp_fds[node], &ready, 1);
        for (uint32_t node = 1; node < num_nodes; ++node) close(tcp_fds[node]);
        close(listen_fd);
    } else {
        send_all(tcp_fds[0], &ready, 1);
        recv_all(tcp_fds[0], &ready, 1);
        close(tcp_fds[0]);
    }

    fprintf(stderr, "[Transport %u] Mesh complete — %u nodes, %u threads\n", node_id, num_nodes, num_threads);
}

Transport::~Transport() {}
