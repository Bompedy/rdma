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
        uint32_t node_id;
        uint32_t num_threads;
        uint16_t lid;
        uint16_t pad;
        uint32_t rkey;
        uint64_t addr;
        uint32_t qpns[MAX_THREADS][MAX_REPLICAS];
    };

    void send_all(const int32_t fd, const void *buf, size_t remaining) {
        const auto *cursor = static_cast<const uint8_t *>(buf);
        while (remaining > 0) {
            const auto sent = ::send(fd, cursor, remaining, 0);
            if (sent <= 0) throw std::runtime_error("TCP send failed");
            cursor += sent;
            remaining -= static_cast<size_t>(sent);
        }
    }

    void recv_all(const int32_t fd, void *buf, size_t remaining) {
        auto *cursor = static_cast<uint8_t *>(buf);
        while (remaining > 0) {
            const auto received = recv(fd, cursor, remaining, 0);
            if (received <= 0) throw std::runtime_error("TCP recv failed");
            cursor += received;
            remaining -= static_cast<size_t>(received);
        }
    }
} // namespace

Transport::Transport(
    const uint32_t node_id,
    const std::vector<std::string>& all_ips,
    const uint32_t num_threads,
    const uint16_t tcp_port
) : node_id_(node_id),
    num_nodes_(static_cast<uint32_t>(all_ips.size())),
    num_threads_(num_threads) {
    constexpr uint8_t ib_port = 1;

    if (num_threads_ > MAX_THREADS) throw std::runtime_error("too many threads");
    if (num_nodes_ > MAX_REPLICAS) throw std::runtime_error("too many nodes");

    int32_t device_count = 0;
    auto *device_list = ibv_get_device_list(&device_count);
    if (!device_list || device_count == 0) throw std::runtime_error("no RDMA devices");

    ctx_ = ibv_open_device(device_list[0]);
    ibv_free_device_list(device_list);
    if (!ctx_) throw std::runtime_error("ibv_open_device failed");

    pd_ = ibv_alloc_pd(ctx_);
    if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

    buf_ = mmap(nullptr, BUF_SIZE, PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (buf_ == MAP_FAILED) {
        buf_ = nullptr;
        throw std::runtime_error("mmap hugepages failed");
    }
    std::memset(buf_, 0, BUF_SIZE);

    constexpr int32_t access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                                     IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
    mr_ = ibv_reg_mr(pd_, buf_, BUF_SIZE, access_flags);
    if (!mr_) throw std::runtime_error("ibv_reg_mr failed");

    ibv_port_attr port_attr{};
    if (ibv_query_port(ctx_, ib_port, &port_attr))
        throw std::runtime_error("ibv_query_port failed");

    ibv_qp_attr init_attr{};
    init_attr.qp_state = IBV_QPS_INIT;
    init_attr.port_num = ib_port;
    init_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
    constexpr int32_t init_mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

    for (uint32_t t = 0; t < num_threads_; ++t) {
        cqs_[t] = ibv_create_cq(ctx_, QP_DEPTH * 2, nullptr, nullptr, 0);
        if (!cqs_[t]) throw std::runtime_error("ibv_create_cq failed");

        for (uint32_t n = 0; n < num_nodes_; ++n) {
            if (n == node_id_) continue;

            ibv_qp_init_attr qp_init{};
            qp_init.qp_type = IBV_QPT_RC;
            qp_init.send_cq = cqs_[t];
            qp_init.recv_cq = cqs_[t];
            qp_init.cap = {QP_DEPTH, QP_DEPTH, 1, 1, MAX_INLINE_DATA};
            qp_init.sq_sig_all = 0;

            qps_[t][n] = ibv_create_qp(pd_, &qp_init);
            if (!qps_[t][n]) throw std::runtime_error("ibv_create_qp failed");
            if (ibv_modify_qp(qps_[t][n], &init_attr, init_mask))
                throw std::runtime_error("QP INIT failed");
        }
    }

    NodeInfo local_info{};
    local_info.node_id = node_id_;
    local_info.num_threads = num_threads_;
    local_info.lid = port_attr.lid;
    local_info.rkey = mr_->rkey;
    local_info.addr = reinterpret_cast<uint64_t>(buf_);
    for (uint32_t t = 0; t < num_threads_; ++t)
        for (uint32_t n = 0; n < num_nodes_; ++n)
            if (n != node_id_) local_info.qpns[t][n] = qps_[t][n]->qp_num;

    std::vector<NodeInfo> all_info(num_nodes_);
    all_info[node_id_] = local_info;

    int32_t tcp_fds[MAX_REPLICAS] = {};
    int32_t listen_fd = -1;

    if (node_id_ == 0) {
        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        const int32_t opt = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_port = htons(tcp_port);
        sa.sin_addr.s_addr = INADDR_ANY;
        if (bind(listen_fd, reinterpret_cast<sockaddr *>(&sa), sizeof(sa)) < 0)
            throw std::runtime_error("TCP bind failed");
        if (listen(listen_fd, static_cast<int32_t>(num_nodes_)) < 0)
            throw std::runtime_error("TCP listen failed");

        for (uint32_t i = 0; i < num_nodes_ - 1; ++i) {
            const int32_t client_fd = accept(listen_fd, nullptr, nullptr);
            if (client_fd < 0) throw std::runtime_error("TCP accept failed");
            NodeInfo info{};
            recv_all(client_fd, &info, sizeof(info));
            all_info[info.node_id] = info;
            tcp_fds[info.node_id] = client_fd;
            fprintf(stderr, "[Transport 0] Got node %u\n", info.node_id);
        }

        for (uint32_t n = 1; n < num_nodes_; ++n)
            send_all(tcp_fds[n], all_info.data(), sizeof(NodeInfo) * num_nodes_);
    } else {
        int32_t connect_fd = -1;
        for (int32_t attempt = 0; attempt < 60; ++attempt) {
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
        recv_all(connect_fd, all_info.data(), sizeof(NodeInfo) * num_nodes_);
        tcp_fds[0] = connect_fd;
    }

    for (uint32_t n = 0; n < num_nodes_; ++n) {
        remotes_[n].rkey = all_info[n].rkey;
        remotes_[n].addr = all_info[n].addr;
    }

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
    constexpr int32_t rtr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                                 IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                                 IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    ibv_qp_attr rts_attr{};
    rts_attr.qp_state = IBV_QPS_RTS;
    rts_attr.sq_psn = 0;
    rts_attr.timeout = 14;
    rts_attr.retry_cnt = 7;
    rts_attr.rnr_retry = 7;
    rts_attr.max_rd_atomic = INITIATOR_DEPTH;
    constexpr int32_t rts_mask = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                                 IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;

    for (uint32_t t = 0; t < num_threads_; ++t) {
        for (uint32_t n = 0; n < num_nodes_; ++n) {
            if (n == node_id_) continue;
            rtr_attr.dest_qp_num = all_info[n].qpns[t][node_id_];
            rtr_attr.ah_attr.dlid = all_info[n].lid;
            if (ibv_modify_qp(qps_[t][n], &rtr_attr, rtr_mask))
                throw std::runtime_error("QP RTR failed");
            if (ibv_modify_qp(qps_[t][n], &rts_attr, rts_mask))
                throw std::runtime_error("QP RTS failed");
        }
    }

    uint8_t ready = 1;
    if (node_id_ == 0) {
        for (uint32_t n = 1; n < num_nodes_; ++n) recv_all(tcp_fds[n], &ready, 1);
        for (uint32_t n = 1; n < num_nodes_; ++n) send_all(tcp_fds[n], &ready, 1);
        for (uint32_t n = 1; n < num_nodes_; ++n) close(tcp_fds[n]);
        close(listen_fd);
    } else {
        send_all(tcp_fds[0], &ready, 1);
        recv_all(tcp_fds[0], &ready, 1);
        close(tcp_fds[0]);
    }

    fprintf(stderr, "[Transport %u] Mesh complete — %u nodes, %u threads\n",
            node_id_, num_nodes_, num_threads_);
}

int32_t Transport::faa(
    const uint32_t thread, const uint32_t remote_node,
    const uint64_t local_addr, const uint64_t remote_offset,
    const uint64_t add_value, const uint64_t wr_id,
    const uint32_t flags
) const {
    ibv_sge sge{};
    sge.addr = local_addr;
    sge.length = 8;
    sge.lkey = mr_->lkey;

    ibv_send_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = flags;
    wr.wr.atomic.remote_addr = remotes_[remote_node].addr + remote_offset;
    wr.wr.atomic.rkey = remotes_[remote_node].rkey;
    wr.wr.atomic.compare_add = add_value;

    ibv_send_wr *bad = nullptr;
    return ibv_post_send(qps_[thread][remote_node], &wr, &bad);
}

int32_t Transport::cas(
    const uint32_t thread, const uint32_t remote_node,
    const uint64_t local_addr, const uint64_t remote_offset,
    const uint64_t compare, const uint64_t swap, const uint64_t wr_id,
    const uint32_t flags
) const {
    ibv_sge sge{};
    sge.addr = local_addr;
    sge.length = 8;
    sge.lkey = mr_->lkey;

    ibv_send_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = flags;
    wr.wr.atomic.remote_addr = remotes_[remote_node].addr + remote_offset;
    wr.wr.atomic.rkey = remotes_[remote_node].rkey;
    wr.wr.atomic.compare_add = compare;
    wr.wr.atomic.swap = swap;

    ibv_send_wr *bad = nullptr;
    return ibv_post_send(qps_[thread][remote_node], &wr, &bad);
}

int32_t Transport::read(
    const uint32_t thread, const uint32_t remote_node,
    const uint64_t local_addr, const uint64_t remote_offset,
    const uint32_t length, const uint64_t wr_id,
    const uint32_t flags
) const {
    ibv_sge sge{};
    sge.addr = local_addr;
    sge.length = length;
    sge.lkey = mr_->lkey;

    ibv_send_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = flags;
    wr.wr.rdma.remote_addr = remotes_[remote_node].addr + remote_offset;
    wr.wr.rdma.rkey = remotes_[remote_node].rkey;

    ibv_send_wr *bad = nullptr;
    return ibv_post_send(qps_[thread][remote_node], &wr, &bad);
}

int32_t Transport::write(
    const uint32_t thread, const uint32_t remote_node,
    const uint64_t local_addr, const uint64_t remote_offset,
    const uint32_t length, const uint64_t wr_id,
    const uint32_t flags
) const {
    ibv_sge sge{};
    sge.addr = local_addr;
    sge.length = length;
    sge.lkey = mr_->lkey;

    ibv_send_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = flags;
    wr.wr.rdma.remote_addr = remotes_[remote_node].addr + remote_offset;
    wr.wr.rdma.rkey = remotes_[remote_node].rkey;

    ibv_send_wr *bad = nullptr;
    return ibv_post_send(qps_[thread][remote_node], &wr, &bad);
}

int32_t Transport::send(
    const uint32_t thread, const uint32_t remote_node,
    const uint64_t local_addr, const uint32_t length,
    const uint64_t wr_id, const uint32_t flags
) const {
    ibv_sge sge{};
    sge.addr = local_addr;
    sge.length = length;
    sge.lkey = mr_->lkey;

    ibv_send_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = flags;

    ibv_send_wr *bad = nullptr;
    return ibv_post_send(qps_[thread][remote_node], &wr, &bad);
}

int32_t Transport::post_recv(
    const uint32_t thread, const uint32_t remote_node,
    const uint64_t local_addr, const uint32_t length,
    const uint64_t wr_id
) const {
    ibv_sge sge{};
    sge.addr = local_addr;
    sge.length = length;
    sge.lkey = mr_->lkey;

    ibv_recv_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    ibv_recv_wr *bad = nullptr;
    return ibv_post_recv(qps_[thread][remote_node], &wr, &bad);
}

int32_t Transport::poll(
    const uint32_t thread, ibv_wc* wc, const int32_t max
) const {
    return ibv_poll_cq(cqs_[thread], max, wc);
}

void Transport::poll_one(
    const uint32_t thread, ibv_wc* wc
) const {
    while (ibv_poll_cq(cqs_[thread], 1, wc) == 0);
}

Transport::~Transport() {
    for (uint32_t t = 0; t < num_threads_; ++t)
        for (uint32_t n = 0; n < num_nodes_; ++n)
            if (qps_[t][n]) ibv_destroy_qp(qps_[t][n]);
    for (uint32_t t = 0; t < num_threads_; ++t)
        if (cqs_[t]) ibv_destroy_cq(cqs_[t]);
    if (mr_) ibv_dereg_mr(mr_);
    if (pd_) ibv_dealloc_pd(pd_);
    if (ctx_) ibv_close_device(ctx_);
    if (buf_) munmap(buf_, BUF_SIZE);
}
