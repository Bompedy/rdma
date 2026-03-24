#include "rdma/client.h"

// Client RDMA connection management and local buffer lifetime.
#include "rdma/common.h"

#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <chrono>
#include <sys/mman.h>
#include <poll.h>

#include "rdma/mu_encoding.h"

// Poll event channel with timeout (returns nullptr on timeout)
static rdma_cm_event* poll_for_event(
    rdma_event_channel* ec,
    const rdma_cm_event_type expected,
    const std::string& step,
    int timeout_ms
) {
    pollfd pfd{};
    pfd.fd = ec->fd;
    pfd.events = POLLIN;

    int ret = poll(&pfd, 1, timeout_ms);
    if (ret < 0) {
        throw std::runtime_error("poll() failed during " + step);
    }
    if (ret == 0) {
        return nullptr;  // Timeout
    }

    rdma_cm_event* event = nullptr;
    if (rdma_get_cm_event(ec, &event)) {
        throw std::runtime_error("rdma_get_cm_event failed during " + step);
    }
    if (event->event != expected) {
        const int status = event->status;
        const auto actual = event->event;
        rdma_ack_cm_event(event);
        throw std::runtime_error(
            "Expected " + step + " but got event " + std::to_string(actual)
            + " status " + std::to_string(status));
    }
    return event;
}

// Construct a client shell; RDMA resources are created lazily on connect.
Client::Client(const uint32_t id, const size_t buffer_size)
    : id_(id)
    , buffer_size_(std::max(align_up(buffer_size, PAGE_SIZE), PAGE_SIZE)) {
}

// Tear down all client-side RDMA resources and free the huge-page buffer.
Client::~Client() {
    for (const auto& conn : connections_) {
        if (conn.id && conn.id->qp) rdma_destroy_qp(conn.id);
        if (conn.id) rdma_destroy_id(conn.id);
    }
    if (mr_) ibv_dereg_mr(mr_);
    if (cq_) ibv_destroy_cq(cq_);
    if (pd_) ibv_dealloc_pd(pd_);
    if (buf_) free_hugepage_buffer(buf_, buffer_size_);
    if (ec_) rdma_destroy_event_channel(ec_);
}

// Connect this client to the target server nodes and initialize the local MR/CQ
// on the first successful connection.
void Client::connect(const std::vector<std::string>& node_ips, const uint16_t port) {
    if (!ec_) {
        ec_ = rdma_create_event_channel();
        if (!ec_) throw std::runtime_error("rdma_create_event_channel failed");
    }

    if (!buf_) {
        buf_ = allocate_client_buffer(buffer_size_);
    }

    for (size_t i = 0; i < node_ips.size(); ++i) {
        std::cout << "[Client " << id_ << "] Connecting to " << node_ips[i] << "...\n";

        rdma_cm_id* cm_id = nullptr;
        if (rdma_create_id(ec_, &cm_id, nullptr, RDMA_PS_TCP)) {
            throw std::runtime_error(
                "rdma_create_id failed for node " + std::to_string(i));
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (inet_pton(AF_INET, node_ips[i].c_str(), &addr.sin_addr) <= 0) {
            throw std::runtime_error("Invalid IP: " + node_ips[i]);
        }

        if (rdma_resolve_addr(cm_id, nullptr,
                              reinterpret_cast<sockaddr*>(&addr), 2000)) {
            throw std::runtime_error(
                "rdma_resolve_addr failed for node " + std::to_string(i));
        }

        auto* ev_addr = poll_for_event(ec_, RDMA_CM_EVENT_ADDR_RESOLVED,
                                       "ADDR_RESOLVE", 10000);
        if (!ev_addr) throw std::runtime_error("ADDR_RESOLVE timeout for node " + std::to_string(i));
        rdma_ack_cm_event(ev_addr);

        if (rdma_resolve_route(cm_id, 2000)) {
            throw std::runtime_error(
                "rdma_resolve_route failed for node " + std::to_string(i));
        }

        auto* ev_route = poll_for_event(ec_, RDMA_CM_EVENT_ROUTE_RESOLVED,
                                        "ROUTE_RESOLVE", 10000);
        if (!ev_route) throw std::runtime_error("ROUTE_RESOLVE timeout for node " + std::to_string(i));
        rdma_ack_cm_event(ev_route);

        if (!pd_) {
            pd_ = ibv_alloc_pd(cm_id->verbs);
            if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

            cq_ = ibv_create_cq(
                cm_id->verbs,
                QP_DEPTH * static_cast<int>(std::max<size_t>(node_ips.size() + CLUSTER_NODES.size(), 32)),
                nullptr, nullptr, 0);
            if (!cq_) throw std::runtime_error("ibv_create_cq failed");

            mr_ = ibv_reg_mr(
                pd_, buf_, buffer_size_,
                IBV_ACCESS_LOCAL_WRITE);
            if (!mr_) throw std::runtime_error("ibv_reg_mr failed");
        }

        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.send_cq = cq_;
        qp_attr.recv_cq = cq_;
        qp_attr.cap.max_send_wr = QP_DEPTH;
        qp_attr.cap.max_recv_wr = QP_DEPTH;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;

        if (rdma_create_qp(cm_id, pd_, &qp_attr)) {
            throw std::runtime_error(
                "rdma_create_qp failed for node " + std::to_string(i));
        }

        ConnPrivateData priv{};
        priv.node_id = id_;
        priv.type = ConnType::CLIENT;
        priv.addr = reinterpret_cast<uintptr_t>(buf_);
        priv.rkey = mr_->rkey;

        rdma_conn_param param{};
        param.private_data = &priv;
        param.private_data_len = sizeof(priv);
        param.responder_resources = RDMA_RESPONDER_RESOURCES;
        param.initiator_depth = RDMA_INITIATOR_DEPTH;
        param.rnr_retry_count = 10;

        if (rdma_connect(cm_id, &param)) {
            throw std::runtime_error("rdma_connect failed for node " + std::to_string(i));
        }

        auto* ev_conn = poll_for_event(ec_, RDMA_CM_EVENT_ESTABLISHED, "ESTABLISHED", 10000);
        if (!ev_conn) throw std::runtime_error("ESTABLISHED timeout for node " + std::to_string(i));
        if (!ev_conn->param.conn.private_data ||
            ev_conn->param.conn.private_data_len < sizeof(ConnPrivateData)) {
            rdma_ack_cm_event(ev_conn);
            throw std::runtime_error("No private data from node " + std::to_string(i));
        }

        auto* remote = static_cast<const ConnPrivateData*>(ev_conn->param.conn.private_data);

        connections_.push_back({
            .id = cm_id,
            .addr = remote->addr,
            .rkey = remote->rkey,
        });

        ibv_recv_wr rr{}, *bad_rr = nullptr;
        rr.wr_id = 0xBEEF0000 | (connections_.size() - 1);
        rr.sg_list = nullptr;
        rr.num_sge = 0;
        if (ibv_post_recv(cm_id->qp, &rr, &bad_rr)) {
            throw std::runtime_error("Failed to pre-post GO recv");
        }

        rdma_ack_cm_event(ev_conn);

        std::cout << "[Client " << id_ << "] Connected to node " << i << "\n";
    }

    std::cout << "[Client " << id_ << "] All " << node_ips.size() << " node connections established\n";
}

