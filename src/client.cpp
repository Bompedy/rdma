#include "rdma/client.h"

// Client RDMA connection management and local buffer lifetime.
#include "rdma/common.h"

#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <chrono>
#include <sys/mman.h>

#include "rdma/mu_encoding.h"

namespace {

constexpr uint64_t kClientControlRecvTag = 0xC100000000000000ULL;

uint64_t make_client_control_recv_wr_id(const uint16_t conn_index, const uint16_t slot) {
    return kClientControlRecvTag
        | (static_cast<uint64_t>(conn_index) << 16)
        | static_cast<uint64_t>(slot);
}

bool is_client_control_recv_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == kClientControlRecvTag;
}

uint16_t client_control_conn_index(const uint64_t wr_id) {
    return static_cast<uint16_t>((wr_id >> 16) & 0xFFFFu);
}

uint16_t client_control_slot_index(const uint64_t wr_id) {
    return static_cast<uint16_t>(wr_id & 0xFFFFu);
}

} // namespace

// Wait for one specific CM event and fail fast if the connection state machine
// deviates from the expected handshake step.
static rdma_cm_event* wait_for_event(
    rdma_event_channel* ec,
    const rdma_cm_event_type expected,
    const std::string& step
) {
    rdma_cm_event* event = nullptr;
    if (rdma_get_cm_event(ec, &event)) {
        throw std::runtime_error("rdma_get_cm_event failed during " + step);
    }
    if (event->event != expected) {
        const int status = event->status;
        const auto actual = event->event;
        rdma_ack_cm_event(event);
        throw std::runtime_error(
            "Expected " + step + " but got event " + std::to_string(actual) + " status " + std::to_string(status));
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
    if (control_send_mr_) ibv_dereg_mr(control_send_mr_);
    if (control_mr_) ibv_dereg_mr(control_mr_);
    if (mr_) ibv_dereg_mr(mr_);
    if (cq_) ibv_destroy_cq(cq_);
    if (pd_) ibv_dealloc_pd(pd_);
    if (buf_) free_hugepage_buffer(buf_, buffer_size_);
    if (ec_) rdma_destroy_event_channel(ec_);
}

void Client::init_recovery_route() {
    recovery_route_ = RecoveryRoute{};
    recovery_route_.epoch = 0;
    recovery_route_.frontier_host = RECOVERY_FAILED_NODE;
    recovery_route_.recovering = false;
    recovery_route_.live_mask = recovery_live_mask_all_nodes();
    for (const auto& conn : connections_) {
        if (conn.node_id >= MAX_REPLICAS) {
            continue;
        }
        recovery_route_.log_creds[conn.node_id] = conn.prototype_log;
        if (conn.node_id == RECOVERY_FAILED_NODE) {
            recovery_route_.frontier = conn.prototype_frontier;
            recovery_route_.turn = conn.prototype_turn;
        }
    }
}

void Client::post_control_recvs(const size_t conn_index) {
    if (conn_index >= connections_.size()) {
        throw std::runtime_error("Client::post_control_recvs: connection index out of range");
    }
    for (uint16_t slot = 0; slot < RECOVERY_CTRL_RECV_RING; ++slot) {
        const size_t buffer_index = conn_index * RECOVERY_CTRL_RECV_RING + slot;
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&control_recv_buffers_[buffer_index]);
        sge.length = sizeof(RecoveryControlMessage);
        sge.lkey = control_mr_->lkey;

        ibv_recv_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_client_control_recv_wr_id(static_cast<uint16_t>(conn_index), slot);
        wr.sg_list = &sge;
        wr.num_sge = 1;

        if (ibv_post_recv(connections_[conn_index].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("Client: failed to post control recv");
        }
    }
}

void Client::send_control_message(const size_t conn_index, const RecoveryControlMessage& msg) {
    if (conn_index >= connections_.size()) {
        throw std::runtime_error("Client::send_control_message: connection index out of range");
    }
    const size_t slot = control_send_slots_[conn_index]++ % RECOVERY_CTRL_SEND_RING;
    const size_t buffer_index = conn_index * RECOVERY_CTRL_SEND_RING + slot;
    control_send_buffers_[buffer_index] = msg;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&control_send_buffers_[buffer_index]);
    sge.length = sizeof(RecoveryControlMessage);
    sge.lkey = control_send_mr_->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = static_cast<uint64_t>(buffer_index);
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = 0;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_send(connections_[conn_index].id->qp, &wr, &bad_wr)) {
        throw std::runtime_error(
            "Client: failed to send control message type="
            + std::to_string(static_cast<int>(msg.type))
            + " conn_index=" + std::to_string(conn_index)
            + " size=" + std::to_string(sizeof(RecoveryControlMessage)));
    }
}

void Client::apply_new_creds(const RecoveryControlMessage& msg) {
    recovery_route_.epoch = msg.epoch;
    recovery_route_.frontier_host = msg.frontier_host;
    recovery_route_.frontier = msg.frontier_cred;
    recovery_route_.turn = msg.turn_cred;
    recovery_route_.live_mask = msg.live_mask;
    recovery_route_.log_creds = msg.log_creds;
}

void Client::handle_control_message(const RecoveryControlMessage& msg, const uint32_t remote_node) {
    switch (msg.type) {
    case RecoveryMsgType::go:
        go_messages_++;
        break;
    case RecoveryMsgType::recovery_start:
    case RecoveryMsgType::baseline_reset_start:
        if (msg.lock_id == RECOVERY_TARGET_LOCK) {
            recovery_route_.recovering = true;
            recovery_route_.epoch = std::max(recovery_route_.epoch, msg.epoch);
            recovery_route_.live_mask = msg.live_mask;
            recovery_route_.frontier_host = msg.replacement_node;
            recovery_retry_pending_ = true;
            recovery_quiesce_sent_ = false;
            if constexpr (RECOVERY_VERBOSE_LOGS) {
                std::cout << "[Client " << id_ << "] Received "
                          << (msg.type == RecoveryMsgType::recovery_start ? "recovery_start" : "baseline_reset_start")
                          << " epoch=" << msg.epoch << "\n";
            }
        }
        break;
    case RecoveryMsgType::recovery_switch:
        break;
    case RecoveryMsgType::new_creds:
        if (msg.lock_id == RECOVERY_TARGET_LOCK) {
            apply_new_creds(msg);
            if constexpr (RECOVERY_VERBOSE_LOGS) {
                std::cout << "[Client " << id_ << "] Installed new creds epoch=" << msg.epoch
                          << " frontier_host=" << msg.frontier_host << "\n";
            }
        }
        break;
    case RecoveryMsgType::recovery_done:
        if (msg.lock_id == RECOVERY_TARGET_LOCK) {
            recovery_route_.epoch = std::max(recovery_route_.epoch, msg.epoch);
            recovery_route_.recovering = false;
            recovery_quiesce_sent_ = false;
            recovery_route_.frontier_host = msg.frontier_host;
            recovery_route_.live_mask = msg.live_mask;
            recovery_route_.log_creds = msg.log_creds;
            if (msg.frontier_cred.addr != 0) {
                recovery_route_.frontier = msg.frontier_cred;
            }
            if (msg.turn_cred.addr != 0) {
                recovery_route_.turn = msg.turn_cred;
            }
            if constexpr (RECOVERY_VERBOSE_LOGS) {
                std::cout << "[Client " << id_ << "] Recovery done epoch=" << msg.epoch
                          << " frontier_host=" << msg.frontier_host << "\n";
            }
        }
        break;
    case RecoveryMsgType::experiment_done:
        experiment_done_ = true;
        recovery_route_.recovering = false;
        recovery_quiesce_sent_ = false;
        break;
    case RecoveryMsgType::client_quiesced:
    case RecoveryMsgType::replica_report:
    case RecoveryMsgType::invalid:
        break;
    }

    (void)remote_node;
}

void Client::maybe_send_recovery_quiesced(const size_t active_ops) {
    if (!recovery_route_.recovering || recovery_quiesce_sent_ || active_ops != 0) {
        return;
    }
    for (size_t i = 0; i < connections_.size(); ++i) {
        if (connections_[i].node_id != RECOVERY_COORD_NODE) {
            continue;
        }
        RecoveryControlMessage msg{};
        msg.type = RecoveryMsgType::client_quiesced;
        msg.epoch = recovery_route_.epoch;
        msg.lock_id = RECOVERY_TARGET_LOCK;
        msg.from_node = id_;
        msg.failed_node = RECOVERY_FAILED_NODE;
        msg.replacement_node = recovery_route_.frontier_host;
        msg.frontier_host = recovery_route_.frontier_host;
        msg.live_mask = recovery_route_.live_mask;
        send_control_message(i, msg);
        if constexpr (RECOVERY_VERBOSE_LOGS) {
            std::cout << "[Client " << id_ << "] Sent client_quiesced epoch="
                      << msg.epoch << " to node " << connections_[i].node_id << "\n";
        }
        recovery_quiesce_sent_ = true;
        return;
    }
    throw std::runtime_error("Client: no connection to recovery coordinator");
}

bool Client::handle_control_completion(const ibv_wc& wc) {
    if ((wc.opcode & IBV_WC_RECV) == 0 || !is_client_control_recv_wr_id(wc.wr_id)) {
        return false;
    }
    const uint16_t conn_index = client_control_conn_index(wc.wr_id);
    const uint16_t slot = client_control_slot_index(wc.wr_id);
    if (wc.status == IBV_WC_WR_FLUSH_ERR) {
        return true;
    }
    if (wc.status != IBV_WC_SUCCESS) {
        throw std::runtime_error(
            std::string("Client: control recv completion failed status=")
            + ibv_wc_status_str(wc.status)
            + "(" + std::to_string(wc.status) + ")"
            + " opcode=" + std::to_string(wc.opcode)
            + " wr_id=" + std::to_string(wc.wr_id)
            + " conn_index=" + std::to_string(conn_index)
            + " slot=" + std::to_string(slot)
            + " vendor_err=" + std::to_string(wc.vendor_err));
    }
    if (conn_index >= connections_.size()) {
        throw std::runtime_error("Client: control recv connection index out of range");
    }
    if (slot >= RECOVERY_CTRL_RECV_RING) {
        throw std::runtime_error("Client: control recv slot out of range");
    }

    const size_t buffer_index = static_cast<size_t>(conn_index) * RECOVERY_CTRL_RECV_RING + slot;
    const RecoveryControlMessage msg = control_recv_buffers_[buffer_index];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&control_recv_buffers_[buffer_index]);
    sge.length = sizeof(RecoveryControlMessage);
    sge.lkey = control_mr_->lkey;

    ibv_recv_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = wc.wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    if (ibv_post_recv(connections_[conn_index].id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("Client: failed to repost control recv");
    }

    handle_control_message(msg, connections_[conn_index].node_id);
    return true;
}

void Client::mark_recovery_retry_pending() {
    recovery_retry_pending_ = true;
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

        auto* ev_addr = wait_for_event(ec_, RDMA_CM_EVENT_ADDR_RESOLVED,
                                       "ADDR_RESOLVE");
        rdma_ack_cm_event(ev_addr);

        if (rdma_resolve_route(cm_id, 2000)) {
            throw std::runtime_error(
                "rdma_resolve_route failed for node " + std::to_string(i));
        }

        auto* ev_route = wait_for_event(ec_, RDMA_CM_EVENT_ROUTE_RESOLVED,
                                        "ROUTE_RESOLVE");
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

            control_recv_buffers_.resize(node_ips.size() * RECOVERY_CTRL_RECV_RING);
            control_mr_ = ibv_reg_mr(
                pd_, control_recv_buffers_.data(),
                control_recv_buffers_.size() * sizeof(RecoveryControlMessage),
                IBV_ACCESS_LOCAL_WRITE);
            if (!control_mr_) throw std::runtime_error("ibv_reg_mr failed for client control buffers");

            control_send_buffers_.resize(node_ips.size() * RECOVERY_CTRL_SEND_RING);
            control_send_slots_.assign(node_ips.size(), 0);
            control_send_mr_ = ibv_reg_mr(
                pd_, control_send_buffers_.data(),
                control_send_buffers_.size() * sizeof(RecoveryControlMessage),
                IBV_ACCESS_LOCAL_WRITE);
            if (!control_send_mr_) throw std::runtime_error("ibv_reg_mr failed for client control send buffers");
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

        auto* ev_conn = wait_for_event(ec_, RDMA_CM_EVENT_ESTABLISHED, "ESTABLISHED");
        if (!ev_conn->param.conn.private_data ||
            ev_conn->param.conn.private_data_len < sizeof(ConnPrivateData)) {
            rdma_ack_cm_event(ev_conn);
            throw std::runtime_error("No private data from node " + std::to_string(i));
        }

        auto* remote = static_cast<const ConnPrivateData*>(ev_conn->param.conn.private_data);

        connections_.push_back({
            .id = cm_id,
            .node_id = remote->node_id,
            .addr = remote->addr,
            .rkey = remote->rkey,
            .prototype_frontier = remote->prototype_frontier,
            .prototype_turn = remote->prototype_turn,
            .prototype_log = remote->prototype_log,
        });

        post_control_recvs(connections_.size() - 1);

        rdma_ack_cm_event(ev_conn);

        std::cout << "[Client " << id_ << "] Connected to node " << i << "\n";
    }

    std::cout << "[Client " << id_ << "] All " << node_ips.size() << " node connections established\n";
    init_recovery_route();
}

