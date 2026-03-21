#include "rdma/server.h"

// Generic server/node RDMA endpoint setup and shared lock-table MR registration.

#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>
#include <thread>
#include <chrono>

namespace {

constexpr uint64_t kServerPeerControlRecvTag = 0xD100000000000000ULL;
constexpr uint64_t kServerClientControlRecvTag = 0xD200000000000000ULL;
constexpr uint64_t kServerControlSendBaseWrId = 0xD300000000000000ULL;

uint64_t make_server_control_recv_wr_id(const bool from_client, const uint16_t conn_index, const uint16_t slot) {
    return (from_client ? kServerClientControlRecvTag : kServerPeerControlRecvTag)
        | (static_cast<uint64_t>(conn_index) << 16)
        | static_cast<uint64_t>(slot);
}

bool is_server_control_recv_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == kServerPeerControlRecvTag
        || (wr_id & 0xFFFF000000000000ULL) == kServerClientControlRecvTag;
}

bool server_control_from_client(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == kServerClientControlRecvTag;
}

uint16_t server_control_conn_index(const uint64_t wr_id) {
    return static_cast<uint16_t>((wr_id >> 16) & 0xFFFFu);
}

uint16_t server_control_slot_index(const uint64_t wr_id) {
    return static_cast<uint16_t>(wr_id & 0xFFFFu);
}

} // namespace

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
            "Expected " + step + " but got event " + std::to_string(actual)
            + " status " + std::to_string(status));
    }
    return event;
}

// Construct one server endpoint and pre-size the peer/client connection tables.
Server::Server(const uint32_t node_id)
    : node_id_(node_id)
    , peers_(CLUSTER_NODES.size())
    , clients_(TOTAL_CLIENTS)
{
    server_creds_.node_id = node_id;
    server_creds_.type    = ConnType::FOLLOWER;  // node-to-node type
}

// Tear down all server-side RDMA resources and free the huge-page lock-table MR.
Server::~Server() {
    for (auto& c : clients_) {
        if (c.cm_id && c.cm_id->qp) rdma_destroy_qp(c.cm_id);
        if (c.cm_id) rdma_destroy_id(c.cm_id);
    }
    for (auto& p : peers_) {
        if (p.cm_id && p.cm_id->qp) rdma_destroy_qp(p.cm_id);
        if (p.cm_id) rdma_destroy_id(p.cm_id);
    }
    if (recovery_log_mr_) ibv_dereg_mr(recovery_log_mr_);
    if (recovery_turn_mr_) ibv_dereg_mr(recovery_turn_mr_);
    if (recovery_frontier_mr_) ibv_dereg_mr(recovery_frontier_mr_);
    if (control_send_mr_) ibv_dereg_mr(control_send_mr_);
    if (control_mr_) ibv_dereg_mr(control_mr_);
    if (mr_)       ibv_dereg_mr(mr_);
    if (cq_)       ibv_destroy_cq(cq_);
    if (pd_)       ibv_dealloc_pd(pd_);
    if (buf_)      free_hugepage_buffer(buf_, SERVER_ALIGNED_SIZE);
    if (listener_) rdma_destroy_id(listener_);
    if (ec_)       rdma_destroy_event_channel(ec_);
}

void Server::init_recovery_regions() {
    auto* base = static_cast<uint8_t*>(buf_);
    *reinterpret_cast<uint64_t*>(base + recovery_frontier_control_offset()) = 0;
    *reinterpret_cast<uint64_t*>(base + recovery_frontier_turn_offset()) = 0;
    *reinterpret_cast<uint64_t*>(base + recovery_metadata_offset()) = 0;
    for (size_t slot = 0; slot < MAX_LOG_PER_LOCK; ++slot) {
        *reinterpret_cast<uint64_t*>(base + recovery_log_slot_offset(slot)) = EMPTY_SLOT;
    }
}

void Server::register_recovery_regions() {
    auto* base = static_cast<uint8_t*>(buf_);
    recovery_frontier_mr_ = ibv_reg_mr(
        pd_,
        base + recovery_frontier_control_offset(),
        RECOVERY_FRONTIER_REGION_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (!recovery_frontier_mr_) {
        throw std::runtime_error("Server: failed to register recovery frontier MR");
    }

    recovery_turn_mr_ = ibv_reg_mr(
        pd_,
        base + recovery_frontier_turn_offset(),
        RECOVERY_TURN_REGION_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (!recovery_turn_mr_) {
        throw std::runtime_error("Server: failed to register recovery turn MR");
    }

    recovery_log_mr_ = ibv_reg_mr(
        pd_,
        base + recovery_log_region_offset(),
        RECOVERY_LOG_REGION_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (!recovery_log_mr_) {
        throw std::runtime_error("Server: failed to register recovery log MR");
    }

    server_creds_.prototype_frontier = {
        reinterpret_cast<uintptr_t>(base + recovery_frontier_control_offset()),
        recovery_frontier_mr_->rkey,
    };
    server_creds_.prototype_turn = {
        reinterpret_cast<uintptr_t>(base + recovery_frontier_turn_offset()),
        recovery_turn_mr_->rkey,
    };
    server_creds_.prototype_log = {
        reinterpret_cast<uintptr_t>(base + recovery_log_region_offset()),
        recovery_log_mr_->rkey,
    };
}

void Server::reregister_recovery_log_readonly() {
    auto* base = static_cast<uint8_t*>(buf_);
    if (recovery_log_mr_) {
        ibv_dereg_mr(recovery_log_mr_);
        recovery_log_mr_ = nullptr;
    }
    recovery_log_mr_ = ibv_reg_mr(
        pd_,
        base + recovery_log_region_offset(),
        RECOVERY_LOG_REGION_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!recovery_log_mr_) {
        throw std::runtime_error("Server: failed to reregister recovery log MR readonly");
    }
    server_creds_.prototype_log.rkey = recovery_log_mr_->rkey;
}

void Server::reregister_recovery_frontiers_writable() {
    auto* base = static_cast<uint8_t*>(buf_);
    if (recovery_frontier_mr_) {
        ibv_dereg_mr(recovery_frontier_mr_);
        recovery_frontier_mr_ = nullptr;
    }
    if (recovery_turn_mr_) {
        ibv_dereg_mr(recovery_turn_mr_);
        recovery_turn_mr_ = nullptr;
    }

    recovery_frontier_mr_ = ibv_reg_mr(
        pd_,
        base + recovery_frontier_control_offset(),
        RECOVERY_FRONTIER_REGION_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (!recovery_frontier_mr_) {
        throw std::runtime_error("Server: failed to reregister recovery frontier MR writable");
    }

    recovery_turn_mr_ = ibv_reg_mr(
        pd_,
        base + recovery_frontier_turn_offset(),
        RECOVERY_TURN_REGION_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (!recovery_turn_mr_) {
        throw std::runtime_error("Server: failed to reregister recovery turn MR writable");
    }

    server_creds_.prototype_frontier = {
        reinterpret_cast<uintptr_t>(base + recovery_frontier_control_offset()),
        recovery_frontier_mr_->rkey,
    };
    server_creds_.prototype_turn = {
        reinterpret_cast<uintptr_t>(base + recovery_frontier_turn_offset()),
        recovery_turn_mr_->rkey,
    };
}

void Server::reregister_recovery_log_writable() {
    auto* base = static_cast<uint8_t*>(buf_);
    if (recovery_log_mr_) {
        ibv_dereg_mr(recovery_log_mr_);
        recovery_log_mr_ = nullptr;
    }
    recovery_log_mr_ = ibv_reg_mr(
        pd_,
        base + recovery_log_region_offset(),
        RECOVERY_LOG_REGION_SIZE,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
    if (!recovery_log_mr_) {
        throw std::runtime_error("Server: failed to reregister recovery log MR writable");
    }
    server_creds_.prototype_log = {
        reinterpret_cast<uintptr_t>(base + recovery_log_region_offset()),
        recovery_log_mr_->rkey,
    };
}

void Server::reregister_recovery_regions_writable() {
    reregister_recovery_frontiers_writable();
    reregister_recovery_log_writable();
}

void Server::post_control_recvs() {
    const size_t peer_region_size = peers_.size() * RECOVERY_CTRL_RECV_RING;
    for (uint16_t peer_idx = 0; peer_idx < peers_.size(); ++peer_idx) {
        if (peers_[peer_idx].cm_id == nullptr) continue;
        for (uint16_t slot = 0; slot < RECOVERY_CTRL_RECV_RING; ++slot) {
            const size_t buffer_index = static_cast<size_t>(peer_idx) * RECOVERY_CTRL_RECV_RING + slot;
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(&control_recv_buffers_[buffer_index]);
            sge.length = sizeof(RecoveryControlMessage);
            sge.lkey = control_mr_->lkey;

            ibv_recv_wr wr{}, *bad_wr = nullptr;
            wr.wr_id = make_server_control_recv_wr_id(false, peer_idx, slot);
            wr.sg_list = &sge;
            wr.num_sge = 1;

            if (ibv_post_recv(peers_[peer_idx].cm_id->qp, &wr, &bad_wr)) {
                throw std::runtime_error("Server: failed to post peer control recv");
            }
        }
    }

    for (uint16_t client_idx = 0; client_idx < clients_.size(); ++client_idx) {
        if (clients_[client_idx].cm_id == nullptr) continue;
        for (uint16_t slot = 0; slot < RECOVERY_CTRL_RECV_RING; ++slot) {
            const size_t buffer_index = peer_region_size
                + static_cast<size_t>(client_idx) * RECOVERY_CTRL_RECV_RING + slot;
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(&control_recv_buffers_[buffer_index]);
            sge.length = sizeof(RecoveryControlMessage);
            sge.lkey = control_mr_->lkey;

            ibv_recv_wr wr{}, *bad_wr = nullptr;
            wr.wr_id = make_server_control_recv_wr_id(true, client_idx, slot);
            wr.sg_list = &sge;
            wr.num_sge = 1;

            if (ibv_post_recv(clients_[client_idx].cm_id->qp, &wr, &bad_wr)) {
                throw std::runtime_error("Server: failed to post client control recv");
            }
        }
    }
}

void Server::send_control_message(rdma_cm_id* cm_id, const RecoveryControlMessage& msg, const uint64_t wr_id) {
    if (cm_id == nullptr || cm_id->qp == nullptr) {
        return;
    }

    const size_t peer_slots = peers_.size();
    size_t buffer_index = 0;
    bool found = false;
    for (size_t i = 0; i < peer_slots; ++i) {
        if (peers_[i].cm_id == cm_id) {
            buffer_index = i;
            found = true;
            break;
        }
    }
    if (!found) {
        for (size_t i = 0; i < clients_.size(); ++i) {
            if (clients_[i].cm_id == cm_id) {
                buffer_index = peer_slots + i;
                found = true;
                break;
            }
        }
    }
    if (!found || buffer_index >= control_send_slots_.size()) {
        throw std::runtime_error("Server: control send buffer lookup failed");
    }

    const size_t slot = control_send_slots_[buffer_index]++ % RECOVERY_CTRL_SEND_RING;
    const size_t send_index = buffer_index * RECOVERY_CTRL_SEND_RING + slot;
    if (send_index >= control_send_buffers_.size()) {
        throw std::runtime_error("Server: control send ring index out of range");
    }

    control_send_buffers_[send_index] = msg;
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&control_send_buffers_[send_index]);
    sge.length = sizeof(RecoveryControlMessage);
    sge.lkey = control_send_mr_->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = wr_id != 0 ? wr_id : (kServerControlSendBaseWrId | send_index);
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(cm_id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("Server: failed to send control message");
    }
}

void Server::broadcast_control_message(const RecoveryControlMessage& msg, const bool include_failed_peer) {
    for (const auto& peer : peers_) {
        if (peer.cm_id == nullptr || peer.id == node_id_) continue;
        if (!include_failed_peer && peer.id == RECOVERY_FAILED_NODE && peer.id != RECOVERY_COORD_NODE) continue;
        send_control_message(peer.cm_id, msg);
    }
    for (const auto& client : clients_) {
        if (client.cm_id == nullptr) continue;
        send_control_message(client.cm_id, msg);
    }
}

bool Server::poll_control_completion(const ibv_wc& wc, RecoveryControlMessage& out_msg, bool& from_client, uint32_t& sender_id) {
    if ((wc.opcode & IBV_WC_RECV) == 0 || !is_server_control_recv_wr_id(wc.wr_id)) {
        return false;
    }
    from_client = server_control_from_client(wc.wr_id);
    const uint16_t conn_index = server_control_conn_index(wc.wr_id);
    const uint16_t slot = server_control_slot_index(wc.wr_id);
    if (wc.status != IBV_WC_SUCCESS) {
        throw std::runtime_error(
            std::string("Server: control recv completion failed status=")
            + ibv_wc_status_str(wc.status)
            + "(" + std::to_string(wc.status) + ")"
            + " opcode=" + std::to_string(wc.opcode)
            + " wr_id=" + std::to_string(wc.wr_id)
            + " from_client=" + (from_client ? std::string("true") : std::string("false"))
            + " conn_index=" + std::to_string(conn_index)
            + " slot=" + std::to_string(slot)
            + " vendor_err=" + std::to_string(wc.vendor_err));
    }
    if (slot >= RECOVERY_CTRL_RECV_RING) {
        throw std::runtime_error("Server: control recv slot out of range");
    }

    const size_t peer_region_size = peers_.size() * RECOVERY_CTRL_RECV_RING;
    const size_t buffer_index = from_client
        ? peer_region_size + static_cast<size_t>(conn_index) * RECOVERY_CTRL_RECV_RING + slot
        : static_cast<size_t>(conn_index) * RECOVERY_CTRL_RECV_RING + slot;
    if (buffer_index >= control_recv_buffers_.size()) {
        throw std::runtime_error("Server: control recv buffer index out of range");
    }
    out_msg = control_recv_buffers_[buffer_index];
    sender_id = out_msg.from_node;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&control_recv_buffers_[buffer_index]);
    sge.length = sizeof(RecoveryControlMessage);
    sge.lkey = control_mr_->lkey;

    ibv_recv_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = wc.wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    rdma_cm_id* cm_id = nullptr;
    if (from_client) {
        if (conn_index >= clients_.size()) {
            throw std::runtime_error("Server: client control recv connection index out of range");
        }
        cm_id = clients_[conn_index].cm_id;
    } else {
        if (conn_index >= peers_.size()) {
            throw std::runtime_error("Server: peer control recv connection index out of range");
        }
        cm_id = peers_[conn_index].cm_id;
    }

    if (cm_id != nullptr && ibv_post_recv(cm_id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("Server: failed to repost control recv");
    }
    return true;
}

// ─── Active connect to a peer node ───

// Actively connect to a lower-id peer node and exchange MR credentials.
RemoteConnection Server::connect_to_node(const std::string& ip, uint16_t port) {
    rdma_event_channel* outbound_ec = rdma_create_event_channel();
    if (!outbound_ec) throw std::runtime_error("rdma_create_event_channel failed");

    rdma_cm_id* cm_id = nullptr;
    if (rdma_create_id(outbound_ec, &cm_id, nullptr, RDMA_PS_TCP)) {
        rdma_destroy_event_channel(outbound_ec);
        throw std::runtime_error("rdma_create_id failed");
    }

    try {

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

        if (rdma_resolve_addr(cm_id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000))
            throw std::runtime_error("resolve_addr failed for " + ip);

        auto* ev = wait_for_event(outbound_ec, RDMA_CM_EVENT_ADDR_RESOLVED, "ADDR_RESOLVE");
        rdma_ack_cm_event(ev);

        if (rdma_resolve_route(cm_id, 2000))
            throw std::runtime_error("resolve_route failed for " + ip);

        ev = wait_for_event(outbound_ec, RDMA_CM_EVENT_ROUTE_RESOLVED, "ROUTE_RESOLVE");
        rdma_ack_cm_event(ev);

        // init RDMA resources on first connection
        if (!pd_) {
            pd_ = ibv_alloc_pd(cm_id->verbs);
            if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

            cq_ = ibv_create_cq(cm_id->verbs,
                                 QP_DEPTH * (TOTAL_CLIENTS + CLUSTER_NODES.size()),
                                 nullptr, nullptr, 0);
            if (!cq_) throw std::runtime_error("ibv_create_cq failed");

            mr_ = ibv_reg_mr(pd_, buf_, SERVER_ALIGNED_SIZE,
                             IBV_ACCESS_LOCAL_WRITE |
                             IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ |
                             IBV_ACCESS_REMOTE_ATOMIC);
            if (!mr_) throw std::runtime_error("ibv_reg_mr failed");

            const size_t endpoint_count = TOTAL_CLIENTS + CLUSTER_NODES.size();
            control_recv_buffers_.resize(endpoint_count * RECOVERY_CTRL_RECV_RING);
            control_mr_ = ibv_reg_mr(
                pd_,
                control_recv_buffers_.data(),
                control_recv_buffers_.size() * sizeof(RecoveryControlMessage),
                IBV_ACCESS_LOCAL_WRITE);
            if (!control_mr_) throw std::runtime_error("ibv_reg_mr failed for server control recv buffers");

            control_send_buffers_.resize(endpoint_count * RECOVERY_CTRL_SEND_RING);
            control_send_slots_.assign(endpoint_count, 0);
            control_send_mr_ = ibv_reg_mr(
                pd_,
                control_send_buffers_.data(),
                control_send_buffers_.size() * sizeof(RecoveryControlMessage),
                IBV_ACCESS_LOCAL_WRITE);
            if (!control_send_mr_) throw std::runtime_error("ibv_reg_mr failed for server control send buffers");

            init_recovery_regions();
            register_recovery_regions();

            server_creds_.addr = reinterpret_cast<uintptr_t>(buf_);
            server_creds_.rkey = mr_->rkey;
        }

        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type            = IBV_QPT_RC;
        qp_attr.send_cq            = cq_;
        qp_attr.recv_cq            = cq_;
        qp_attr.cap.max_send_wr    = QP_DEPTH;
        qp_attr.cap.max_recv_wr    = QP_DEPTH;
        qp_attr.cap.max_send_sge   = 1;
        qp_attr.cap.max_recv_sge   = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;
        qp_attr.sq_sig_all         = 0;

        if (rdma_create_qp(cm_id, pd_, &qp_attr))
            throw std::runtime_error("rdma_create_qp failed for " + ip);

        ConnPrivateData priv = server_creds_;

        rdma_conn_param param{};
        param.private_data = &priv;
        param.private_data_len = sizeof(priv);
        param.responder_resources = RDMA_RESPONDER_RESOURCES;
        param.initiator_depth = RDMA_INITIATOR_DEPTH;
        param.rnr_retry_count = 7;

        if (rdma_connect(cm_id, &param))
            throw std::runtime_error("rdma_connect failed for " + ip);

        ev = wait_for_event(outbound_ec, RDMA_CM_EVENT_ESTABLISHED, "ESTABLISHED");

        RemoteConnection conn{};
        if (ev->param.conn.private_data &&
            ev->param.conn.private_data_len >= sizeof(ConnPrivateData)) {
            auto* remote = static_cast<const ConnPrivateData*>(
                ev->param.conn.private_data);
            conn = {
                remote->node_id,
                cm_id,
                remote->addr,
                remote->rkey,
                remote->type,
                remote->prototype_frontier,
                remote->prototype_turn,
                remote->prototype_log,
            };
        }
        rdma_ack_cm_event(ev);
        rdma_destroy_event_channel(outbound_ec);
        return conn;
    } catch (...) {
        if (cm_id && cm_id->qp) rdma_destroy_qp(cm_id);
        if (cm_id) rdma_destroy_id(cm_id);
        rdma_destroy_event_channel(outbound_ec);
        throw;
    }
}

// ─── Main startup: mesh nodes + accept clients ───

// Start the server listener, build the server mesh, accept clients, then enter
// the subclass-specific run loop.
void Server::start(uint16_t port) {
    ec_ = rdma_create_event_channel();
    if (!ec_) throw std::runtime_error("rdma_create_event_channel failed");

    if (rdma_create_id(ec_, &listener_, nullptr, RDMA_PS_TCP))
        throw std::runtime_error("rdma_create_id failed");

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (rdma_bind_addr(listener_, reinterpret_cast<sockaddr*>(&addr)))
        throw std::runtime_error("rdma_bind_addr failed");
    if (rdma_listen(listener_, 32))
        throw std::runtime_error("rdma_listen failed");

    buf_ = allocate_server_buffer();

    auto* base = static_cast<uint8_t*>(buf_);
    for (uint32_t i = 0; i < MAX_LOCKS; ++i) {
        *reinterpret_cast<volatile uint64_t*>(base + lock_control_offset(i)) = 0;
        *reinterpret_cast<volatile uint64_t*>(base + lock_turn_offset(i)) = 0;
    }

    const size_t num_nodes = CLUSTER_NODES.size();
    const uint32_t num_clients = expected_clients();

    std::cout << "[Server " << node_id_ << "] Listening on port " << port << "\n";

    // ── Phase 1: Connect to all lower-id nodes ──
    for (uint32_t target = 0; target < node_id_; ++target) {
        std::cout << "[Server " << node_id_ << "] Connecting to node " << target << "...\n";
        RemoteConnection conn{};
        bool connected = false;
        for (int attempt = 0; attempt < 60; ++attempt) {
            try {
                conn = connect_to_node(CLUSTER_NODES[target], port);
                connected = true;
                break;
            } catch (const std::exception& e) {
                std::cerr << "[Server " << node_id_ << "] connect attempt "
                          << (attempt + 1) << " to node " << target
                          << " failed: " << e.what() << "\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        if (!connected)
            throw std::runtime_error("Failed to connect to node " + std::to_string(target));
        peers_[target] = conn;
        std::cout << "[Server " << node_id_ << "] Peer " << target << " connected\n";
    }

    // ── Phase 2: Accept from higher-id nodes + all clients ──
    const uint32_t expect_higher = num_nodes - 1 - node_id_;
    uint32_t higher_connected = 0;
    uint32_t clients_connected = 0;

    std::cout << "[Server " << node_id_ << "] Waiting for "
              << expect_higher << " higher nodes + "
              << num_clients << " clients\n";

    while (higher_connected < expect_higher ||
           clients_connected < num_clients) {

        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(ec_, &event))
            throw std::runtime_error("rdma_get_cm_event failed");

        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_ack_cm_event(event);
            continue;
        }

        rdma_cm_id* new_id = event->id;
        auto* incoming = static_cast<const ConnPrivateData*>(
            event->param.conn.private_data);

        if (!incoming ||
            event->param.conn.private_data_len < sizeof(ConnPrivateData)) {
            std::cerr << "[Server " << node_id_ << "] Rejecting connect request: private data len="
                      << event->param.conn.private_data_len
                      << " expected=" << sizeof(ConnPrivateData) << "\n";
            rdma_reject(new_id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        // init RDMA resources if first accepted connection
        if (!pd_) {
            pd_ = ibv_alloc_pd(new_id->verbs);
            if (!pd_) throw std::runtime_error("ibv_alloc_pd failed");

            cq_ = ibv_create_cq(new_id->verbs,
                                 QP_DEPTH * (TOTAL_CLIENTS + num_nodes),
                                 nullptr, nullptr, 0);
            if (!cq_) throw std::runtime_error("ibv_create_cq failed");

            mr_ = ibv_reg_mr(pd_, buf_, SERVER_ALIGNED_SIZE,
                             IBV_ACCESS_LOCAL_WRITE  |
                             IBV_ACCESS_REMOTE_WRITE |
                             IBV_ACCESS_REMOTE_READ  |
                             IBV_ACCESS_REMOTE_ATOMIC);
            if (!mr_) throw std::runtime_error("ibv_reg_mr failed");

            const size_t endpoint_count = TOTAL_CLIENTS + num_nodes;
            control_recv_buffers_.resize(endpoint_count * RECOVERY_CTRL_RECV_RING);
            control_mr_ = ibv_reg_mr(
                pd_,
                control_recv_buffers_.data(),
                control_recv_buffers_.size() * sizeof(RecoveryControlMessage),
                IBV_ACCESS_LOCAL_WRITE);
            if (!control_mr_) throw std::runtime_error("ibv_reg_mr failed for server control recv buffers");

            control_send_buffers_.resize(endpoint_count * RECOVERY_CTRL_SEND_RING);
            control_send_slots_.assign(endpoint_count, 0);
            control_send_mr_ = ibv_reg_mr(
                pd_,
                control_send_buffers_.data(),
                control_send_buffers_.size() * sizeof(RecoveryControlMessage),
                IBV_ACCESS_LOCAL_WRITE);
            if (!control_send_mr_) throw std::runtime_error("ibv_reg_mr failed for server control send buffers");

            init_recovery_regions();
            register_recovery_regions();

            server_creds_.addr = reinterpret_cast<uintptr_t>(buf_);
            server_creds_.rkey = mr_->rkey;
        }

        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type            = IBV_QPT_RC;
        qp_attr.send_cq            = cq_;
        qp_attr.recv_cq            = cq_;
        qp_attr.cap.max_send_wr    = QP_DEPTH;
        qp_attr.cap.max_recv_wr    = QP_DEPTH;
        qp_attr.cap.max_send_sge   = 1;
        qp_attr.cap.max_recv_sge   = 1;
        qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;
        qp_attr.sq_sig_all         = 0;

        if (rdma_create_qp(new_id, pd_, &qp_attr)) {
            std::cerr << "[Server " << node_id_ << "] rdma_create_qp failed for incoming connection from node "
                      << incoming->node_id << " type=" << static_cast<int>(incoming->type) << "\n";
            rdma_reject(new_id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        rdma_conn_param accept_params{};
        accept_params.private_data     = &server_creds_;
        accept_params.private_data_len = sizeof(server_creds_);
        accept_params.responder_resources = RDMA_RESPONDER_RESOURCES;
        accept_params.initiator_depth     = RDMA_INITIATOR_DEPTH;
        accept_params.rnr_retry_count = 7;

        if (rdma_accept(new_id, &accept_params)) {
            std::cerr << "[Server " << node_id_ << "] rdma_accept failed for node "
                      << incoming->node_id << " type=" << static_cast<int>(incoming->type)
                      << " private_data_len=" << sizeof(server_creds_) << "\n";
            rdma_destroy_qp(new_id);
            rdma_ack_cm_event(event);
            continue;
        }

        const uint32_t nid = incoming->node_id;

        if (incoming->type == ConnType::FOLLOWER) {
            peers_[nid] = {
                nid,
                new_id,
                incoming->addr,
                incoming->rkey,
                incoming->type,
                incoming->prototype_frontier,
                incoming->prototype_turn,
                incoming->prototype_log,
            };
            higher_connected++;
            std::cout << "[Server " << node_id_ << "] Peer " << nid << " accepted\n";
        } else if (incoming->type == ConnType::CLIENT) {
            clients_[nid] = {
                nid,
                new_id,
                incoming->addr,
                incoming->rkey,
                incoming->type,
                incoming->prototype_frontier,
                incoming->prototype_turn,
                incoming->prototype_log,
            };
            clients_connected++;
            std::cout << "[Server " << node_id_ << "] Client "
                      << clients_connected << "/" << num_clients << "\n";
        }

        rdma_ack_cm_event(event);
    }

    // Mark self in peers
    peers_[node_id_].id = node_id_;
    peers_[node_id_].prototype_frontier = server_creds_.prototype_frontier;
    peers_[node_id_].prototype_turn = server_creds_.prototype_turn;
    peers_[node_id_].prototype_log = server_creds_.prototype_log;

    std::cout << "[Server " << node_id_ << "] Ready — "
              << (num_nodes - 1) << " peers + "
              << clients_connected << " clients\n";

    post_control_recvs();
    signal_clients_ready();
    run();
}

void Server::signal_clients_ready() {
    const uint32_t num_clients = expected_clients();
    if (num_clients == 0) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    RecoveryControlMessage go_msg{};
    go_msg.type = RecoveryMsgType::go;
    go_msg.epoch = recovery_epoch_;
    go_msg.lock_id = RECOVERY_TARGET_LOCK;
    go_msg.from_node = node_id_;
    go_msg.failed_node = RECOVERY_FAILED_NODE;
    go_msg.replacement_node = RECOVERY_REPLACEMENT_NODE;
    go_msg.frontier_host = RECOVERY_FAILED_NODE;
    go_msg.live_mask = recovery_live_mask_all_nodes();

    for (uint32_t i = 0; i < num_clients; ++i) {
        send_control_message(clients_[i].cm_id, go_msg, kServerControlSendBaseWrId | i);
    }

    uint32_t done = 0;
    while (done < num_clients) {
        ibv_wc wc{};
        int n = ibv_poll_cq(cq_, 1, &wc);
        if (n > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "GO signal failed for wr_id " + std::to_string(wc.wr_id)
                    + " status " + std::to_string(wc.status));
            }
            if ((wc.opcode & IBV_WC_RECV) != 0) {
                RecoveryControlMessage msg{};
                bool from_client = false;
                uint32_t sender_id = 0;
                if (poll_control_completion(wc, msg, from_client, sender_id)) {
                    continue;
                }
            }
            done++;
        }
    }

    std::cout << "[Server " << node_id_ << "] GO sent to " << num_clients << " clients\n";
}
