#include "rdma/server.h"

// Generic server/node RDMA endpoint setup and shared lock-table MR registration.

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>
#include <thread>
#include <chrono>

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

Server::Server(const uint32_t node_id)
    : node_id_(node_id)
    , peers_(CLUSTER_NODES.size())
    , clients_(TOTAL_CLIENTS)
{
    server_creds_.node_id = node_id;
    server_creds_.type    = ConnType::FOLLOWER;  // node-to-node type
}

Server::~Server() {
    for (auto& c : clients_) {
        if (c.cm_id && c.cm_id->qp) rdma_destroy_qp(c.cm_id);
        if (c.cm_id) rdma_destroy_id(c.cm_id);
    }
    for (auto& p : peers_) {
        if (p.cm_id && p.cm_id->qp) rdma_destroy_qp(p.cm_id);
        if (p.cm_id) rdma_destroy_id(p.cm_id);
    }
    if (mr_)       ibv_dereg_mr(mr_);
    if (cq_)       ibv_destroy_cq(cq_);
    if (pd_)       ibv_dealloc_pd(pd_);
    if (buf_)      free_hugepage_buffer(buf_, SERVER_ALIGNED_SIZE);
    if (listener_) rdma_destroy_id(listener_);
    if (ec_)       rdma_destroy_event_channel(ec_);
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

        std::cerr << "[DEBUG] Route resolved to " << ip << std::endl;
        if (cm_id->verbs) {
            std::cerr << "[DEBUG] Using device: " << ibv_get_device_name(cm_id->verbs->device)
                      << " port: " << (int)cm_id->port_num << std::endl;
        }
        std::cerr.flush();

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
            conn = {remote->node_id, cm_id, remote->addr, remote->rkey, remote->type};
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

void Server::start(uint16_t port) {
    std::cerr << "[DEBUG] Server::start() called for node " << node_id_ << std::endl;
    std::cerr.flush();

    std::cerr << "[DEBUG] Creating event channel..." << std::endl;
    std::cerr.flush();
    ec_ = rdma_create_event_channel();
    if (!ec_) throw std::runtime_error("rdma_create_event_channel failed");
    std::cerr << "[DEBUG] Event channel created" << std::endl;
    std::cerr.flush();

    std::cerr << "[DEBUG] Creating listener cm_id..." << std::endl;
    std::cerr.flush();
    if (rdma_create_id(ec_, &listener_, nullptr, RDMA_PS_TCP))
        throw std::runtime_error("rdma_create_id failed");
    std::cerr << "[DEBUG] Listener cm_id created" << std::endl;
    std::cerr.flush();

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    // Bind to the specific node IP instead of INADDR_ANY for RDMA device selection
    inet_pton(AF_INET, CLUSTER_NODES[node_id_].c_str(), &addr.sin_addr);

    std::cerr << "[DEBUG] Calling rdma_bind_addr on " << CLUSTER_NODES[node_id_] << ":" << port << "..." << std::endl;
    std::cerr.flush();
    if (rdma_bind_addr(listener_, reinterpret_cast<sockaddr*>(&addr))) {
        std::cerr << "[ERROR] rdma_bind_addr failed with errno=" << errno
                  << " (" << strerror(errno) << ")" << std::endl;
        if (listener_->verbs) {
            std::cerr << "[ERROR] Device was: " << ibv_get_device_name(listener_->verbs->device) << std::endl;
            std::cerr << "[ERROR] Port num: " << (int)listener_->port_num << std::endl;
        } else {
            std::cerr << "[ERROR] listener_->verbs is NULL (no device selected yet)" << std::endl;
        }
        std::cerr.flush();
        throw std::runtime_error("rdma_bind_addr failed: " + std::string(strerror(errno)));
    }
    std::cerr << "[DEBUG] rdma_bind_addr succeeded" << std::endl;
    if (listener_->verbs) {
        std::cerr << "[DEBUG] Bound to device: " << ibv_get_device_name(listener_->verbs->device)
                  << " port: " << (int)listener_->port_num << std::endl;
    }
    std::cerr.flush();

    std::cerr << "[DEBUG] Calling rdma_listen..." << std::endl;
    std::cerr.flush();
    if (rdma_listen(listener_, 32))
        throw std::runtime_error("rdma_listen failed");
    std::cerr << "[DEBUG] rdma_listen succeeded" << std::endl;
    std::cerr.flush();

    std::cerr << "[DEBUG] About to allocate server buffer ("
              << (SERVER_ALIGNED_SIZE / 1024 / 1024) << " MB)..." << std::endl;
    std::cerr.flush();
    buf_ = allocate_server_buffer();
    std::cerr << "[DEBUG] Server buffer allocated!" << std::endl;
    std::cerr.flush();

    std::cerr << "[DEBUG] Initializing " << MAX_LOCKS << " locks..." << std::endl;
    std::cerr.flush();
    auto* base = static_cast<uint8_t*>(buf_);
    for (uint32_t i = 0; i < MAX_LOCKS; ++i) {
        *reinterpret_cast<volatile uint64_t*>(base + lock_control_offset(i)) = 0;
        *reinterpret_cast<volatile uint64_t*>(base + lock_turn_offset(i)) = 0;
        if (i % 200 == 0) {
            std::cerr << "[DEBUG] Initialized " << i << " locks..." << std::endl;
            std::cerr.flush();
        }
    }
    std::cerr << "[DEBUG] All locks initialized!" << std::endl;
    std::cerr.flush();

    std::cerr << "[DEBUG] Getting cluster size..." << std::endl;
    std::cerr.flush();
    const size_t num_nodes = CLUSTER_NODES.size();
    std::cerr << "[DEBUG] num_nodes=" << num_nodes << std::endl;
    std::cerr.flush();

    std::cerr << "[DEBUG] Calling expected_clients()..." << std::endl;
    std::cerr.flush();
    const uint32_t num_clients = expected_clients();
    std::cerr << "[DEBUG] num_clients=" << num_clients << std::endl;
    std::cerr.flush();

    std::cerr << "[DEBUG] About to print 'Listening' message to stdout..." << std::endl;
    std::cerr.flush();
    std::cout << "[Server " << node_id_ << "] Listening on port " << port << std::endl;
    std::cout.flush();
    std::cerr << "[DEBUG] 'Listening' message printed!" << std::endl;
    std::cerr.flush();

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
            } catch (...) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        if (!connected)
            throw std::runtime_error("Failed to connect to node " + std::to_string(target));
        peers_[target] = conn;
        std::cout << "[Server " << node_id_ << "] Peer " << target << " connected\n";
    }

    // ── Phase 2: Accept from higher-id nodes + all clients ──
    std::cerr << "[DEBUG] Entering Phase 2 (accept connections)..." << std::endl;
    std::cerr.flush();
    const uint32_t expect_higher = num_nodes - 1 - node_id_;
    std::cerr << "[DEBUG] expect_higher=" << expect_higher << std::endl;
    std::cerr.flush();
    uint32_t higher_connected = 0;
    uint32_t clients_connected = 0;

    std::cout << "[Server " << node_id_ << "] Waiting for "
              << expect_higher << " higher nodes + "
              << num_clients << " clients\n";
    std::cout.flush();

    while (higher_connected < expect_higher ||
           clients_connected < num_clients) {

        std::cerr << "[DEBUG] Calling rdma_get_cm_event (expecting " << (expect_higher - higher_connected)
                  << " peers, " << (num_clients - clients_connected) << " clients)..." << std::endl;
        std::cerr.flush();

        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(ec_, &event))
            throw std::runtime_error("rdma_get_cm_event failed");

        std::cerr << "[DEBUG] Got event type: " << event->event << std::endl;
        std::cerr.flush();

        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            std::cerr << "[DEBUG] Ignoring event (not CONNECT_REQUEST)" << std::endl;
            std::cerr.flush();
            rdma_ack_cm_event(event);
            continue;
        }

        std::cerr << "[DEBUG] Processing CONNECT_REQUEST..." << std::endl;
        std::cerr.flush();

        rdma_cm_id* new_id = event->id;
        auto* incoming = static_cast<const ConnPrivateData*>(
            event->param.conn.private_data);

        std::cerr << "[DEBUG] Checking private data..." << std::endl;
        std::cerr.flush();
        if (!incoming ||
            event->param.conn.private_data_len < sizeof(ConnPrivateData)) {
            std::cerr << "[DEBUG] Invalid private data, rejecting" << std::endl;
            std::cerr.flush();
            rdma_reject(new_id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }
        std::cerr << "[DEBUG] Private data OK, node_id=" << incoming->node_id << " type=" << (int)incoming->type << std::endl;
        std::cerr.flush();

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

            server_creds_.addr = reinterpret_cast<uintptr_t>(buf_);
            server_creds_.rkey = mr_->rkey;
        }

        std::cerr << "[DEBUG] Creating QP..." << std::endl;
        std::cerr.flush();
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
            std::cerr << "[DEBUG] QP creation failed, rejecting" << std::endl;
            std::cerr.flush();
            rdma_reject(new_id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }
        std::cerr << "[DEBUG] QP created successfully" << std::endl;
        std::cerr.flush();

        std::cerr << "[DEBUG] Calling rdma_accept..." << std::endl;
        std::cerr.flush();
        rdma_conn_param accept_params{};
        accept_params.private_data     = &server_creds_;
        accept_params.private_data_len = sizeof(server_creds_);
        accept_params.responder_resources = RDMA_RESPONDER_RESOURCES;
        accept_params.initiator_depth     = RDMA_INITIATOR_DEPTH;
        accept_params.rnr_retry_count = 7;

        if (rdma_accept(new_id, &accept_params)) {
            std::cerr << "[DEBUG] rdma_accept failed, rejecting" << std::endl;
            std::cerr.flush();
            rdma_destroy_qp(new_id);
            rdma_ack_cm_event(event);
            continue;
        }
        std::cerr << "[DEBUG] rdma_accept succeeded" << std::endl;
        std::cerr.flush();

        std::cerr << "[DEBUG] About to read node_id from incoming..." << std::endl;
        std::cerr.flush();
        const uint32_t nid = incoming->node_id;
        std::cerr << "[DEBUG] Got node_id=" << nid << " type=" << (int)incoming->type << std::endl;
        std::cerr.flush();

        if (incoming->type == ConnType::FOLLOWER) {
            std::cerr << "[DEBUG] Storing peer " << nid << "..." << std::endl;
            std::cerr.flush();
            peers_[nid] = {nid, new_id, incoming->addr, incoming->rkey, incoming->type};
            higher_connected++;
            std::cout << "[Server " << node_id_ << "] Peer " << nid << " accepted\n";
        } else if (incoming->type == ConnType::CLIENT) {
            std::cerr << "[DEBUG] Storing client " << nid << " (clients_ size=" << clients_.size() << ")..." << std::endl;
            std::cerr.flush();
            clients_[nid] = {nid, new_id, incoming->addr, incoming->rkey, incoming->type};
            std::cerr << "[DEBUG] Client stored, incrementing counter..." << std::endl;
            std::cerr.flush();
            clients_connected++;
            std::cerr << "[DEBUG] Printing acceptance message..." << std::endl;
            std::cerr.flush();
            std::cout << "[Server " << node_id_ << "] Client "
                      << clients_connected << "/" << num_clients << "\n";
            std::cout.flush();
            std::cerr << "[DEBUG] Printed acceptance message" << std::endl;
            std::cerr.flush();
        }

        std::cerr << "[DEBUG] About to ack event..." << std::endl;
        std::cerr.flush();
        rdma_ack_cm_event(event);
        std::cerr << "[DEBUG] Event acked" << std::endl;
        std::cerr.flush();
    }

    std::cerr << "[DEBUG] Loop exited! higher_connected=" << higher_connected << " expect_higher=" << expect_higher
              << " clients_connected=" << clients_connected << " num_clients=" << num_clients << std::endl;
    std::cerr.flush();

    // Mark self in peers
    std::cerr << "[DEBUG] Marking self in peers..." << std::endl;
    std::cerr.flush();
    peers_[node_id_].id = node_id_;
    std::cerr << "[DEBUG] Self marked" << std::endl;
    std::cerr.flush();

    std::cerr << "[DEBUG] Printing Ready message..." << std::endl;
    std::cerr.flush();
    std::cout << "[Server " << node_id_ << "] Ready — "
              << (num_nodes - 1) << " peers + "
              << clients_connected << " clients\n";
    std::cout.flush();
    std::cerr << "[DEBUG] Ready message printed" << std::endl;
    std::cerr.flush();

    signal_clients_ready();
    run();
}

void Server::signal_clients_ready() {
    const uint32_t num_clients = expected_clients();
    if (num_clients == 0) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Post all GO sends at once (don't wait one-by-one)
    for (uint32_t i = 0; i < num_clients; ++i) {
        ibv_send_wr swr{}, *bad_wr = nullptr;
        swr.wr_id = 0xBEEF0000 | i;
        swr.opcode = IBV_WR_SEND_WITH_IMM;
        swr.num_sge = 0;
        swr.sg_list = nullptr;
        swr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
        swr.imm_data = htonl(0x60606060);

        if (ibv_post_send(clients_[i].cm_id->qp, &swr, &bad_wr)) {
            throw std::runtime_error("Failed to send GO signal to client " + std::to_string(i));
        }
    }

    // Wait for all completions
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
            done++;
        }
    }

    std::cout << "[Server " << node_id_ << "] GO sent to " << num_clients << " clients\n";
}
