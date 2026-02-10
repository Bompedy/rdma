#pragma once
#include "temp.h"

inline void run_leader_sequential(
    const unsigned int node_id,
    const std::vector<RemoteConnection>& peers,
    const std::vector<RemoteConnection>& clients,
    const char* local_log,
    const ibv_mr* local_mr,
    const char* client_pool,
    const ibv_mr* client_mr
) {
    const uint32_t majority = peers.size() - 1;
    ibv_cq* cq = peers[1].cm_id->qp->send_cq;
    uint32_t current_index = 0;

    for (int i = 0; i < NUM_CLIENTS; i++) {
        ibv_recv_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = i;
        wr.sg_list = nullptr;
        wr.num_sge = 0;

        if (ibv_post_recv(clients[i].cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("Failed to post initial recv");
        }
    }

    Queue<uint32_t, NUM_CLIENTS> pending_requests;
    bool should_write = true;
    uint32_t inflight_client_id;
    int acks = 0;

    while (true) {
        ibv_wc wc[32];
        const int n = ibv_poll_cq(cq, 32, wc);
        for (int i = 0; i < n; ++i) {
            const auto current_wc = wc[i];
            if (current_wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error("Failed to post and poll completion");
            }
            if (current_wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                const uint32_t client_id = current_wc.imm_data;
                pending_requests.push(client_id);
                ibv_recv_wr next_wr{}, *next_bad = nullptr;
                next_wr.wr_id = client_id;
                if (ibv_post_recv(clients[client_id].cm_id->qp, &next_wr, &next_bad)) {
                    throw std::runtime_error("Failed to post next recv");
                }
            } else if (current_wc.opcode == IBV_WC_RDMA_WRITE) {
                if (current_wc.wr_id == current_index) {
                    if (++acks >= majority) {
                        ibv_send_wr swr {};
                        swr.wr_id = current_index;
                        swr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                        swr.num_sge = 0;
                        swr.send_flags = IBV_SEND_INLINE;
                        swr.wr.rdma.remote_addr = 0;
                        swr.wr.rdma.rkey = 0;
                        swr.imm_data = current_index;
                        ibv_send_wr* bad_wr;
                        std::cout << "Got majority for going to write back for " << current_index << std::endl;
                        if (ibv_post_send(clients[inflight_client_id].cm_id->qp, &swr, &bad_wr)) {
                            throw std::runtime_error("Failed to ack back to client");
                        }

                        should_write = true;
                        acks = 0;
                        current_index++;
                    }
                }
            }
        }

        if (should_write) {
            if (!pending_requests.pop(inflight_client_id)) continue;
            const uint32_t slot = current_index % MAX_LOG_ENTRIES;
            // copy client memory into our local log

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
                swr.imm_data = current_index;
                ibv_send_wr* bad_wr;
                if (ibv_post_send(peer.cm_id->qp, &swr, &bad_wr)) {
                    throw std::runtime_error("Failed to propose to followers");
                }
            }

            should_write = false;
        }
    }
}

inline void run_leader(const uint32_t node_id) {
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
            throw std::runtime_error("rdma_get_cm_event");
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
                cq = ibv_create_cq(id->verbs, QP_DEPTH * 2, nullptr, nullptr, 0);
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
            qp_attr.cap.max_send_wr = QP_DEPTH;
            qp_attr.cap.max_recv_wr = QP_DEPTH;
            qp_attr.cap.max_send_sge = 1;
            qp_attr.cap.max_recv_sge = 1;
            qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;
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
                rdma_destroy_qp(id);
                rdma_ack_cm_event(event);
                throw std::runtime_error("rdma_accept failed");
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

    run_leader_sequential(node_id, peers, clients, log_pool, log_mr, client_pool, client_mr);
}
