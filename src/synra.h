#pragma once

#include "temp.h"

inline void run_synra_handler(
    const uint32_t node_id,
    std::vector<RemoteConnection>& peers,
    std::vector<RemoteConnection>& clients,
    char* log_pool,
    ibv_mr* log_mr,
    char* client_pool,
    ibv_mr* client_mr,
    ibv_cq* cq
) {
    std::cout << "[synra handler] Entering main execution loop\n";

    ibv_wc wc[32];
    while (true) {
        const int n = ibv_poll_cq(cq, 32, wc);
        if (n < 0) throw std::runtime_error("ibv_poll_cq failed");

        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::cerr << "Work Completion Error: " << ibv_wc_status_str(wc[i].status) << " (opcode: " << wc[i].opcode << ")\n";
                continue;
            }

            switch (wc[i].opcode) {
                case IBV_WC_RECV:
                    std::cout << "Received a message from a client/peer!\n";

                    break;

                case IBV_WC_RDMA_WRITE:
                    break;

                case IBV_WC_SEND:
                    break;

                default:
                    break;
            }
        }
    }
}

inline void run_synra_node(const uint32_t node_id) {
    std::cout << "[synra node] starting\n";

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

    std::cout << "[synra node] waiting for " << expected_followers << " nodes and " << NUM_CLIENTS << " clients\n";

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

    while (
        // followers_connected < expected_followers ||
        clients_connected < NUM_CLIENTS) {
        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(ec, &event)) {
            throw std::runtime_error("rdma_get_cm_event");
        }

        std::cout << "[DEBUG] Received Event ID: " << event->event
              << " Status: " << event->status << std::endl;

        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_cm_id* id = event->id;
            const auto* incoming = (ConnPrivateData*)event->param.conn.private_data;

            if (!incoming || event->param.conn.private_data_len < sizeof(ConnPrivateData)) {
                std::cerr << "[synra node] Rejecting connection: Private data invalid\n";
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
                std::cout << "[synra node] Connected Follower Node: " << nid << "\n";
            } else if (incoming->type == ConnType::CLIENT) {
                const uint32_t nid = incoming->node_id;
                clients[nid] = RemoteConnection{nid, id, incoming->addr, incoming->rkey, incoming->type};
                clients_connected++;
                std::cout << "[synra node] Connected Client (" << clients_connected << "/" << NUM_CLIENTS << ")\n";
            }
        }

        rdma_ack_cm_event(event);
    }

    run_synra_handler(
        node_id,
        peers,
        clients,
        log_pool,
        log_mr,
        client_pool,
        client_mr,
        cq
    );
}