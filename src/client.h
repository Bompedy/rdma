#pragma once

void run_client(
    uint32_t client_id,
    rdma_cm_id* id,
    ibv_cq* cq,
    ibv_mr* local_mr,
    uintptr_t remote_addr,
    uint32_t remote_rkey
) {

}

inline void run_clients() {
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
            ibv_cq* cq = ibv_create_cq(id->verbs, QP_DEPTH, nullptr, nullptr, 0);

            char* client_mem = static_cast<char*>(allocate_rdma_buffer());
            const ibv_mr* mr = ibv_reg_mr(pd, client_mem, FINAL_POOL_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
            if (!mr) throw std::runtime_error("ibv_reg_mr failed");

            ibv_qp_init_attr qp_attr{};
            qp_attr.qp_type = IBV_QPT_RC;
            qp_attr.send_cq = cq;
            qp_attr.recv_cq = cq;
            qp_attr.cap.max_send_wr = QP_DEPTH;
            qp_attr.cap.max_recv_wr = QP_DEPTH;
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

            run_client(i, id, cq, const_cast<ibv_mr*>(mr), leader_pool_addr, leader_rkey);
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }
}
