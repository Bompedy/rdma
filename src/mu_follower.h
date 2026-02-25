#pragma once

#include "temp.h"

inline void run_follower_sequential_mu(const unsigned int node_id, char* log_pool, ibv_cq* cq, ibv_qp* qp) {
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

            // std::cout << "Follower processed: " << current_index << std::endl;
            current_index++;
            //
            // if (current_index % 100000 == 0) {
            //     std::cout << "[Follower] Processed up to: " << received_index << "\n";
            // }
        }
    }
}

inline void run_follower_mu(const unsigned int node_id) {
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

    ibv_cq* cq = ibv_create_cq(id->verbs, QP_DEPTH * 2, nullptr, nullptr, 0);
    if (!cq) throw std::runtime_error("ibv_create_cq failed");

    ibv_qp_init_attr qp_attr{};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.cap.max_send_wr = QP_DEPTH;
    qp_attr.cap.max_recv_wr = QP_DEPTH;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;

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
        throw std::runtime_error("rdma_connect");
    }

    if (rdma_get_cm_event(ec, &event)) {
        throw std::runtime_error("rdma_get_cm_event(connect) failed");
    }

    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        throw std::runtime_error("rdma_ack_cm_event hum");
    }

    std::cout << "[follower " << node_id << "] Connected and Established!\n";
    rdma_ack_cm_event(event);

    run_follower_sequential_mu(node_id, log_pool, id->qp->send_cq, id->qp);
}