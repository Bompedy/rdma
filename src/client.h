#pragma once
#include <latch>

#include "temp.h"

inline void run_client(
    const uint32_t client_id,
    const rdma_cm_id* id,
    ibv_cq* cq,
    const ibv_mr* local_mr,
    const uintptr_t remote_addr,
    const uint32_t remote_rkey
) {
    const uintptr_t remote_slot = remote_addr + (client_id * CLIENT_SLOT_SIZE);

    constexpr int WINDOW_SIZE = QP_DEPTH / 2;
    for (int i = 0; i < WINDOW_SIZE; i++) {
        ibv_recv_wr rr{}, *bad;
        rr.wr_id = 0;
        ibv_post_recv(id->qp, &rr, &bad);
    }

    for (size_t i = 0; i < NUM_OPS_PER_CLIENT; i++) {
        const char* local_buf = static_cast<char*>(local_mr->addr);
        ibv_send_wr swr {};
        ibv_sge sge {};
        sge.addr = reinterpret_cast<uintptr_t>(local_buf);
        sge.length = ENTRY_SIZE;
        sge.lkey = local_mr->lkey;
        swr.wr_id = client_id;
        swr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        swr.sg_list = &sge;
        swr.num_sge = 1;
        swr.send_flags = IBV_SEND_INLINE;
        swr.wr.rdma.remote_addr = remote_slot;
        swr.wr.rdma.rkey = remote_rkey;
        swr.imm_data = client_id;

        ibv_send_wr* bad_wr = nullptr;
        if (ibv_post_send(id->qp, &swr, &bad_wr)) {
            throw std::runtime_error("ibv_post_send failed");
        }

        ibv_wc wc {};
        while (ibv_poll_cq(cq, 1, &wc) == 0) {}

        if (wc.status != IBV_WC_SUCCESS) {
            throw std::runtime_error("Leader write failed or connection lost");
        }

        if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
            // std::cout << "GOT RESPONSE FROM LEADER: " << i << std::endl;
        }

        ibv_recv_wr rr{}, *bad_rr;
        rr.wr_id = 0;
        ibv_post_recv(id->qp, &rr, &bad_rr);
    }
}

inline void run_clients() {
    std::vector<std::thread> workers;
    std::latch start_latch(NUM_CLIENTS + 1);

    workers.reserve(NUM_CLIENTS);
    for (int i = 0; i < NUM_CLIENTS; i++) {
        workers.emplace_back([i, &start_latch]() {
            try {
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
                const ibv_mr* mr = ibv_reg_mr(pd, client_mem, FINAL_POOL_SIZE,
                                              IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
                if (!mr) throw std::runtime_error("ibv_reg_mr failed");

                ibv_qp_init_attr qp_attr{};
                qp_attr.qp_type = IBV_QPT_RC;
                qp_attr.send_cq = cq;
                qp_attr.recv_cq = cq;
                qp_attr.cap.max_send_wr = QP_DEPTH;
                qp_attr.cap.max_recv_wr = QP_DEPTH;
                qp_attr.cap.max_send_sge = 1;
                qp_attr.cap.max_recv_sge = 1;
                qp_attr.cap.max_inline_data = MAX_INLINE_DEPTH;

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
                    // std::cout << "[client " << i << "] Leader gave me access to pool at 0x" << std::hex << leader_pool_addr << std::dec << " with rkey " << leader_rkey << "\n";
                }

                rdma_ack_cm_event(event);
                std::cout << "[client " << i << "] Connected to Leader!\n";
                start_latch.arrive_and_wait();
                run_client(i, id, cq, const_cast<ibv_mr*>(mr), leader_pool_addr, leader_rkey);
            }
            catch (const std::exception& e) {
                std::cerr << "Exception on thread [" << i << "] " << e.what() << std::endl;
                throw e;
            }
        });
    }

    start_latch.arrive_and_wait();
    const auto start_time = std::chrono::high_resolution_clock::now();

    for (auto& worker : workers) {
        worker.join();
    }
    const auto end_time = std::chrono::high_resolution_clock::now();
}
