#pragma once
#include <cmath>
#include <iomanip>
#include <latch>

#include "temp.h"

inline void run_synra_clients() {
    std::vector<std::thread> workers;
    auto all_latencies = std::make_unique<std::array<uint64_t, NUM_TOTAL_OPS>>();

    std::latch start_latch(NUM_CLIENTS * CLUSTER_NODES.size() + 1);

    workers.reserve(NUM_CLIENTS);
    for (int i = 0; i < NUM_CLIENTS; i++) {
        workers.emplace_back([i, &start_latch, &all_latencies]() {
            struct RemoteNode {
                rdma_cm_id* id;
                uintptr_t addr;
                uint32_t rkey;
            };

            try {
                std::vector<RemoteNode> connections;
                rdma_event_channel* ec = rdma_create_event_channel();
                if (!ec) return;

                ibv_context* verbs = nullptr;

                ibv_pd* pd = nullptr;
                ibv_cq* cq = nullptr;
                ibv_mr* mr = nullptr;
                char* client_mem = static_cast<char*>(allocate_rdma_buffer());


                for (int node_id = 0; node_id < CLUSTER_NODES.size(); node_id++) {
                    rdma_cm_id* id = nullptr;
                    if (rdma_create_id(ec, &id, nullptr, RDMA_PS_TCP)) return;

                    sockaddr_in addr{};
                    addr.sin_family = AF_INET;
                    addr.sin_port = htons(RDMA_PORT);
                    inet_pton(AF_INET, CLUSTER_NODES[node_id].c_str(), &addr.sin_addr);

                    if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000)) return;

                    auto wait_event = [&](rdma_cm_event_type expected) -> rdma_cm_event* {
                        rdma_cm_event* event = nullptr;
                        if (rdma_get_cm_event(ec, &event)) return nullptr;
                        if (event->event != expected) {
                            rdma_ack_cm_event(event);
                            return nullptr;
                        }
                        return event;
                    };

                    if (!wait_event(RDMA_CM_EVENT_ADDR_RESOLVED)) return;
                    rdma_ack_cm_event(nullptr);

                    if (rdma_resolve_route(id, 2000)) return;
                    if (!wait_event(RDMA_CM_EVENT_ROUTE_RESOLVED)) return;
                    rdma_ack_cm_event(nullptr);

                    if (pd == nullptr) {
                        pd = ibv_alloc_pd(id->verbs);
                        cq = ibv_create_cq(id->verbs, QP_DEPTH * CLUSTER_NODES.size(), nullptr, nullptr, 0);
                        mr = ibv_reg_mr(pd, client_mem, FINAL_POOL_SIZE,
                                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
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

                    if (rdma_create_qp(id, pd, &qp_attr)) return;

                    ConnPrivateData priv{};
                    priv.node_id = i;
                    priv.addr = reinterpret_cast<uintptr_t>(client_mem);
                    priv.rkey = mr->rkey;

                    rdma_conn_param param{};
                    param.private_data = &priv;
                    param.private_data_len = sizeof(priv);
                    param.responder_resources = 1;
                    param.initiator_depth = 1;

                    if (rdma_connect(id, &param)) return;
                    auto* ev = wait_event(RDMA_CM_EVENT_ESTABLISHED);

                    uintptr_t r_addr = 0;
                    uint32_t r_key = 0;
                    if (ev->param.conn.private_data) {
                        auto* remote_creds = static_cast<const ConnPrivateData*>(ev->param.conn.private_data);
                        r_addr = remote_creds->addr;
                        r_key = remote_creds->rkey;
                    }
                    rdma_ack_cm_event(ev);

                    connections.push_back({id, r_addr, r_key});
                    std::cout << "[Client " << i << "] Connected to Node " << node_id << "\n";

                    start_latch.count_down();
                }

                std::cout << "[Client " << i << "] Connected to all followers! " << "\n";
                start_latch.wait();
                uint64_t* latencies = &((*all_latencies)[i * NUM_OPS_PER_CLIENT]);
                // You'll need to update run_client to handle a vector of IDs/Keys
                // run_client_multi(i, connections, cq, mr, latencies);
                while (true) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }

            } catch (const std::exception& e) {
                std::cerr << "Thread " << i << " error: " << e.what() << "\n";
            }
        });
    }

    std::cout << "All clients connected. Starting benchmark..." << std::endl;
    start_latch.arrive_and_wait();
    const auto start_time = std::chrono::steady_clock::now();

    for (auto& worker : workers) {
        worker.join();
    }
    const auto end_time = std::chrono::steady_clock::now();

    std::sort(all_latencies->begin(), all_latencies->end());

    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    const double seconds = duration_ns / 1'000'000'000.0;
    const double throughput = NUM_TOTAL_OPS / seconds;

    auto get_p = [&](double p) {
        size_t idx = static_cast<size_t>(p * (NUM_TOTAL_OPS - 1));
        return (*all_latencies)[idx] / 1000.0; // ns to us
    };

    double sum = 0;
    for (const auto& lat : *all_latencies) sum += (lat / 1000.0);
    double mean = sum / NUM_TOTAL_OPS;

    double sq_sum = 0;
    for (const auto& lat : *all_latencies) {
        const double diff = (lat / 1000.0) - mean;
        sq_sum += diff * diff;
    }
    const double std_dev = std::sqrt(sq_sum / NUM_TOTAL_OPS);

    std::cout << "\n" << std::string(42, '=') << "\n";
    std::cout << " RDMA BENCHMARK RESULTS\n";
    std::cout << std::string(42, '=') << "\n";
    std::cout << "Clients:      " << std::setw(10) << NUM_CLIENTS << "\n";
    std::cout << "Ops/Client:   " << std::setw(10) << NUM_OPS_PER_CLIENT << "\n";
    std::cout << "Total Ops:    " << std::setw(10) << NUM_TOTAL_OPS << "\n";
    std::cout << "Total Time:   " << std::setw(10) << std::fixed << std::setprecision(3) << seconds << " s\n";
    std::cout << "Throughput:   " << std::setw(10) << std::fixed << std::setprecision(0) << throughput << " ops/s\n";
    std::cout << std::string(42, '-') << "\n";
    std::cout << "LATENCY (Microseconds)\n";
    std::cout << "Mean:         " << std::setw(10) << std::setprecision(2) << mean << " us\n";
    std::cout << "StdDev:       " << std::setw(10) << std::setprecision(2) << std_dev << " us\n";
    std::cout << "P0 (Min):     " << std::setw(10) << get_p(0.0) << " us\n";
    std::cout << "P50 (Med):    " << std::setw(10) << get_p(0.5) << " us\n";
    std::cout << "P90:          " << std::setw(10) << get_p(0.9) << " us\n";
    std::cout << "P99:          " << std::setw(10) << get_p(0.99) << " us\n";
    std::cout << "P99.9:        " << std::setw(10) << get_p(0.999) << " us\n";
    std::cout << "P100 (Max):   " << std::setw(10) << get_p(1.0) << " us\n";
    std::cout << std::string(42, '=') << std::endl;
}

inline void run_mu_client(
    const uint32_t client_id,
    const rdma_cm_id* id,
    ibv_cq* cq,
    const ibv_mr* local_mr,
    const uintptr_t remote_addr,
    const uint32_t remote_rkey,
    uint64_t* latencies
) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(client_id % std::thread::hardware_concurrency(), &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    bool got_ack = false;
    const uintptr_t remote_slot = remote_addr + (client_id * CLIENT_SLOT_SIZE);

    constexpr int WINDOW_SIZE = QP_DEPTH / 2;
    for (int i = 0; i < WINDOW_SIZE; i++) {
        ibv_recv_wr rr{}, *bad;
        rr.wr_id = 0;
        ibv_post_recv(id->qp, &rr, &bad);
    }

    for (size_t i = 0; i < NUM_OPS_PER_CLIENT; i++) {
        auto op_start = std::chrono::steady_clock::now();
        got_ack = false;
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
        swr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        swr.wr.rdma.remote_addr = remote_slot;
        swr.wr.rdma.rkey = remote_rkey;
        swr.imm_data = client_id;

        ibv_send_wr* bad_wr = nullptr;
        if (const auto send = ibv_post_send(id->qp, &swr, &bad_wr)) {
            std::cerr << "ibv_post_send failed: " << strerror(send) << " (error code: " << send << ")" << std::endl;
            if (bad_wr) {
                std::cerr << "Failed at WR ID: " << bad_wr->wr_id << std::endl;
            }
            throw std::runtime_error("ibv_post_send failed");
        }

        while (!got_ack) {
            ibv_wc wc {};
            while (ibv_poll_cq(cq, 1, &wc) == 0) {}

            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error("Leader write failed or connection lost");
            }

            if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                got_ack = true;
                ibv_recv_wr rr{}, *bad_rr;
                rr.wr_id = 0;
                ibv_post_recv(id->qp, &rr, &bad_rr);
            }
        }

        auto op_end = std::chrono::steady_clock::now();
        latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
    }
}

inline void run_mu_clients() {
    std::vector<std::thread> workers;
    auto all_latencies = std::make_unique<std::array<uint64_t, NUM_TOTAL_OPS>>();
    std::latch start_latch(NUM_CLIENTS + 1);

    workers.reserve(NUM_CLIENTS);
    for (int i = 0; i < NUM_CLIENTS; i++) {
        workers.emplace_back([i, &start_latch, &all_latencies]() {
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
                uint64_t* latencies = &((*all_latencies)[i * NUM_OPS_PER_CLIENT]);
                run_mu_client(i, id, cq, const_cast<ibv_mr*>(mr), leader_pool_addr, leader_rkey, latencies);
            }
            catch (const std::exception& e) {
                std::cerr << "Exception on thread [" << i << "] " << e.what() << std::endl;
                throw e;
            }
        });
    }

    std::cout << "All clients connected. Starting benchmark..." << std::endl;
    start_latch.arrive_and_wait();
    const auto start_time = std::chrono::steady_clock::now();

    for (auto& worker : workers) {
        worker.join();
    }
    const auto end_time = std::chrono::steady_clock::now();

    std::sort(all_latencies->begin(), all_latencies->end());

    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    const double seconds = duration_ns / 1'000'000'000.0;
    const double throughput = NUM_TOTAL_OPS / seconds;

    auto get_p = [&](double p) {
        size_t idx = static_cast<size_t>(p * (NUM_TOTAL_OPS - 1));
        return (*all_latencies)[idx] / 1000.0; // ns to us
    };

    double sum = 0;
    for (const auto& lat : *all_latencies) sum += (lat / 1000.0);
    double mean = sum / NUM_TOTAL_OPS;

    double sq_sum = 0;
    for (const auto& lat : *all_latencies) {
        const double diff = (lat / 1000.0) - mean;
        sq_sum += diff * diff;
    }
    const double std_dev = std::sqrt(sq_sum / NUM_TOTAL_OPS);

    std::cout << "\n" << std::string(42, '=') << "\n";
    std::cout << " RDMA BENCHMARK RESULTS\n";
    std::cout << std::string(42, '=') << "\n";
    std::cout << "Clients:      " << std::setw(10) << NUM_CLIENTS << "\n";
    std::cout << "Ops/Client:   " << std::setw(10) << NUM_OPS_PER_CLIENT << "\n";
    std::cout << "Total Ops:    " << std::setw(10) << NUM_TOTAL_OPS << "\n";
    std::cout << "Total Time:   " << std::setw(10) << std::fixed << std::setprecision(3) << seconds << " s\n";
    std::cout << "Throughput:   " << std::setw(10) << std::fixed << std::setprecision(0) << throughput << " ops/s\n";
    std::cout << std::string(42, '-') << "\n";
    std::cout << "LATENCY (Microseconds)\n";
    std::cout << "Mean:         " << std::setw(10) << std::setprecision(2) << mean << " us\n";
    std::cout << "StdDev:       " << std::setw(10) << std::setprecision(2) << std_dev << " us\n";
    std::cout << "P0 (Min):     " << std::setw(10) << get_p(0.0) << " us\n";
    std::cout << "P50 (Med):    " << std::setw(10) << get_p(0.5) << " us\n";
    std::cout << "P90:          " << std::setw(10) << get_p(0.9) << " us\n";
    std::cout << "P99:          " << std::setw(10) << get_p(0.99) << " us\n";
    std::cout << "P99.9:        " << std::setw(10) << get_p(0.999) << " us\n";
    std::cout << "P100 (Max):   " << std::setw(10) << get_p(1.0) << " us\n";
    std::cout << std::string(42, '=') << std::endl;
}
