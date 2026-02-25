#pragma once

#include <cmath>
#include <iomanip>
#include <latch>

#include "temp.h"
#include "synra_tas.h"
#include "synra_faa.h"

inline void run_synra_clients() {
    std::vector<std::thread> workers;
    auto all_latencies = std::make_unique<std::array<uint64_t, NUM_TOTAL_OPS>>();

    std::latch start_latch(NUM_CLIENTS * CLUSTER_NODES.size() + 1);

    workers.reserve(NUM_CLIENTS);
    for (int i = 0; i < NUM_CLIENTS; i++) {
        workers.emplace_back([i, &start_latch, &all_latencies]() {
            try {
                pin_thread_to_cpu(pick_cpu_for_client(i));

                std::vector<RemoteNode> connections;
                rdma_event_channel* ec = rdma_create_event_channel();
                if (!ec) return;

                ibv_context* verbs = nullptr;

                ibv_pd* pd = nullptr;
                ibv_cq* cq = nullptr;
                ibv_mr* mr = nullptr;
                char* client_mem = static_cast<char*>(allocate_rdma_buffer());


                for (int node_id = 0; node_id < CLUSTER_NODES.size(); node_id++) {
                    std::cout << "[DEBUG Client " << i << "] Connecting to " << CLUSTER_NODES[node_id] << "..." <<
                        std::endl;

                    rdma_cm_id* id = nullptr;
                    if (rdma_create_id(ec, &id, nullptr, RDMA_PS_TCP)) {
                        throw std::runtime_error("rdma_create_id failed for node " + std::to_string(node_id));
                    }

                    sockaddr_in addr{};
                    addr.sin_family = AF_INET;
                    addr.sin_port = htons(RDMA_PORT);
                    if (inet_pton(AF_INET, CLUSTER_NODES[node_id].c_str(), &addr.sin_addr) <= 0) {
                        throw std::runtime_error("Invalid IP address format: " + CLUSTER_NODES[node_id]);
                    }

                    if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000)) {
                        throw std::runtime_error("rdma_resolve_addr failed for node " + std::to_string(node_id));
                    }

                    auto wait_event = [&](const rdma_cm_event_type expected, const std::string& step) -> rdma_cm_event* {
                        rdma_cm_event* event = nullptr;
                        if (rdma_get_cm_event(ec, &event)) {
                            throw std::runtime_error("rdma_get_cm_event failed during " + step);
                        }
                        if (event->event != expected) {
                            int status = event->status;
                            rdma_ack_cm_event(event);
                            throw std::runtime_error("Expected " + step + " but got event ID: " +
                                std::to_string(event->event) + " with status: " + std::to_string(status));
                        }
                        return event;
                    };

                    auto* ev_addr = wait_event(RDMA_CM_EVENT_ADDR_RESOLVED, "ADDR_RESOLVE");
                    rdma_ack_cm_event(ev_addr);

                    if (rdma_resolve_route(id, 2000)) {
                        throw std::runtime_error("rdma_resolve_route failed for node " + std::to_string(node_id));
                    }
                    auto* ev_route = wait_event(RDMA_CM_EVENT_ROUTE_RESOLVED, "ROUTE_RESOLVE");
                    rdma_ack_cm_event(ev_route);

                    if (pd == nullptr) {
                        pd = ibv_alloc_pd(id->verbs);
                        if (!pd) throw std::runtime_error("ibv_alloc_pd failed");

                        cq = ibv_create_cq(id->verbs, QP_DEPTH * (int)CLUSTER_NODES.size(), nullptr, nullptr, 0);
                        if (!cq) throw std::runtime_error("ibv_create_cq failed");

                        mr = ibv_reg_mr(pd, client_mem, FINAL_POOL_SIZE,
                                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
                        if (!mr) throw std::runtime_error("ibv_reg_mr failed");
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

                    if (rdma_create_qp(id, pd, &qp_attr)) {
                        throw std::runtime_error("rdma_create_qp failed for node " + std::to_string(node_id));
                    }

                    ConnPrivateData priv{};
                    priv.node_id = i;
                    priv.type = ConnType::CLIENT;
                    priv.addr = reinterpret_cast<uintptr_t>(client_mem);
                    priv.rkey = mr->rkey;

                    rdma_conn_param param{};
                    param.private_data = &priv;
                    param.private_data_len = sizeof(priv);
                    param.responder_resources = 1;
                    param.initiator_depth = 1;

                    if (rdma_connect(id, &param)) {
                        throw std::runtime_error("rdma_connect call failed for node " + std::to_string(node_id));
                    }

                    auto* ev_conn = wait_event(RDMA_CM_EVENT_ESTABLISHED, "ESTABLISHED");

                    uintptr_t r_addr = 0;
                    uint32_t r_key = 0;
                    if (ev_conn->param.conn.private_data) {
                        auto* remote_creds = static_cast<const ConnPrivateData*>(ev_conn->param.conn.private_data);
                        r_addr = remote_creds->addr;
                        r_key = remote_creds->rkey;
                    } else {
                        rdma_ack_cm_event(ev_conn);
                        throw std::runtime_error("No private data received from node " + std::to_string(node_id));
                    }
                    rdma_ack_cm_event(ev_conn);

                    connections.push_back({id, r_addr, r_key});
                    std::cout << "[Client " << i << "] SUCCESS: Connected to Node " << node_id << std::endl;

                    start_latch.count_down();
                }

                std::cout << "[Client " << i << "] Connected to all followers! " << "\n";
                start_latch.wait();
                uint64_t* latencies = &((*all_latencies)[i * NUM_OPS_PER_CLIENT]);
                run_synra_tas_client(i, connections, cq, mr, latencies);
            } catch (const std::exception& e) {
                std::cerr << "Thread " << i << " error: " << e.what() << "\n";
            }
        });
    }

    start_latch.arrive_and_wait();
    std::cout << "All clients connected. Starting benchmark..." << std::endl;
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