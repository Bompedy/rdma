#pragma once
#include <cmath>
#include <iomanip>
#include <latch>
#include <map>

#include "temp.h"

struct RemoteNode {
    rdma_cm_id* id;
    uintptr_t addr;
    uint32_t rkey;
};

inline void run_synra_tas_client(
    int client_id,
    const std::vector<RemoteNode>& connections,
    ibv_cq* cq,
    ibv_mr* mr,
    uint64_t* latencies
) {
    if (connections.empty()) return;

    ibv_wc wc_batch[32];
    uint64_t* remote_values = static_cast<uint64_t*>(mr->addr) + 10;

    bool responded[connections.size()];
    uint32_t counts[connections.size()];
    std::fill_n(counts, connections.size(), 0);
    std::fill_n(responded, connections.size(), false);

    for (int op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
        auto start_time = std::chrono::high_resolution_clock::now();
        while (true) {
            constexpr long READ_ID = 0xABC000;
            for (size_t i = 0; i < connections.size(); ++i) {
                ibv_sge read_sge{};
                read_sge.addr = reinterpret_cast<uintptr_t>(&remote_values[i]);
                read_sge.length = 8;
                read_sge.lkey = mr->lkey;

                ibv_send_wr read_wr{}, *bad_read_wr = nullptr;
                read_wr.wr_id = (static_cast<uint64_t>(op) << 32) | READ_ID | i;
                read_wr.sg_list = &read_sge;
                read_wr.num_sge = 1;
                read_wr.opcode = IBV_WR_RDMA_READ;
                read_wr.send_flags = IBV_SEND_SIGNALED;
                read_wr.wr.rdma.remote_addr = connections[i].addr + (ALIGNED_SIZE - 8);
                read_wr.wr.rdma.rkey = connections[i].rkey;

                if (ibv_post_send(connections[i].id->qp, &read_wr, &bad_read_wr)) {
                    throw std::runtime_error("Read post failed");
                }
            }

            std::fill_n(responded, connections.size(), false);
            int completed_reads = 0;
            while (completed_reads < QUORUM) {
                const int n = ibv_poll_cq(cq, 32, wc_batch);
                for (int i = 0; i < n; i++) {
                    if (wc_batch[i].status != IBV_WC_SUCCESS) continue;
                    const uint64_t completion_op = wc_batch[i].wr_id >> 32;
                    const uint64_t magic_tag = wc_batch[i].wr_id & 0xFFF000;
                    if (magic_tag == READ_ID && completion_op == op) {
                        const uint32_t node_idx = wc_batch[i].wr_id & 0xFFF;
                        responded[node_idx] = true;
                        completed_reads++;
                    }
                }
            }

            uint64_t max_val = 0;
            for (size_t i = 0; i < connections.size(); ++i) {
                if (responded[i] && remote_values[i] > max_val) {
                    max_val = remote_values[i];
                }
            }
            std::cout << "The max value is: " << max_val << std::endl;
            if (max_val % 2 == 0) {
                constexpr long CAS_ID = 0xDEF000;
                const uint64_t next_slot = max_val + 1;
                uint64_t* cas_results = static_cast<uint64_t*>(mr->addr) + 20;

                for (size_t i = 0; i < connections.size(); ++i) {
                    ibv_send_wr cas_wr{}, *bad_cas_wr = nullptr;
                    ibv_sge cas_sge{};

                    cas_sge.addr = reinterpret_cast<uintptr_t>(&cas_results[i]);
                    cas_sge.length = 8;
                    cas_sge.lkey = mr->lkey;

                    cas_wr.wr_id = (static_cast<uint64_t>(op) << 32) | CAS_ID | i;
                    cas_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
                    cas_wr.send_flags = IBV_SEND_SIGNALED;
                    cas_wr.sg_list = &cas_sge;
                    cas_wr.num_sge = 1;

                    cas_wr.wr.rdma.remote_addr = connections[i].addr + (next_slot * 8);
                    cas_wr.wr.atomic.rkey = connections[i].rkey;
                    cas_wr.wr.atomic.compare_add = 0;
                    cas_wr.wr.atomic.swap = static_cast<uint64_t>(client_id);

                    if (ibv_post_send(connections[i].id->qp, &cas_wr, &bad_cas_wr)) {
                        throw std::runtime_error("CAS post failed");
                    }
                }

                int wins = 0;
                int cas_responses = 0;
                while (cas_responses < QUORUM) {
                    const int n = ibv_poll_cq(cq, 32, wc_batch);
                    for (int j = 0; j < n; ++j) {
                        if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
                        if ((wc_batch[j].wr_id >> 32) == op && (wc_batch[j].wr_id & 0xFFF000) == CAS_ID) {
                            const uint32_t node_idx = wc_batch[j].wr_id & 0xFFF;
                            cas_responses++;
                            if (cas_results[node_idx] == 0) wins++;
                        }
                    }
                }

                if (wins >= QUORUM) {
                    std::cout << "I won fast path. Register is 1." << std::endl;
                    uint64_t* local_val = static_cast<uint64_t*>(mr->addr) + 30;
                    *local_val = next_slot;

                    for (size_t i = 0; i < connections.size(); ++i) {
                        ibv_sge sge{};
                        sge.addr = reinterpret_cast<uintptr_t>(local_val);
                        sge.length = 8;
                        sge.lkey = mr->lkey;

                        ibv_send_wr wr{}, *bad_wr = nullptr;
                        wr.wr_id = 0x111000 | i;
                        wr.opcode = IBV_WR_RDMA_WRITE;
                        wr.sg_list = &sge;
                        wr.num_sge = 1;
                        wr.send_flags = 0;
                        wr.wr.rdma.remote_addr = connections[i].addr + (ALIGNED_SIZE - 8);
                        wr.wr.rdma.rkey = connections[i].rkey;

                        ibv_post_send(connections[i].id->qp, &wr, &bad_wr);
                    }
                    break;
                }

                std::cout << "Op " << op << ": No immediate CAS quorum. Reading to find majority..." << std::endl;

                for (size_t i = 0; i < connections.size(); ++i) {
                    ibv_send_wr read_slot_wr{}, *bad_wr = nullptr;
                    ibv_sge sge{};
                    sge.addr = reinterpret_cast<uintptr_t>(&cas_results[i]);
                    sge.length = 8;
                    sge.lkey = mr->lkey;

                    read_slot_wr.wr_id = (static_cast<uint64_t>(op) << 32) | 0x999000 | i;
                    read_slot_wr.opcode = IBV_WR_RDMA_READ;
                    read_slot_wr.sg_list = &sge;
                    read_slot_wr.num_sge = 1;
                    read_slot_wr.send_flags = IBV_SEND_SIGNALED;
                    read_slot_wr.wr.rdma.remote_addr = connections[i].addr + (next_slot * 8);
                    read_slot_wr.wr.rdma.rkey = connections[i].rkey;
                    ibv_post_send(connections[i].id->qp, &read_slot_wr, &bad_wr);
                }

                std::fill_n(counts, connections.size(), 0);
                int reads_done = 0;
                while (reads_done < connections.size()) {
                    const int n = ibv_poll_cq(cq, 32, wc_batch);
                    for (int j = 0; j < n; ++j) {
                        if ((wc_batch[j].wr_id & 0xFFF000) == 0x999000) {
                            uint32_t idx = wc_batch[j].wr_id & 0xFFF;
                            counts[cas_results[idx]]++;
                            reads_done++;
                        }
                    }
                }


                bool i_won = false;
                bool someone_won = false;
                for (int i = 0; i < connections.size(); ++i) {
                    if (const uint32_t count = counts[i]; count >= QUORUM && i != 0) {
                        someone_won = true;
                        if (i == client_id) i_won = true;
                        break;
                    }
                }

                if (i_won) {
                    uint64_t* local_val = static_cast<uint64_t*>(mr->addr) + 30;
                    *local_val = next_slot;

                    for (size_t i = 0; i < connections.size(); ++i) {
                        ibv_sge sge{};
                        sge.addr = reinterpret_cast<uintptr_t>(local_val);
                        sge.length = 8;
                        sge.lkey = mr->lkey;

                        ibv_send_wr wr{}, *bad_wr = nullptr;
                        wr.wr_id = 0x111000 | i;
                        wr.opcode = IBV_WR_RDMA_WRITE;
                        wr.sg_list = &sge;
                        wr.num_sge = 1;
                        wr.send_flags = 0;
                        wr.wr.rdma.remote_addr = connections[i].addr + (ALIGNED_SIZE - 8);
                        wr.wr.rdma.rkey = connections[i].rkey;

                        ibv_post_send(connections[i].id->qp, &wr, &bad_wr);
                    }
                    std::cout << "Confirmed: I won. Register is 1." << std::endl;
                    break;
                }

                if (someone_won) {
                    std::cout << "Confirmed: Someone else won. Register is 1." << std::endl;
                    break;
                }
            }

            break;
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    }
}

inline void run_synra_faa_client(
    int client_id,
    const std::vector<RemoteNode>& connections,
    ibv_cq* cq,
    ibv_mr* mr,
    uint64_t* latencies
) {
    if (connections.empty()) return;

    ibv_wc wc_batch[32];
    const auto& sequencer = connections[0];

    ibv_sge ticket_sge{};
    ticket_sge.addr = reinterpret_cast<uintptr_t>(mr->addr);
    ticket_sge.length = 8;
    ticket_sge.lkey = mr->lkey;

    uint64_t* local_data_ptr = reinterpret_cast<uint64_t*>(mr->addr) + 1;
    *local_data_ptr = 0xDEADBEEF;
    ibv_sge write_sge{};
    write_sge.addr = reinterpret_cast<uintptr_t>(local_data_ptr);
    write_sge.length = 8;
    write_sge.lkey = mr->lkey;

    for (int op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
        constexpr uint64_t FAA_MAGIC_ID = 0xFFFFFFFFFFFFFFFF;
        auto start_time = std::chrono::high_resolution_clock::now();

        ibv_send_wr faa_wr{}, *bad_faa_wr = nullptr;
        faa_wr.wr_id = FAA_MAGIC_ID;
        faa_wr.sg_list = &ticket_sge;
        faa_wr.num_sge = 1;
        faa_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
        faa_wr.send_flags = IBV_SEND_SIGNALED;
        faa_wr.wr.atomic.remote_addr = sequencer.addr + (ALIGNED_SIZE - 8);
        faa_wr.wr.atomic.rkey = sequencer.rkey;
        faa_wr.wr.atomic.compare_add = 1;

        if (ibv_post_send(sequencer.id->qp, &faa_wr, &bad_faa_wr)) {
            throw std::runtime_error("FAA post failed");
        }

        bool ticket_received = false;
        while (!ticket_received) {
            int n = ibv_poll_cq(cq, 1, &wc_batch[0]);
            if (n < 0) throw std::runtime_error("Poll failed");
            if (n > 0) {
                if (wc_batch[0].status != IBV_WC_SUCCESS) {
                    throw std::runtime_error("FAA failed with status: " + std::to_string(wc_batch[0].status));
                }
                if (wc_batch[0].wr_id == FAA_MAGIC_ID) {
                    ticket_received = true;
                } else {
                }
            }
        }

        std::atomic_thread_fence(std::memory_order_acquire);
        const uint64_t my_ticket = *static_cast<uint64_t*>(mr->addr);
        const uint64_t log_offset = my_ticket * 8;

        for (size_t i = 0; i < connections.size(); ++i) {
            ibv_send_wr wr{}, *bad_wr = nullptr;
            wr.wr_id = (my_ticket << 32) | (static_cast<uint32_t>(i));
            wr.sg_list = &write_sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = connections[i].addr + log_offset;
            wr.wr.rdma.rkey = connections[i].rkey;

            if (ibv_post_send(connections[i].id->qp, &wr, &bad_wr)) {
                throw std::runtime_error("Replication post failed");
            }
        }

        int acks = 0;
        while (acks < QUORUM) {
            const int pulled = ibv_poll_cq(cq, 32, wc_batch);
            for (int j = 0; j < pulled; ++j) {
                if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
                const uint64_t completion_ticket = wc_batch[j].wr_id >> 32;
                if (completion_ticket == my_ticket) {
                    acks++;
                } else {
                    // std::cout << "got stray ack here of: " << completion_ticket << std::endl;
                }
            }
        }

        // std::cout << "Moving forward on op: " << op << std::endl;
        auto end_time = std::chrono::high_resolution_clock::now();
        latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    }
}

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

inline void run_mu_client(
    const uint32_t client_id,
    const rdma_cm_id* id,
    ibv_cq* cq,
    const ibv_mr* local_mr,
    const uintptr_t remote_addr,
    const uint32_t remote_rkey,
    uint64_t* latencies
) {
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
                pin_thread_to_cpu(pick_cpu_for_client(i));
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
