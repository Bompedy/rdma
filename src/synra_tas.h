#pragma once

#include "temp.h"
#include <algorithm>
#include <iostream>
#include <chrono>

constexpr auto DISCOVER_FRONTIER_ID = 0xABC000;
constexpr auto ADVANCE_FRONTIER_ID = 0x111000;
constexpr auto COMMIT_ID   = 0xDEF000;
constexpr auto MASK  = 0xFFF000;

constexpr auto MAX_REPLICAS = 10;

namespace {
    struct alignas(64) LocalState {
        uint64_t frontier_values[MAX_REPLICAS];
        uint64_t cas_results[MAX_REPLICAS];
        uint64_t learn_results[MAX_REPLICAS];
        uint64_t next_frontier;
        uint64_t metadata;
    };

    uint64_t discover_frontier(
        LocalState* state,
        const int op,
        const std::vector<RemoteNode>& conns,
        ibv_cq* cq,
        const ibv_mr* mr
    ) {
        for (size_t i = 0; i < conns.size(); ++i) {
            ibv_sge sge{
                .addr = reinterpret_cast<uintptr_t>(&state->frontier_values[i]),
                .length = 8,
                .lkey = mr->lkey
            };
            ibv_send_wr wr{}, *bad;
            wr.wr_id = (static_cast<uint64_t>(op) << 32) | DISCOVER_FRONTIER_ID | i;
            wr.opcode = IBV_WR_RDMA_READ;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = conns[i].addr + FRONTIER_OFFSET;
            wr.wr.rdma.rkey = conns[i].rkey;
            if (int ret = ibv_post_send(conns[i].id->qp, &wr, &bad)) {
                fprintf(stderr, "Post failed: %s (errno: %d) | Addr: %p | LKey: %u\n",
                        strerror(ret), ret, (void*)sge.addr, sge.lkey);
                throw std::runtime_error("Failed to discover frontier");
            }
        }

        int received = 0;
        uint64_t max_v = 0;

        ibv_wc wcs[MAX_REPLICAS];
        while (received < QUORUM) {
            int n = ibv_poll_cq(cq, MAX_REPLICAS, wcs);
            if (n > 0) {
                for (int i = 0; i < n; ++i) {
                    const auto& wc = wcs[i];
                    if (wc.status != IBV_WC_SUCCESS) {
                        throw std::runtime_error("Work Completion Error read: " + std::to_string(wc.status));
                    }
                    const bool is_current_op = (wc.wr_id >> 32) == static_cast<uint64_t>(op);
                    const bool is_discover = (wc.wr_id & MASK) == DISCOVER_FRONTIER_ID;
                    if (is_current_op && is_discover) {
                        max_v = std::max(max_v, state->frontier_values[wc.wr_id & 0xFFF]);
                        received++;
                    }
                }
            }
        }
        return max_v;
    }

    int commit_cas(
        LocalState* state,
        const int op,
        const uint64_t slot,
        const int cid,
        const std::vector<RemoteNode>& connections,
        ibv_cq* cq,
        const ibv_mr* mr
    ) {
        std::fill_n(state->cas_results, connections.size(), 0xFEFEFEFEFEFEFEFE);
        for (size_t i = 0; i < connections.size(); ++i) {
            ibv_sge sge{
                .addr = reinterpret_cast<uintptr_t>(&state->cas_results[i]),
                .length = 8,
                .lkey = mr->lkey
            };
            ibv_send_wr wr{}, *bad;
            wr.wr_id = (static_cast<uint64_t>(op) << 32) | COMMIT_ID | i;
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = connections[i].addr + (slot * 8);
            wr.wr.atomic.rkey = connections[i].rkey;
            wr.wr.atomic.compare_add = EMPTY_SLOT;
            wr.wr.atomic.swap = static_cast<uint64_t>(cid);
            if (ibv_post_send(connections[i].id->qp, &wr, &bad)) {
                throw std::runtime_error("Failed to commit cas");
            }
        }

        int responses = 0, wins = 0;
        ibv_wc wc{};
        while (responses < QUORUM) {
            if (ibv_poll_cq(cq, 1, &wc) > 0) {
                const bool is_current_op = (wc.wr_id >> 32) == static_cast<uint64_t>(op);
                const bool is_cas = (wc.wr_id & MASK) == COMMIT_ID;
                if (is_current_op && is_cas) {
                    if (state->cas_results[wc.wr_id & 0xFFF] == EMPTY_SLOT) wins++;
                    responses++;
                }
            }
        }
        return wins;
    }

    bool learn_majority(
        LocalState* state,
        const int op,
        const uint64_t slot,
        const int client_id,
        const std::vector<RemoteNode>& connections,
        ibv_cq* cq,
        const ibv_mr* mr
    ) {
        std::fill_n(state->learn_results, connections.size(), 0xFEFEFEFEFEFEFEFE);

        for (size_t i = 0; i < connections.size(); ++i) {
            ibv_sge sge{
                .addr = reinterpret_cast<uintptr_t>(&state->learn_results[i]),
                .length = 8,
                .lkey = mr->lkey
            };
            ibv_send_wr wr{}, *bad;
            wr.wr_id = (static_cast<uint64_t>(op) << 32) | 0x999000 | i;
            wr.opcode = IBV_WR_RDMA_READ;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = connections[i].addr + (slot * 8);
            wr.wr.rdma.rkey = connections[i].rkey;

            if (ibv_post_send(connections[i].id->qp, &wr, &bad)) {
                throw std::runtime_error("Failed to post learn majority reads");
            }
        }

        int reads_done = 0;
        ibv_wc wc{};
        while (reads_done < static_cast<int>(connections.size())) {
            if (ibv_poll_cq(cq, 1, &wc) > 0) {
                if ((wc.wr_id >> 32) == static_cast<uint64_t>(op) && (wc.wr_id & 0xFFF000) == 0x999000) {
                    reads_done++;
                }
            }
        }

        uint64_t quorum_winner = 0xFFFFFFFFFFFFFFFF;
        uint64_t lowest_id_seen = 0xFFFFFFFFFFFFFFFF;
        bool found_quorum = false;
        bool found_any = false;

        constexpr uint64_t LOCAL_SENTINEL = 0xFEFEFEFEFEFEFEFE;

        for (size_t i = 0; i < connections.size(); ++i) {
            const uint64_t val = state->learn_results[i];

            if (val == LOCAL_SENTINEL || val == EMPTY_SLOT) continue;

            found_any = true;

            if (val < lowest_id_seen) {
                lowest_id_seen = val;
            }

            int count = 0;
            for (size_t j = 0; j < connections.size(); ++j) {
                if (state->learn_results[j] == val) count++;
            }

            if (count >= QUORUM) {
                if (val < quorum_winner) {
                    quorum_winner = val;
                    found_quorum = true;
                }
            }
        }

        if (found_quorum) {
            return (quorum_winner == static_cast<uint64_t>(client_id));
        }

        if (found_any) {
            return (lowest_id_seen == static_cast<uint64_t>(client_id));
        }

        return false;
    }


    void advance_frontier(
        LocalState* state,
        const uint64_t slot,
        const std::vector<RemoteNode>& connections,
        const ibv_mr* mr
    ) {
        state->next_frontier = slot;

        for (size_t i = 0; i < connections.size(); ++i) {
            ibv_sge sge{
                .addr = reinterpret_cast<uintptr_t>(&state->next_frontier),
                .length = 8,
                .lkey = mr->lkey
            };
            ibv_send_wr wr{}, *bad;
            wr.wr_id = ADVANCE_FRONTIER_ID | i;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = connections[i].addr + FRONTIER_OFFSET;
            wr.wr.rdma.rkey = connections[i].rkey;
            if (ibv_post_send(connections[i].id->qp, &wr, &bad)) {
                throw std::runtime_error("Failed to advance frontier");
            }
        }
    }

    inline void run_synra_reset(
        LocalState* state,
        const int client_id,
        const std::vector<RemoteNode>& connections,
        ibv_cq* cq,
        const ibv_mr* mr
    ) {
        const uint64_t current_idx = discover_frontier(state, 0, connections, cq, mr);

        if (current_idx % 2 == 0) {
            return;
        }

        const uint64_t next_slot = current_idx + 1;
        uint64_t* write_val = static_cast<uint64_t*>(mr->addr) + 40;
        *write_val = static_cast<uint64_t>(client_id);

        for (size_t i = 0; i < connections.size(); ++i) {
            ibv_sge sge{reinterpret_cast<uintptr_t>(write_val), 8, mr->lkey};
            ibv_send_wr wr{}, *bad;
            wr.wr_id = 0x777000 | i;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = connections[i].addr + (next_slot * 8);
            wr.wr.rdma.rkey = connections[i].rkey;
            if (ibv_post_send(connections[i].id->qp, &wr, &bad)) {
                throw std::runtime_error("Failed to reset");
            }
        }

        int responses = 0;
        ibv_wc wc{};
        while (responses < connections.size()) {
            if (ibv_poll_cq(cq, 1, &wc) > 0) {
                if ((wc.wr_id & 0xFFF000) == 0x777000) {
                    responses++;
                }
            }
        }

        advance_frontier(state, next_slot, connections, mr);
    }
}


inline void run_synra_tas_client(
    const int client_id,
    const std::vector<RemoteNode>& connections,
    ibv_cq* cq,
    const ibv_mr* mr,
    uint64_t* latencies
) {
    ibv_wc wc_batch[32];
    const auto state = static_cast<LocalState*>(mr->addr);
    for (int op = 0; op < NUM_OPS_PER_CLIENT; ++op) {

        int no_responses = 0;
        while (no_responses < 50) {
            const int n = ibv_poll_cq(cq, 1, &wc_batch[0]);
            if (n < 0) throw std::runtime_error("Poll failed");
            if (n == 0) ++no_responses;
        }

        auto start_time = std::chrono::high_resolution_clock::now();

        while (true) {
            const uint64_t max_val = discover_frontier(state, op, connections, cq, mr);

            if (max_val % 2 != 0) {
                continue;
            }

            const uint64_t next_slot = max_val + 1;
            if (commit_cas(state, op, next_slot, client_id, connections, cq, mr) >= QUORUM) {
                // std::cout << client_id << " - we fast path won: " << next_slot << std::endl;
                advance_frontier(state, next_slot, connections, mr);
                break;
            }

            if (learn_majority(state, op, next_slot, client_id, connections, cq, mr)) {
                std::cout << client_id << " - we slow path won and advanced slot to: " << next_slot << std::endl;
                advance_frontier(state, next_slot, connections, mr);
                break;
            }

            std::cout << client_id << " - we slow path lost on slot: " << next_slot << std::endl;
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

        run_synra_reset(state, client_id, connections, cq, mr);
    }
}

inline void run_synra_cas_client(
    const int client_id,
    const std::vector<RemoteNode>& connections,
    ibv_cq* cq,
    const ibv_mr* mr,
    uint64_t* latencies
) {

    const auto state = static_cast<LocalState*>(mr->addr);
    uint64_t target_slot = 1;

    for (int op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
        auto start_time = std::chrono::high_resolution_clock::now();

        while (true) {
            const uint64_t expected = target_slot - 1;

            state->cas_results[0] = 0xFEFEFEFEFEFEFEFE;

            ibv_sge sge {
                .addr = reinterpret_cast<uintptr_t>(&state->cas_results[0]),
                .length = 8,
                .lkey = mr->lkey
            };

            ibv_send_wr wr{}, *bad_wr;
            wr.wr_id = (static_cast<uint64_t>(op) << 32) | 0x123;
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.atomic.remote_addr = connections[0].addr + (FRONTIER_OFFSET);
            wr.wr.atomic.rkey = connections[0].rkey;
            wr.wr.atomic.compare_add = expected;
            wr.wr.atomic.swap = target_slot;

            if (ibv_post_send(connections[0].id->qp, &wr, &bad_wr)) {
                std::cerr << "Post failed: " << strerror(errno) << std::endl;
                continue;
            }

            ibv_wc wc;
            const uint64_t expected_id = (static_cast<uint64_t>(op) << 32) | 0x123;

            while (true) {
                const int n = ibv_poll_cq(cq, 1, &wc);
                if (n < 0) throw std::runtime_error("Poll CQ failed");
                if (n > 0) if (wc.wr_id == expected_id) break;
            }

            if (wc.status != IBV_WC_SUCCESS) {
                std::cerr << "CAS completion error: " << wc.status << std::endl;
                continue;
            }

            const uint64_t result = state->cas_results[0];

            if (result == target_slot - 1) {
                state->next_frontier = static_cast<uint64_t>(client_id);

                ibv_sge replication_sge {
                    .addr = reinterpret_cast<uintptr_t>(&state->next_frontier),
                    .length = 8,
                    .lkey = mr->lkey
                };

                const uint64_t log_offset = (target_slot * 8);

                for (size_t i = 0; i < connections.size(); ++i) {
                    ibv_send_wr wr{}, *bad_wr = nullptr;
                    wr.wr_id = (target_slot << 32) | (static_cast<uint32_t>(i));
                    wr.sg_list = &replication_sge;
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
                    ibv_wc wc_batch[32];
                    const int pulled = ibv_poll_cq(cq, 32, wc_batch);
                    for (int j = 0; j < pulled; ++j) {
                        if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
                        const uint64_t completion_ticket = wc_batch[j].wr_id >> 32;
                        if (completion_ticket == target_slot) {
                            acks++;
                        }
                    }
                }

                advance_frontier(state, target_slot+1, connections, mr);
                target_slot += 2;
                break;
            }

            if (result % 2 != 0) {
                target_slot = result + 2;
            } else {
                target_slot = result + 1;
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    }
}