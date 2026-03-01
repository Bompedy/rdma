#pragma once

#include "temp.h"
#include <algorithm>
#include <iostream>
#include <chrono>

constexpr auto DISCOVER_FRONTIER_ID = 0xABC000;
constexpr auto ADVANCE_FRONTIER_ID = 0x111000;
constexpr auto COMMIT_ID   = 0xDEF000;
constexpr auto MASK  = 0xFFF000;

constexpr auto FRONTIER_OFFSET = ALIGNED_SIZE - 8;

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
            if (ibv_post_send(conns[i].id->qp, &wr, &bad)) {
                throw std::runtime_error("Failed to discover frontier");
            }
        }

        int received = 0;
        uint64_t max_v = 0;
        ibv_wc wc{};
        while (received < QUORUM) {
            if (ibv_poll_cq(cq, 1, &wc) > 0) {
                const auto res_op = static_cast<uint32_t>(wc.wr_id >> 32);
                const auto res_meta = static_cast<uint32_t>(wc.wr_id & EMPTY_SLOT);
                if (res_op == static_cast<uint32_t>(op) && (res_meta & MASK) == DISCOVER_FRONTIER_ID) {
                    const uint32_t idx = res_meta & ~MASK;
                    max_v = std::max(max_v, state->frontier_values[idx]);
                    received++;
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
                    if (&state->cas_results[wc.wr_id & 0xFFF] == nullptr) wins++;
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

        uint64_t winner = 0xFFFFFFFFFFFFFFFF;
        bool quorum_met = false;

        constexpr uint64_t LOCAL_SENTINEL = 0xFEFEFEFEFEFEFEFE;

        for (size_t i = 0; i < connections.size(); ++i) {
            const uint64_t val = state->learn_results[i];

            if (val == LOCAL_SENTINEL) continue;
            if (val == EMPTY_SLOT) continue;

            int count = 0;
            for (size_t j = 0; j < connections.size(); ++j) {
                if (state->learn_results[j] == val) count++;
            }

            if (count >= QUORUM) {
                winner = val;
                quorum_met = true;
                break;
            }
        }

        if (quorum_met) return (winner == static_cast<uint64_t>(client_id));

        return false;
    }


    void advance_frontier(
        LocalState* state,
        const uint64_t slot,
        const std::vector<RemoteNode>& connections,
        const ibv_mr* mr
    ) {
        uint64_t* val_ptr = static_cast<uint64_t*>(mr->addr) + 30;
        *val_ptr = slot;

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
        while (responses < QUORUM) {
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
    const auto state = static_cast<LocalState*>(mr->addr);
    for (int op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
        auto start_time = std::chrono::high_resolution_clock::now();

        while (true) {
            const uint64_t max_val = discover_frontier(state, op, connections, cq, mr);

            if (max_val % 2 != 0) {
                continue;
            }

            const uint64_t next_slot = max_val + 1;
            if (commit_cas(state, op, next_slot, client_id, connections, cq, mr) >= QUORUM) {
                advance_frontier(state, next_slot, connections, mr);
                break;
            }

            if (learn_majority(state, op, next_slot, client_id, connections, cq, mr)) {
                std::cout << "We slow path won and advanced slot to: " << next_slot << std::endl;
                advance_frontier(state, next_slot, connections, mr);
                break;
            }

            std::cout << "We slow path lost on slot: " << max_val << std::endl;
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

        run_synra_reset(state, client_id, connections, cq, mr);
    }
}
