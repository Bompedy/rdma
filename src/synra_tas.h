#pragma once

#include "temp.h"
#include <algorithm>
#include <iostream>
#include <chrono>

namespace {

inline uint64_t discover_frontier(
    const int op,
    const std::vector<RemoteNode>& conns,
    ibv_cq* cq,
    const ibv_mr* mr
) {
    uint64_t* remote_values = static_cast<uint64_t*>(mr->addr) + 10;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{reinterpret_cast<uintptr_t>(&remote_values[i]), 8, mr->lkey};
        ibv_send_wr wr{}, *bad;
        wr.wr_id = (static_cast<uint64_t>(op) << 32) | 0xABC000 | i;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge; wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + (ALIGNED_SIZE - 8);
        wr.wr.rdma.rkey = conns[i].rkey;
        ibv_post_send(conns[i].id->qp, &wr, &bad);
    }

    int received = 0;
    uint64_t max_v = 0;
    ibv_wc wc{};
    while (received < QUORUM) {
        if (ibv_poll_cq(cq, 1, &wc) > 0) {
            max_v = std::max(max_v, remote_values[wc.wr_id & 0xFFF]);
            received++;
        }
    }
    return max_v;
}

inline int commit_cas(
    const int op,
    const uint64_t slot,
    const int cid,
    const std::vector<RemoteNode>& connections,
    ibv_cq* cq,
    const ibv_mr* mr
) {
    uint64_t* results = static_cast<uint64_t*>(mr->addr) + 20;
    for (size_t i = 0; i < connections.size(); ++i) {
        ibv_sge sge{reinterpret_cast<uintptr_t>(&results[i]), 8, mr->lkey};
        ibv_send_wr wr{}, *bad;
        wr.wr_id = (static_cast<uint64_t>(op) << 32) | 0xDEF000 | i;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge; wr.num_sge = 1;
        wr.wr.rdma.remote_addr = connections[i].addr + (slot * 8);
        wr.wr.atomic.rkey = connections[i].rkey;
        wr.wr.atomic.compare_add = 0;
        wr.wr.atomic.swap = static_cast<uint64_t>(cid);
        ibv_post_send(connections[i].id->qp, &wr, &bad);
    }

    int responses = 0, wins = 0;
    ibv_wc wc{};
    while (responses < QUORUM) {
        if (ibv_poll_cq(cq, 1, &wc) > 0) {
            if (results[wc.wr_id & 0xFFF] == 0) wins++;
            responses++;
        }
    }
    return wins;
}

inline bool learn_majority(
    const int op,
    const uint64_t slot,
    const int client_id,
    const std::vector<RemoteNode>& connections,
    ibv_cq* cq,
    const ibv_mr* mr
) {
    uint64_t* cas_results = static_cast<uint64_t*>(mr->addr) + 20;
    std::fill_n(cas_results, connections.size(), 0);

    for (size_t i = 0; i < connections.size(); ++i) {
        ibv_sge sge{reinterpret_cast<uintptr_t>(&cas_results[i]), 8, mr->lkey};
        ibv_send_wr wr{}, *bad;
        wr.wr_id = (static_cast<uint64_t>(op) << 32) | 0x999000 | i;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = connections[i].addr + (slot * 8);
        wr.wr.rdma.rkey = connections[i].rkey;
        ibv_post_send(connections[i].id->qp, &wr, &bad);
    }

    int reads_done = 0;
    uint32_t counts[1024] = {0};
    ibv_wc wc{};
    while (reads_done < static_cast<int>(connections.size())) {
        if (ibv_poll_cq(cq, 1, &wc) > 0) {
            const uint64_t val = cas_results[wc.wr_id & 0xFFF];
            if (val > 0 && val < 1024) counts[val]++;
            reads_done++;
        }
    }

    for (int i = 1; i < 1024; ++i) {
        if (counts[i] >= QUORUM) return (i == client_id);
    }
    return false;
}


inline void advance_frontier(const uint64_t slot, const std::vector<RemoteNode>& conns, const ibv_mr* mr, ibv_cq* cq) {
    uint64_t* val_ptr = static_cast<uint64_t*>(mr->addr) + 30;
    *val_ptr = slot;

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{reinterpret_cast<uintptr_t>(val_ptr), 8, mr->lkey};
        ibv_send_wr wr{}, *bad;
        wr.wr_id = 0x111000 | i;
        wr.opcode = IBV_WR_RDMA_WRITE;
        // wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + (ALIGNED_SIZE - 8);
        wr.wr.rdma.rkey = conns[i].rkey;
        ibv_post_send(conns[i].id->qp, &wr, &bad);
    }
    //
    // int polled_frontier_updates = 0;
    // ibv_wc wc{};
    //
    // while (polled_frontier_updates < QUORUM) {
    //     if (ibv_poll_cq(cq, 1, &wc) > 0) {
    //         if ((wc.wr_id & 0xFFF000) == 0x111000) {
    //             polled_frontier_updates++;
    //         } else {
    //         }
    //     }
    // }
}

    inline void run_synra_reset(
        const int client_id,
        const std::vector<RemoteNode>& connections,
        ibv_cq* cq,
        const ibv_mr* mr
    ) {
    uint64_t current_idx = discover_frontier(0, connections, cq, mr);

    if (current_idx % 2 == 0) {
        return;
    }

    uint64_t next_slot = current_idx + 1;
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
        ibv_post_send(connections[i].id->qp, &wr, &bad);
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

    advance_frontier(next_slot, connections, mr, cq);
}
}


inline void run_synra_tas_client(
    const int client_id,
    const std::vector<RemoteNode>& connections,
    ibv_cq* cq,
    const ibv_mr* mr,
    uint64_t* latencies
) {
    if (connections.empty()) return;

    for (int op = 0; op < NUM_OPS_PER_CLIENT; ++op) {
        auto start_time = std::chrono::high_resolution_clock::now();
        bool won = false;

        while (true) {
            const uint64_t max_val = discover_frontier(op, connections, cq, mr);

            if (max_val % 2 != 0) {
                std::cout << "We fast path lost?" << std::endl;
                won = false;
                break;
            }

            const uint64_t next_slot = max_val + 1;
            if (commit_cas(op, next_slot, client_id, connections, cq, mr) >= QUORUM) {
                std::cout << "We fast path won and advanced slot to: " << next_slot << std::endl;
                advance_frontier(next_slot, connections, mr, cq);
                std::cout << "Advanced the frontier!" << std::endl;
                won = true;
                break;
            }

            if (learn_majority(op, next_slot, client_id, connections, cq, mr)) {
                std::cout << "We slow path won and advanced slot to: " << next_slot << std::endl;
                advance_frontier(next_slot, connections, mr, cq);
                won = true;
                break;
            } else {
                std::cout << "We slow path lost on slot: " << max_val << std::endl;
                won = false;

            }

        }

        if (won) run_synra_reset(client_id, connections, cq, mr);

        auto end_time = std::chrono::high_resolution_clock::now();
        latencies[op] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    }
}