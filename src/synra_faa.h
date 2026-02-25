#pragma once

#include "temp.h"

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