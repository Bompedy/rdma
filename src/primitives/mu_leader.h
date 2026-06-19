#pragma once

#include <cstdint>
#include <cstdio>
#include <infiniband/verbs.h>

#include "rdma/config.h"
#include "rdma/transport.h"

constexpr uint32_t MU_RECV  = 0;
constexpr uint32_t MU_WRITE = 1;
constexpr uint32_t MU_SEND  = 2;
constexpr uint32_t MU_MAX_PENDING = 256;

// leader wr_id: [63..10] = generation, [9..2] = slot, [1..0] = type
inline uint64_t mu_encode(const uint32_t slot, const uint32_t type, const uint64_t gen) {
    return (gen << 10) | (static_cast<uint64_t>(slot) << 2) | type;
}
inline uint32_t mu_slot(const uint64_t wr_id) { return static_cast<uint32_t>((wr_id >> 2) & 0xFF); }
inline uint32_t mu_type(const uint64_t wr_id) { return static_cast<uint32_t>(wr_id & 3); }
inline uint64_t mu_gen(const uint64_t wr_id) { return wr_id >> 10; }

// per-slot buffer layout (64 bytes):
//   [0..7]   recv buffer (client request payload)
//   [8..15]  send buffer (leader response payload)
//   [16..23] write value (replicated to followers)

inline void run_mu_leader(
    const Transport& transport,
    const uint32_t client_node,
    const uint32_t total_ops
) {
    const uint32_t num_nodes = transport.num_nodes();
    const uint32_t my_node = transport.node_id();
    const uint64_t buf_addr = transport.buffer_addr();
    constexpr uint32_t thread = 0;

    uint32_t followers[MAX_REPLICAS];
    uint32_t num_followers = 0;
    for (uint32_t node = 0; node < num_nodes; ++node)
        if (node != my_node && node != client_node)
            followers[num_followers++] = node;

    const uint32_t total_replicas = num_followers + 1;
    const uint32_t remote_quorum = total_replicas / 2;

    uint32_t acks[MU_MAX_PENDING] = {};
    bool quorum_reached[MU_MAX_PENDING] = {};
    bool send_done[MU_MAX_PENDING] = {};
    uint64_t gen[MU_MAX_PENDING] = {};
    uint64_t counter = 0;

    // commit_head: next slot index (into circular ring) to commit/respond
    // assign_head: next slot index to assign to an incoming request
    uint32_t commit_head = 0;
    uint32_t assign_head = 0;

    for (uint32_t slot = 0; slot < MU_MAX_PENDING; ++slot)
        transport.post_recv(thread, client_node,
            buf_addr + slot * 64, 8, mu_encode(slot, MU_RECV, 0));

    uint32_t ops_completed = 0;
    ibv_wc wc_batch[32];

    std::fprintf(stderr, "[Mu Leader] Running. followers=%u remote_quorum=%u\n",
                 num_followers, remote_quorum);

    auto drain = [&]() {
        while (commit_head != assign_head && quorum_reached[commit_head]) {
            const uint32_t slot = commit_head;
            auto* response = reinterpret_cast<uint64_t*>(buf_addr + slot * 64 + 8);
            *response = *reinterpret_cast<uint64_t*>(buf_addr + slot * 64);
            transport.send(thread, client_node,
                buf_addr + slot * 64 + 8, 8, mu_encode(slot, MU_SEND, gen[slot]));
            commit_head = (commit_head + 1) % MU_MAX_PENDING;
        }
    };

    auto try_recycle = [&](const uint32_t slot) {
        if (acks[slot] >= num_followers && send_done[slot]) {
            acks[slot] = 0;
            quorum_reached[slot] = false;
            send_done[slot] = false;
            gen[slot]++;
            transport.post_recv(thread, client_node,
                buf_addr + slot * 64, 8, mu_encode(slot, MU_RECV, gen[slot]));
            ops_completed++;
        }
    };

    while (ops_completed < total_ops) {
        const int32_t pulled = transport.poll(thread, wc_batch, 32);
        for (int32_t j = 0; j < pulled; ++j) {
            if (wc_batch[j].status != IBV_WC_SUCCESS) continue;

            const uint64_t wr_id = wc_batch[j].wr_id;
            const uint32_t slot = mu_slot(wr_id);
            const uint32_t type = mu_type(wr_id);
            const uint64_t wc_gen = mu_gen(wr_id);

            if (wc_gen != gen[slot]) continue;

            if (type == MU_RECV) {
                const uint64_t ticket = counter++;
                acks[slot] = 0;
                quorum_reached[slot] = false;
                send_done[slot] = false;
                assign_head = (slot + 1) % MU_MAX_PENDING;

                auto* write_val = reinterpret_cast<uint64_t*>(buf_addr + slot * 64 + 16);
                *write_val = ticket;

                for (uint32_t i = 0; i < num_followers; ++i)
                    transport.write(thread, followers[i],
                        buf_addr + slot * 64 + 16, ticket * 8, 8,
                        mu_encode(slot, MU_WRITE, gen[slot]));

            } else if (type == MU_WRITE) {
                if (++acks[slot] >= remote_quorum && !quorum_reached[slot]) {
                    quorum_reached[slot] = true;
                    drain();
                }
                try_recycle(slot);

            } else if (type == MU_SEND) {
                send_done[slot] = true;
                try_recycle(slot);
            }
        }
    }

    std::fprintf(stderr, "[Mu Leader] Done. Processed %u ops\n", ops_completed);
}
