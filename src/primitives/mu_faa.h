#pragma once

#include <chrono>
#include <cstdint>
#include <infiniband/verbs.h>

#include "rdma/config.h"
#include "rdma/transport.h"

// request/response payload: [63..32] = client_id, [31..0] = op
inline uint64_t mu_encode_payload(const uint32_t client_id, const uint32_t op) {
    return (static_cast<uint64_t>(client_id) << 32) | op;
}
inline uint32_t mu_decode_client(const uint64_t payload) { return static_cast<uint32_t>(payload >> 32); }
inline uint32_t mu_decode_op(const uint64_t payload) { return static_cast<uint32_t>(payload & 0xFFFFFFFF); }

inline void run_mu_faa(
    const uint32_t thread,
    const uint32_t num_clients,
    const Transport& transport,
    const uint32_t ops_per_client,
    uint64_t* latencies
) {
    const uint64_t buf_addr = transport.buffer_addr();
    constexpr uint32_t leader = 0;

    // buffer layout:
    //   send region: [client_id * 8] per client (8 bytes each)
    //   recv pool:   [num_clients * 8 + slot * 8] (8 bytes each, pool of num_clients slots)
    const uint64_t send_base = buf_addr;
    const uint64_t recv_base = buf_addr + num_clients * 8;

    auto* starts = new std::chrono::high_resolution_clock::time_point[num_clients];

    uint32_t total_done = 0;
    const uint32_t total_ops = num_clients * ops_per_client;
    ibv_wc wc_batch[32];

    for (uint32_t client_id = 0; client_id < num_clients; ++client_id) {
        *reinterpret_cast<uint64_t*>(send_base + client_id * 8) =
            mu_encode_payload(client_id, 0);

        transport.post_recv(thread, leader,
            recv_base + client_id * 8, 8, client_id);
        starts[client_id] = std::chrono::high_resolution_clock::now();
        transport.send(thread, leader,
            send_base + client_id * 8, 8, client_id);
    }

    while (total_done < total_ops) {
        const int32_t pulled = transport.poll(thread, wc_batch, 32);
        for (int32_t j = 0; j < pulled; ++j) {
            if (wc_batch[j].status != IBV_WC_SUCCESS) continue;
            if (wc_batch[j].opcode != IBV_WC_RECV) continue;

            const uint32_t slot = static_cast<uint32_t>(wc_batch[j].wr_id);
            const uint64_t payload = *reinterpret_cast<uint64_t*>(recv_base + slot * 8);
            const uint32_t client_id = mu_decode_client(payload);
            const uint32_t op = mu_decode_op(payload);

            const auto end = std::chrono::high_resolution_clock::now();
            latencies[client_id * ops_per_client + op] = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    end - starts[client_id]).count());
            total_done++;

            const uint32_t next_op = op + 1;
            if (next_op < ops_per_client) {
                *reinterpret_cast<uint64_t*>(send_base + client_id * 8) =
                    mu_encode_payload(client_id, next_op);

                transport.post_recv(thread, leader,
                    recv_base + slot * 8, 8, slot);
                starts[client_id] = std::chrono::high_resolution_clock::now();
                transport.send(thread, leader,
                    send_base + client_id * 8, 8, slot);
            }
        }
    }

    delete[] starts;
}
