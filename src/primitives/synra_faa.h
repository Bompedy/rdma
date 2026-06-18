#pragma once

#include <chrono>
#include <infiniband/verbs.h>

#include "rdma/config.h"
#include "rdma/transport.h"

constexpr uint64_t WRITE_PAYLOAD = 0xDEADBEEF;

// wr_id encoding: [63..32] client | [31..1] op | [0] is_faa
inline uint64_t encode_wr_id(const uint32_t client_id, const uint32_t op, const bool is_faa) {
    return (static_cast<uint64_t>(client_id) << 32) | (op << 1) | (is_faa ? 1 : 0);
}
inline uint32_t decode_client(const uint64_t wr_id) { return static_cast<uint32_t>(wr_id >> 32); }
inline uint32_t decode_op(const uint64_t wr_id) { return static_cast<uint32_t>((wr_id >> 1) & 0x7FFFFFFF); }
inline bool decode_is_faa(const uint64_t wr_id) { return (wr_id & 1) == 1; }

inline void run_synra_faa(
    const uint32_t thread,
    const uint32_t num_clients,
    const Transport& transport,
    const uint32_t ops_per_client,
    uint64_t* latencies
) {
    const uint32_t num_nodes = transport.num_nodes();
    const uint32_t quorum = num_nodes / 2 + 1;
    const uint64_t buf_addr = transport.buffer_addr();
    const uint32_t client_base = thread * num_clients;

    constexpr uint64_t frontier_offset = BUF_SIZE - 8;

    for (uint32_t client_id = 0; client_id < num_clients; ++client_id)
        *reinterpret_cast<uint64_t*>(buf_addr + (client_base + client_id) * 64 + 8) = WRITE_PAYLOAD;

    auto* acks = new uint32_t[num_clients]();
    auto* starts = new std::chrono::high_resolution_clock::time_point[num_clients];

    uint32_t total_done = 0;
    const uint32_t total_ops = num_clients * ops_per_client;
    ibv_wc wc_batch[32];

    for (uint32_t client_id = 0; client_id < num_clients; ++client_id) {
        starts[client_id] = std::chrono::high_resolution_clock::now();
        const uint64_t scratch = buf_addr + (client_base + client_id) * 64;
        transport.faa(thread, 0, scratch, frontier_offset, 1,
            encode_wr_id(client_id, 0, true));
    }

    while (total_done < total_ops) {
        const int32_t pulled = transport.poll(thread, wc_batch, 32);
        for (int32_t j = 0; j < pulled; ++j) {
            if (wc_batch[j].status != IBV_WC_SUCCESS) continue;

            const uint64_t wr_id = wc_batch[j].wr_id;
            const uint32_t client_id = decode_client(wr_id);
            const uint32_t op = decode_op(wr_id);

            if (decode_is_faa(wr_id)) {
                const uint64_t scratch = buf_addr + (client_base + client_id) * 64;
                const uint64_t ticket = *reinterpret_cast<uint64_t*>(scratch);

                for (uint32_t node = 0; node < num_nodes; ++node)
                    transport.write(thread, node,
                        scratch + 8, ticket * 8, 8, encode_wr_id(client_id, op, false));
            } else {
                if (++acks[client_id] >= quorum) {
                    const auto end = std::chrono::high_resolution_clock::now();
                    latencies[client_id * ops_per_client + op] = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            end - starts[client_id]).count());
                    acks[client_id] = 0;
                    total_done++;

                    const uint32_t next_op = op + 1;
                    if (next_op < ops_per_client) {
                        starts[client_id] = std::chrono::high_resolution_clock::now();
                        const uint64_t scratch = buf_addr + (client_base + client_id) * 64;
                        transport.faa(thread, 0,
                            scratch, frontier_offset, 1,
                            encode_wr_id(client_id, next_op, true));
                    }
                }
            }
        }
    }

    delete[] starts;
    delete[] acks;
}
