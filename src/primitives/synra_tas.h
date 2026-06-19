#pragma once

#include <barrier>
#include <chrono>
#include <cstdint>
#include <infiniband/verbs.h>

#include "rdma/config.h"
#include "rdma/transport.h"

constexpr uint64_t TAS_FREE = 0;
constexpr uint64_t TAS_HELD = 1;
constexpr uint64_t TAS_TOGGLE = 1;
constexpr uint64_t TAS_REGISTER_OFFSET = BUF_SIZE - 8;

// wr_id encoding: [63..32] = client_id, [31..2] = wave, [1..0] = type
constexpr uint64_t TAS_CAS = 0;
constexpr uint64_t TAS_WRITE = 1;
constexpr uint64_t TAS_RESET = 2;

inline uint64_t tas_encode_wr(const uint32_t client_id, const uint32_t wave, const uint64_t type) {
    return (static_cast<uint64_t>(client_id) << 32) | (static_cast<uint64_t>(wave) << 2) | type;
}
inline uint32_t tas_decode_client(const uint64_t wr_id) { return static_cast<uint32_t>(wr_id >> 32); }
inline uint32_t tas_decode_wave(const uint64_t wr_id) { return static_cast<uint32_t>((wr_id >> 2) & 0x3FFFFFFF); }
inline uint64_t tas_decode_type(const uint64_t wr_id) { return wr_id & 3; }

inline void run_synra_tas(
    const uint32_t thread,
    const uint32_t num_clients,
    const Transport& transport,
    const uint32_t num_waves,
    uint64_t* latencies,
    std::barrier<>& sync_barrier
) {
    const uint32_t num_nodes = transport.num_nodes();
    const uint32_t num_replicas = num_nodes - 1;
    const uint32_t quorum = num_replicas / 2 + 1;
    const uint64_t buf_addr = transport.buffer_addr();
    const uint32_t client_base = thread * num_clients;

    auto* acks = new uint32_t[num_clients]();
    auto* starts = new std::chrono::high_resolution_clock::time_point[num_clients];

    ibv_wc wc_batch[32];

    for (uint32_t wave = 0; wave < num_waves; ++wave) {
        sync_barrier.arrive_and_wait();

        for (uint32_t client_id = 0; client_id < num_clients; ++client_id) {
            const uint64_t scratch = buf_addr + (client_base + client_id) * 64;
            *reinterpret_cast<uint64_t*>(scratch + 8) = TAS_TOGGLE;
            *reinterpret_cast<uint64_t*>(scratch + 16) = TAS_FREE;
            starts[client_id] = std::chrono::high_resolution_clock::now();
            transport.cas(thread, 0, scratch, TAS_REGISTER_OFFSET,
                          TAS_FREE, TAS_HELD, tas_encode_wr(client_id, wave, TAS_CAS));
        }

        uint32_t clients_done = 0;
        while (clients_done < num_clients) {
            const int32_t pulled = transport.poll(thread, wc_batch, 32);
            for (int32_t j = 0; j < pulled; ++j) {
                if (wc_batch[j].status != IBV_WC_SUCCESS) continue;

                const uint64_t wr_id = wc_batch[j].wr_id;
                if (tas_decode_wave(wr_id) != wave) continue;

                const uint32_t client_id = tas_decode_client(wr_id);
                const uint64_t type = tas_decode_type(wr_id);
                const uint64_t scratch = buf_addr + (client_base + client_id) * 64;

                if (type == TAS_CAS) {
                    const uint64_t old = *reinterpret_cast<uint64_t*>(scratch);
                    if (old == TAS_FREE) {
                        acks[client_id] = 0;
                        for (uint32_t node = 0; node < num_replicas; ++node)
                            transport.write(thread, node,
                                scratch + 8, static_cast<uint64_t>(wave) * 8, 8,
                                tas_encode_wr(client_id, wave, TAS_WRITE));
                    } else {
                        latencies[client_id * num_waves + wave] = static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::high_resolution_clock::now() - starts[client_id]).count());
                        clients_done++;
                    }
                } else if (type == TAS_WRITE) {
                    if (++acks[client_id] == quorum) {
                        latencies[client_id * num_waves + wave] = static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::high_resolution_clock::now() - starts[client_id]).count());
                        transport.write(thread, 0,
                            scratch + 16, TAS_REGISTER_OFFSET, 8,
                            tas_encode_wr(client_id, wave, TAS_RESET));
                    }
                } else {
                    clients_done++;
                }
            }
        }
    }

    delete[] starts;
    delete[] acks;
}
