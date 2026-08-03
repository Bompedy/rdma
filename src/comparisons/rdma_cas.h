#pragma once

#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <vector>

#include <infiniband/verbs.h>

#include "comparisons/comparison_common.h"
#include "rdma/transport.h"

// Canonical one-sided RDMA CAS spinlock:
//   0: unlocked
//   1: locked
// Every acquisition attempts CAS(0, 1), retrying after a failed CAS. Release
// writes 0 back to the lock entry.

constexpr uint64_t RDMA_CAS_ACQUIRE = 0;
constexpr uint64_t RDMA_CAS_RELEASE = 1;

inline uint64_t rdma_cas_wr_id(
    const uint32_t client, const uint32_t op, const uint64_t phase
) {
    return (static_cast<uint64_t>(client) << 32) |
           (static_cast<uint64_t>(op) << 1) | phase;
}

inline uint32_t rdma_cas_client(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

inline uint32_t rdma_cas_op(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> 1) & 0x7fffffff);
}

inline uint64_t rdma_cas_phase(const uint64_t wr_id) {
    return wr_id & 1;
}

inline void run_rdma_cas(
    const uint32_t thread,
    const uint32_t client_base,
    const uint32_t num_clients,
    const Transport& transport,
    const uint32_t ops_per_client,
    const uint32_t extra_ops_clients,
    const uint32_t num_locks,
    const double zipf_skew,
    uint64_t* const latencies
) {
    using Clock = std::chrono::steady_clock;

    if (num_locks == 0 || num_locks > COMPARISON_MAX_LOCKS)
        throw std::runtime_error("RDMA-CAS lock count is out of range");

    std::vector<double> lock_weights(num_locks);
    for (uint32_t lock = 0; lock < num_locks; ++lock) {
        lock_weights[lock] = zipf_skew > 0.0
            ? 1.0 / std::pow(static_cast<double>(lock + 1), zipf_skew)
            : 1.0;
    }
    std::mt19937 rng(0x9e3779b9u ^ thread);
    std::discrete_distribution<uint32_t> pick_lock(
        lock_weights.begin(), lock_weights.end());

    struct ClientState {
        uint32_t lock_id = 0;
        uint32_t num_ops = 0;
        uint64_t latency_offset = 0;
        Clock::time_point start{};
    };

    const uint64_t buf_addr = transport.buffer_addr();
    std::vector<ClientState> clients(num_clients);
    ibv_wc completions[32];

    auto post_acquire = [&](const uint32_t client, const uint32_t op) {
        const uint64_t scratch =
            buf_addr + static_cast<uint64_t>(client_base + client) * 64;
        const uint32_t lock_id = clients[client].lock_id;
        comparison_check_post(
            transport.cas(thread, 0, scratch,
                          COMPARISON_LOCK_OFFSET +
                              static_cast<uint64_t>(lock_id) * 8,
                          0, 1,
                          rdma_cas_wr_id(client, op, RDMA_CAS_ACQUIRE)),
            "RDMA-CAS acquire");
    };

    auto post_release = [&](const uint32_t client, const uint32_t op) {
        const uint64_t scratch =
            buf_addr + static_cast<uint64_t>(client_base + client) * 64;
        *reinterpret_cast<uint64_t*>(scratch) = 0;
        comparison_check_post(
            transport.write(thread, 0, scratch,
                            COMPARISON_LOCK_OFFSET +
                                static_cast<uint64_t>(clients[client].lock_id) * 8,
                            sizeof(uint64_t),
                            rdma_cas_wr_id(client, op, RDMA_CAS_RELEASE),
                            IBV_SEND_SIGNALED | IBV_SEND_INLINE),
            "RDMA-CAS release");
    };

    uint64_t local_total_ops = 0;
    for (uint32_t client = 0; client < num_clients; ++client) {
        const uint32_t global_client = client_base + client;
        clients[client].lock_id = pick_lock(rng);
        clients[client].num_ops = ops_per_client +
                                  (global_client < extra_ops_clients ? 1u : 0u);
        clients[client].latency_offset = local_total_ops;
        local_total_ops += clients[client].num_ops;
        clients[client].start = Clock::now();
        post_acquire(client, 0);
    }

    uint64_t completed = 0;

    while (completed < local_total_ops) {
        const int32_t count = transport.poll(thread, completions, 32);
        if (count < 0) throw std::runtime_error("RDMA-CAS CQ poll failed");

        for (int32_t i = 0; i < count; ++i) {
            const ibv_wc& wc = completions[i];
            comparison_check_completion(wc, "RDMA-CAS");

            const uint32_t client = rdma_cas_client(wc.wr_id);
            const uint32_t op = rdma_cas_op(wc.wr_id);
            const uint64_t scratch =
                buf_addr + static_cast<uint64_t>(client_base + client) * 64;
            const uint64_t observed =
                *reinterpret_cast<const uint64_t*>(scratch);

            if (rdma_cas_phase(wc.wr_id) == RDMA_CAS_ACQUIRE) {
                if (observed == 0) {
                    latencies[clients[client].latency_offset + op] =
                        static_cast<uint64_t>(std::chrono::duration_cast<
                            std::chrono::nanoseconds>(Clock::now() -
                                                      clients[client].start)
                                                  .count());
                    post_release(client, op);
                } else {
                    post_acquire(client, op);
                }
                continue;
            }

            ++completed;

            const uint32_t next_op = op + 1;
            if (next_op < clients[client].num_ops) {
                clients[client].lock_id = pick_lock(rng);
                clients[client].start = Clock::now();
                post_acquire(client, next_op);
            }
        }
    }
}
