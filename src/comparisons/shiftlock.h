#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <vector>

#include <infiniband/verbs.h>

#include "comparisons/comparison_common.h"
#include "rdma/transport.h"

// Writer-only ShiftLock-RC comparison for mlx4 RNICs.
//
// Native ShiftLock stores {NodeID, DCT QPN} in its 40-bit tail and uses DCI/DCT
// SENDs plus masked extended atomics. mlx4 does not provide those facilities,
// so this fixed-topology variant stores {NodeID, EndpointID} instead. EndpointID
// identifies one of at most 16 logical clients multiplexed by a worker thread.
// Successor announcements and ownership grants are direct RDMA writes into the
// destination client machine's registered memory; waiters poll only local
// memory. The lock server participates only in the 64-bit CAS-loop tail swap.
//
// This deliberately omits ShiftLock's reader mode, RelCnt, leases, and recovery.
// Reference: https://github.com/thustorage/shiftlock

constexpr uint32_t SHIFTLOCK_CLIENTS_PER_THREAD = 16;
constexpr uint32_t SHIFTLOCK_ENDPOINT_BITS = 24;
constexpr uint32_t SHIFTLOCK_NODE_BITS = 16;
constexpr uint64_t SHIFTLOCK_ENDPOINT_MASK =
    (uint64_t{1} << SHIFTLOCK_ENDPOINT_BITS) - 1;
constexpr uint64_t SHIFTLOCK_TAIL_MASK =
    (uint64_t{1} << (SHIFTLOCK_ENDPOINT_BITS + SHIFTLOCK_NODE_BITS)) - 1;
constexpr uint64_t SHIFTLOCK_BACKOFF_INITIAL_NS = 1862;
constexpr uint32_t SHIFTLOCK_BACKOFF_SHIFT_LIMIT = 10;

enum class ShiftLockStage : uint64_t {
    Enqueue = 0,
    Announce = 1,
    Release = 2,
    Grant = 3,
};

enum class ShiftLockClientPhase : uint8_t {
    Enqueueing,
    BackingOff,
    Announcing,
    WaitingForGrant,
    Releasing,
    WaitingForSuccessor,
    Granting,
};

struct ShiftLockStats {
    uint64_t enqueue_cas_attempts = 0;
    uint64_t enqueue_cas_failures = 0;
    uint64_t release_cas_attempts = 0;
    uint64_t successor_announcements = 0;
    uint64_t direct_handovers = 0;

    ShiftLockStats& operator+=(const ShiftLockStats& other) {
        enqueue_cas_attempts += other.enqueue_cas_attempts;
        enqueue_cas_failures += other.enqueue_cas_failures;
        release_cas_attempts += other.release_cas_attempts;
        successor_announcements += other.successor_announcements;
        direct_handovers += other.direct_handovers;
        return *this;
    }
};

inline uint64_t shiftlock_wr_id(
    const uint32_t client, const uint32_t op, const ShiftLockStage stage
) {
    return (static_cast<uint64_t>(client) << 32) |
           (static_cast<uint64_t>(op) << 4) |
           static_cast<uint64_t>(stage);
}

inline uint32_t shiftlock_client(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

inline uint32_t shiftlock_op(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> 4) & 0x0fffffff);
}

inline ShiftLockStage shiftlock_stage(const uint64_t wr_id) {
    return static_cast<ShiftLockStage>(wr_id & 0xf);
}

inline uint32_t shiftlock_endpoint(
    const uint32_t thread, const uint32_t slot
) {
    if (slot >= SHIFTLOCK_CLIENTS_PER_THREAD)
        throw std::runtime_error(
            "ShiftLock-RC supports at most 16 clients per thread");
    const uint32_t endpoint = thread * SHIFTLOCK_CLIENTS_PER_THREAD + slot;
    if (endpoint > SHIFTLOCK_ENDPOINT_MASK)
        throw std::runtime_error("ShiftLock-RC endpoint does not fit in 24 bits");
    return endpoint;
}

inline uint64_t shiftlock_tail(
    const uint32_t node_id, const uint32_t endpoint
) {
    // Tail zero is the empty queue, so participating client node IDs are nonzero.
    if (node_id == 0 || node_id >= (uint32_t{1} << SHIFTLOCK_NODE_BITS))
        throw std::runtime_error("ShiftLock-RC client node ID is out of range");
    if (endpoint > SHIFTLOCK_ENDPOINT_MASK)
        throw std::runtime_error("ShiftLock-RC endpoint is out of range");
    return (static_cast<uint64_t>(node_id) << SHIFTLOCK_ENDPOINT_BITS) |
           endpoint;
}

inline uint32_t shiftlock_tail_node(const uint64_t tail) {
    return static_cast<uint32_t>(
        (tail >> SHIFTLOCK_ENDPOINT_BITS) &
        ((uint64_t{1} << SHIFTLOCK_NODE_BITS) - 1));
}

inline uint32_t shiftlock_tail_endpoint(const uint64_t tail) {
    return static_cast<uint32_t>(tail & SHIFTLOCK_ENDPOINT_MASK);
}

inline uint64_t shiftlock_successor_offset(const uint32_t endpoint) {
    if (endpoint >= COMPARISON_MAX_CLIENTS)
        throw std::runtime_error(
            "ShiftLock-RC successor endpoint is out of range");
    return COMPARISON_SUCCESSOR_BASE + static_cast<uint64_t>(endpoint) * 8;
}

inline uint64_t shiftlock_grant_offset(const uint32_t endpoint) {
    if (endpoint >= COMPARISON_MAX_CLIENTS)
        throw std::runtime_error("ShiftLock-RC grant endpoint is out of range");
    return COMPARISON_GRANT_BASE + static_cast<uint64_t>(endpoint) * 8;
}

inline uint64_t shiftlock_local_load(
    const Transport& transport, const uint64_t offset
) {
    auto* const address = reinterpret_cast<uint64_t*>(
        transport.buffer_addr() + offset);
    return __atomic_load_n(address, __ATOMIC_ACQUIRE);
}

inline void shiftlock_local_store(
    const Transport& transport, const uint64_t offset, const uint64_t value
) {
    auto* const address = reinterpret_cast<uint64_t*>(
        transport.buffer_addr() + offset);
    __atomic_store_n(address, value, __ATOMIC_RELEASE);
}

inline void run_shiftlock(
    const uint32_t thread,
    const uint32_t client_base,
    const uint32_t num_clients,
    const Transport& transport,
    const uint32_t ops_per_client,
    const uint32_t extra_ops_clients,
    const uint32_t num_locks,
    const double zipf_skew,
    uint64_t* const latencies,
    ShiftLockStats* const stats_out
) {
    using Clock = std::chrono::steady_clock;

    if (num_clients > SHIFTLOCK_CLIENTS_PER_THREAD)
        throw std::runtime_error(
            "ShiftLock-RC supports at most 16 clients per thread");
    if (num_locks == 0 || num_locks > COMPARISON_MAX_LOCKS)
        throw std::runtime_error("ShiftLock-RC lock count is out of range");
    if (transport.node_id() == 0)
        throw std::runtime_error(
            "ShiftLock-RC clients must use nonzero node IDs");

    std::vector<double> lock_weights(num_locks);
    for (uint32_t lock = 0; lock < num_locks; ++lock) {
        lock_weights[lock] = zipf_skew > 0.0
            ? 1.0 / std::pow(static_cast<double>(lock + 1), zipf_skew)
            : 1.0;
    }
    std::mt19937 rng(
        0x51f710ccu ^ transport.node_id() ^ (thread << 16));
    std::discrete_distribution<uint32_t> pick_lock(
        lock_weights.begin(), lock_weights.end());

    struct ClientState {
        uint32_t endpoint = 0;
        uint32_t lock_id = 0;
        uint32_t current_op = 0;
        uint32_t num_ops = 0;
        uint64_t latency_offset = 0;
        uint64_t tail = 0;
        uint64_t expected_tail = 0;
        uint32_t retry_count = 0;
        Clock::time_point retry_at{};
        ShiftLockClientPhase phase = ShiftLockClientPhase::Enqueueing;
        Clock::time_point start{};
    };

    const uint64_t buf_addr = transport.buffer_addr();
    std::vector<ClientState> clients(num_clients);
    ShiftLockStats stats{};
    ibv_wc completions[32];

    auto scratch_for = [&](const uint32_t client) {
        return buf_addr + static_cast<uint64_t>(client_base + client) * 64;
    };

    auto clear_mailboxes = [&](const ClientState& client) {
        shiftlock_local_store(
            transport, shiftlock_successor_offset(client.endpoint), 0);
        shiftlock_local_store(
            transport, shiftlock_grant_offset(client.endpoint), 0);
    };

    auto post_enqueue = [&](const uint32_t client, const uint32_t op) {
        ClientState& state = clients[client];
        state.phase = ShiftLockClientPhase::Enqueueing;
        ++stats.enqueue_cas_attempts;
        comparison_check_post(
            transport.cas(
                thread, 0, scratch_for(client),
                COMPARISON_LOCK_OFFSET +
                    static_cast<uint64_t>(state.lock_id) * 8,
                state.expected_tail, state.tail,
                shiftlock_wr_id(client, op, ShiftLockStage::Enqueue)),
            "ShiftLock-RC enqueue");
    };

    auto post_direct_write = [&](
        const uint32_t client, const uint32_t op,
        const uint64_t destination, const uint64_t remote_offset,
        const uint64_t value, const ShiftLockStage stage
    ) {
        const uint32_t remote_node = shiftlock_tail_node(destination);
        if (remote_node == 0 || remote_node >= transport.num_nodes())
            throw std::runtime_error(
                "ShiftLock-RC destination node is invalid");

        if (remote_node == transport.node_id()) {
            shiftlock_local_store(transport, remote_offset, value);
            return false;
        }

        const uint64_t payload = scratch_for(client) + 8;
        *reinterpret_cast<uint64_t*>(payload) = value;
        comparison_check_post(
            transport.write(
                thread, remote_node, payload, remote_offset, sizeof(uint64_t),
                shiftlock_wr_id(client, op, stage),
                IBV_SEND_SIGNALED | IBV_SEND_INLINE),
            stage == ShiftLockStage::Announce
                ? "ShiftLock-RC successor announcement"
                : "ShiftLock-RC ownership grant");
        return true;
    };

    auto announce_to_predecessor = [&](
        const uint32_t client, const uint32_t op,
        const uint64_t predecessor
    ) {
        ClientState& state = clients[client];
        ++stats.successor_announcements;
        const uint32_t predecessor_endpoint =
            shiftlock_tail_endpoint(predecessor);
        state.phase = ShiftLockClientPhase::Announcing;
        if (!post_direct_write(
                client, op, predecessor,
                shiftlock_successor_offset(predecessor_endpoint),
                state.tail, ShiftLockStage::Announce)) {
            state.phase = ShiftLockClientPhase::WaitingForGrant;
        }
    };

    auto post_release = [&](const uint32_t client, const uint32_t op) {
        ClientState& state = clients[client];
        state.phase = ShiftLockClientPhase::Releasing;
        ++stats.release_cas_attempts;
        comparison_check_post(
            transport.cas(
                thread, 0, scratch_for(client),
                COMPARISON_LOCK_OFFSET +
                    static_cast<uint64_t>(state.lock_id) * 8,
                state.tail, 0,
                shiftlock_wr_id(client, op, ShiftLockStage::Release)),
            "ShiftLock-RC release");
    };

    uint64_t completed = 0;

    auto start_operation = [&](const uint32_t client, const uint32_t op) {
        ClientState& state = clients[client];
        clear_mailboxes(state);
        state.current_op = op;
        state.lock_id = pick_lock(rng);
        state.expected_tail = 0;
        state.retry_count = 0;
        state.start = Clock::now();
        post_enqueue(client, op);
    };

    auto record_acquisition = [&](const uint32_t client) {
        const ClientState& state = clients[client];
        latencies[state.latency_offset + state.current_op] =
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    Clock::now() - state.start).count());
    };

    auto complete_operation = [&](const uint32_t client) {
        ClientState& state = clients[client];
        const uint32_t completed_op = state.current_op;
        ++completed;
        if (completed_op + 1 < state.num_ops)
            start_operation(client, completed_op + 1);
    };

    auto grant_successor = [&](
        const uint32_t client, const uint64_t successor
    ) {
        ClientState& state = clients[client];
        ++stats.direct_handovers;
        const uint32_t successor_endpoint =
            shiftlock_tail_endpoint(successor);
        state.phase = ShiftLockClientPhase::Granting;
        if (!post_direct_write(
                client, state.current_op, successor,
                shiftlock_grant_offset(successor_endpoint),
                successor, ShiftLockStage::Grant)) {
            complete_operation(client);
        }
    };

    auto release_or_handoff = [&](const uint32_t client) {
        ClientState& state = clients[client];
        const uint64_t successor = shiftlock_local_load(
            transport, shiftlock_successor_offset(state.endpoint));
        if (successor != 0) {
            shiftlock_local_store(
                transport, shiftlock_successor_offset(state.endpoint), 0);
            grant_successor(client, successor);
        } else {
            post_release(client, state.current_op);
        }
    };

    uint64_t local_total_ops = 0;
    for (uint32_t client = 0; client < num_clients; ++client) {
        ClientState& state = clients[client];
        const uint32_t global_client = client_base + client;
        state.endpoint = shiftlock_endpoint(thread, client);
        state.tail = shiftlock_tail(transport.node_id(), state.endpoint);
        state.num_ops = ops_per_client +
                        (global_client < extra_ops_clients ? 1u : 0u);
        if (state.num_ops > 0x0fffffff)
            throw std::runtime_error(
                "ShiftLock-RC operation count exceeds WR-ID capacity");
        state.latency_offset = local_total_ops;
        local_total_ops += state.num_ops;
        start_operation(client, 0);
    }

    while (completed < local_total_ops) {
        const int32_t count = transport.poll(thread, completions, 32);
        if (count < 0)
            throw std::runtime_error("ShiftLock-RC CQ poll failed");

        for (int32_t i = 0; i < count; ++i) {
            const ibv_wc& wc = completions[i];
            comparison_check_completion(wc, "ShiftLock-RC");

            const uint32_t client = shiftlock_client(wc.wr_id);
            const uint32_t op = shiftlock_op(wc.wr_id);
            if (client >= num_clients ||
                op != clients[client].current_op) {
                throw std::runtime_error(
                    "ShiftLock-RC received a stale completion");
            }
            ClientState& state = clients[client];

            switch (shiftlock_stage(wc.wr_id)) {
                case ShiftLockStage::Enqueue: {
                    const uint64_t observed =
                        *reinterpret_cast<const uint64_t*>(
                            scratch_for(client)) & SHIFTLOCK_TAIL_MASK;
                    if (observed != state.expected_tail) {
                        ++stats.enqueue_cas_failures;
                        state.expected_tail = observed;
                        const uint32_t shift = std::min(
                            state.retry_count,
                            SHIFTLOCK_BACKOFF_SHIFT_LIMIT);
                        const uint64_t delay_ns =
                            (SHIFTLOCK_BACKOFF_INITIAL_NS << shift) +
                            (rng() % SHIFTLOCK_BACKOFF_INITIAL_NS);
                        ++state.retry_count;
                        state.retry_at =
                            Clock::now() +
                            std::chrono::nanoseconds(delay_ns);
                        state.phase = ShiftLockClientPhase::BackingOff;
                    } else if (observed == 0) {
                        state.retry_count = 0;
                        record_acquisition(client);
                        release_or_handoff(client);
                    } else {
                        state.retry_count = 0;
                        announce_to_predecessor(client, op, observed);
                    }
                    break;
                }
                case ShiftLockStage::Announce:
                    state.phase = ShiftLockClientPhase::WaitingForGrant;
                    break;
                case ShiftLockStage::Release: {
                    const uint64_t observed =
                        *reinterpret_cast<const uint64_t*>(
                            scratch_for(client)) & SHIFTLOCK_TAIL_MASK;
                    if (observed == state.tail)
                        complete_operation(client);
                    else
                        state.phase =
                            ShiftLockClientPhase::WaitingForSuccessor;
                    break;
                }
                case ShiftLockStage::Grant:
                    complete_operation(client);
                    break;
            }
        }

        // Mailbox events do not create local CQ entries. Scanning at most
        // sixteen slots avoids RDMA polling traffic while preserving the
        // project's logical-client multiplexing model.
        for (uint32_t client = 0; client < num_clients; ++client) {
            ClientState& state = clients[client];
            if (state.phase == ShiftLockClientPhase::BackingOff) {
                if (Clock::now() >= state.retry_at)
                    post_enqueue(client, state.current_op);
            } else if (
                state.phase == ShiftLockClientPhase::WaitingForGrant) {
                const uint64_t grant = shiftlock_local_load(
                    transport, shiftlock_grant_offset(state.endpoint));
                if (grant == state.tail) {
                    shiftlock_local_store(
                        transport,
                        shiftlock_grant_offset(state.endpoint), 0);
                    record_acquisition(client);
                    release_or_handoff(client);
                }
            } else if (
                state.phase ==
                ShiftLockClientPhase::WaitingForSuccessor) {
                const uint64_t successor = shiftlock_local_load(
                    transport,
                    shiftlock_successor_offset(state.endpoint));
                if (successor != 0) {
                    shiftlock_local_store(
                        transport,
                        shiftlock_successor_offset(state.endpoint), 0);
                    grant_successor(client, successor);
                }
            }
        }
    }

    if (stats_out != nullptr) *stats_out = stats;
}
