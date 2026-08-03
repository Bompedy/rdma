#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include <infiniband/verbs.h>

#include "rdma/config.h"
#include "rdma/transport.h"

// Reserve the last 64 KiB of the registered region for non-replicated lock
// comparisons. Synra's append-only slots grow upward from offset zero.
constexpr uint64_t COMPARISON_CONTROL_OFFSET = BUF_SIZE - 64 * 1024;
constexpr uint64_t COMPARISON_LOCK_OFFSET = COMPARISON_CONTROL_OFFSET;
constexpr uint32_t COMPARISON_MAX_LOCKS = 2048;
constexpr uint64_t COMPARISON_READY_OFFSET =
    COMPARISON_LOCK_OFFSET + static_cast<uint64_t>(COMPARISON_MAX_LOCKS) * 8;
constexpr uint64_t COMPARISON_START_OFFSET = COMPARISON_READY_OFFSET + 8;
constexpr uint64_t COMPARISON_DONE_OFFSET = COMPARISON_READY_OFFSET + 16;

// ShiftLock-RC uses the same offsets on every machine as per-endpoint local
// mailboxes. The 24-bit compact endpoint maps to thread * 16 + logical slot.
constexpr uint32_t COMPARISON_MAX_CLIENTS = 2048;
constexpr uint64_t COMPARISON_SUCCESSOR_BASE = COMPARISON_DONE_OFFSET + 64;
constexpr uint64_t COMPARISON_GRANT_BASE =
    COMPARISON_SUCCESSOR_BASE + static_cast<uint64_t>(COMPARISON_MAX_CLIENTS) * 8;

static_assert(COMPARISON_GRANT_BASE +
                  static_cast<uint64_t>(COMPARISON_MAX_CLIENTS) * 8 <=
              BUF_SIZE);

inline void comparison_check_post(const int32_t rc, const char* const operation) {
    if (rc != 0)
        throw std::runtime_error(std::string(operation) + " post failed");
}

inline void comparison_check_completion(
    const ibv_wc& wc, const char* const benchmark
) {
    if (wc.status != IBV_WC_SUCCESS) {
        throw std::runtime_error(
            std::string(benchmark) + " completion failed: " +
            ibv_wc_status_str(wc.status));
    }
}

inline uint64_t comparison_local_counter(
    const Transport& transport, const uint64_t offset
) {
    return *reinterpret_cast<volatile const uint64_t*>(
        transport.buffer_addr() + offset);
}

inline void comparison_wait_for_counter(
    const Transport& transport,
    const uint64_t offset,
    const uint64_t target
) {
    while (comparison_local_counter(transport, offset) < target) usleep(50);
}

inline void comparison_release_clients(const Transport& transport) {
    *reinterpret_cast<volatile uint64_t*>(
        transport.buffer_addr() + COMPARISON_START_OFFSET) = 1;
}

inline void comparison_client_arrive_and_wait(const Transport& transport) {
    constexpr uint64_t ready_wr_id = ~uint64_t{1};
    constexpr uint64_t start_wr_id = ~uint64_t{2};
    const uint64_t scratch = transport.buffer_addr();

    comparison_check_post(
        transport.faa(0, 0, scratch, COMPARISON_READY_OFFSET,
                      1, ready_wr_id),
        "comparison ready");

    ibv_wc wc{};
    transport.poll_one(0, &wc);
    comparison_check_completion(wc, "comparison ready");

    while (true) {
        comparison_check_post(
            transport.read(0, 0, scratch, COMPARISON_START_OFFSET,
                           sizeof(uint64_t), start_wr_id),
            "comparison start read");
        transport.poll_one(0, &wc);
        comparison_check_completion(wc, "comparison start read");
        if (*reinterpret_cast<const uint64_t*>(scratch) != 0) break;
        usleep(50);
    }
}

inline void signal_comparison_server(const Transport& transport) {
    constexpr uint64_t done_wr_id = ~uint64_t{3};
    const uint64_t scratch = transport.buffer_addr();
    comparison_check_post(
        transport.faa(0, 0, scratch, COMPARISON_DONE_OFFSET,
                      1, done_wr_id),
        "comparison done");

    ibv_wc wc{};
    transport.poll_one(0, &wc);
    comparison_check_completion(wc, "comparison done");
}
