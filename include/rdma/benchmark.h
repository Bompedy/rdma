#pragma once

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <vector>

struct BenchmarkResult {
    const uint32_t num_clients;
    const uint32_t ops_per_client;
    std::vector<uint64_t> latencies_ns;
    uint64_t wall_time_ns = 0;

    BenchmarkResult(
        const uint32_t num_clients,
        const uint32_t ops_per_client,
        const uint64_t total_ops = 0
    )
        : num_clients(num_clients),
          ops_per_client(ops_per_client),
          latencies_ns(total_ops != 0
              ? total_ops
              : static_cast<uint64_t>(num_clients) * ops_per_client) {}
};

inline void print_results(const BenchmarkResult& result) {
    auto sorted = result.latencies_ns;
    std::sort(sorted.begin(), sorted.end());

    const uint64_t total_ops = sorted.size();
    if (total_ops == 0) return;

    double sum = 0.0;
    for (const auto ns : sorted) sum += static_cast<double>(ns) / 1000.0;
    const double mean = sum / static_cast<double>(total_ops);

    double sq_sum = 0.0;
    for (const auto ns : sorted) {
        const double diff = static_cast<double>(ns) / 1000.0 - mean;
        sq_sum += diff * diff;
    }
    const double stddev = std::sqrt(sq_sum / static_cast<double>(total_ops));

    auto pctl = [&](const double p) -> double {
        const auto idx = static_cast<uint64_t>(p * static_cast<double>(total_ops - 1));
        return static_cast<double>(sorted[idx]) / 1000.0;
    };

    // Comparison benchmarks set wall_time_ns around the synchronized worker
    // interval. This is the same successful-operations / wall-clock accounting
    // used by the original continuous simple-CAS pipeline. Retain the latency
    // estimate only for primitives that have not yet supplied a wall time.
    double wall_seconds = static_cast<double>(result.wall_time_ns) / 1e9;
    double total_goodput = 0.0;
    if (result.wall_time_ns != 0) {
        total_goodput = static_cast<double>(total_ops) / wall_seconds;
    } else {
        for (uint32_t c = 0; c < result.num_clients; ++c) {
            uint64_t client_sum = 0;
            for (uint32_t op = 0; op < result.ops_per_client; ++op)
                client_sum += result.latencies_ns[c * result.ops_per_client + op];
            const double client_sec = static_cast<double>(client_sum) / 1e9;
            total_goodput += static_cast<double>(result.ops_per_client) / client_sec;
        }
    }

    std::fprintf(stderr, "\n==========================================\n");
    std::fprintf(stderr, " BENCHMARK RESULTS\n");
    std::fprintf(stderr, "==========================================\n");
    std::fprintf(stderr, "Clients:      %10u\n", result.num_clients);
    std::fprintf(stderr, "Ops/Client:   %10u\n", result.ops_per_client);
    std::fprintf(stderr, "Total Ops:    %10" PRIu64 "\n", total_ops);
    if (result.wall_time_ns != 0)
        std::fprintf(stderr, "Wall Clock:   %10.3f s\n", wall_seconds);
    std::fprintf(stderr, "Goodput:      %10.0f ops/s\n", total_goodput);
    std::fprintf(stderr, "------------------------------------------\n");
    std::fprintf(stderr, "LATENCY (Microseconds)\n");
    std::fprintf(stderr, "Mean:         %10.2f us\n", mean);
    std::fprintf(stderr, "StdDev:       %10.2f us\n", stddev);
    std::fprintf(stderr, "P0 (Min):     %10.2f us\n", pctl(0.0));
    std::fprintf(stderr, "P50 (Med):    %10.2f us\n", pctl(0.5));
    std::fprintf(stderr, "P90:          %10.2f us\n", pctl(0.9));
    std::fprintf(stderr, "P99:          %10.2f us\n", pctl(0.99));
    std::fprintf(stderr, "P99.9:        %10.2f us\n", pctl(0.999));
    std::fprintf(stderr, "P100 (Max):   %10.2f us\n", pctl(1.0));
    std::fprintf(stderr, "==========================================\n");
}
