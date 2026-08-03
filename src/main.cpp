#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "rdma/benchmark.h"
#include "rdma/config.h"
#include "rdma/thread_barrier.h"
#include "rdma/transport.h"
#include "primitives/synra_faa.h"
#include "primitives/synra_tas.h"
#include "primitives/mu_faa.h"
#include "primitives/mu_leader.h"
#include "comparisons/comparison_common.h"
#include "comparisons/rdma_cas.h"
#include "comparisons/shiftlock.h"

static std::vector<std::string> split(const char* s, const char delim) {
    std::vector<std::string> out;
    std::istringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, delim))
        if (!tok.empty()) out.push_back(tok);
    return out;
}

static uint32_t env_u32(const char* name, const uint32_t fallback) {
    const char* val = std::getenv(name);
    if (!val || val[0] == '\0') return fallback;
    return static_cast<uint32_t>(std::stoul(val));
}

static double env_double(const char* name, const double fallback) {
    const char* val = std::getenv(name);
    if (!val || val[0] == '\0') return fallback;
    return std::stod(val);
}

static const char* env_str(const char* name, const char* fallback) {
    const char* val = std::getenv(name);
    if (!val || val[0] == '\0') return fallback;
    return val;
}

int main() {
    const char* const servers_env = std::getenv("SERVERS");
    if (!servers_env) {
        std::fprintf(stderr, "SERVERS not set\n");
        return 1;
    }

    const auto all_ips = split(servers_env, ',');
    const uint32_t node_id = env_u32("NODE_ID", 0);
    const uint32_t num_threads = env_u32("NUM_THREADS", 1);
    const uint32_t clients_per_thread = env_u32("CLIENTS_PER_THREAD", 8);
    const uint32_t num_ops = env_u32("NUM_OPS", 100000);
    const uint32_t num_locks = env_u32("NUM_LOCKS", 1);
    const double zipf_skew = env_double("ZIPF_SKEW", 0.0);
    const char* primitive = env_str("PRIMITIVE", "synra_faa");
    const bool is_rdma_cas = std::strcmp(primitive, "rdma_cas") == 0;
    const bool is_shiftlock = std::strcmp(primitive, "shiftlock") == 0;
    const bool is_comparison = is_rdma_cas || is_shiftlock;

    const uint32_t configured_clients = num_threads * clients_per_thread;
    const uint32_t comparison_client_nodes = is_comparison
        ? static_cast<uint32_t>(all_ips.size() - 1)
        : 1;
    const uint32_t total_clients = is_comparison
        ? env_u32("TOTAL_CLIENTS", configured_clients)
        : configured_clients;

    uint32_t local_clients = configured_clients;
    uint32_t global_client_base = 0;
    if (is_comparison) {
        if (all_ips.size() < 2) {
            std::fprintf(stderr, "%s requires at least one server and one client\n",
                         primitive);
            return 1;
        }
        if (node_id == 0) {
            local_clients = 0;
        } else {
            const uint32_t client_machine = node_id - 1;
            const uint32_t base = total_clients / comparison_client_nodes;
            const uint32_t remainder = total_clients % comparison_client_nodes;
            local_clients = base + (client_machine < remainder ? 1u : 0u);
            global_client_base = client_machine * base +
                                 std::min(client_machine, remainder);
        }
    }

    if (total_clients == 0) {
        std::fprintf(stderr,
                     "NUM_THREADS and CLIENTS_PER_THREAD must be nonzero\n");
        return 1;
    }
    if (is_comparison && local_clients > configured_clients) {
        std::fprintf(stderr,
                     "Local client count %u exceeds NUM_THREADS * "
                     "CLIENTS_PER_THREAD capacity %u\n",
                     local_clients, configured_clients);
        return 1;
    }

    const uint32_t ops_per_client = num_ops / total_clients;
    const uint32_t extra_ops_clients = num_ops % total_clients;
    if (ops_per_client == 0) {
        std::fprintf(stderr,
                     "NUM_OPS must be at least NUM_THREADS * CLIENTS_PER_THREAD\n");
        return 1;
    }

    Transport transport(node_id, all_ips, num_threads);
    std::fprintf(stderr,
                 "[Node %u] Ready. Primitive=%s threads=%u clients/thread=%u "
                 "local_clients=%u total_clients=%u locks=%u zipf=%.2f ops=%u\n",
                 node_id, primitive, num_threads, clients_per_thread,
                 local_clients, total_clients, num_locks, zipf_skew, num_ops);

    const uint64_t local_extra_ops = is_comparison
        ? std::min<uint32_t>(global_client_base + local_clients,
                             extra_ops_clients) -
              std::min(global_client_base, extra_ops_clients)
        : 0;
    const uint64_t local_total_ops = is_comparison
        ? static_cast<uint64_t>(local_clients) * ops_per_client +
              local_extra_ops
        : static_cast<uint64_t>(total_clients) * ops_per_client;
    BenchmarkResult result(
        is_comparison ? local_clients : total_clients,
        ops_per_client,
        is_comparison
            ? local_total_ops
            : static_cast<uint64_t>(total_clients) * ops_per_client);

    if (std::strcmp(primitive, "synra_faa") == 0) {
        std::vector<std::thread> workers;
        workers.reserve(num_threads);

        for (uint32_t t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                uint64_t* lat = &result.latencies_ns[
                    static_cast<uint64_t>(t) * clients_per_thread * ops_per_client];
                run_synra_faa(t, clients_per_thread, transport,
                              ops_per_client, lat);
            });
        }

        for (auto& w : workers) w.join();
    } else if (std::strcmp(primitive, "mu_faa") == 0) {
        const uint32_t num_nodes = static_cast<uint32_t>(all_ips.size());
        const uint32_t leader_node = 0;
        const uint32_t client_node = num_nodes - 1;

        if (node_id == leader_node) {
            run_mu_leader(transport, client_node, total_clients * ops_per_client);
        } else if (node_id == client_node) {
            std::vector<std::thread> workers;
            workers.reserve(num_threads);

            for (uint32_t t = 0; t < num_threads; ++t) {
                workers.emplace_back([&, t]() {
                    uint64_t* lat = &result.latencies_ns[
                        static_cast<uint64_t>(t) * clients_per_thread * ops_per_client];
                    run_mu_faa(t, clients_per_thread, transport,
                               ops_per_client, lat);
                });
            }

            for (auto& w : workers) w.join();
        } else {
            std::fprintf(stderr, "[Node %u] Mu follower, idle.\n", node_id);
            sleep(3600);
        }
    } else if (std::strcmp(primitive, "synra_tas") == 0) {
        const uint32_t client_node = static_cast<uint32_t>(all_ips.size()) - 1;

        if (node_id == client_node) {
            ThreadBarrier sync_barrier(num_threads);
            std::vector<std::thread> workers;
            workers.reserve(num_threads);

            for (uint32_t t = 0; t < num_threads; ++t) {
                workers.emplace_back([&, t]() {
                    uint64_t* lat = &result.latencies_ns[
                        static_cast<uint64_t>(t) * clients_per_thread * ops_per_client];
                    run_synra_tas(t, clients_per_thread, transport,
                                  ops_per_client, lat, sync_barrier);
                });
            }

            for (auto& w : workers) w.join();
        } else {
            std::fprintf(stderr, "[Node %u] TAS replica, idle.\n", node_id);
            sleep(3600);
        }
    } else if (is_comparison) {
        constexpr uint32_t lock_server = 0;

        if (node_id == lock_server) {
            std::fprintf(stderr,
                         "[Node %u] %s lock server, waiting for %u client "
                         "machines.\n",
                         node_id, primitive, comparison_client_nodes);
            comparison_wait_for_counter(
                transport, COMPARISON_READY_OFFSET, comparison_client_nodes);
            comparison_release_clients(transport);
            const auto global_start = std::chrono::steady_clock::now();
            comparison_wait_for_counter(
                transport, COMPARISON_DONE_OFFSET, comparison_client_nodes);
            const auto global_end = std::chrono::steady_clock::now();
            const double global_seconds =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    global_end - global_start).count() / 1e9;
            const double global_goodput = num_ops / global_seconds;

            std::fprintf(stderr,
                         "\n==========================================\n"
                         " GLOBAL COMPARISON RESULTS\n"
                         "==========================================\n"
                         "Client Nodes: %10u\n"
                         "Clients:      %10u\n"
                         "Total Ops:    %10u\n"
                         "Wall Clock:   %10.3f s\n"
                         "Goodput:      %10.0f ops/s\n"
                         "==========================================\n",
                         comparison_client_nodes, total_clients, num_ops,
                         global_seconds, global_goodput);
        } else {
            std::vector<uint32_t> thread_clients(num_threads, 0);
            std::vector<uint32_t> thread_client_prefix(num_threads, 0);
            const uint32_t base = local_clients / num_threads;
            const uint32_t remainder = local_clients % num_threads;
            uint32_t active_threads = 0;
            uint32_t prefix = 0;
            for (uint32_t t = 0; t < num_threads; ++t) {
                thread_clients[t] = base + (t < remainder ? 1u : 0u);
                thread_client_prefix[t] = prefix;
                prefix += thread_clients[t];
                if (thread_clients[t] != 0) ++active_threads;
            }

            ThreadBarrier start_barrier(active_threads + 1);
            std::vector<std::thread> workers;
            workers.reserve(active_threads);
            std::vector<ShiftLockStats> shiftlock_stats(num_threads);

            for (uint32_t t = 0; t < num_threads; ++t) {
                if (thread_clients[t] == 0) continue;
                workers.emplace_back([&, t]() {
                    const uint32_t thread_global_base = global_client_base +
                        thread_client_prefix[t];
                    const uint64_t local_latency_offset =
                        static_cast<uint64_t>(thread_client_prefix[t]) *
                            ops_per_client +
                        std::min(thread_global_base, extra_ops_clients) -
                        std::min(global_client_base, extra_ops_clients);
                    uint64_t* lat = &result.latencies_ns[
                        local_latency_offset];
                    start_barrier.arrive_and_wait();
                    if (is_rdma_cas) {
                        run_rdma_cas(t, thread_global_base,
                                     thread_clients[t], transport,
                                     ops_per_client, extra_ops_clients,
                                     num_locks, zipf_skew, lat);
                    } else {
                        run_shiftlock(t, thread_global_base,
                                      thread_clients[t], transport,
                                      ops_per_client, extra_ops_clients,
                                      num_locks, zipf_skew, lat,
                                      &shiftlock_stats[t]);
                    }
                });
            }

            comparison_client_arrive_and_wait(transport);
            start_barrier.arrive_and_wait();
            const auto wall_start = std::chrono::steady_clock::now();
            for (auto& w : workers) w.join();
            const auto wall_end = std::chrono::steady_clock::now();
            result.wall_time_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    wall_end - wall_start).count());
            if (is_shiftlock) {
                ShiftLockStats total_stats{};
                for (const auto& stats : shiftlock_stats)
                    total_stats += stats;
                const double server_atomics_per_op =
                    local_total_ops == 0 ? 0.0 :
                    static_cast<double>(
                        total_stats.enqueue_cas_attempts +
                        total_stats.release_cas_attempts) /
                    static_cast<double>(local_total_ops);
                std::fprintf(
                    stderr,
                    "\n==========================================\n"
                    " SHIFTLOCK-RC TRAFFIC\n"
                    "==========================================\n"
                    "Enqueue CAS Attempts:   %10" PRIu64 "\n"
                    "Failed Enqueue CAS:      %10" PRIu64 "\n"
                    "Release CAS Attempts:   %10" PRIu64 "\n"
                    "Successor Announcements:%10" PRIu64 "\n"
                    "Direct Handovers:        %10" PRIu64 "\n"
                    "Server Atomics/Op:       %10.3f\n"
                    "==========================================\n",
                    total_stats.enqueue_cas_attempts,
                    total_stats.enqueue_cas_failures,
                    total_stats.release_cas_attempts,
                    total_stats.successor_announcements,
                    total_stats.direct_handovers,
                    server_atomics_per_op);
            }
            signal_comparison_server(transport);
        }
    } else {
        std::fprintf(stderr, "Unknown primitive: %s\n", primitive);
        return 1;
    }

    if ((is_comparison && node_id != 0) ||
        (!is_comparison &&
         (node_id == static_cast<uint32_t>(all_ips.size()) - 1 ||
          std::strcmp(primitive, "synra_faa") == 0)))
        print_results(result);

    return 0;
}
