#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rdma/benchmark.h"
#include "rdma/config.h"
#include "rdma/transport.h"
#include "primitives/synra_faa.h"

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
    const uint32_t total_clients = num_threads * clients_per_thread;
    const uint32_t num_ops = env_u32("NUM_OPS", 100000);
    const uint32_t ops_per_client = num_ops / total_clients;
    const char* primitive = env_str("PRIMITIVE", "synra_faa");

    Transport transport(node_id, all_ips, num_threads);
    std::fprintf(stderr, "[Node %u] Ready. Primitive=%s threads=%u clients/thread=%u ops=%u\n",
                 node_id, primitive, num_threads, clients_per_thread, num_ops);

    BenchmarkResult result(total_clients, ops_per_client);

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
    } else {
        std::fprintf(stderr, "Unknown primitive: %s\n", primitive);
        return 1;
    }

    print_results(result);
    return 0;
}
