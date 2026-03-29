#include "rdma/common.h"
#include "rdma/server.h"
#include "rdma/servers/mu_leader.h"
#include "rdma/servers/mu_follower.h"
#include "rdma/servers/synra_node.h"
#include "rdma/client.h"
#include "rdma/pipelines/cas_pipeline.h"
#include "rdma/pipelines/mu_pipeline.h"
#include "rdma/pipelines/simple_cas_pipeline.h"
#include "rdma/pipelines/simple_watch_pipeline.h"
#include "rdma/pipelines/ticket_faa_lock_pipeline.h"
#include "rdma/pipelines/watch_pipeline.h"
#include "rdma/pipelines/mu_watch_pipeline.h"
#include "rdma/mu_encoding.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <thread>

// ─── Configuration ───
// Add new strategies here and wire them into the config load, buffer sizing,
// dispatch, and summary branches below.
constexpr const char* STRATEGY = "watch";

int main() {
    std::cerr << "[DEBUG] main() started" << std::endl;
    std::cerr.flush();
    try {
        std::cerr << "[DEBUG] In try block, loading config..." << std::endl;
        std::cerr.flush();
        // Each pipeline exposes a config struct plus a client buffer-size helper.
        // New pipelines should follow that pattern so main stays uniform.
        const bool is_mu  = (std::string(STRATEGY) == "mu");
        const bool is_ticket_faa = (std::string(STRATEGY) == "ticket_faa");
        const bool is_cas = (std::string(STRATEGY) == "cas");
        const bool is_simple_cas = (std::string(STRATEGY) == "simple_cas");
        const bool is_watch = (std::string(STRATEGY) == "watch");
        const bool is_simple_watch = (std::string(STRATEGY) == "simple_watch");
        const bool is_mu_watch = (std::string(STRATEGY) == "mu_watch");
        std::cerr << "[DEBUG] Strategy flags set, loading pipeline configs..." << std::endl;
        std::cerr.flush();
        const CasPipelineConfig cas_config = is_cas ? load_cas_pipeline_config() : CasPipelineConfig{};
        const SimpleCasPipelineConfig simple_cas_config =
            is_simple_cas ? load_simple_cas_pipeline_config() : SimpleCasPipelineConfig{};
        const TicketFaaLockPipelineConfig ticket_faa_config =
            is_ticket_faa ? load_ticket_faa_lock_pipeline_config() : TicketFaaLockPipelineConfig{};
        const MuPipelineConfig mu_config = is_mu ? load_mu_pipeline_config() : MuPipelineConfig{};
        const WatchPipelineConfig watch_config = is_watch ? load_watch_pipeline_config() : WatchPipelineConfig{};
        const SimpleWatchPipelineConfig simple_watch_config =
            is_simple_watch ? load_simple_watch_pipeline_config() : SimpleWatchPipelineConfig{};
        const MuWatchPipelineConfig mu_watch_config = is_mu_watch ? load_mu_watch_pipeline_config() : MuWatchPipelineConfig{};
        std::cerr << "[DEBUG] Pipeline configs loaded" << std::endl;
        std::cerr.flush();

        // Client mode runs worker threads for the selected pipeline. Server mode
        // below launches either the MU leader/follower path or generic nodes.
        std::cerr << "[DEBUG] Checking IS_CLIENT env var..." << std::endl;
        std::cerr.flush();
        if (get_uint_env("IS_CLIENT") != 0) {
            std::cerr << "[DEBUG] IS_CLIENT=1, entering client mode..." << std::endl;
            std::cerr.flush();
            std::cerr << "[DEBUG] Getting MACHINE_ID..." << std::endl;
            std::cerr.flush();
            const uint32_t machine_id = get_uint_env("MACHINE_ID");
            std::cerr << "[DEBUG] MACHINE_ID=" << machine_id << std::endl;
            std::cerr.flush();

            std::cerr << "[DEBUG] Allocating latency array..." << std::endl;
            std::cerr.flush();
            auto all_latencies = std::make_unique<std::array<uint64_t, MAX_TOTAL_OPS>>();
            std::cerr << "[DEBUG] Latency array allocated, creating latch..." << std::endl;
            std::cerr.flush();
            std::latch start_latch(NUM_CLIENTS_PER_MACHINE + 1);
            std::vector<std::thread> workers;

            std::cerr << "[DEBUG] Allocating lock_counts array..." << std::endl;
            std::cerr.flush();
            auto lock_counts = std::make_unique<
                std::array<std::array<uint64_t, MAX_LOCKS>, TOTAL_CLIENTS>>();
            std::cerr << "[DEBUG] lock_counts allocated, initializing..." << std::endl;
            std::cerr.flush();
            for (auto& client_counts : *lock_counts) client_counts.fill(0);
            std::cerr << "[DEBUG] lock_counts initialized" << std::endl;
            std::cerr.flush();

            std::atomic<Client*> verify_client{nullptr};

            std::cerr << "[DEBUG] Creating " << NUM_CLIENTS_PER_MACHINE << " worker threads..." << std::endl;
            std::cerr.flush();
            for (uint32_t i = 0; i < NUM_CLIENTS_PER_MACHINE; ++i) {
                std::cerr << "[DEBUG] Creating worker thread " << i << "..." << std::endl;
                std::cerr.flush();
                const uint32_t global_id = machine_id * NUM_CLIENTS_PER_MACHINE + i;

                workers.emplace_back(
                    [i, global_id, is_mu, is_ticket_faa, is_cas, is_simple_cas, is_watch, is_simple_watch, is_mu_watch,
                     &start_latch, &all_latencies, &lock_counts, &verify_client,
                     &cas_config, &simple_cas_config, &ticket_faa_config, &mu_config, &watch_config, &simple_watch_config, &mu_watch_config]() {
                        try {
                            std::cerr << "[DEBUG] Worker " << i << " started" << std::endl;
                            std::cerr.flush();
                            pin_thread_to_cpu(pick_cpu_for_client(i));
                            std::cerr << "[DEBUG] Worker " << i << " pinned to CPU" << std::endl;
                            std::cerr.flush();

                            std::cerr << "[DEBUG] Worker " << i << " allocating Client..." << std::endl;
                            std::cerr.flush();
                            auto client = std::make_unique<Client>(
                                global_id,
                                is_cas ? cas_pipeline_client_buffer_size(cas_config)
                                        : (is_simple_cas ? simple_cas_pipeline_client_buffer_size(simple_cas_config)
                                                         : (is_ticket_faa ? ticket_faa_lock_pipeline_client_buffer_size(ticket_faa_config)
                                                                          : (is_mu ? mu_pipeline_client_buffer_size(mu_config)
                                                                                   : (is_watch ? watch_pipeline_client_buffer_size(watch_config)
                                                                                               : (is_simple_watch ? simple_watch_pipeline_client_buffer_size(simple_watch_config)
                                                                                                                  : (is_mu_watch ? mu_watch_pipeline_client_buffer_size(mu_watch_config)
                                                                                                                                 : CLIENT_ALIGNED_SIZE)))))));
                            std::cerr << "[DEBUG] Worker " << i << " Client allocated" << std::endl;
                            std::cerr.flush();

                            std::cerr << "[DEBUG] Worker " << i << " connecting..." << std::endl;
                            std::cerr.flush();
                            if (is_mu || is_mu_watch) {
                                std::vector leader_only = {CLUSTER_NODES[0]};
                                client->connect(leader_only, RDMA_PORT);
                            } else {
                                client->connect(CLUSTER_NODES, RDMA_PORT);
                            }
                            std::cerr << "[DEBUG] Worker " << i << " connected, waiting for establishment..." << std::endl;
                            std::cerr.flush();

                            {
                                const size_t num_go = (is_mu || is_mu_watch) ? 1 : CLUSTER_NODES.size();
                                auto* cq = client->cq();

                                std::cerr << "[DEBUG Client " << i << "] Waiting for " << num_go << " GO signals..." << std::endl;
                                std::cerr.flush();

                                size_t got = 0;
                                uint64_t poll_attempts = 0;
                                while (got < num_go) {
                                    ibv_wc wc{};
                                    int n = ibv_poll_cq(cq, 1, &wc);
                                    poll_attempts++;

                                    if (poll_attempts % 100000000 == 0) {
                                        std::cerr << "[DEBUG Client " << i << "] Still waiting... poll_attempts=" << poll_attempts << " got=" << got << std::endl;
                                        std::cerr.flush();
                                    }

                                    if (n > 0) {
                                        std::cerr << "[DEBUG Client " << i << "] Got completion: status=" << wc.status << " opcode=" << wc.opcode << " wr_id=0x" << std::hex << wc.wr_id << std::dec << std::endl;
                                        std::cerr.flush();

                                        if (wc.status == IBV_WC_SUCCESS && (wc.opcode & IBV_WC_RECV)) {
                                            got++;
                                            std::cerr << "[DEBUG Client " << i << "] GO signal received! got=" << got << "/" << num_go << std::endl;
                                            std::cerr.flush();
                                        }
                                    }
                                }

                                std::cerr << "[DEBUG Client " << i << "] All GO signals received, proceeding..." << std::endl;
                                std::cerr.flush();
                            }

                            std::cerr << "[DEBUG Client " << i << "] Arriving at latch..." << std::endl;
                            std::cerr.flush();
                            start_latch.arrive_and_wait();
                            std::cerr << "[DEBUG Client " << i << "] Passed latch, starting benchmark..." << std::endl;
                            std::cerr.flush();

                            // Watch strategies have extra notification ops, so use larger stride
                            const size_t per_client_stride = (is_watch || is_mu_watch)
                                ? (NUM_OPS_PER_CLIENT + (2000 / TOTAL_CLIENTS))
                                : NUM_OPS_PER_CLIENT;
                            uint64_t* latencies = &((*all_latencies)[i * per_client_stride]);

                            if (is_cas) {
                                run_cas_pipeline(
                                    *client,
                                    latencies,
                                    (*lock_counts)[global_id].data(),
                                    cas_config);
                            } else if (is_simple_cas) {
                                run_simple_cas_pipeline(
                                    *client,
                                    latencies,
                                    (*lock_counts)[global_id].data(),
                                    simple_cas_config);
                            } else if (is_ticket_faa) {
                                run_ticket_faa_lock_pipeline(
                                    *client,
                                    latencies,
                                    (*lock_counts)[global_id].data(),
                                    ticket_faa_config);
                            } else if (is_mu) {
                                run_mu_pipeline(
                                    *client,
                                    latencies,
                                    (*lock_counts)[global_id].data(),
                                    mu_config);
                            } else if (is_watch) {
                                run_watch_pipeline(
                                    *client,
                                    latencies,
                                    (*lock_counts)[global_id].data(),
                                    watch_config);
                            } else if (is_simple_watch) {
                                run_simple_watch_pipeline(
                                    *client,
                                    latencies,
                                    (*lock_counts)[global_id].data(),
                                    simple_watch_config);
                            } else if (is_mu_watch) {
                                std::cerr << "[DEBUG Client " << i << "] Calling run_mu_watch_pipeline..." << std::endl;
                                std::cerr.flush();
                                run_mu_watch_pipeline(
                                    *client,
                                    latencies,
                                    (*lock_counts)[global_id].data(),
                                    mu_watch_config);
                            } else {
                                throw std::runtime_error("Unsupported strategy");
                            }

                            Client* expected = nullptr;
                            if (verify_client.compare_exchange_strong(expected, client.get())) {
                                client.release();
                            }
                        }
                        catch (const std::exception& e) {
                            std::cerr << "[Client " << global_id << " error] " << e.what() << "\n";
                        }
                    });
            }

            start_latch.arrive_and_wait();
            auto wall_start = std::chrono::steady_clock::now();

            for (auto& w : workers) w.join();
            auto wall_end = std::chrono::steady_clock::now();

            // ─── Post-benchmark stats ───

            size_t local_total_ops = NUM_CLIENTS_PER_MACHINE * NUM_OPS_PER_CLIENT;

            // Watch strategies have extra notification ops
            if (is_watch || is_mu_watch) {
                constexpr size_t WATCH_EXTRA_NOTIFICATIONS = 2000;
                local_total_ops += WATCH_EXTRA_NOTIFICATIONS;
            }

            const double wall_s = std::chrono::duration_cast<std::chrono::microseconds>(
                wall_end - wall_start).count() / 1'000'000.0;

            // ─── Phase-separated stats for watch strategies ───
            // IMPORTANT: Separate phases BEFORE sorting, accounting for per-client stride layout
            std::vector<uint64_t> reg_lats, notif_lats;
            if (is_watch || is_mu_watch) {
                constexpr size_t WATCH_EXTRA_NOTIFICATIONS = 2000;
                const size_t registration_ops_total = NUM_TOTAL_OPS;
                const size_t notification_ops_total = WATCH_EXTRA_NOTIFICATIONS;
                const size_t notif_per_client = WATCH_EXTRA_NOTIFICATIONS / TOTAL_CLIENTS;
                const size_t per_client_stride = NUM_OPS_PER_CLIENT + notif_per_client;

                reg_lats.reserve(registration_ops_total);
                notif_lats.reserve(notification_ops_total);

                // Each client's data layout: [registrations (NUM_OPS_PER_CLIENT)][notifications (notif_per_client)]
                // We need to iterate through each client's stride region and collect registrations and notifications separately
                for (size_t client_id = 0; client_id < NUM_CLIENTS_PER_MACHINE; ++client_id) {
                    const size_t client_offset = client_id * per_client_stride;

                    // Collect registrations from this client
                    for (size_t j = 0; j < NUM_OPS_PER_CLIENT; ++j) {
                        reg_lats.push_back((*all_latencies)[client_offset + j]);
                    }

                    // Collect notifications from this client
                    for (size_t j = 0; j < notif_per_client; ++j) {
                        notif_lats.push_back((*all_latencies)[client_offset + NUM_OPS_PER_CLIENT + j]);
                    }
                }

                std::sort(reg_lats.begin(), reg_lats.end());
                std::sort(notif_lats.begin(), notif_lats.end());
            }

            // Now sort for overall stats
            std::sort(all_latencies->begin(), all_latencies->begin() + local_total_ops);

            auto get_p = [&](double p) -> double {
                size_t idx = static_cast<size_t>(p * (local_total_ops - 1));
                return (*all_latencies)[idx] / 1000.0;
            };

            double sum_us = 0;
            for (size_t i = 0; i < local_total_ops; ++i)
                sum_us += (*all_latencies)[i] / 1000.0;
            double mean = sum_us / local_total_ops;

            double sq_sum = 0;
            for (size_t i = 0; i < local_total_ops; ++i) {
                double diff = ((*all_latencies)[i] / 1000.0) - mean;
                sq_sum += diff * diff;
            }
            double std_dev = std::sqrt(sq_sum / local_total_ops);

            const double goodput = local_total_ops / wall_s;

            if (verify_client.load()) delete verify_client.load();

            // Print phase-separated stats (already computed above)
            if (is_watch || is_mu_watch) {
                constexpr size_t WATCH_EXTRA_NOTIFICATIONS = 2000;
                const size_t registration_ops_total = NUM_TOTAL_OPS;
                const size_t notification_ops_total = WATCH_EXTRA_NOTIFICATIONS;

                auto calc_p = [](const std::vector<uint64_t>& lats, double p) -> double {
                    if (lats.empty()) return 0.0;
                    size_t idx = static_cast<size_t>(p * (lats.size() - 1));
                    return lats[std::min(idx, lats.size() - 1)] / 1000.0;
                };

                std::cout << "\n" << std::string(50, '=') << "\n";
                std::cout << " PHASE-SEPARATED RESULTS\n";
                std::cout << std::string(50, '=') << "\n";
                std::cout << "REGISTRATION PHASE (" << registration_ops_total << " ops):\n";
                std::cout << "  P50:   " << std::fixed << std::setprecision(2) << std::setw(8) << calc_p(reg_lats, 0.50) << " μs\n";
                std::cout << "  P90:   " << std::setw(8) << calc_p(reg_lats, 0.90) << " μs\n";
                std::cout << "  P99:   " << std::setw(8) << calc_p(reg_lats, 0.99) << " μs\n";
                std::cout << "  P99.9: " << std::setw(8) << calc_p(reg_lats, 0.999) << " μs\n";
                std::cout << "\nNOTIFICATION PHASE (" << notification_ops_total << " ops):\n";
                std::cout << "  P50:   " << std::setw(8) << calc_p(notif_lats, 0.50) << " μs\n";
                std::cout << "  P90:   " << std::setw(8) << calc_p(notif_lats, 0.90) << " μs\n";
                std::cout << "  P99:   " << std::setw(8) << calc_p(notif_lats, 0.99) << " μs\n";
                std::cout << "  P99.9: " << std::setw(8) << calc_p(notif_lats, 0.999) << " μs\n";
                std::cout << std::string(50, '=') << "\n";
            }

            // ─── Human-readable output ───

            std::cout << "\n" << std::string(50, '=') << "\n";
            std::cout << " RDMA LOCK BENCHMARK RESULTS\n";
            std::cout << std::string(50, '=') << "\n";
            std::cout << "Strategy:       " << std::setw(14) << STRATEGY << "\n";
            std::cout << "Locks:          " << std::setw(14) << MAX_LOCKS << "\n";
            if (is_cas) {
                std::cout << "Active Window:  " << std::setw(14) << cas_config.active_window << "\n";
                std::cout << "Zipf Skew:      " << std::setw(14) << std::fixed << std::setprecision(2)
                          << cas_config.zipf_skew << "\n";
                std::cout << "Owner Mode:     " << std::setw(14)
                          << (cas_config.shard_owner ? "sharded" : "leader") << "\n";
                std::cout << "Log Mode:       " << std::setw(14) << "wrapped_cas" << "\n";
                std::cout << "Ctl Release:    " << std::setw(14)
                          << (cas_config.release_control_with_cas ? "cas" : "write") << "\n";
                std::cout << "Log Release:    " << std::setw(14)
                          << (cas_config.release_log_with_cas ? "cas" : "write") << "\n";
            } else if (is_simple_cas) {
                std::cout << "Active Window:  " << std::setw(14) << simple_cas_config.active_window << "\n";
                std::cout << "Zipf Skew:      " << std::setw(14) << std::fixed << std::setprecision(2)
                          << simple_cas_config.zipf_skew << "\n";
                std::cout << "Owner Mode:     " << std::setw(14)
                          << (simple_cas_config.shard_owner ? "sharded" : "leader") << "\n";
                std::cout << "Release Mode:   " << std::setw(14)
                          << (simple_cas_config.release_with_cas ? "cas" : "write") << "\n";
            } else if (is_ticket_faa) {
                std::cout << "Active Window:  " << std::setw(14) << ticket_faa_config.active_window << "\n";
                std::cout << "Zipf Skew:      " << std::setw(14) << std::fixed << std::setprecision(2)
                          << ticket_faa_config.zipf_skew << "\n";
                std::cout << "Replicate Mode: " << std::setw(14) << "cas" << "\n";
                std::cout << "Log Release:    " << std::setw(14)
                          << (ticket_faa_config.release_log_with_cas ? "cas" : "write") << "\n";
                std::cout << "Turn Release:   " << std::setw(14)
                          << (ticket_faa_config.release_turn_mode == 0 ? "write"
                              : (ticket_faa_config.release_turn_mode == 1 ? "cas" : "faa")) << "\n";
                std::cout << "Owner Mode:     " << std::setw(14)
                          << (ticket_faa_config.shard_owner ? "sharded" : "leader") << "\n";
            } else if (is_mu) {
                std::cout << "Active Window:  " << std::setw(14) << mu_config.active_window << "\n";
                std::cout << "Zipf Skew:      " << std::setw(14) << std::fixed << std::setprecision(2)
                          << mu_config.zipf_skew << "\n";
            } else if (is_watch) {
                std::cout << "Active Window:  " << std::setw(14) << watch_config.active_window << "\n";
                std::cout << "Zipf Skew:      " << std::setw(14) << std::fixed << std::setprecision(2)
                          << watch_config.zipf_skew << "\n";
                std::cout << "Owner Mode:     " << std::setw(14)
                          << (watch_config.shard_owner ? "sharded" : "leader") << "\n";
            } else if (is_simple_watch) {
                std::cout << "Active Window:  " << std::setw(14) << simple_watch_config.active_window << "\n";
                std::cout << "Zipf Skew:      " << std::setw(14) << std::fixed << std::setprecision(2)
                          << simple_watch_config.zipf_skew << "\n";
                std::cout << "Owner Mode:     " << std::setw(14)
                          << (simple_watch_config.shard_owner ? "sharded" : "leader") << "\n";
            } else if (is_mu_watch) {
                std::cout << "Active Window:  " << std::setw(14) << mu_watch_config.active_window << "\n";
                std::cout << "Zipf Skew:      " << std::setw(14) << std::fixed << std::setprecision(2)
                          << mu_watch_config.zipf_skew << "\n";
            }
            std::cout << "Clients:        " << std::setw(14) << TOTAL_CLIENTS
                      << " (" << NUM_CLIENTS_PER_MACHINE << " on this machine)\n";
            std::cout << "Ops/Client:     " << std::setw(14) << NUM_OPS_PER_CLIENT << "\n";
            std::cout << "Total Ops:      " << std::setw(14) << local_total_ops << "\n";
            std::cout << std::string(50, '-') << "\n";
            std::cout << "Wall Clock:     " << std::setw(14) << std::fixed
                      << std::setprecision(3) << wall_s << " s\n";
            std::cout << "Goodput:        " << std::setw(14) << std::fixed
                      << std::setprecision(0) << goodput << " ops/s\n";
            std::cout << std::string(50, '-') << "\n";
            std::cout << "ACQUIRE LATENCY (us)\n";
            std::cout << "Mean:           " << std::setw(14) << std::setprecision(2) << mean << "\n";
            std::cout << "StdDev:         " << std::setw(14) << std::setprecision(2) << std_dev << "\n";
            std::cout << "P0  (Min):      " << std::setw(14) << get_p(0.0) << "\n";
            std::cout << "P50 (Med):      " << std::setw(14) << get_p(0.5) << "\n";
            std::cout << "P90:            " << std::setw(14) << get_p(0.9) << "\n";
            std::cout << "P99:            " << std::setw(14) << get_p(0.99) << "\n";
            std::cout << "P99.9:          " << std::setw(14) << get_p(0.999) << "\n";
            std::cout << "P100 (Max):     " << std::setw(14) << get_p(1.0) << "\n";
            std::cout << std::string(50, '=') << "\n";

            // ─── CSV row for spreadsheets ───
            // Paste this line into a cell, then use "Split text to columns" with comma delimiter
            std::cout << "\nCSV: "
                      << STRATEGY
                      << "," << machine_id
                      << "," << TOTAL_CLIENTS
                      << "," << MAX_LOCKS
                      << "," << (is_cas ? cas_config.active_window : (is_simple_cas ? simple_cas_config.active_window : (is_ticket_faa ? ticket_faa_config.active_window : (is_mu ? mu_config.active_window : (is_watch ? watch_config.active_window : (is_simple_watch ? simple_watch_config.active_window : (is_mu_watch ? mu_watch_config.active_window : 0)))))))
                      << "," << std::fixed << std::setprecision(2)
                      << (is_cas ? cas_config.zipf_skew : (is_simple_cas ? simple_cas_config.zipf_skew : (is_ticket_faa ? ticket_faa_config.zipf_skew : (is_mu ? mu_config.zipf_skew : (is_watch ? watch_config.zipf_skew : (is_simple_watch ? simple_watch_config.zipf_skew : (is_mu_watch ? mu_watch_config.zipf_skew : 0.0)))))))
                      << "," << local_total_ops
                      << "," << std::fixed << std::setprecision(3) << wall_s
                      << "," << std::setprecision(0) << goodput
                      << "," << std::setprecision(2) << mean
                      << "," << std::setprecision(2) << get_p(0.5)
                      << "," << std::setprecision(2) << get_p(0.9)
                      << "," << std::setprecision(2) << get_p(0.99)
                      << "," << std::setprecision(2) << get_p(0.999)
                      << "," << std::setprecision(2) << get_p(1.0)
                      << std::endl;

            // Header reminder (print once for reference)
            std::cout << "HDR: strategy,client_machine_id,clients,locks,active_window,zipf_skew,total_ops,wall_s,goodput,mean_us,p50_us,p90_us,p99_us,p99.9_us,max_us"
                      << std::endl;

        } else {
            std::cerr << "[DEBUG] Server mode, getting NODE_ID..." << std::endl;
            std::cerr.flush();
            const uint32_t node_id = get_uint_env("NODE_ID");
            std::cerr << "[DEBUG] NODE_ID=" << node_id << std::endl;
            std::cerr.flush();

            if (is_mu || is_mu_watch) {
                std::cerr << "[DEBUG] MU mode, pinning to CPU 0..." << std::endl;
                std::cerr.flush();
                pin_thread_to_cpu(0);
                if (node_id == 0) {
                    std::cerr << "[DEBUG] Creating MuLeader..." << std::endl;
                    std::cerr.flush();
                    MuLeader leader(node_id, 0, MAX_LOCKS);
                    std::cerr << "[DEBUG] MuLeader created, starting..." << std::endl;
                    std::cerr.flush();
                    leader.start(RDMA_PORT);
                } else {
                    std::cerr << "[DEBUG] Creating MuFollower..." << std::endl;
                    std::cerr.flush();
                    MuFollower follower(node_id, 0, MAX_LOCKS);
                    std::cerr << "[DEBUG] MuFollower created, starting..." << std::endl;
                    std::cerr.flush();
                    follower.start(RDMA_PORT);
                }
            } else {
                std::cerr << "[DEBUG] Regular mode, pinning to CPU 1..." << std::endl;
                std::cerr.flush();
                pin_thread_to_cpu(1);
                std::cerr << "[DEBUG] Creating SynraNode..." << std::endl;
                std::cerr.flush();
                SynraNode node(node_id);
                std::cerr << "[DEBUG] SynraNode created, starting..." << std::endl;
                std::cerr.flush();
                node.start(RDMA_PORT);
            }
        }
    }
    catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
