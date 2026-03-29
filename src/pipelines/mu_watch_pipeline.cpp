#include "rdma/pipelines/mu_watch_pipeline.h"

// Mu watch pipeline: Leader-based watch registration and notification.

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mu_encoding.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

enum class MuWatchPhase : uint8_t {
    idle = 0,
    wait_register_ack = 1,    // Waiting for leader to ACK registration
    wait_notify_ack = 2,       // Waiting for leader to ACK notification
};

constexpr uint64_t MU_RECV_WR_TAG = 0xA1ULL;
constexpr uint64_t MU_SEND_WR_TAG = 0xA2ULL;
constexpr uint64_t MU_WR_TAG_SHIFT = 56;
constexpr uint64_t MU_WR_GEN_SHIFT = 24;
constexpr uint64_t MU_WR_SLOT_SHIFT = 8;
constexpr uint64_t MU_WR_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t MU_WR_SLOT_MASK = 0xFFFFULL;
constexpr size_t MU_CLIENT_RECV_RING_MIN = 32;

struct MuWatchClientBuffers {
    MuResponse* responses = nullptr;
    size_t recv_ring = 0;
};

struct MuWatchOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t object_id = 0;
    uint32_t req_id = 0;
    size_t latency_index = 0;
    MuWatchPhase phase = MuWatchPhase::idle;
    std::chrono::steady_clock::time_point started_at{};
};

// Encode a receive WR id.
uint64_t make_recv_wr_id(const uint32_t recv_slot) {
    return (MU_RECV_WR_TAG << MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(recv_slot) & MU_WR_SLOT_MASK) << MU_WR_SLOT_SHIFT);
}

// Encode a send WR id.
uint64_t make_send_wr_id(const MuWatchOpCtx& op, const MuWatchPhase phase) {
    return (MU_SEND_WR_TAG << MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(op.generation) & MU_WR_GEN_MASK) << MU_WR_GEN_SHIFT)
         | ((static_cast<uint64_t>(op.slot) & MU_WR_SLOT_MASK) << MU_WR_SLOT_SHIFT)
         | static_cast<uint64_t>(phase);
}

// Check WR type.
bool is_recv_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_RECV_WR_TAG;
}

bool is_send_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_SEND_WR_TAG;
}

// Extract fields from WR id.
uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> MU_WR_GEN_SHIFT) & MU_WR_GEN_MASK);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> MU_WR_SLOT_SHIFT) & MU_WR_SLOT_MASK);
}

MuWatchPhase wr_phase(const uint64_t wr_id) {
    return static_cast<MuWatchPhase>(wr_id & 0xFFu);
}

// Map client buffer.
MuWatchClientBuffers map_client_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t recv_ring
) {
    MuWatchClientBuffers buffers{};
    auto* base = static_cast<uint8_t*>(raw_buffer);
    const size_t response_bytes = align_up(recv_ring * sizeof(MuResponse), 64);
    if (response_bytes > buffer_size) {
        throw std::runtime_error("MU watch pipeline: client buffer too small");
    }
    buffers.responses = reinterpret_cast<MuResponse*>(base);
    buffers.recv_ring = recv_ring;
    return buffers;
}

// Post receive buffer on leader connection.
void post_recv(Client& client, MuResponse* response, const uint32_t recv_slot) {
    auto* mr = client.mr();
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(response);
    sge.length = sizeof(MuResponse);
    sge.lkey = mr->lkey;

    ibv_recv_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_recv_wr_id(recv_slot);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(leader.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("MU watch pipeline: failed to post recv");
    }
}

// Send request to leader (register or notify).
void post_request(
    Client& client,
    const MuWatchOpCtx& op,
    const MuRequest& request,
    uint32_t& signal_count,
    const uint32_t signal_every
) {
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&request);
    sge.length = sizeof(MuRequest);
    sge.lkey = 0;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_send_wr_id(op, op.phase);
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_INLINE;
    if (++signal_count % std::max(signal_every, 1u) == 0) {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }

    if (ibv_post_send(leader.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("MU watch pipeline: send request failed");
    }
}

} // namespace

// Load Mu watch pipeline config.
MuWatchPipelineConfig load_mu_watch_pipeline_config() {
    MuWatchPipelineConfig config{};
    config.active_window = std::max<size_t>(1, MU_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, MU_CQ_BATCH);
    config.client_send_signal_every = MU_CLIENT_SEND_SIGNAL_EVERY;
    config.zipf_skew = MU_ZIPF_SKEW;
    return config;
}

// Report required client buffer size.
size_t mu_watch_pipeline_client_buffer_size(const MuWatchPipelineConfig& config) {
    const size_t recv_ring = std::max(config.active_window * 2, MU_CLIENT_RECV_RING_MIN);
    const size_t response_bytes = align_up(recv_ring * sizeof(MuResponse), 64);
    return align_up(response_bytes + PAGE_SIZE, PAGE_SIZE);
}

// Main Mu watch pipeline: two-phase benchmark (registration, then notification).
void run_mu_watch_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* object_counts,
    const MuWatchPipelineConfig& config
) {

    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("MU watch pipeline: no leader connection");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("MU watch pipeline: active window exceeds wr_id slot encoding");
    }


    const size_t recv_ring = std::max(config.active_window * 2, MU_CLIENT_RECV_RING_MIN);
    auto buffers = map_client_buffers(client.buffer(), client.buffer_size(), recv_ring);

    std::vector<MuWatchOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);  // Reuse for object selection
    std::unordered_map<uint32_t, uint32_t> req_to_slot;
    req_to_slot.reserve(config.active_window * 2);

    // Post all receive buffers upfront
    for (size_t i = 0; i < recv_ring; ++i) {
        post_recv(client, &buffers.responses[i], static_cast<uint32_t>(i));
    }


    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 0;
    size_t recv_posted = recv_ring;

    // Two-phase benchmark: all ops are registrations, then fixed number of notifications
    // Registration: Use all NUM_OPS operations to register watchers
    // Notification: Fixed 2000 total notifications to test notification performance
    constexpr size_t TOTAL_NOTIFICATIONS = 2000;  // Fixed across all experiments
    const size_t notification_ops = TOTAL_NOTIFICATIONS / TOTAL_CLIENTS;  // Per-client share = 250
    const size_t registration_ops = NUM_OPS_PER_CLIENT;  // All ops are registrations
    bool in_registration_phase = true;

    // Verification tracking
    uint64_t total_registrations_completed = 0;
    uint64_t total_notifications_completed = 0;

    // Phase timing for separate throughput reporting
    auto registration_start_time = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point registration_end_time;
    std::chrono::steady_clock::time_point notification_start_time;
    bool registration_timing_done = false;
    bool notification_timing_started = false;

    // Track phase-separated latency indices
    size_t registration_latency_start = 0;
    size_t notification_latency_start = registration_ops;

    // Send signaling control (match mu_pipeline pattern)
    uint32_t signal_count = 0;
    const uint32_t signal_every = config.client_send_signal_every;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.object_id = picker.next();  // Random object
        op.req_id = next_req_id++;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        req_to_slot[op.req_id] = op.slot;

        MuRequest req{};
        req.client_id = static_cast<uint16_t>(client.id());
        req.lock_id = op.object_id;  // Reuse lock_id field for object_id
        req.req_id = op.req_id;

        if (in_registration_phase) {
            // Registration phase: send WatchRegister
            op.phase = MuWatchPhase::wait_register_ack;
            req.op = static_cast<uint8_t>(MuRpcOp::WatchRegister);
        } else {
            // Notification phase: send WatchNotify
            if (!notification_timing_started) {
                notification_start_time = std::chrono::steady_clock::now();
                notification_timing_started = true;
            }
            op.phase = MuWatchPhase::wait_notify_ack;
            req.op = static_cast<uint8_t>(MuRpcOp::WatchNotify);
        }

        post_request(client, op, req, signal_count, signal_every);
        submitted++;
        active++;
    };

    // Fill pipeline
    const size_t total_ops = registration_ops + notification_ops;

    std::cerr << "[Client " << client.id() << "] Starting initial submission: total_ops=" << total_ops
              << " active_window=" << config.active_window << std::endl;

    while (active < config.active_window && submitted < total_ops) {
        if (submitted < 10) {
            std::cerr << "[Client " << client.id() << "] Submitting op " << submitted << std::endl;
        }
        submit_op(active);
        if (submitted < 10) {
            std::cerr << "[Client " << client.id() << "] Submitted op " << submitted << " successfully" << std::endl;
        }
    }

    std::cerr << "[Client " << client.id() << "] Initial submission done: submitted=" << submitted
              << " active=" << active << " entering polling loop..." << std::endl;

    // Main completion loop
    uint64_t poll_count = 0;
    auto last_progress_time = std::chrono::steady_clock::now();
    size_t last_completed = 0;
    while (completed < total_ops) {
        poll_count++;
        auto now = std::chrono::steady_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_progress_time).count();

        if (elapsed_ms > 3000) {
            std::cerr << "[Client " << client.id() << "] Progress: completed=" << completed << "/" << total_ops
                      << " active=" << active << " submitted=" << submitted << " poll_count=" << poll_count << std::endl;
            last_progress_time = now;
            if (completed == last_completed) {
                std::cerr << "[Client " << client.id() << "] WARNING: No progress in 3s!" << std::endl;
            }
            last_completed = completed;
        }
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()),
                                      completions.data());
        if (polled < 0) {
            throw std::runtime_error("MU watch pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        if (completed < 10 || poll_count % 1000000 == 0) {
            std::cerr << "[Client " << client.id() << "] Polled " << polled << " completions" << std::endl;
        }


        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error("MU watch pipeline: WC error status=" + std::to_string(wc.status));
            }

            if (is_recv_wr_id(wc.wr_id)) {
                // Received response from leader
                const uint32_t recv_slot = wr_slot(wc.wr_id);
                if (recv_slot >= buffers.recv_ring) {
                    throw std::runtime_error("MU watch pipeline: recv slot out of range");
                }

                const MuResponse& resp = buffers.responses[recv_slot];

                // Look up op slot from req_id
                const auto it = req_to_slot.find(resp.req_id);
                if (it == req_to_slot.end()) {
                    // Unknown req_id, repost and continue
                    post_recv(client, &buffers.responses[recv_slot], recv_slot);
                    continue;
                }

                const uint32_t op_slot = it->second;
                auto& op = ops[op_slot];

                if (!op.active || op.req_id != resp.req_id) {
                    // Stale response, repost
                    post_recv(client, &buffers.responses[recv_slot], recv_slot);
                    continue;
                }

                // Check response status (match mu_pipeline error handling)
                if (resp.status != static_cast<uint8_t>(MuRpcStatus::Ok)) {
                    throw std::runtime_error("MU watch pipeline: leader returned error status " + std::to_string(resp.status));
                }

                if (op.phase == MuWatchPhase::wait_register_ack) {
                    // Registration ACK received - measure and complete
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    req_to_slot.erase(it);
                    op.active = false;
                    op.phase = MuWatchPhase::idle;
                    completed++;
                    active--;
                    total_registrations_completed++;

                    // Mark registration phase timing as done when last registration completes
                    if (completed >= registration_ops && !registration_timing_done) {
                        registration_end_time = std::chrono::steady_clock::now();
                        registration_timing_done = true;
                    }

                    if (submitted < total_ops) {
                        // Switch to notification phase when we've submitted all registrations
                        if (submitted >= registration_ops) {
                            in_registration_phase = false;
                        }
                        submit_op(op_slot);
                    }
                } else if (op.phase == MuWatchPhase::wait_notify_ack) {
                    // Notification ACK received - measure and complete
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    req_to_slot.erase(it);
                    op.active = false;
                    op.phase = MuWatchPhase::idle;
                    completed++;
                    active--;
                    total_notifications_completed++;

                    if (submitted < total_ops) {
                        submit_op(op_slot);
                    }
                }

                // Repost receive buffer
                post_recv(client, &buffers.responses[recv_slot], recv_slot);
                recv_posted++;
            } else if (is_send_wr_id(wc.wr_id)) {
                // Send completion (signaled sends only)
                // Nothing to do here
            }
        }
    }

    // Report verification results
    std::cerr << "\n[VERIFICATION-START] Client " << client.id() << " reached end of benchmark\n";
    std::cerr << "========================================\n";
    std::cerr << "[Client " << client.id() << "] MU Watch Verification\n";
    std::cerr << "========================================\n";

    // Registration verification
    std::cerr << "REGISTRATION PHASE:\n";
    std::cerr << "  Completed registrations: " << total_registrations_completed
              << " / " << registration_ops;
    if (total_registrations_completed == registration_ops) {
        std::cerr << " ✓ MATCH\n";
    } else {
        std::cerr << " ✗ MISMATCH\n";
    }

    // Notification verification
    std::cerr << "NOTIFICATION PHASE:\n";
    std::cerr << "  Completed notifications: " << total_notifications_completed
              << " / " << notification_ops;
    if (total_notifications_completed == notification_ops) {
        std::cerr << " ✓ MATCH\n";
    } else {
        std::cerr << " ✗ MISMATCH\n";
    }

    // Overall correctness check
    std::cerr << "CORRECTNESS CHECKS:\n";
    const bool all_ops_completed = (total_registrations_completed + total_notifications_completed) == total_ops;
    std::cerr << "  Total ops completed: " << (total_registrations_completed + total_notifications_completed)
              << " / " << total_ops;
    if (all_ops_completed) {
        std::cerr << " ✓ MATCH\n";
    } else {
        std::cerr << " ✗ MISMATCH\n";
    }

    const bool registration_match = (total_registrations_completed == registration_ops);
    const bool notification_match = (total_notifications_completed == notification_ops);

    if (registration_match && notification_match && all_ops_completed) {
        std::cerr << "  ✓ Check 1: All operation counts are correct\n";
    } else {
        std::cerr << "  ✗ Check 1: Operation count mismatch detected\n";
    }

    std::cerr << "[VERIFICATION-END]\n";

    // Print phase-specific throughput to stdout
    if (registration_timing_done && notification_timing_started) {
        const double reg_wall_s = std::chrono::duration_cast<std::chrono::microseconds>(
            registration_end_time - registration_start_time).count() / 1'000'000.0;
        const double notif_wall_s = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - notification_start_time).count() / 1'000'000.0;
        const double reg_throughput = registration_ops / reg_wall_s;
        const double notif_throughput = notification_ops / notif_wall_s;

        std::cout << "\n========================================\n";
        std::cout << " PHASE THROUGHPUT\n";
        std::cout << "========================================\n";
        std::cout << "REGISTRATION PHASE:\n";
        std::cout << "  Ops: " << registration_ops << "\n";
        std::cout << "  Wall Clock: " << std::fixed << std::setprecision(6) << reg_wall_s << " s\n";
        std::cout << "  Throughput: " << static_cast<uint64_t>(reg_throughput) << " ops/s\n";
        std::cout << "\nNOTIFICATION PHASE:\n";
        std::cout << "  Ops: " << notification_ops << "\n";
        std::cout << "  Wall Clock: " << notif_wall_s << " s\n";
        std::cout << "  Throughput: " << static_cast<uint64_t>(notif_throughput) << " ops/s\n";
        std::cout << "========================================\n" << std::flush;
    }

    // Build complete output atomically to prevent interleaving with other clients
    // Phase-separated stats now aggregated and printed in main.cpp
}
