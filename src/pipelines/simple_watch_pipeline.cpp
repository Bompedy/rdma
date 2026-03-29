#include "rdma/pipelines/simple_watch_pipeline.h"

// Minimal one-sided watch registration implementation (no replication).

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr size_t MAX_NOTIFY_BATCH = 1024;  // Safe with active_window (match watch_pipeline)

enum class SimpleWatchPhase : uint8_t {
    idle = 0,
    faa_slot = 1,        // Registration: FAA to get watcher slot from owner
    write_id = 2,        // Registration: Write client ID to watcher array
    read_count = 3,      // Notification: Read watcher count from owner
    read_watcher_ids = 4, // Notification: Read all watcher IDs
    notify_watchers = 5,  // Notification: Send invalidations to watchers
};

struct SimpleWatchOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t object_id = 0;
    uint32_t owner_node = 0;
    SimpleWatchPhase phase = SimpleWatchPhase::idle;
    size_t latency_index = 0;

    // Registration state
    uint64_t watcher_slot = 0;
    uint64_t faa_result = 0;

    // Notification state
    uint64_t total_watchers = 0;
    uint64_t notify_sent = 0;
    uint64_t notify_completed = 0;
    uint64_t notify_batch_size = 0;  // Size of current batch
    std::vector<uint64_t> watcher_ids;

    std::chrono::steady_clock::time_point started_at{};
};

struct SimpleWatchBuffers {
    uint64_t* faa_results = nullptr;
    uint64_t* watcher_counts = nullptr;
    uint64_t* watcher_array = nullptr;
};

uint64_t encode_wr_id(const SimpleWatchOpCtx& op, const SimpleWatchPhase phase) {
    return (static_cast<uint64_t>(op.generation) << 32)
         | (static_cast<uint64_t>(op.slot) << 16)
         | static_cast<uint64_t>(phase);
}

uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> 16) & 0xFFFFu);
}

SimpleWatchPhase wr_phase(const uint64_t wr_id) {
    return static_cast<SimpleWatchPhase>(wr_id & 0xFFu);
}

SimpleWatchBuffers map_buffers(void* raw_buffer, const size_t buffer_size, const size_t active_window) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;
    SimpleWatchBuffers buffers{};

    const size_t faa_bytes = align_up(active_window * sizeof(uint64_t), 64);
    buffers.faa_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += faa_bytes;

    const size_t count_bytes = align_up(active_window * sizeof(uint64_t), 64);
    buffers.watcher_counts = reinterpret_cast<uint64_t*>(base + offset);
    offset += count_bytes;

    const size_t array_bytes = align_up(active_window * MAX_WATCHERS_PER_OBJECT * sizeof(uint64_t), 64);
    buffers.watcher_array = reinterpret_cast<uint64_t*>(base + offset);
    offset += array_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("simple_watch pipeline: registered client buffer too small");
    }
    return buffers;
}

void post_faa_slot(Client& client, SimpleWatchOpCtx& op, const SimpleWatchBuffers& buffers) {
    const auto& conns = client.connections();
    const auto& owner = conns[op.owner_node];
    auto* mr = client.mr();
    auto* result = &buffers.faa_results[op.slot];
    *result = EMPTY_SLOT;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, SimpleWatchPhase::faa_slot);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = owner.addr + watch_counter_offset(op.object_id);
    wr.wr.atomic.rkey = owner.rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("simple_watch pipeline: FAA post failed");
    }

    op.phase = SimpleWatchPhase::faa_slot;
}

void post_write_id(Client& client, SimpleWatchOpCtx& op) {
    const auto& conns = client.connections();
    const auto& owner = conns[op.owner_node];
    const uint64_t watcher_id = client.id();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&watcher_id);
    sge.length = sizeof(uint64_t);
    sge.lkey = 0;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, SimpleWatchPhase::write_id);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    wr.wr.rdma.remote_addr = owner.addr + watch_id_slot_offset(op.object_id, op.watcher_slot);
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("simple_watch pipeline: write_id post failed");
    }

    op.phase = SimpleWatchPhase::write_id;
}

void post_read_count(Client& client, SimpleWatchOpCtx& op, const SimpleWatchBuffers& buffers) {
    const auto& conns = client.connections();
    const auto& owner = conns[op.owner_node];
    auto* mr = client.mr();
    auto* result = &buffers.watcher_counts[op.slot];
    *result = 0;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, SimpleWatchPhase::read_count);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = owner.addr + watch_counter_offset(op.object_id);
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("simple_watch pipeline: read_count post failed");
    }

    op.phase = SimpleWatchPhase::read_count;
}

void post_read_watcher_ids(Client& client, SimpleWatchOpCtx& op, const SimpleWatchBuffers& buffers) {
    const auto& conns = client.connections();
    const auto& owner = conns[op.owner_node];
    auto* mr = client.mr();

    const size_t read_count = std::min(op.total_watchers, static_cast<uint64_t>(MAX_WATCHERS_PER_OBJECT));
    auto* result = &buffers.watcher_array[op.slot * MAX_WATCHERS_PER_OBJECT];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = static_cast<uint32_t>(read_count * sizeof(uint64_t));
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, SimpleWatchPhase::read_watcher_ids);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = owner.addr + watch_id_slot_offset(op.object_id, 0);
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("simple_watch pipeline: read_watcher_ids post failed");
    }

    op.phase = SimpleWatchPhase::read_watcher_ids;
}

void post_notify_watchers(Client& client, SimpleWatchOpCtx& op, const SimpleWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();

    // Calculate how many watchers remain to be notified
    const uint64_t watchers_remaining = op.total_watchers - op.notify_sent;
    const uint64_t notify_count = std::min(watchers_remaining, static_cast<uint64_t>(MAX_NOTIFY_BATCH));
    const uint32_t batch_start = static_cast<uint32_t>(op.notify_sent);

    // For each watcher in this batch, post RDMA_WRITE to simulate notification
    for (uint64_t i = 0; i < notify_count; ++i) {
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&op.total_watchers);  // Dummy data
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        // Write to metadata area at end of watch table (simulating notification)
        // Distribute writes across servers in round-robin fashion
        const uint32_t target_node = static_cast<uint32_t>((batch_start + i) % conns.size());

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, SimpleWatchPhase::notify_watchers);
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[target_node].addr + WATCH_TABLE_SIZE + ((batch_start + i) * sizeof(uint64_t));
        wr.wr.rdma.rkey = conns[target_node].rkey;

        if (ibv_post_send(conns[target_node].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("simple_watch pipeline: notify watcher post failed");
        }
    }

    op.notify_sent += static_cast<uint32_t>(notify_count);
    op.notify_batch_size = notify_count;  // Track current batch size
    op.notify_completed = 0;  // Reset completion counter for this batch
    op.phase = SimpleWatchPhase::notify_watchers;
}

} // namespace

SimpleWatchPipelineConfig load_simple_watch_pipeline_config() {
    SimpleWatchPipelineConfig config{};
    config.active_window = SIMPLE_CAS_ACTIVE_WINDOW;
    config.cq_batch = SIMPLE_CAS_CQ_BATCH;
    config.zipf_skew = SIMPLE_CAS_ZIPF_SKEW;
    config.shard_owner = SIMPLE_CAS_SHARD_OWNER;
    return config;
}

size_t simple_watch_pipeline_client_buffer_size(const SimpleWatchPipelineConfig& config) {
    const size_t faa_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t count_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t array_bytes = align_up(config.active_window * MAX_WATCHERS_PER_OBJECT * sizeof(uint64_t), 64);
    return align_up(faa_bytes + count_bytes + array_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_simple_watch_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* object_counts,
    const SimpleWatchPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("simple_watch pipeline: no server connections");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("simple_watch pipeline: active window exceeds wr_id slot encoding");
    }

    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window);
    std::vector<SimpleWatchOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);

    // Two-phase benchmark: 99% registration, 1% notification
    const size_t registration_ops = NUM_OPS_PER_CLIENT * 99 / 100;
    const size_t notification_ops = NUM_OPS_PER_CLIENT - registration_ops;
    bool in_registration_phase = true;

    // Verification statistics
    uint64_t total_watchers_seen = 0;
    uint64_t max_watchers = 0;
    uint64_t min_watchers = UINT64_MAX;
    size_t zero_watcher_objects = 0;
    uint64_t total_registrations = 0;
    uint64_t total_notifications_sent = 0;
    uint64_t invalid_watcher_ids = 0;

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.object_id = picker.next();
        op.owner_node = config.shard_owner ? (op.object_id % conns.size()) : 0;
        op.phase = SimpleWatchPhase::idle;
        op.latency_index = submitted;
        op.watcher_slot = 0;
        op.faa_result = 0;
        op.total_watchers = 0;
        op.notify_sent = 0;
        op.notify_completed = 0;
        op.notify_batch_size = 0;
        op.started_at = std::chrono::steady_clock::now();

        if (in_registration_phase) {
            post_faa_slot(client, op, buffers);
        } else {
            post_read_count(client, op, buffers);
        }

        submitted++;
        active++;
    };

    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    while (completed < NUM_OPS_PER_CLIENT) {
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) {
            throw std::runtime_error("simple_watch pipeline: CQ poll failed");
        }

        for (int i = 0; i < polled; ++i) {
            const auto& wc = completions[i];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error("simple_watch pipeline: WC error status=" + std::to_string(wc.status));
            }

            const uint32_t gen = wr_generation(wc.wr_id);
            const uint32_t slot = wr_slot(wc.wr_id);
            const auto phase = wr_phase(wc.wr_id);

            if (slot >= ops.size()) {
                throw std::runtime_error("simple_watch pipeline: slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != gen) {
                continue;
            }

            if (phase == SimpleWatchPhase::faa_slot) {
                op.watcher_slot = buffers.faa_results[op.slot];

                if (op.watcher_slot >= MAX_WATCHERS_PER_OBJECT) {
                    throw std::runtime_error(
                        "simple_watch pipeline: watcher slot " + std::to_string(op.watcher_slot) +
                        " exceeds MAX_WATCHERS_PER_OBJECT " + std::to_string(MAX_WATCHERS_PER_OBJECT));
                }

                post_write_id(client, op);
                continue;
            }

            if (phase == SimpleWatchPhase::write_id) {
                latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - op.started_at).count();
                object_counts[op.object_id]++;

                // Track registration
                total_registrations++;

                completed++;
                active--;
                op.active = false;
                op.phase = SimpleWatchPhase::idle;

                // Check if registration phase is complete
                if (in_registration_phase && completed >= registration_ops) {
                    in_registration_phase = false;
                }

                if (submitted < NUM_OPS_PER_CLIENT) {
                    submit_op(slot);
                }
                continue;
            }

            if (phase == SimpleWatchPhase::read_count) {
                op.total_watchers = buffers.watcher_counts[op.slot];

                // Track statistics
                total_watchers_seen += op.total_watchers;
                max_watchers = std::max(max_watchers, op.total_watchers);
                if (op.total_watchers > 0) {
                    min_watchers = std::min(min_watchers, op.total_watchers);
                } else {
                    zero_watcher_objects++;
                }

                if (op.total_watchers > 0) {
                    post_read_watcher_ids(client, op, buffers);
                } else {
                    // No watchers, complete immediately
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    completed++;
                    active--;
                    op.active = false;
                    op.phase = SimpleWatchPhase::idle;

                    if (submitted < NUM_OPS_PER_CLIENT) {
                        submit_op(slot);
                    }
                }
                continue;
            }

            if (phase == SimpleWatchPhase::read_watcher_ids) {
                // Verify watcher IDs are valid (within client ID range)
                const size_t read_count = std::min(op.total_watchers, static_cast<uint64_t>(MAX_WATCHERS_PER_OBJECT));
                const uint64_t* watcher_ids = &buffers.watcher_array[op.slot * MAX_WATCHERS_PER_OBJECT];
                for (size_t i = 0; i < read_count; ++i) {
                    if (watcher_ids[i] >= TOTAL_CLIENTS) {
                        invalid_watcher_ids++;
                    }
                }

                // Start sending notifications in batches
                op.notify_sent = 0;
                op.notify_completed = 0;
                post_notify_watchers(client, op, buffers);
                continue;
            }

            if (phase == SimpleWatchPhase::notify_watchers) {
                // Count completed notifications
                op.notify_completed++;
                total_notifications_sent++;

                if (op.notify_completed >= op.notify_batch_size) {
                    // Completed current batch - check if more watchers remain
                    if (op.notify_sent < op.total_watchers) {
                        // More watchers to notify - send next batch
                        post_notify_watchers(client, op, buffers);
                        continue;
                    }

                    // All notifications sent! E2E operation complete
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    completed++;
                    active--;
                    op.active = false;
                    op.phase = SimpleWatchPhase::idle;

                    if (submitted < NUM_OPS_PER_CLIENT) {
                        submit_op(slot);
                    }
                }
                continue;
            }
        }
    }

    // Print verification statistics
    std::cout << "\n========================================\n";
    std::cout << "[Client " << client.id() << "] Watch Verification\n";
    std::cout << "========================================\n";
    std::cout << "REGISTRATION PHASE:\n";
    std::cout << "  Completed registrations: " << total_registrations << " / " << registration_ops;
    if (total_registrations == registration_ops) {
        std::cout << " ✓\n";
    } else {
        std::cout << " ✗ MISMATCH\n";
    }

    std::cout << "\nNOTIFICATION PHASE:\n";
    std::cout << "  Completed notifications: " << notification_ops << "\n";
    std::cout << "  Total watchers seen: " << total_watchers_seen << "\n";
    std::cout << "  Total notification messages sent: " << total_notifications_sent << "\n";
    std::cout << "  Avg watchers/object: " << (notification_ops > 0 ? total_watchers_seen / notification_ops : 0) << "\n";
    std::cout << "  Min watchers: " << (min_watchers == UINT64_MAX ? 0 : min_watchers) << "\n";
    std::cout << "  Max watchers: " << max_watchers << "\n";
    std::cout << "  Objects with 0 watchers: " << zero_watcher_objects << "\n";
    std::cout << "  Invalid watcher IDs: " << invalid_watcher_ids;
    if (invalid_watcher_ids == 0) {
        std::cout << " ✓\n";
    } else {
        std::cout << " ✗ ERROR\n";
    }

    std::cout << "\nCORRECTNESS CHECKS:\n";

    // Check 1: Expected watcher count
    const size_t expected_total_registrations = registration_ops * TOTAL_CLIENTS;
    const size_t expected_avg = expected_total_registrations / MAX_LOCKS;
    if (notification_ops > 0) {
        const size_t actual_avg = total_watchers_seen / notification_ops;
        std::cout << "  Expected avg watchers/object: ~" << expected_avg << "\n";
        std::cout << "  Actual avg watchers/object: " << actual_avg << "\n";
        if (actual_avg >= expected_avg * 0.9 && actual_avg <= expected_avg * 1.1) {
            std::cout << "  ✓ Watcher counts are correct (within 10%)\n";
        } else {
            std::cout << "  ✗ Watcher counts are incorrect!\n";
        }
    }

    // Check 2: Notifications match watchers
    if (total_notifications_sent == total_watchers_seen) {
        std::cout << "  ✓ All watchers were notified\n";
    } else {
        std::cout << "  ✗ Notification count mismatch: sent=" << total_notifications_sent
                  << " expected=" << total_watchers_seen << "\n";
    }

    // Check 3: No invalid IDs
    if (invalid_watcher_ids == 0) {
        std::cout << "  ✓ All watcher IDs are valid\n";
    } else {
        std::cout << "  ✗ Found " << invalid_watcher_ids << " invalid watcher IDs\n";
    }

    std::cout << "========================================\n";
}
