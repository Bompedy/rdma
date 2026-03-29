#include "rdma/pipelines/watch_pipeline.h"

// Synra watch pipeline: Single FAA on owner node + replicated watcher ID writes.

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr uint64_t kConnBits = 8;
constexpr uint64_t kRoundBits = 8;
constexpr uint64_t kPhaseBits = 8;
constexpr uint64_t kSlotBits = 16;
constexpr uint64_t kConnShift = 0;
constexpr uint64_t kRoundShift = kConnShift + kConnBits;
constexpr uint64_t kPhaseShift = kRoundShift + kRoundBits;
constexpr uint64_t kSlotShift = kPhaseShift + kPhaseBits;
constexpr uint64_t kGenerationShift = kSlotShift + kSlotBits;
constexpr uint64_t kConnMask = (1ULL << kConnBits) - 1;
constexpr uint64_t kRoundMask = (1ULL << kRoundBits) - 1;
constexpr uint64_t kPhaseMask = (1ULL << kPhaseBits) - 1;
constexpr uint64_t kSlotMask = (1ULL << kSlotBits) - 1;
constexpr uint64_t kGenerationMask = (1ULL << kSlotShift) - 1;

enum class WatchPhase : uint8_t {
    idle = 0,
    faa_slot = 1,          // Registration: FAA to get watcher slot from owner node
    write_id = 2,          // Registration: Write client ID to watcher array (super-quorum)
    read_count = 3,        // Notification: Read watcher count from owner
    read_watcher_ids = 4,  // Notification: Read all watcher IDs from owner
    notify_watchers = 5,   // Notification: Broadcast invalidations to all watchers
};

constexpr size_t MAX_NOTIFY_BATCH = 1024;  // Max watchers to notify per batch (safe with active_window=8)

struct RegisteredWatchBuffers {
    uint64_t* faa_results = nullptr;         // FAA slot result (single per op)
    uint64_t* write_results = nullptr;       // Write completion results (per replica)
    uint64_t* count_result = nullptr;        // Watcher count read result
    uint64_t* watcher_ids_buffer = nullptr;  // Buffer for reading watcher IDs
    uint64_t* notify_results = nullptr;      // Notification write results
};

struct WatchOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t object_id = 0;
    uint32_t owner_node = 0;
    uint8_t round = 0;
    uint64_t watcher_slot = 0;           // Assigned slot from FAA
    uint64_t watcher_id = 0;             // Our unique watcher ID
    WatchPhase phase = WatchPhase::idle;
    uint32_t responses = 0;
    uint32_t response_target = 0;

    // Notification phase fields
    uint64_t total_watchers = 0;         // Total watchers for this object
    uint32_t notify_sent = 0;            // Number of notifications sent
    uint32_t notify_completed = 0;       // Number of notifications completed

    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

// Encode a unique watcher ID from client ID, operation slot, and request ID.
uint64_t encode_watcher_id(const uint16_t client_id, const uint16_t op_slot, const uint32_t req_id) {
    return (static_cast<uint64_t>(client_id) << 47)
         | (static_cast<uint64_t>(op_slot) << 32)
         | static_cast<uint64_t>(req_id);
}

// Encode generation, slot, phase, round, and connection index into one WR id.
uint64_t encode_wr_id(const WatchOpCtx& op, const WatchPhase phase, const uint8_t conn_index) {
    return ((static_cast<uint64_t>(op.generation) & kGenerationMask) << kGenerationShift)
         | ((static_cast<uint64_t>(op.slot) & kSlotMask) << kSlotShift)
         | ((static_cast<uint64_t>(phase) & kPhaseMask) << kPhaseShift)
         | ((static_cast<uint64_t>(op.round) & kRoundMask) << kRoundShift)
         | ((static_cast<uint64_t>(conn_index) & kConnMask) << kConnShift);
}

// Extract generation, slot, phase, round, and connection index from WR id.
uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> kGenerationShift);
}

uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> kSlotShift) & kSlotMask);
}

WatchPhase wr_phase(const uint64_t wr_id) {
    return static_cast<WatchPhase>((wr_id >> kPhaseShift) & kPhaseMask);
}

uint8_t wr_round(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> kRoundShift) & kRoundMask);
}

uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> kConnShift) & kConnMask);
}

// Compute row-major offset for per-op-by-replica buffer.
uint64_t* row_ptr(uint64_t* base, const uint32_t row, const size_t cols) {
    return base + (static_cast<size_t>(row) * cols);
}

// Map client buffer into per-op result arrays.
RegisteredWatchBuffers map_buffers(void* raw_buffer, const size_t buffer_size,
                                   const size_t active_window, const size_t num_replicas) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;
    RegisteredWatchBuffers buffers{};

    // FAA results: single result per op (not per replica)
    const size_t faa_bytes = align_up(active_window * sizeof(uint64_t), 64);
    const size_t write_bytes = align_up(active_window * num_replicas * sizeof(uint64_t), 64);
    const size_t count_bytes = align_up(active_window * sizeof(uint64_t), 64);
    const size_t ids_bytes = align_up(active_window * MAX_NOTIFY_BATCH * sizeof(uint64_t), 64);
    const size_t notify_bytes = align_up(active_window * MAX_NOTIFY_BATCH * sizeof(uint64_t), 64);

    buffers.faa_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += faa_bytes;
    buffers.write_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += write_bytes;
    buffers.count_result = reinterpret_cast<uint64_t*>(base + offset);
    offset += count_bytes;
    buffers.watcher_ids_buffer = reinterpret_cast<uint64_t*>(base + offset);
    offset += ids_bytes;
    buffers.notify_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += notify_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("watch pipeline: registered client buffer too small");
    }
    return buffers;
}

// Post FAA to owner node to get watcher slot assignment (single FAA, not replicated).
void post_faa_slot(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* result = &buffers.faa_results[op.slot];

    op.round++;
    op.phase = WatchPhase::faa_slot;
    op.responses = 0;
    op.response_target = 1;

    // FAA only on owner node (like Synra paper)
    const auto& owner = conns[op.owner_node];
    *result = 0;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, WatchPhase::faa_slot, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.atomic.remote_addr = owner.addr + watch_counter_offset(op.object_id);
    wr.wr.atomic.rkey = owner.rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("watch pipeline: FAA slot post failed");
    }
}

// Write watcher ID to super-quorum after getting slot.
void post_write_id(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.write_results, op.slot, conns.size());

    op.round++;
    op.phase = WatchPhase::write_id;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());

    const uint64_t write_offset = watch_id_slot_offset(op.object_id, op.watcher_slot);
    // if (op.slot == 0) {  // Log first operation only
    //     std::cerr << "[DEBUG] WRITE_ID: object_id=" << op.object_id
    //               << " watcher_slot=" << op.watcher_slot
    //               << " watch_id_slot_offset=" << write_offset
    //               << " (MAX_WATCHERS=" << MAX_WATCHERS_PER_OBJECT << ")" << std::endl;
    // }

    // Write our watcher ID to all nodes in parallel
    for (size_t i = 0; i < conns.size(); ++i) {
        results[i] = op.watcher_id;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, WatchPhase::write_id, static_cast<uint8_t>(i));
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conns[i].addr + write_offset;
        wr.wr.rdma.rkey = conns[i].rkey;

        // if (op.slot == 0 && i == 0) {
        //     std::cerr << "[DEBUG] WRITE_ID to node " << i << ": remote_addr=0x" << std::hex
        //               << wr.wr.rdma.remote_addr << std::dec
        //               << " rkey=" << wr.wr.rdma.rkey << std::endl;
        // }

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("watch pipeline: write ID post failed");
        }
    }
}

// Read watcher count from owner node to start notification.
void post_read_count(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];
    uint64_t* result = &buffers.count_result[op.slot];
    *result = 0;

    op.round++;
    op.phase = WatchPhase::read_count;
    op.responses = 0;
    op.response_target = 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, WatchPhase::read_count, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = owner.addr + watch_counter_offset(op.object_id);
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("watch pipeline: read count post failed");
    }
}

// Read watcher IDs from owner node (batched if needed).
void post_read_watcher_ids(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];

    // Limit reads to MAX_NOTIFY_BATCH watchers at a time
    const uint64_t read_count = std::min(op.total_watchers, static_cast<uint64_t>(MAX_NOTIFY_BATCH));
    uint64_t* ids_buf = &buffers.watcher_ids_buffer[op.slot * MAX_NOTIFY_BATCH];

    op.round++;
    op.phase = WatchPhase::read_watcher_ids;
    op.responses = 0;
    op.response_target = 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(ids_buf);
    sge.length = read_count * sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, WatchPhase::read_watcher_ids, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = owner.addr + watch_id_slot_offset(op.object_id, 0);  // Start from slot 0
    wr.wr.rdma.rkey = owner.rkey;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("watch pipeline: read watcher IDs post failed");
    }
}

// Broadcast invalidations to all watchers (write to metadata area to simulate).
void post_notify_watchers(Client& client, WatchOpCtx& op, const RegisteredWatchBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();

    // Calculate how many watchers remain to be notified
    const uint64_t watchers_remaining = op.total_watchers - op.notify_sent;
    const uint64_t notify_count = std::min(watchers_remaining, static_cast<uint64_t>(MAX_NOTIFY_BATCH));
    uint64_t* notify_buf = &buffers.notify_results[op.slot * MAX_NOTIFY_BATCH];

    op.round++;
    op.phase = WatchPhase::notify_watchers;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(notify_count);
    const uint32_t batch_start = op.notify_sent;

    // For each watcher in this batch, WRITE invalidation (simulate by writing to metadata area)
    uint64_t actually_posted = 0;
    for (uint64_t i = 0; i < notify_count; ++i) {
        notify_buf[i] = 1;  // Invalidation flag

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&notify_buf[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        // Choose a random node to write to (simulate distributed watchers)
        const uint32_t target_node = static_cast<uint32_t>(i % conns.size());

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, WatchPhase::notify_watchers, static_cast<uint8_t>(target_node));
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        // Write to metadata area at end of watch table (simulating dirty bit)
        wr.wr.rdma.remote_addr = conns[target_node].addr + WATCH_TABLE_SIZE + ((batch_start + i) * sizeof(uint64_t));
        wr.wr.rdma.rkey = conns[target_node].rkey;

        if (ibv_post_send(conns[target_node].id->qp, &wr, &bad_wr)) {
            // Queue overflow - log error but continue to allow verification
            std::cerr << "[Client " << client.id() << " error] watch pipeline: notify watcher post failed"
                      << " (posted " << actually_posted << "/" << notify_count << " in this batch)\n";
            break;
        }
        actually_posted++;
    }

    // Update response_target and track how many notifications we've actually sent
    op.response_target = static_cast<uint32_t>(actually_posted);
    op.notify_sent += static_cast<uint32_t>(actually_posted);

    // If queue was completely full (posted 0), force completion to avoid infinite loop
    if (actually_posted == 0 && notify_count > 0) {
        op.notify_sent = op.total_watchers;  // Force completion
    }
}

} // namespace

// Load watch pipeline config from compile-time constants.
WatchPipelineConfig load_watch_pipeline_config() {
    WatchPipelineConfig config{};
    config.active_window = std::max<size_t>(1, WATCH_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, WATCH_CQ_BATCH);
    config.zipf_skew = WATCH_ZIPF_SKEW;
    config.shard_owner = WATCH_SHARD_OWNER;
    return config;
}

// Report required client buffer size for watch pipeline.
size_t watch_pipeline_client_buffer_size(const WatchPipelineConfig& config) {
    const size_t num_replicas = CLUSTER_NODES.size();
    const size_t faa_bytes = align_up(config.active_window * num_replicas * sizeof(uint64_t), 64);
    const size_t write_bytes = align_up(config.active_window * num_replicas * sizeof(uint64_t), 64);
    const size_t count_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t ids_bytes = align_up(config.active_window * MAX_NOTIFY_BATCH * sizeof(uint64_t), 64);
    const size_t notify_bytes = align_up(config.active_window * MAX_NOTIFY_BATCH * sizeof(uint64_t), 64);
    return align_up(faa_bytes + write_bytes + count_bytes + ids_bytes + notify_bytes + PAGE_SIZE, PAGE_SIZE);
}

// Main watch pipeline: two-phase benchmark (registration, then notification).
void run_watch_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* object_counts,
    const WatchPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("watch pipeline: no server connections");
    }
    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("watch pipeline: active window exceeds wr_id slot encoding");
    }

    auto buffers = map_buffers(client.buffer(), client.buffer_size(),
                              config.active_window, conns.size());
    std::vector<WatchOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    ZipfLockPicker picker(config.zipf_skew);  // Reuse for object selection

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 0;

    // Two-phase benchmark: all ops are registrations, then fixed number of notifications
    // Registration: Use all NUM_OPS operations to register watchers
    // Notification: Fixed 2000 total notifications to test notification performance
    constexpr size_t TOTAL_NOTIFICATIONS = 2000;  // Fixed across all experiments
    const size_t notification_ops = TOTAL_NOTIFICATIONS / TOTAL_CLIENTS;  // Per-client share = 250
    const size_t registration_ops = NUM_OPS_PER_CLIENT;  // All ops are registrations
    bool in_registration_phase = true;

    // Verification statistics
    uint64_t total_registrations = 0;
    uint64_t total_notifications_sent = 0;
    uint64_t total_watchers_seen = 0;
    uint64_t max_watchers = 0;
    uint64_t min_watchers = UINT64_MAX;
    size_t zero_watcher_objects = 0;
    uint64_t invalid_watcher_ids = 0;

    // Phase timing for separate throughput reporting
    auto registration_start_time = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point registration_end_time;
    std::chrono::steady_clock::time_point notification_start_time;
    bool registration_timing_done = false;
    bool notification_timing_started = false;

    // Track phase-separated latency indices
    size_t registration_latency_start = 0;
    size_t notification_latency_start = registration_ops;

    auto submit_op = [&](const size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.object_id = picker.next();  // Random object (like lock_id)
        op.owner_node = config.shard_owner ? (op.object_id % conns.size()) : 0;
        op.phase = WatchPhase::idle;
        op.latency_index = submitted;
        op.watcher_id = encode_watcher_id(client.id(), static_cast<uint16_t>(slot), next_req_id++);
        op.started_at = std::chrono::steady_clock::now();

        if (in_registration_phase) {
            // Registration phase: FAA to get slot
            post_faa_slot(client, op, buffers);
        } else {
            // Notification phase: read watcher count
            if (!notification_timing_started) {
                notification_start_time = std::chrono::steady_clock::now();
                notification_timing_started = true;
            }
            op.notify_sent = 0;
            op.notify_completed = 0;
            post_read_count(client, op, buffers);
        }
        submitted++;
        active++;
    };

    // Fill pipeline
    const size_t total_ops = registration_ops + notification_ops;

    while (active < config.active_window && submitted < total_ops) {
        submit_op(active);
    }

    // Main completion loop
    while (completed < total_ops) {
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()),
                                      completions.data());
        if (polled < 0) {
            throw std::runtime_error("watch pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (wc.status != IBV_WC_SUCCESS) {
                const uint32_t slot = wr_slot(wc.wr_id);
                const WatchPhase phase = wr_phase(wc.wr_id);
                const uint8_t conn_idx = wr_conn(wc.wr_id);
                const char* phase_name = "unknown";
                if (phase == WatchPhase::faa_slot) phase_name = "faa_slot";
                else if (phase == WatchPhase::write_id) phase_name = "write_id";
                else if (phase == WatchPhase::read_count) phase_name = "read_count";
                else if (phase == WatchPhase::read_watcher_ids) phase_name = "read_watcher_ids";
                else if (phase == WatchPhase::notify_watchers) phase_name = "notify_watchers";

                std::cerr << "[ERROR] WC failed: status=" << wc.status
                          << " vendor_err=" << wc.vendor_err
                          << " phase=" << phase_name
                          << " slot=" << slot
                          << " conn=" << static_cast<int>(conn_idx)
                          << " opcode=" << wc.opcode
                          << " byte_len=" << wc.byte_len << std::endl;
                throw std::runtime_error("watch pipeline: WC error status=" + std::to_string(wc.status));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("watch pipeline: completion slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) {
                continue;
            }

            const WatchPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) {
                continue;
            }

            const uint8_t conn_idx = wr_conn(wc.wr_id);
            op.responses++;

            if (phase == WatchPhase::faa_slot) {
                // Got slot from single FAA (no super-quorum checking needed)
                op.watcher_slot = buffers.faa_results[op.slot];

                // Now replicate watcher_id to all nodes
                post_write_id(client, op, buffers);
                continue;
            }

            if (phase == WatchPhase::write_id) {
                // Wait for quorum writes to complete
                if (op.responses >= QUORUM) {
                    // Track registration completion
                    total_registrations++;

                    // Complete write_id regardless of phase (handles late completions)
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    op.active = false;
                    op.phase = WatchPhase::idle;
                    completed++;
                    active--;

                    // Check if registration phase is complete - switch immediately when count reached
                    if (in_registration_phase && completed >= registration_ops) {
                        in_registration_phase = false;
                        if (!registration_timing_done) {
                            registration_end_time = std::chrono::steady_clock::now();
                            registration_timing_done = true;
                        }
                    }

                    if (submitted < total_ops) {
                        submit_op(slot);
                    }
                }
                continue;
            }

            if (phase == WatchPhase::read_count) {
                // Got watcher count, now read all watcher IDs (notification phase only)
                op.total_watchers = buffers.count_result[op.slot];

                // Track verification stats
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
                    // No watchers (edge case), complete notification with zero broadcasts
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;
                    op.active = false;
                    op.phase = WatchPhase::idle;
                    completed++;
                    active--;
                    if (submitted < total_ops) {
                        submit_op(slot);
                    }
                }
                continue;
            }

            if (phase == WatchPhase::read_watcher_ids) {
                // Got watcher IDs, now broadcast notifications

                // Validate watcher IDs
                const uint64_t* watcher_ids = &buffers.watcher_ids_buffer[op.slot * MAX_NOTIFY_BATCH];
                for (size_t i = 0; i < op.total_watchers && i < MAX_NOTIFY_BATCH; ++i) {
                    // Extract client ID from watcher ID (top 17 bits)
                    const uint16_t client_from_id = static_cast<uint16_t>(watcher_ids[i] >> 47);
                    if (client_from_id >= TOTAL_CLIENTS) {
                        invalid_watcher_ids++;
                    }
                }

                post_notify_watchers(client, op, buffers);
                continue;
            }

            if (phase == WatchPhase::notify_watchers) {
                // Count completed notifications
                op.notify_completed++;

                if (op.notify_completed >= op.response_target) {
                    // Completed current batch - check if more watchers remain
                    if (op.notify_sent < op.total_watchers) {
                        // More watchers to notify - send next batch
                        post_notify_watchers(client, op, buffers);
                        continue;
                    }

                    // All notifications sent! Track total notifications sent
                    total_notifications_sent += op.notify_sent;

                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    object_counts[op.object_id]++;

                    op.active = false;
                    op.phase = WatchPhase::idle;
                    completed++;
                    active--;

                    if (submitted < total_ops) {
                        submit_op(slot);
                    }
                }
            }
        }
    }

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

    // VERIFICATION START - ENSURE THIS PRINTS
    std::cerr << "\n[VERIFICATION-START] Client " << client.id() << " reached end of benchmark\n" << std::flush;

    // Print verification statistics (use cerr for immediate visibility)
    std::cerr << "\n========================================\n";
    std::cerr << "[Client " << client.id() << "] Watch Verification\n";
    std::cerr << "========================================\n";
    std::cerr << "REGISTRATION PHASE:\n";
    std::cerr << "  Completed registrations: " << total_registrations << " / " << registration_ops;
    if (total_registrations == registration_ops) {
        std::cerr << " ✓\n";
    } else {
        std::cerr << " ✗ MISMATCH\n";
    }

    std::cerr << "\nNOTIFICATION PHASE:\n";
    std::cerr << "  Completed notifications: " << (completed - total_registrations) << " / " << notification_ops;
    if ((completed - total_registrations) == notification_ops) {
        std::cerr << " ✓\n";
    } else {
        std::cerr << " ✗ MISMATCH\n";
    }
    std::cerr << "  Total RDMA_WRITEs sent: " << total_notifications_sent << "\n";
    std::cerr << "  Avg RDMA_WRITEs/notify: " << (notification_ops > 0 ? total_notifications_sent / notification_ops : 0) << "\n";

    std::cerr << "\nWATCHER STATISTICS:\n";
    std::cerr << "  Total watchers seen: " << total_watchers_seen << "\n";
    std::cerr << "  Avg watchers/object: " << (notification_ops > 0 ? total_watchers_seen / notification_ops : 0) << "\n";
    std::cerr << "  Min watchers: " << (min_watchers == UINT64_MAX ? 0 : min_watchers) << "\n";
    std::cerr << "  Max watchers: " << max_watchers << "\n";
    std::cerr << "  Objects with 0 watchers: " << zero_watcher_objects << "\n";

    std::cerr << "\nCORRECTNESS CHECKS:\n";
    // Expected: Each client registers registration_ops times across 1000 objects
    // With uniform distribution: ~(registration_ops/1000) registrations per object per client
    // Total across 8 clients: ~(registration_ops * 8 / 1000) watchers per object
    const uint64_t expected_avg = (registration_ops * 8) / 1000;
    const uint64_t actual_avg = notification_ops > 0 ? total_watchers_seen / notification_ops : 0;
    std::cerr << "  Expected avg watchers/object: ~" << expected_avg << "\n";
    std::cerr << "  Actual avg watchers/object: " << actual_avg << "\n";

    // Check 1: Watcher counts within 10%
    if (actual_avg < expected_avg * 0.9 || actual_avg > expected_avg * 1.1) {
        std::cerr << "  ✗ Check 1: Watcher count mismatch (outside 10% tolerance)\n";
    } else {
        std::cerr << "  ✓ Check 1: Watcher counts look correct\n";
    }

    // Check 2: All watchers were notified
    if (total_notifications_sent >= total_watchers_seen) {
        std::cerr << "  ✓ Check 2: All watchers received notifications\n";
    } else {
        std::cerr << "  ✗ Check 2: Missing notifications (" << total_notifications_sent << " < " << total_watchers_seen << ")\n";
    }

    // Check 3: No invalid watcher IDs
    if (invalid_watcher_ids == 0) {
        std::cerr << "  ✓ Check 3: All watcher IDs are valid\n";
    } else {
        std::cerr << "  ✗ Check 3: Found " << invalid_watcher_ids << " invalid watcher IDs\n";
    }
    // Phase-separated stats now aggregated and printed in main.cpp
}
