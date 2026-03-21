#include "rdma/pipelines/cas_pipeline.h"

// Wrapped per-lock CAS pipeline implementation.

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr size_t kConnBits = 8;

enum class OpPhase : uint8_t {
    idle = 0,
    acquire = 1,
    replicate = 2,
    release = 3,
};
// Lifecycle per op: win the owner-node control slot, claim a wrapped log slot
// on quorum, then release both the control word and the wrapped log state.

struct RegisteredCasBuffers {
    uint64_t* acquire_results = nullptr;
    uint64_t* replicate_results = nullptr;
    uint64_t* release_log_results = nullptr;
};

struct CasOpCtx {
    bool active = false;
    bool retry_pending = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t owner_node = 0;
    uint32_t epoch = 0;
    OpPhase phase = OpPhase::idle;
    uint64_t target_slot = 1;
    uint64_t held_slot = 0;
    uint64_t logical_seq = 0;
    uint64_t physical_log_slot = 0;
    uint32_t active_replica_targets = 0;
    uint32_t replicate_responses = 0;
    uint32_t replicate_acks = 0;
    uint32_t release_log_responses = 0;
    uint32_t release_log_acks = 0;
    bool release_owner_log_done = false;
    bool release_log_quorum = false;
    size_t latency_index = 0;
    uint64_t* acquire_result = nullptr;
    std::chrono::steady_clock::time_point started_at{};
};

constexpr uint8_t kReleaseLogConnFlag = 0x80u;
constexpr uint64_t kLogLiveBit = 1ULL << 63;
constexpr uint64_t kLogSeqMask = (1ULL << 31) - 1;

// Convert the monotonic frontier slot into the logical wrapped-log generation.
uint64_t cas_logical_seq(const uint64_t held_slot) {
    return held_slot >> 1;
}

// Map a logical sequence onto the bounded physical per-lock log ring.
uint64_t cas_physical_log_slot(const uint64_t logical_seq) {
    return logical_seq % CAS_LOG_CAPACITY;
}

// Wrapped replicated log state. FREE(seq) means this physical slot is reusable
// by that exact logical sequence; LIVE(seq, client, slot) means it is owned by
// one in-flight op generation.

// Encode a wrapped log slot as free for one exact logical sequence.
uint64_t pack_cas_log_free(const uint64_t logical_seq) {
    return (logical_seq & kLogSeqMask) << 32;
}

// Encode a wrapped log slot as owned by one active client/pipeline-slot generation.
uint64_t pack_cas_log_live(const uint64_t logical_seq, const uint16_t client_id, const uint16_t active_slot) {
    return kLogLiveBit
         | ((logical_seq & kLogSeqMask) << 32)
         | (static_cast<uint64_t>(client_id) << 16)
         | static_cast<uint64_t>(active_slot);
}

// The first time a physical slot is used it must still be EMPTY; later reuses
// require the versioned free value for the next logical sequence.
uint64_t cas_log_expected_free_value(const uint64_t logical_seq) {
    return logical_seq < CAS_LOG_CAPACITY ? EMPTY_SLOT : pack_cas_log_free(logical_seq);
}

// Freeing a wrapped log slot advances it to the next logical generation that may claim it.
uint64_t cas_log_next_free_value(const uint64_t logical_seq) {
    return pack_cas_log_free(logical_seq + CAS_LOG_CAPACITY);
}

// Returns true once the logical sequence has wrapped past the initial physical allocation.
bool cas_log_is_wrapped(const uint64_t logical_seq) {
    return logical_seq >= CAS_LOG_CAPACITY;
}

// Catch configuration mistakes where the wrapped physical slot would exceed the allocated log size.
void ensure_log_seq_in_bounds(const uint32_t lock_id, const uint64_t logical_seq, const char* context) {
    if (cas_physical_log_slot(logical_seq) >= MAX_LOG_PER_LOCK) {
        throw std::runtime_error(
            std::string("CAS pipeline: wrapped log overflow during ") + context
            + " lock=" + std::to_string(lock_id)
            + " logical_seq=" + std::to_string(logical_seq)
            + " max_log_per_lock=" + std::to_string(MAX_LOG_PER_LOCK));
    }
}

// Encode the active op generation/slot/phase into one CQE identifier.
uint64_t encode_wr_id(const CasOpCtx& op, const OpPhase phase, const uint8_t conn_index) {
    return (static_cast<uint64_t>(op.generation) << 32)
         | (static_cast<uint64_t>(op.slot) << 16)
         | (static_cast<uint64_t>(phase) << kConnBits)
         | static_cast<uint64_t>(conn_index);
}

// Recover the op generation from a CAS pipeline WR id.
uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> 32);
}

// Recover the op slot from a CAS pipeline WR id.
uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> 16) & 0xFFFFu);
}

// Recover the state-machine phase from a CAS pipeline WR id.
OpPhase wr_phase(const uint64_t wr_id) {
    return static_cast<OpPhase>((wr_id >> kConnBits) & 0xFFu);
}

// Recover the replica index from a CAS pipeline WR id.
uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>(wr_id & 0xFFu);
}

// Compute one row offset inside a per-op-by-replica matrix buffer.
size_t row_offset(const uint32_t slot, const size_t replica_count) {
    return static_cast<size_t>(slot) * replica_count;
}

// Return the start of one per-op row inside a replica matrix.
uint64_t* row_ptr(uint64_t* base, const uint32_t slot, const size_t replica_count) {
    return base + row_offset(slot, replica_count);
}

bool use_recovery_route(const uint32_t lock_id) {
    return is_recovery_target_lock(lock_id);
}

bool recovery_node_active(const RecoveryRoute& route, const uint32_t node_id) {
    return recovery_live_mask_contains(route.live_mask, node_id);
}

uint32_t active_replica_targets(const Client& client, const uint32_t lock_id) {
    if (!use_recovery_route(lock_id)) {
        return static_cast<uint32_t>(client.connections().size());
    }
    const auto& route = client.recovery_route();
    uint32_t count = 0;
    for (const auto& conn : client.connections()) {
        if (recovery_node_active(route, conn.node_id)) {
            count++;
        }
    }
    return count;
}

uintptr_t frontier_remote_addr(const Client& client, const CasOpCtx& op) {
    if (use_recovery_route(op.lock_id)) {
        return client.recovery_route().frontier.addr;
    }
    return client.connections()[op.owner_node].addr + lock_control_offset(op.lock_id);
}

uint32_t frontier_remote_rkey(const Client& client, const CasOpCtx& op) {
    if (use_recovery_route(op.lock_id)) {
        return client.recovery_route().frontier.rkey;
    }
    return client.connections()[op.owner_node].rkey;
}

uintptr_t log_remote_addr(const Client& client, const uint32_t lock_id, const uint32_t node_id, const uint64_t physical_slot) {
    if (use_recovery_route(lock_id)) {
        return client.recovery_route().log_creds[node_id].addr + physical_slot * ENTRY_SIZE;
    }
    return client.connections()[node_id].addr + lock_log_slot_offset(lock_id, physical_slot);
}

uint32_t log_remote_rkey(const Client& client, const uint32_t lock_id, const uint32_t node_id) {
    if (use_recovery_route(lock_id)) {
        return client.recovery_route().log_creds[node_id].rkey;
    }
    return client.connections()[node_id].rkey;
}

void mirror_frontier_value(const Client& client, const uint32_t lock_id, const uint64_t frontier_value) {
    if (!use_recovery_route(lock_id)) {
        return;
    }
    const auto& conns = client.connections();
    const auto& route = client.recovery_route();
    for (const auto& conn : conns) {
        if (!recovery_node_active(route, conn.node_id)) {
            continue;
        }
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&frontier_value);
        sge.length = sizeof(uint64_t);
        sge.lkey = client.mr()->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = 0;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_INLINE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conn.addr + lock_control_offset(lock_id);
        wr.wr.rdma.rkey = conn.rkey;
        ibv_post_send(conn.id->qp, &wr, &bad_wr);
    }
}

// Map the shared client MR into the per-op result matrices used by wrapped CAS.
RegisteredCasBuffers map_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t active_window,
    const size_t replica_count
) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;

    RegisteredCasBuffers buffers{};

    const size_t acquire_bytes = align_up(active_window * sizeof(uint64_t), 64);
    buffers.acquire_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += acquire_bytes;

    const size_t replicate_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += replicate_bytes;

    const size_t release_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    buffers.release_log_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += release_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("CAS pipeline: registered client buffer too small");
    }

    return buffers;
}

// Post the owner-node frontier CAS that tries to acquire one logical slot.
void post_acquire(const Client& client, CasOpCtx& op) {
    const auto& conns = client.connections();
    auto* mr = client.mr();

    *op.acquire_result = std::numeric_limits<uint64_t>::max() - 1;

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(op.acquire_result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, OpPhase::acquire, static_cast<uint8_t>(op.owner_node));
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = frontier_remote_addr(client, op);
    wr.wr.atomic.rkey = frontier_remote_rkey(client, op);
    wr.wr.atomic.compare_add = op.target_slot - 1;
    wr.wr.atomic.swap = op.target_slot;

    if (ibv_post_send(conns[op.owner_node].id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("CAS pipeline: acquire post failed");
    }

    op.phase = OpPhase::acquire;
}

void post_replicate(const Client& client, CasOpCtx& op, const RegisteredCasBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());

    // The control-word CAS only wins local ownership. The acquire is not treated
    // as complete until the wrapped replicated log slot is claimed on quorum.
    op.logical_seq = cas_logical_seq(op.held_slot);
    op.physical_log_slot = cas_physical_log_slot(op.logical_seq);
    ensure_log_seq_in_bounds(op.lock_id, op.logical_seq, "replicate");
    const uint64_t      compare_value = cas_log_expected_free_value(op.logical_seq);
    const uint64_t swap_value = pack_cas_log_live(op.logical_seq, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot));

    op.active_replica_targets = active_replica_targets(client, op.lock_id);

    for (size_t i = 0; i < conns.size(); ++i) {
        if (use_recovery_route(op.lock_id) && !recovery_node_active(client.recovery_route(), conns[i].node_id)) {
            continue;
        }
        results[i] = EMPTY_SLOT - 1;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, OpPhase::replicate, static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = log_remote_addr(client, op.lock_id, conns[i].node_id, op.physical_log_slot);
        wr.wr.atomic.rkey = log_remote_rkey(client, op.lock_id, conns[i].node_id);
        wr.wr.atomic.compare_add = compare_value;
        wr.wr.atomic.swap = swap_value;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("CAS pipeline: replicate post failed");
        }
    }

    op.phase = OpPhase::replicate;
    op.replicate_responses = 0;
    op.replicate_acks = 0;
}

void post_release(
    Client& client,
    CasOpCtx& op,
    const RegisteredCasBuffers& buffers,
    const CasPipelineConfig& config
) {
    const auto& conns = client.connections();
    const auto* mr = client.mr();
    auto* log_results = row_ptr(buffers.release_log_results, op.slot, conns.size());
    const uint64_t live_value = pack_cas_log_live(op.logical_seq, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot));
    const uint64_t free_value = cas_log_next_free_value(op.logical_seq);
    const auto& owner = conns[op.owner_node];

    op.phase = OpPhase::release;
    op.release_log_responses = 0;
    op.release_log_acks = 0;
    op.release_owner_log_done = false;
    op.release_log_quorum = false;

    // Release has two independent pieces of work: advance the owner-node control
    // word and free the wrapped replicated log slot. The two toggles let us
    // compare CAS vs WRITE for each piece independently.
    uint64_t control_value = op.held_slot + 1;

    ibv_sge control_sge{};
    control_sge.addr = reinterpret_cast<uintptr_t>(&control_value);
    control_sge.length = sizeof(uint64_t);
    control_sge.lkey = mr->lkey;

    ibv_send_wr control_wr{}, *bad_wr = nullptr;
    control_wr.wr_id = encode_wr_id(op, OpPhase::release, static_cast<uint8_t>(op.owner_node));
    control_wr.sg_list = &control_sge;
    control_wr.num_sge = 1;
    if (config.release_control_with_cas) {
        control_wr.send_flags = IBV_SEND_SIGNALED;
        control_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        control_wr.wr.atomic.remote_addr = frontier_remote_addr(client, op);
        control_wr.wr.atomic.rkey = frontier_remote_rkey(client, op);
        control_wr.wr.atomic.compare_add = op.held_slot;
        control_wr.wr.atomic.swap = op.held_slot + 1;
    } else {
        control_wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
        control_wr.opcode = IBV_WR_RDMA_WRITE;
        control_wr.wr.rdma.remote_addr = frontier_remote_addr(client, op);
        control_wr.wr.rdma.rkey = frontier_remote_rkey(client, op);
    }

    if (ibv_post_send(owner.id->qp, &control_wr, &bad_wr)) {
        throw std::runtime_error("CAS pipeline: release post failed");
    }

    for (size_t i = 0; i < conns.size(); ++i) {
        if (use_recovery_route(op.lock_id) && !recovery_node_active(client.recovery_route(), conns[i].node_id)) {
            continue;
        }
        auto* log_result = log_results + i;
        *log_result = EMPTY_SLOT - 1;

        ibv_sge log_sge{};
        log_sge.addr = reinterpret_cast<uintptr_t>(log_result);
        log_sge.length = sizeof(uint64_t);
        log_sge.lkey = mr->lkey;

        ibv_send_wr log_wr{}, *bad_log = nullptr;
        log_wr.wr_id = encode_wr_id(op, OpPhase::release, static_cast<uint8_t>(kReleaseLogConnFlag | i));
        log_wr.sg_list = &log_sge;
        log_wr.num_sge = 1;
        if (config.release_log_with_cas) {
            log_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            log_wr.send_flags = IBV_SEND_SIGNALED;
            log_wr.wr.atomic.remote_addr = log_remote_addr(client, op.lock_id, conns[i].node_id, op.physical_log_slot);
            log_wr.wr.atomic.rkey = log_remote_rkey(client, op.lock_id, conns[i].node_id);
            log_wr.wr.atomic.compare_add = live_value;
            log_wr.wr.atomic.swap = free_value;
        } else {
            *log_result = free_value;
            log_wr.opcode = IBV_WR_RDMA_WRITE;
            log_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
            log_wr.wr.rdma.remote_addr = log_remote_addr(client, op.lock_id, conns[i].node_id, op.physical_log_slot);
            log_wr.wr.rdma.rkey = log_remote_rkey(client, op.lock_id, conns[i].node_id);
        }

        if (ibv_post_send(conns[i].id->qp, &log_wr, &bad_log)) {
            throw std::runtime_error("CAS pipeline: release log post failed");
        }
    }
}

}  // namespace

// Load the compile-time wrapped CAS tuning knobs into the runtime config struct.
CasPipelineConfig load_cas_pipeline_config() {
    CasPipelineConfig config{};
    config.active_window = CAS_ACTIVE_WINDOW;
    config.cq_batch = std::max<size_t>(1, CAS_CQ_BATCH);
    config.zipf_skew = CAS_ZIPF_SKEW;
    config.shard_owner = CAS_SHARD_OWNER;
    config.release_control_with_cas = CAS_RELEASE_CONTROL_USE_CAS;
    config.release_log_with_cas = CAS_RELEASE_LOG_USE_CAS;
    return config;
}

// Report how much client MR space the wrapped CAS pipeline needs.
size_t cas_pipeline_client_buffer_size(const CasPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t acquire_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t replicate_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    const size_t release_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    return align_up(acquire_bytes + replicate_bytes + release_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_cas_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const CasPipelineConfig& config
) {
    // Validate connection availability and ensure the active window fits inside
    // the compact WR-id encoding used by the wrapped CAS pipeline.
    const auto& conns = client.connections();
    if (conns.empty()) {
        throw std::runtime_error("CAS pipeline: no server connections");
    }

    if (config.active_window > static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        throw std::runtime_error("CAS pipeline: active window exceeds wr_id slot encoding");
    }

    // The client MR holds per-op matrices for wrapped-log replication/release.
    // frontier_hints caches the next likely logical slot for each lock locally.
    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<CasOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    std::array<uint64_t, MAX_LOCKS> frontier_hints{};
    frontier_hints.fill(1);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;

    auto prime_op = [&](CasOpCtx& op, const size_t slot, const bool is_retry) {
        op.active = true;
        op.retry_pending = false;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.lock_id = RECOVERY_TARGET_LOCK;
        op.owner_node = use_recovery_route(op.lock_id) ? client.recovery_route().frontier_host
                                                       : (config.shard_owner ? (op.lock_id % conns.size()) : 0);
        op.epoch = client.recovery_route().epoch;
        op.phase = OpPhase::idle;
        op.target_slot = frontier_hints[op.lock_id];
        op.held_slot = 0;
        op.logical_seq = 0;
        op.physical_log_slot = 0;
        op.active_replica_targets = active_replica_targets(client, op.lock_id);
        op.replicate_responses = 0;
        op.replicate_acks = 0;
        op.release_log_responses = 0;
        op.release_log_acks = 0;
        op.release_owner_log_done = false;
        op.release_log_quorum = false;
        if (!is_retry) {
            op.latency_index = submitted % NUM_OPS_PER_CLIENT;
            submitted++;
        }
        op.acquire_result = &buffers.acquire_results[slot];
        op.started_at = std::chrono::steady_clock::now();
        post_acquire(client, op);
        active++;
    };

    auto submit_op = [&](const size_t slot) {
        // Start one wrapped CAS acquire:
        // 1. choose a lock id,
        // 2. choose the owner node,
        // 3. seed the target logical slot from the frontier hint,
        // 4. post the owner control-word CAS.
        prime_op(ops[slot], slot, false);
    };

    auto retry_op = [&](const size_t slot) {
        prime_op(ops[slot], slot, true);
    };

    // Fill the active window before the CQ loop so multiple acquires can overlap.
    while (active < config.active_window && !client.experiment_done()) {
        submit_op(active);
    }

    while (!client.experiment_done() || active > 0) {
        if (client.recovery_active()) {
            for (auto& op : ops) {
                if (!op.active || !use_recovery_route(op.lock_id)) continue;
                op.active = false;
                op.retry_pending = true;
                op.phase = OpPhase::idle;
                active--;
            }
            client.maybe_send_recovery_quiesced(active);
        } else {
            for (size_t slot = 0; slot < ops.size(); ++slot) {
                if (!client.experiment_done() && ops[slot].retry_pending && !ops[slot].active) {
                    retry_op(slot);
                }
            }
        }

        while (!client.experiment_done() && !client.recovery_active() && active < config.active_window) {
            bool submitted_any = false;
            for (size_t slot = 0; slot < ops.size() && active < config.active_window; ++slot) {
                if (!ops[slot].active && !ops[slot].retry_pending) {
                    submit_op(slot);
                    submitted_any = true;
                }
            }
            if (!submitted_any) {
                break;
            }
        }

        // One completion advances exactly one op phase: owner acquire, wrapped-log
        // claim, or wrapped-log/control release.
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) {
            throw std::runtime_error("CAS pipeline: CQ poll failed");
        }
        if (polled == 0) {
            continue;
        }

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[i];
            if (client.handle_control_completion(wc)) {
                continue;
            }
            if (wc.status != IBV_WC_SUCCESS) {
                const uint32_t slot = wr_slot(wc.wr_id);
                if (slot < ops.size()) {
                    auto& op = ops[slot];
                    if (use_recovery_route(op.lock_id)) {
                        if (op.active) {
                            op.active = false;
                            op.retry_pending = true;
                            op.phase = OpPhase::idle;
                            active--;
                        }
                        continue;
                    }
                }
                throw std::runtime_error(
                    "CAS pipeline: WC error status=" + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) {
                throw std::runtime_error("CAS pipeline: completion slot out of range");
            }

            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) {
                continue;
            }
            if (use_recovery_route(op.lock_id) && op.epoch != client.recovery_route().epoch && !client.recovery_active()) {
                op.active = false;
                op.retry_pending = true;
                op.phase = OpPhase::idle;
                active--;
                continue;
            }

            const OpPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) {
                continue;
            }

            if (phase == OpPhase::acquire) {
                // Owner acquire CAS tries to move the control word from target-1
                // to target. On failure, advance the frontier hint and retry.
                const uint64_t expected = op.target_slot - 1;
                const uint64_t result = *op.acquire_result;

                if (result == expected) {
                    op.held_slot = op.target_slot;
                    mirror_frontier_value(client, op.lock_id, op.held_slot);
                    post_replicate(client, op, buffers);
                } else {
                    op.target_slot = (result % 2 != 0) ? result + 2 : result + 1;
                    frontier_hints[op.lock_id] = op.target_slot;
                    post_acquire(client, op);
                }
                continue;
            }

            if (phase == OpPhase::replicate) {
                // Winning the owner control word is not enough; acquire completes
                // only after the wrapped replicated log slot is claimed on quorum.
                auto* replicate_results = row_ptr(buffers.replicate_results, op.slot, conns.size());
                const uint8_t idx = wr_conn(wc.wr_id);
                if (use_recovery_route(op.lock_id) && !recovery_node_active(client.recovery_route(), conns[idx].node_id)) {
                    continue;
                }
                op.replicate_responses++;
                if (replicate_results[idx] == cas_log_expected_free_value(op.logical_seq)) {
                    op.replicate_acks++;
                }
                if (op.replicate_acks >= QUORUM) {
                    latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count();
                    frontier_hints[op.lock_id] = std::max(frontier_hints[op.lock_id], op.held_slot + 2);
                    post_release(client, op, buffers, config);
                    continue;
                }
                const uint32_t remaining = op.active_replica_targets - op.replicate_responses;
                if (op.replicate_acks + remaining < QUORUM) {
                    op.active = false;
                    op.retry_pending = true;
                    op.phase = OpPhase::idle;
                    active--;
                }
                continue;
            }

            if (phase == OpPhase::release) {
                // Release frees the wrapped log slot on quorum and advances the
                // owner control word. The op slot is reused only after wrapped-log
                // release is safe.
                const uint8_t raw_idx = wr_conn(wc.wr_id);
                const bool is_log_release = (raw_idx & kReleaseLogConnFlag) != 0;
                const size_t replica_index = static_cast<size_t>(raw_idx & ~kReleaseLogConnFlag);

                if (replica_index >= conns.size()) {
                    throw std::runtime_error("CAS pipeline: release replica index out of range");
                }

                if (is_log_release) {
                    if (use_recovery_route(op.lock_id)
                        && !recovery_node_active(client.recovery_route(), conns[replica_index].node_id)) {
                        continue;
                    }
                    auto* log_results = row_ptr(buffers.release_log_results, op.slot, conns.size());
                    op.release_log_responses++;
                    if (!config.release_log_with_cas
                        || log_results[replica_index]
                            == pack_cas_log_live(op.logical_seq, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot))) {
                        op.release_log_acks++;
                        if (replica_index == op.owner_node) {
                            op.release_owner_log_done = true;
                        }
                    } else {
                        if (replica_index == op.owner_node) {
                            throw std::runtime_error("CAS pipeline: owner wrapped log release CAS failed");
                        }
                    }
                    if (op.release_log_acks >= QUORUM) {
                        op.release_log_quorum = true;
                    } else {
                        const uint32_t remaining = op.active_replica_targets - op.release_log_responses;
                        if (op.release_log_acks + remaining < QUORUM) {
                            op.active = false;
                            op.retry_pending = true;
                            op.phase = OpPhase::idle;
                            active--;
                            continue;
                        }
                    }
                } else {
                    continue;
                }

                if (op.release_owner_log_done && op.release_log_quorum) {
                    mirror_frontier_value(client, op.lock_id, op.held_slot + 1);
                    lock_counts[op.lock_id]++;
                    op.active = false;
                    op.retry_pending = false;
                    op.phase = OpPhase::idle;
                    completed++;
                    active--;

                    if (!client.experiment_done() && !client.recovery_active()) {
                        submit_op(slot);
                    }
                }
                continue;
            }
        }
    }
}
