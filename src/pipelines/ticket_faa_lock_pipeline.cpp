#include "rdma/pipelines/ticket_faa_lock_pipeline.h"

// Ticket FAA pipeline implementation with wrapped replicated log slots.

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/zipf_lock_picker.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
constexpr uint64_t kLogLiveBit = 1ULL << 63;
constexpr uint64_t kLogTicketMask = (1ULL << 31) - 1;
constexpr uint8_t kReleaseTurnConnIndex = 0xFF;
constexpr uint64_t kDetachedTurnWrTag = 0xFF00000000000000ULL;

constexpr uint32_t kTurnReleaseWrite = 0;
constexpr uint32_t kTurnReleaseCas = 1;
constexpr uint32_t kTurnReleaseFaa = 2;

enum class TicketFaaPhase : uint8_t {
    idle = 0,
    faa_ticket = 1,
    replicate_ticket = 2,
    replicate_ticket_spin = 3,
    wait_turn_read = 4,
    wait_turn_spin = 5,
    release_parallel = 6,
};
// Lifecycle per op: fetch ticket, claim wrapped log space, wait until turn ==
// ticket, then release both the wrapped log slot and the turn register.

struct RegisteredTicketFaaBuffers {
    uint64_t* ticket_results = nullptr;
    uint64_t* replicate_results = nullptr;
    uint64_t* turn_reads = nullptr;
    uint64_t* release_log_values = nullptr;
    uint64_t* release_log_results = nullptr;
    uint64_t* detached_turn_results = nullptr;
};

struct TicketFaaOpCtx {
    bool active = false;
    bool retry_pending = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t lock_id = 0;
    uint32_t owner_node = 0;
    uint32_t epoch = 0;
    uint32_t req_id = 0;
    uint8_t round = 0;
    uint64_t ticket = 0;
    uint64_t waiter_id = 0;
    uint64_t physical_log_slot = 0;
    uint64_t last_turn = 0;
    uint32_t replicate_retry_spin_remaining = 0;
    uint32_t turn_spin_remaining = 0;
    TicketFaaPhase phase = TicketFaaPhase::idle;
    uint32_t responses = 0;
    uint32_t response_target = 0;
    uint32_t active_replica_targets = 0;
    uint32_t quorum_hits = 0;
    uint32_t release_log_responses = 0;
    uint32_t release_log_quorum = 0;
    bool release_owner_log_done = false;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

struct DetachedTurnCtx {
    bool active = false;
    uint32_t turn_mode = kTurnReleaseWrite;
    uint64_t expected_old = 0;
};

// Encode a unique waiter id used when publishing wrapped log ownership.
uint64_t encode_waiter(const uint16_t client_id, const uint16_t op_slot, const uint32_t req_id) {
    return (static_cast<uint64_t>(client_id) << 47)
         | (static_cast<uint64_t>(op_slot) << 32)
         | static_cast<uint64_t>(req_id);
}

// Encode generation, slot, phase, round, and connection index into one WR id.
uint64_t encode_wr_id(const TicketFaaOpCtx& op, const TicketFaaPhase phase, const uint8_t conn_index) {
    return ((static_cast<uint64_t>(op.generation) & kGenerationMask) << kGenerationShift)
         | ((static_cast<uint64_t>(op.slot) & kSlotMask) << kSlotShift)
         | ((static_cast<uint64_t>(phase) & kPhaseMask) << kPhaseShift)
         | ((static_cast<uint64_t>(op.round) & kRoundMask) << kRoundShift)
         | ((static_cast<uint64_t>(conn_index) & kConnMask) << kConnShift);
}

// Check whether a completion belongs to the detached turn-update path.
bool is_detached_turn_wr_id(const uint64_t wr_id) {
    return (wr_id & kDetachedTurnWrTag) == kDetachedTurnWrTag;
}

// Encode a detached turn-update completion id.
uint64_t make_detached_turn_wr_id(const uint32_t ctx_index) {
    return kDetachedTurnWrTag | static_cast<uint64_t>(ctx_index);
}

// Recover the detached turn-update context index from a WR id.
uint32_t detached_turn_ctx_index(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id & 0x00FFFFFFu);
}

// Extract the op generation from a ticket-faa WR id.
uint32_t wr_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id >> kGenerationShift);
}

// Extract the pipeline slot from a ticket-faa WR id.
uint32_t wr_slot(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> kSlotShift) & kSlotMask);
}

// Extract the phase tag from a ticket-faa WR id.
TicketFaaPhase wr_phase(const uint64_t wr_id) {
    return static_cast<TicketFaaPhase>((wr_id >> kPhaseShift) & kPhaseMask);
}

// Extract the per-op round counter from a ticket-faa WR id.
uint8_t wr_round(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> kRoundShift) & kRoundMask);
}

// Extract the replica/connection index from a ticket-faa WR id.
uint8_t wr_conn(const uint64_t wr_id) {
    return static_cast<uint8_t>((wr_id >> kConnShift) & kConnMask);
}

// Compute the row-major offset for a per-op-by-replica buffer.
size_t row_offset(const uint32_t slot, const size_t replica_count) {
    return static_cast<size_t>(slot) * replica_count;
}

// Return the first element of one per-op row inside a replica matrix.
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
    uint32_t count = 0;
    for (const auto& conn : client.connections()) {
        if (recovery_node_active(client.recovery_route(), conn.node_id)) {
            count++;
        }
    }
    return count;
}

uintptr_t frontier_remote_addr(const Client& client, const TicketFaaOpCtx& op) {
    if (use_recovery_route(op.lock_id)) {
        return client.recovery_route().frontier.addr;
    }
    return client.connections()[op.owner_node].addr + lock_control_offset(op.lock_id);
}

uint32_t frontier_remote_rkey(const Client& client, const TicketFaaOpCtx& op) {
    if (use_recovery_route(op.lock_id)) {
        return client.recovery_route().frontier.rkey;
    }
    return client.connections()[op.owner_node].rkey;
}

uintptr_t turn_remote_addr(const Client& client, const TicketFaaOpCtx& op) {
    if (use_recovery_route(op.lock_id)) {
        return client.recovery_route().turn.addr;
    }
    return client.connections()[op.owner_node].addr + lock_turn_offset(op.lock_id);
}

uint32_t turn_remote_rkey(const Client& client, const TicketFaaOpCtx& op) {
    if (use_recovery_route(op.lock_id)) {
        return client.recovery_route().turn.rkey;
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

void mirror_frontier_value(const Client& client, const uint32_t lock_id, const uint64_t value) {
    if (!use_recovery_route(lock_id)) {
        return;
    }
    for (const auto& conn : client.connections()) {
        if (!recovery_node_active(client.recovery_route(), conn.node_id) || conn.prototype_frontier.addr == 0 || conn.prototype_frontier.rkey == 0) {
            continue;
        }
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&value);
        sge.length = sizeof(uint64_t);
        sge.lkey = client.mr()->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_INLINE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conn.prototype_frontier.addr;
        wr.wr.rdma.rkey = conn.prototype_frontier.rkey;
        ibv_post_send(conn.id->qp, &wr, &bad_wr);
    }
}

void mirror_turn_value(const Client& client, const uint32_t lock_id, const uint64_t value) {
    if (!use_recovery_route(lock_id)) {
        return;
    }
    for (const auto& conn : client.connections()) {
        if (!recovery_node_active(client.recovery_route(), conn.node_id) || conn.prototype_turn.addr == 0 || conn.prototype_turn.rkey == 0) {
            continue;
        }
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&value);
        sge.length = sizeof(uint64_t);
        sge.lkey = client.mr()->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_INLINE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = conn.prototype_turn.addr;
        wr.wr.rdma.rkey = conn.prototype_turn.rkey;
        ibv_post_send(conn.id->qp, &wr, &bad_wr);
    }
}

// Size the detached turn-update pool so turn CAS/FAA completions do not block
// op-slot reuse on quorum.
size_t detached_turn_ctx_capacity(const size_t active_window) {
    return std::max<size_t>(active_window * 8, 64);
}

uint64_t ticket_faa_physical_log_slot(const uint64_t ticket) {
    return ticket % TICKET_FAA_LOG_CAPACITY;
}

// Wrapped ticket-faa log state is derived directly from the logical ticket. A
// physical slot is reusable only when its free value matches the exact ticket
// generation currently trying to claim it.

uint64_t pack_ticket_faa_log_free(const uint64_t ticket) {
    return (ticket & kLogTicketMask) << 32;
}

uint64_t pack_ticket_faa_log_live(const uint64_t ticket, const uint16_t client_id, const uint16_t op_slot) {
    return kLogLiveBit
         | ((ticket & kLogTicketMask) << 32)
         | (static_cast<uint64_t>(client_id) << 16)
         | static_cast<uint64_t>(op_slot);
}

uint64_t ticket_faa_log_expected_free_value(const uint64_t ticket) {
    return ticket < TICKET_FAA_LOG_CAPACITY ? EMPTY_SLOT : pack_ticket_faa_log_free(ticket);
}

uint64_t ticket_faa_log_next_free_value(const uint64_t ticket) {
    return pack_ticket_faa_log_free(ticket + TICKET_FAA_LOG_CAPACITY);
}

void ensure_log_slot_in_bounds(const uint32_t lock_id, const uint64_t ticket, const char* context) {
    const uint64_t physical = ticket_faa_physical_log_slot(ticket);
    if (physical >= MAX_LOG_PER_LOCK) {
        throw std::runtime_error(
            std::string("ticket_faa pipeline: wrapped log overflow during ") + context
            + " lock=" + std::to_string(lock_id)
            + " ticket=" + std::to_string(ticket)
            + " physical=" + std::to_string(physical)
            + " max_log_per_lock=" + std::to_string(MAX_LOG_PER_LOCK));
    }
}

// Turn-wait heuristic: farther tickets spin longer before re-reading turn.
uint32_t spin_budget_for_distance(const uint64_t distance) {
    if (distance <= 1) return TICKET_FAA_TURN_SPIN_VERY_NEAR;
    if (distance <= 2) return TICKET_FAA_TURN_SPIN_NEAR;
    if (distance <= 4) return TICKET_FAA_TURN_SPIN_MID;
    return TICKET_FAA_TURN_SPIN_FAR;
}

// Map the client MR into per-op ticket, log, turn, and detached-turn buffers.
RegisteredTicketFaaBuffers map_buffers(
    void* raw_buffer,
    const size_t buffer_size,
    const size_t active_window,
    const size_t replica_count
) {
    auto* base = static_cast<uint8_t*>(raw_buffer);
    size_t offset = 0;
    RegisteredTicketFaaBuffers buffers{};
    const size_t scalar_bytes = align_up(active_window * sizeof(uint64_t), 64);
    const size_t matrix_bytes = align_up(active_window * replica_count * sizeof(uint64_t), 64);
    const size_t detached_turn_bytes = align_up(detached_turn_ctx_capacity(active_window) * sizeof(uint64_t), 64);

    buffers.ticket_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.replicate_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.turn_reads = reinterpret_cast<uint64_t*>(base + offset);
    offset += scalar_bytes;
    buffers.release_log_values = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.release_log_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += matrix_bytes;
    buffers.detached_turn_results = reinterpret_cast<uint64_t*>(base + offset);
    offset += detached_turn_bytes;

    if (offset > buffer_size) {
        throw std::runtime_error("ticket_faa pipeline: registered client buffer too small");
    }
    return buffers;
}

// Fetch the next logical ticket from the owner node's FAA register.
void post_ticket_faa(Client& client, TicketFaaOpCtx& op, const RegisteredTicketFaaBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* result = &buffers.ticket_results[op.slot];
    *result = 0;
    const auto& owner = conns[op.owner_node];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    op.round++;
    op.phase = TicketFaaPhase::faa_ticket;
    op.responses = 0;
    op.response_target = 1;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, TicketFaaPhase::faa_ticket, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.atomic.remote_addr = frontier_remote_addr(client, op);
    wr.wr.atomic.rkey = frontier_remote_rkey(client, op);
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("ticket_faa pipeline: FAA ticket post failed");
    }
}

void post_replicate_ticket(
    Client& client,
    TicketFaaOpCtx& op,
    const RegisteredTicketFaaBuffers& buffers
) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());

    // Ticket assignment can outpace wrapped log reuse, so replicate is always a
    // CAS and the op may later retry until enough replicas expose the expected
    // free generation for this logical ticket.
    ensure_log_slot_in_bounds(op.lock_id, op.ticket, "replicate_ticket");
    op.physical_log_slot = ticket_faa_physical_log_slot(op.ticket);
    const uint64_t compare_value = ticket_faa_log_expected_free_value(op.ticket);
    const uint64_t swap_value = pack_ticket_faa_log_live(op.ticket, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot));

    op.round++;
    op.phase = TicketFaaPhase::replicate_ticket;
    op.responses = 0;
    op.active_replica_targets = active_replica_targets(client, op.lock_id);
    op.response_target = op.active_replica_targets;
    op.quorum_hits = 0;

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
        wr.wr_id = encode_wr_id(op, TicketFaaPhase::replicate_ticket, static_cast<uint8_t>(i));
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.wr.atomic.remote_addr = log_remote_addr(client, op.lock_id, conns[i].node_id, op.physical_log_slot);
        wr.wr.atomic.rkey = log_remote_rkey(client, op.lock_id, conns[i].node_id);
        wr.wr.atomic.compare_add = compare_value;
        wr.wr.atomic.swap = swap_value;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("ticket_faa pipeline: replicate ticket post failed");
        }
    }
}

// Read the owner-node turn register to decide whether this ticket may enter.
// Read the owner-node turn register to decide whether this ticket may enter.
void post_turn_read(Client& client, TicketFaaOpCtx& op, const RegisteredTicketFaaBuffers& buffers) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    auto* result = &buffers.turn_reads[op.slot];
    *result = EMPTY_SLOT;
    const auto& owner = conns[op.owner_node];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = mr->lkey;

    op.round++;
    op.phase = TicketFaaPhase::wait_turn_read;
    op.responses = 0;
    op.response_target = 1;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = encode_wr_id(op, TicketFaaPhase::wait_turn_read, static_cast<uint8_t>(op.owner_node));
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = turn_remote_addr(client, op);
    wr.wr.rdma.rkey = turn_remote_rkey(client, op);

    if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("ticket_faa pipeline: turn read post failed");
    }
}

void post_release_parallel(
    Client& client,
    TicketFaaOpCtx& op,
    const RegisteredTicketFaaBuffers& buffers,
    const TicketFaaLockPipelineConfig& config,
    std::vector<DetachedTurnCtx>& detached_turn_ctxs
) {
    const auto& conns = client.connections();
    auto* mr = client.mr();
    const auto& owner = conns[op.owner_node];
    auto* release_log_values = row_ptr(buffers.release_log_values, op.slot, conns.size());
    auto* release_log_results = row_ptr(buffers.release_log_results, op.slot, conns.size());
    const uint64_t live_value = pack_ticket_faa_log_live(op.ticket, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot));
    const uint64_t free_value = ticket_faa_log_next_free_value(op.ticket);

    // Release frees the wrapped log slot on all replicas and independently
    // updates the owner-node turn register. Those two behaviors are toggled
    // separately so we can compare write/cas/faa combinations.
    ensure_log_slot_in_bounds(op.lock_id, op.ticket, "release_parallel");

    op.round++;
    op.phase = TicketFaaPhase::release_parallel;
    op.responses = 0;
    op.response_target = static_cast<uint32_t>(conns.size());
    op.release_log_responses = 0;
    op.release_log_quorum = 0;
    op.release_owner_log_done = false;

    DetachedTurnCtx* detached_turn_ctx = nullptr;
    uint32_t detached_turn_idx = 0;
    if (config.release_turn_mode != kTurnReleaseWrite) {
        for (uint32_t i = 0; i < detached_turn_ctxs.size(); ++i) {
            if (!detached_turn_ctxs[i].active) {
                detached_turn_ctx = &detached_turn_ctxs[i];
                detached_turn_idx = i;
                break;
            }
        }
        if (detached_turn_ctx == nullptr) {
            throw std::runtime_error("ticket_faa pipeline: detached turn context pool exhausted");
        }

        detached_turn_ctx->active = true;
        detached_turn_ctx->turn_mode = config.release_turn_mode;
        detached_turn_ctx->expected_old = op.ticket;
        buffers.detached_turn_results[detached_turn_idx] = 0;
    }

    if (config.release_turn_mode == kTurnReleaseWrite) {
        uint64_t turn_value = op.ticket + 1;

        ibv_sge turn_sge{};
        turn_sge.addr = reinterpret_cast<uintptr_t>(&turn_value);
        turn_sge.length = sizeof(uint64_t);
        turn_sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TicketFaaPhase::release_parallel, kReleaseTurnConnIndex);
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_INLINE;
        wr.sg_list = &turn_sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = turn_remote_addr(client, op);
        wr.wr.rdma.rkey = turn_remote_rkey(client, op);

        if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("ticket_faa pipeline: release turn post failed");
        }
    } else {
        auto* turn_result = &buffers.detached_turn_results[detached_turn_idx];

        ibv_sge turn_sge{};
        turn_sge.addr = reinterpret_cast<uintptr_t>(turn_result);
        turn_sge.length = sizeof(uint64_t);
        turn_sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_detached_turn_wr_id(detached_turn_idx);
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &turn_sge;
        wr.num_sge = 1;
        if (config.release_turn_mode == kTurnReleaseCas) {
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.wr.atomic.remote_addr = turn_remote_addr(client, op);
            wr.wr.atomic.rkey = turn_remote_rkey(client, op);
            wr.wr.atomic.compare_add = op.ticket;
            wr.wr.atomic.swap = op.ticket + 1;
        } else {
            wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
            wr.wr.atomic.remote_addr = turn_remote_addr(client, op);
            wr.wr.atomic.rkey = turn_remote_rkey(client, op);
            wr.wr.atomic.compare_add = 1;
        }

        if (ibv_post_send(owner.id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("ticket_faa pipeline: release turn post failed");
        }
    }

    for (size_t i = 0; i < conns.size(); ++i) {
        if (use_recovery_route(op.lock_id) && !recovery_node_active(client.recovery_route(), conns[i].node_id)) {
            continue;
        }
        auto* log_value = &release_log_values[i];
        auto* log_result = &release_log_results[i];
        *log_value = free_value;
        *log_result = EMPTY_SLOT - 1;

        ibv_sge log_sge{};
        log_sge.length = sizeof(uint64_t);
        log_sge.lkey = mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = encode_wr_id(op, TicketFaaPhase::release_parallel, static_cast<uint8_t>(i));
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &log_sge;
        wr.num_sge = 1;
        if (config.release_log_with_cas) {
            log_sge.addr = reinterpret_cast<uintptr_t>(log_result);
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.wr.atomic.remote_addr = log_remote_addr(client, op.lock_id, conns[i].node_id, op.physical_log_slot);
            wr.wr.atomic.rkey = log_remote_rkey(client, op.lock_id, conns[i].node_id);
            wr.wr.atomic.compare_add = live_value;
            wr.wr.atomic.swap = free_value;
        } else {
            log_sge.addr = reinterpret_cast<uintptr_t>(log_value);
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags |= IBV_SEND_INLINE;
            wr.wr.rdma.remote_addr = log_remote_addr(client, op.lock_id, conns[i].node_id, op.physical_log_slot);
            wr.wr.rdma.rkey = log_remote_rkey(client, op.lock_id, conns[i].node_id);
        }

        if (ibv_post_send(conns[i].id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("ticket_faa pipeline: release log post failed");
        }
    }
}

} // namespace

// Load compile-time ticket-faa tuning knobs into the runtime config struct.
// Load compile-time ticket-faa tuning knobs into the runtime config struct.
TicketFaaLockPipelineConfig load_ticket_faa_lock_pipeline_config() {
    TicketFaaLockPipelineConfig config{};
    config.active_window = std::max<size_t>(1, TICKET_FAA_ACTIVE_WINDOW);
    config.cq_batch = std::max<size_t>(1, TICKET_FAA_CQ_BATCH);
    config.zipf_skew = TICKET_FAA_ZIPF_SKEW;
    config.shard_owner = TICKET_FAA_SHARD_OWNER;
    config.release_log_with_cas = TICKET_FAA_RELEASE_LOG_USE_CAS;
    config.release_turn_mode = TICKET_FAA_RELEASE_TURN_MODE;
    return config;
}

// Report how much client MR space ticket_faa needs for its per-op state.
// Report how much client MR space ticket_faa needs for its per-op state.
size_t ticket_faa_lock_pipeline_client_buffer_size(const TicketFaaLockPipelineConfig& config) {
    const size_t replica_count = CLUSTER_NODES.size();
    const size_t scalar_bytes = align_up(config.active_window * sizeof(uint64_t), 64);
    const size_t matrix_bytes = align_up(config.active_window * replica_count * sizeof(uint64_t), 64);
    const size_t detached_turn_bytes = align_up(detached_turn_ctx_capacity(config.active_window) * sizeof(uint64_t), 64);
    return align_up(scalar_bytes * 2 + matrix_bytes * 3 + detached_turn_bytes + PAGE_SIZE, PAGE_SIZE);
}

// Run the ticket_faa client state machine across the configured active window.
void run_ticket_faa_lock_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const TicketFaaLockPipelineConfig& config
) {
    // Validate connection availability and ensure the active window fits within
    // the WR-id encoding used by ticket_faa.
    const auto& conns = client.connections();
    if (conns.empty()) throw std::runtime_error("ticket_faa pipeline: no server connections");
    if (config.active_window > 0x7FFFu) throw std::runtime_error("ticket_faa pipeline: active window too large");

    // The client MR holds ticket FAA results, wrapped-log claim/release buffers,
    // turn-read buffers, and detached turn-update result slots.
    auto buffers = map_buffers(client.buffer(), client.buffer_size(), config.active_window, conns.size());
    std::vector<TicketFaaOpCtx> ops(config.active_window);
    std::vector<DetachedTurnCtx> detached_turn_ctxs(detached_turn_ctx_capacity(config.active_window));
    std::vector<ibv_wc> completions(config.cq_batch);
    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    auto begin_release = [&](TicketFaaOpCtx& op) {
        // Acquire latency ends when the lock is granted. Release work starts only
        // after the latency sample has been recorded.
        latencies[op.latency_index] = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - op.started_at).count();
        post_release_parallel(client, op, buffers, config, detached_turn_ctxs);
        mirror_turn_value(client, op.lock_id, op.ticket + 1);
    };

    auto prime_op = [&](TicketFaaOpCtx& op, const size_t slot, const bool is_retry) {
        op.active = true;
        op.retry_pending = false;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.lock_id = RECOVERY_TARGET_LOCK;
        op.owner_node = use_recovery_route(op.lock_id) ? client.recovery_route().frontier_host
                                                       : (config.shard_owner ? (op.lock_id % static_cast<uint32_t>(conns.size())) : 0);
        op.epoch = client.recovery_route().epoch;
        op.req_id = next_req_id++;
        op.round = 0;
        op.ticket = 0;
        op.waiter_id = encode_waiter(static_cast<uint16_t>(client.id()), static_cast<uint16_t>(slot), op.req_id);
        op.physical_log_slot = 0;
        op.last_turn = 0;
        op.replicate_retry_spin_remaining = 0;
        op.turn_spin_remaining = 0;
        op.phase = TicketFaaPhase::idle;
        op.responses = 0;
        op.response_target = 0;
        op.active_replica_targets = active_replica_targets(client, op.lock_id);
        op.quorum_hits = 0;
        op.release_log_responses = 0;
        op.release_log_quorum = 0;
        op.release_owner_log_done = false;
        if (!is_retry) {
            op.latency_index = submitted % NUM_OPS_PER_CLIENT;
            submitted++;
        }
        op.started_at = std::chrono::steady_clock::now();
        post_ticket_faa(client, op, buffers);
        active++;
    };

    auto submit_op = [&](const size_t slot) {
        // Start one ticket_faa request:
        // 1. choose a lock id,
        // 2. choose the owner node,
        // 3. initialize a unique waiter id,
        // 4. fetch the next logical ticket via owner-node FAA.
        prime_op(ops[slot], slot, false);
    };

    auto retry_op = [&](const size_t slot) {
        prime_op(ops[slot], slot, true);
    };

    // Fill the active window so ticket fetches and later waits can overlap.
    while (active < config.active_window && !client.experiment_done()) {
        submit_op(active);
    }

    while (!client.experiment_done() || active > 0) {
        if (client.recovery_active()) {
            for (auto& op : ops) {
                if (!op.active || !use_recovery_route(op.lock_id)) continue;
                op.active = false;
                op.retry_pending = true;
                op.phase = TicketFaaPhase::idle;
                active--;
            }
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
            if (!submitted_any) break;
        }

        // Two local spin states exist:
        // - replicate_ticket_spin: wrapped log space is not reusable yet
        // - wait_turn_spin: wait locally before issuing another turn read
        for (auto& op : ops) {
            if (!op.active) continue;
            if (op.phase == TicketFaaPhase::replicate_ticket_spin) {
                if (op.replicate_retry_spin_remaining > 0) {
                    --op.replicate_retry_spin_remaining;
                }
                if (op.replicate_retry_spin_remaining == 0) {
                    post_replicate_ticket(client, op, buffers);
                }
            } else if (op.phase == TicketFaaPhase::wait_turn_spin) {
                if (op.turn_spin_remaining > 0) {
                    --op.turn_spin_remaining;
                }
                if (op.turn_spin_remaining == 0) {
                    post_turn_read(client, op, buffers);
                }
            }
        }

        // Completion flow is: ticket FAA -> wrapped log claim -> turn reads -> release.
        const int polled = ibv_poll_cq(client.cq(), static_cast<int>(completions.size()), completions.data());
        if (polled < 0) throw std::runtime_error("ticket_faa pipeline: CQ poll failed");
        if (polled == 0) continue;

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[static_cast<size_t>(i)];
            if (client.handle_control_completion(wc)) {
                continue;
            }
            if (wc.status != IBV_WC_SUCCESS) {
                const uint32_t slot = wr_slot(wc.wr_id);
                if (slot < ops.size() && use_recovery_route(ops[slot].lock_id)) {
                    auto& op = ops[slot];
                    if (op.active) {
                        op.active = false;
                        op.retry_pending = true;
                        op.phase = TicketFaaPhase::idle;
                        active--;
                    }
                    continue;
                }
                throw std::runtime_error(
                    "ticket_faa pipeline: completion failed status=" + std::to_string(wc.status)
                    + " vendor=" + std::to_string(wc.vendor_err));
            }

            if (is_detached_turn_wr_id(wc.wr_id)) {
                // Detached turn CAS/FAA completions validate late owner-node turn
                // updates without forcing the main op slot to stay occupied.
                const uint32_t ctx_index = detached_turn_ctx_index(wc.wr_id);
                if (ctx_index >= detached_turn_ctxs.size()) {
                    throw std::runtime_error("ticket_faa pipeline: detached turn context out of range");
                }
                auto& ctx = detached_turn_ctxs[ctx_index];
                if (!ctx.active) continue;
                if (ctx.turn_mode == kTurnReleaseCas && buffers.detached_turn_results[ctx_index] != ctx.expected_old) {
                    throw std::runtime_error("ticket_faa pipeline: turn CAS release failed");
                }
                ctx.active = false;
                continue;
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) continue;
            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) continue;
            if (use_recovery_route(op.lock_id) && op.epoch != client.recovery_route().epoch && !client.recovery_active()) {
                op.active = false;
                op.retry_pending = true;
                op.phase = TicketFaaPhase::idle;
                active--;
                continue;
            }
            const TicketFaaPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) continue;
            if (wr_round(wc.wr_id) != op.round) continue;

            if (phase == TicketFaaPhase::faa_ticket) {
                // Once the ticket is fetched, the op must claim wrapped log space
                // before it can wait for turn.
                op.ticket = buffers.ticket_results[op.slot];
                mirror_frontier_value(client, op.lock_id, op.ticket + 1);
                post_replicate_ticket(client, op, buffers);
                continue;
            }

            op.responses++;

            if (phase == TicketFaaPhase::replicate_ticket) {
                // Wrapped log claim is always CAS. If quorum cannot be reached
                // because the slot is still occupied by an older generation, spin
                // locally and retry later instead of failing immediately.
                auto* results = row_ptr(buffers.replicate_results, op.slot, conns.size());
                const uint8_t idx = wr_conn(wc.wr_id);
                if (use_recovery_route(op.lock_id) && !recovery_node_active(client.recovery_route(), conns[idx].node_id)) {
                    continue;
                }
                if (results[idx] == ticket_faa_log_expected_free_value(op.ticket)) {
                    op.quorum_hits++;
                }
                const uint32_t remaining = op.response_target - op.responses;
                if (op.quorum_hits >= QUORUM) {
                    if (op.ticket == 0) {
                        begin_release(op);
                    } else {
                        post_turn_read(client, op, buffers);
                    }
                    continue;
                }
                if (op.quorum_hits + remaining < QUORUM) {
                    op.active = false;
                    op.retry_pending = true;
                    op.phase = TicketFaaPhase::idle;
                    active--;
                    continue;
                }
                continue;
            }

            if (phase == TicketFaaPhase::wait_turn_read) {
                // The op may enter when turn == ticket. Otherwise use ticket-turn
                // distance to decide whether to spin locally or reread turn now.
                op.last_turn = buffers.turn_reads[op.slot];
                if (op.last_turn == op.ticket) {
                    begin_release(op);
                } else {
                    const uint64_t distance = op.ticket > op.last_turn ? (op.ticket - op.last_turn) : 0;
                    const uint32_t spin_budget = spin_budget_for_distance(distance);
                    if (spin_budget == 0) {
                        post_turn_read(client, op, buffers);
                    } else {
                        op.turn_spin_remaining = spin_budget;
                        op.phase = TicketFaaPhase::wait_turn_spin;
                    }
                }
                continue;
            }

            if (phase == TicketFaaPhase::release_parallel) {
                // Release has two independent pieces of work:
                // - free the wrapped log slot on replicas
                // - advance the owner-node turn register
                // Op reuse is gated by wrapped-log release safety.
                const uint8_t idx = wr_conn(wc.wr_id);
                if (use_recovery_route(op.lock_id) && !recovery_node_active(client.recovery_route(), conns[idx].node_id)) {
                    continue;
                }
                auto* release_log_results = row_ptr(buffers.release_log_results, op.slot, conns.size());
                op.release_log_responses++;
                if (!config.release_log_with_cas
                    || release_log_results[idx] == pack_ticket_faa_log_live(op.ticket, static_cast<uint16_t>(client.id()), static_cast<uint16_t>(op.slot))) {
                    op.release_log_quorum++;
                    if (idx == op.owner_node) {
                        op.release_owner_log_done = true;
                    }
                } else if (idx == op.owner_node) {
                    throw std::runtime_error("ticket_faa pipeline: owner wrapped log release CAS failed");
                }

                const uint32_t remaining = op.active_replica_targets - op.release_log_responses;
                if (op.release_log_quorum + remaining < QUORUM) {
                    op.active = false;
                    op.retry_pending = true;
                    op.phase = TicketFaaPhase::idle;
                    active--;
                    continue;
                }
                if (op.release_owner_log_done && op.release_log_quorum >= QUORUM) {
                    mirror_turn_value(client, op.lock_id, op.ticket + 1);
                    lock_counts[op.lock_id]++;
                    op.active = false;
                    op.retry_pending = false;
                    op.phase = TicketFaaPhase::idle;
                    completed++;
                    active--;
                    if (!client.experiment_done() && !client.recovery_active()) submit_op(slot);
                }
            }
        }
    }
}
