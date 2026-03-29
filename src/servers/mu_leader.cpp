#include "rdma/servers/mu_leader.h"

// MU leader: owns in-memory per-lock state and a single global replicated mutation log.

#include "rdma/common.h"
#include "rdma/mu_encoding.h"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

enum class MutationKind : uint8_t {
    append_lock = 1,
    append_unlock = 2,
    register_watch = 3,
};

struct CommittedWaiter {
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint32_t granted_slot = 0;
};

struct MutationCtx {
    bool in_use = false;
    bool quorum_done = false;
    bool applied = false;
    uint32_t generation = 0;
    MutationKind kind = MutationKind::append_lock;
    uint32_t global_slot = 0;
    uint32_t lock_id = 0;
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint32_t granted_slot = 0;
    uint32_t ack_count = 0;
    uint32_t pending_followers = 0;
};

struct LockState {
    // Authoritative per-lock state lives only in leader memory. The replicated
    // MU log is the globally ordered mutation stream used to replay these state
    // transitions.
    std::deque<MuRequest> pending_locks;
    std::optional<MuRequest> pending_unlock;
    std::deque<CommittedWaiter> committed_waiters;
    bool holder_active = false;
    uint32_t holder_slot = 0;
    uint16_t holder_client_id = 0;
    uint32_t holder_req_id = 0;
    uint32_t append_inflight = 0;
    int unlock_mutation = -1;
};

struct WatchState {
    // Per-object watch registration state (similar to LockState but for watches)
    std::deque<MuRequest> pending_registers;  // Queue of registration requests
    uint32_t register_inflight = 0;           // Number of in-flight registrations
};

constexpr uint32_t MU_MAX_REGISTER_INFLIGHT_PER_OBJECT = MU_MAX_APPEND_INFLIGHT_PER_LOCK;  // Match lock concurrency

struct NotificationCtx {
    bool active = false;
    uint32_t object_id = 0;
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint64_t total_watchers = 0;
    uint64_t notify_sent = 0;
    uint64_t notify_completed = 0;
    uint64_t new_version = 0;
};

struct MuLeaderRuntime {
    // Runtime groups the event-loop state that used to be captured by local
    // lambdas so helper functions can operate on one explicit context object.
    uint32_t node_id;
    uint32_t lock_start;
    uint32_t lock_end;
    bool debug_enabled;
    bool quorum_only_signal;
    uint8_t* local_buf;
    ibv_pd* pd;
    ibv_cq* cq;
    ibv_mr* local_mr;
    std::vector<RemoteConnection>& clients;
    std::vector<RemoteConnection>& peers;
    uint32_t num_clients;
    size_t handled_locks;
    size_t repl_signal_cursor = 0;
    uint32_t global_next_append_slot = 0;
    uint32_t global_commit_tail = 0;
    std::vector<MuRequest> recv_buffers;
    ibv_mr* recv_mr = nullptr;
    std::vector<LockState> locks;
    std::vector<MutationCtx> mutations;
    std::deque<uint32_t> free_mutations;
    std::unordered_map<uint32_t, uint32_t> slot_to_mutation;
    std::deque<uint32_t> ready_locks;
    std::vector<uint8_t> ready_flags;
    std::vector<uint32_t> client_send_signal_counts;
    std::vector<size_t> follower_indices;
    std::optional<NotificationCtx> active_notification;
    std::deque<MuRequest> pending_notifications;  // Queue for WatchNotify requests
    std::vector<WatchState> watch_objects;         // Per-object watch state
    std::deque<uint32_t> ready_watches;            // Ready watch objects queue
    std::vector<uint8_t> ready_watch_flags;        // Track if object is already in ready queue
};

constexpr uint64_t MU_RECV_WR_TAG = 0xB1ULL;
constexpr uint64_t MU_REPL_WR_TAG = 0xB2ULL;
constexpr uint64_t MU_WATCH_REPL_WR_TAG = 0xB3ULL;
constexpr uint64_t MU_NOTIFY_WR_TAG = 0xB4ULL;
constexpr uint64_t MU_RESP_WR_TAG = 0x4C53000000000000ULL;
constexpr uint64_t MU_WR_TAG_SHIFT = 56;
constexpr uint64_t MU_REPL_GEN_SHIFT = 24;
constexpr uint64_t MU_REPL_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t MU_REPL_ID_MASK = 0xFFFFFFULL;

// Encode a posted receive slot so completions can be matched back to client and ring position.
uint64_t make_recv_wr_id(const uint16_t client_id, const uint16_t recv_slot) {
    return (MU_RECV_WR_TAG << MU_WR_TAG_SHIFT)
         | (static_cast<uint64_t>(client_id) << 16)
         | static_cast<uint64_t>(recv_slot);
}

// Extract the client id from a posted receive WR id.
uint16_t recv_client_id(const uint64_t wr_id) {
    return static_cast<uint16_t>((wr_id >> 16) & 0xFFFFu);
}

// Extract the receive-ring slot from a posted receive WR id.
uint16_t recv_slot_index(const uint64_t wr_id) {
    return static_cast<uint16_t>(wr_id & 0xFFFFu);
}

// Encode a replication WR id from mutation id and generation.
uint64_t make_repl_wr_id(const uint32_t mutation_id, const uint32_t generation) {
    return (MU_REPL_WR_TAG << MU_WR_TAG_SHIFT)
         | ((static_cast<uint64_t>(generation) & MU_REPL_GEN_MASK) << MU_REPL_GEN_SHIFT)
         | (static_cast<uint64_t>(mutation_id) & MU_REPL_ID_MASK);
}

// Extract the mutation generation from a replication WR id.
uint32_t repl_generation(const uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> MU_REPL_GEN_SHIFT) & MU_REPL_GEN_MASK);
}

// Extract the mutation id from a replication WR id.
uint32_t repl_mutation_id(const uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id & MU_REPL_ID_MASK);
}

// Identify replication completions in the shared CQ stream.
bool is_repl_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_REPL_WR_TAG;
}

// Identify posted receive completions in the shared CQ stream.
bool is_recv_wr_id(const uint64_t wr_id) {
    return (wr_id >> MU_WR_TAG_SHIFT) == MU_RECV_WR_TAG;
}

// Identify inline SEND completions for leader responses.
bool is_resp_send_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == MU_RESP_WR_TAG;
}

// Emit a leader-scoped debug line when MU_DEBUG is enabled.
void mu_debug(const MuLeaderRuntime& rt, const std::string& msg) {
    if (rt.debug_enabled) {
        std::cout << "[MuLeader " << rt.node_id << "] " << msg << "\n";
    }
}

// Put a lock onto the ready queue exactly once so the service loop can append
// pending lock/unlock mutations without duplicate queue entries.
void enqueue_ready(MuLeaderRuntime& rt, const uint32_t lock_id) {
    if (lock_id < rt.lock_start || lock_id >= rt.lock_end) return;
    if (rt.ready_flags[lock_id] != 0) return;
    rt.ready_flags[lock_id] = 1;
    rt.ready_locks.push_back(lock_id);
}

// Put a watch object onto the ready queue exactly once so the service loop can
// process pending watch registrations without duplicate queue entries.
void enqueue_ready_watch(MuLeaderRuntime& rt, const uint32_t object_id) {
    if (object_id >= MAX_LOCKS) return;  // Watch objects use same ID space as locks
    if (rt.ready_watch_flags[object_id] != 0) return;
    rt.ready_watch_flags[object_id] = 1;
    rt.ready_watches.push_back(object_id);
}

// Send one inline leader response back to the requesting client.
void send_response(MuLeaderRuntime& rt, const MuResponse& resp) {
    if (resp.client_id >= rt.clients.size() || rt.clients[resp.client_id].cm_id == nullptr) {
        throw std::runtime_error("MuLeader: invalid client response target");
    }

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&resp);
    sge.length = sizeof(MuResponse);
    sge.lkey = 0;

    ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = MU_RESP_WR_TAG | resp.client_id;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_INLINE;
    if (++rt.client_send_signal_counts[resp.client_id] % MU_SERVER_SEND_SIGNAL_EVERY == 0) {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }

    if (ibv_post_send(rt.clients[resp.client_id].cm_id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("MuLeader: failed to post client response send");
    }
}

// Keep the per-client receive ring full so lock/unlock RPCs can arrive without
// additional round trips.
void post_recv(MuLeaderRuntime& rt, const uint16_t client_id, const uint16_t recv_slot) {
    auto& buffer = rt.recv_buffers[static_cast<size_t>(client_id) * MU_SERVER_RECV_RING + recv_slot];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&buffer);
    sge.length = sizeof(MuRequest);
    sge.lkey = rt.recv_mr->lkey;

    ibv_recv_wr wr{}, *bad_wr = nullptr;
    wr.wr_id = make_recv_wr_id(client_id, recv_slot);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(rt.clients[client_id].cm_id->qp, &wr, &bad_wr)) {
        throw std::runtime_error("MuLeader: failed to post recv");
    }
}

// Allocate one mutation context from the inflight pool.
std::optional<uint32_t> try_alloc_mutation(MuLeaderRuntime& rt) {
    if (rt.free_mutations.empty()) {
        return std::nullopt;
    }
    const uint32_t id = rt.free_mutations.front();
    rt.free_mutations.pop_front();
    auto& ctx = rt.mutations[id];
    ctx.in_use = true;
    ctx.quorum_done = false;
    ctx.applied = false;
    ctx.generation++;
    ctx.ack_count = 1;
    ctx.pending_followers = 0;
    return id;
}

// Return a mutation context to the free pool once it is both committed and no
// longer waiting on follower completions.
void release_mutation(MuLeaderRuntime& rt, const uint32_t mutation_id) {
    auto& ctx = rt.mutations[mutation_id];
    if (ctx.kind == MutationKind::append_lock) {
        auto& lock = rt.locks[ctx.lock_id];
        if (lock.append_inflight > 0) {
            lock.append_inflight--;
        }
        enqueue_ready(rt, ctx.lock_id);
    }
    // register_watch doesn't need lock state management
    ctx.in_use = false;
    rt.free_mutations.push_back(mutation_id);
}

// Release a mutation context only after it has been applied and all tracked
// follower writes have completed.
void maybe_release_mutation(MuLeaderRuntime& rt, const uint32_t mutation_id) {
    auto& ctx = rt.mutations[mutation_id];
    if (!ctx.in_use) return;
    if (ctx.pending_followers != 0) return;
    if (!ctx.applied) return;
    release_mutation(rt, mutation_id);
}

// Grant the next committed waiter for a lock if the lock is currently free.
void try_grant(MuLeaderRuntime& rt, const uint32_t lock_id) {
    auto& lock = rt.locks[lock_id];
    if (lock.holder_active || lock.committed_waiters.empty()) return;

    const CommittedWaiter waiter = lock.committed_waiters.front();
    lock.committed_waiters.pop_front();

    MuResponse resp{};
    resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
    resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
    resp.client_id = waiter.client_id;
    resp.lock_id = lock_id;
    resp.req_id = waiter.req_id;
    resp.granted_slot = waiter.granted_slot;
    send_response(rt, resp);

    lock.holder_active = true;
    lock.holder_slot = waiter.granted_slot;
    lock.holder_client_id = waiter.client_id;
    lock.holder_req_id = waiter.req_id;
}

void apply_mutation(MuLeaderRuntime& rt, const uint32_t mutation_id) {
    // A committed global-log mutation becomes an in-memory per-lock state change
    // here. Grants and unlock acks are emitted from that in-memory state, not by
    // scanning a replicated per-lock log.
    auto& ctx = rt.mutations[mutation_id];

    if (ctx.kind == MutationKind::register_watch) {
        // Watch registration: ACK the client after quorum is reached
        const uint32_t object_id = ctx.lock_id;
        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::WatchRegister);
        resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
        resp.client_id = ctx.client_id;
        resp.lock_id = object_id;
        resp.req_id = ctx.req_id;
        resp.granted_slot = ctx.granted_slot;  // watch slot stored here
        send_response(rt, resp);

        // Decrement in-flight count and re-enqueue to process more pending registrations
        if (object_id < MAX_LOCKS) {
            auto& watch = rt.watch_objects[object_id];
            if (watch.register_inflight > 0) {
                watch.register_inflight--;
            }
            if (!watch.pending_registers.empty()) {
                enqueue_ready_watch(rt, object_id);
            }
        }

        ctx.applied = true;
        maybe_release_mutation(rt, mutation_id);
        return;
    }

    auto& lock = rt.locks[ctx.lock_id];

    if (ctx.kind == MutationKind::append_lock) {
        if (!lock.holder_active && lock.committed_waiters.empty()) {
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
            resp.client_id = ctx.client_id;
            resp.lock_id = ctx.lock_id;
            resp.req_id = ctx.req_id;
            resp.granted_slot = ctx.global_slot;
            send_response(rt, resp);

            lock.holder_active = true;
            lock.holder_slot = ctx.global_slot;
            lock.holder_client_id = ctx.client_id;
            lock.holder_req_id = ctx.req_id;
        } else {
            lock.committed_waiters.push_back({ctx.client_id, ctx.req_id, ctx.global_slot});
        }
    } else {
        if (lock.unlock_mutation == static_cast<int>(mutation_id)) {
            lock.unlock_mutation = -1;
        }

        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
        resp.client_id = ctx.client_id;
        resp.lock_id = ctx.lock_id;
        resp.req_id = ctx.req_id;
        resp.granted_slot = ctx.granted_slot;

        if (!lock.holder_active
            || lock.holder_client_id != ctx.client_id
            || lock.holder_req_id != ctx.req_id
            || lock.holder_slot != ctx.granted_slot) {
            resp.status = static_cast<uint8_t>(MuRpcStatus::InvalidUnlock);
            send_response(rt, resp);
        } else {
            resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
            send_response(rt, resp);
            lock.holder_active = false;
            lock.holder_slot = 0;
            lock.holder_client_id = 0;
            lock.holder_req_id = 0;
            try_grant(rt, ctx.lock_id);
        }
    }

    ctx.applied = true;
    maybe_release_mutation(rt, mutation_id);
    enqueue_ready(rt, ctx.lock_id);
}

void advance_commit_tail(MuLeaderRuntime& rt) {
    // MU applies mutations strictly in global committed order. A later mutation
    // is not applied until all earlier global slots are also committed.
    static uint64_t stuck_count = 0;
    static uint32_t last_tail = 0;

    while (true) {
        const auto it = rt.slot_to_mutation.find(rt.global_commit_tail);
        if (it == rt.slot_to_mutation.end()) {
            if (rt.global_commit_tail == last_tail) {
                stuck_count++;
                // if (stuck_count == 1 || stuck_count % 100000000 == 0) {
                //     std::cerr << "[MuLeader] advance_commit_tail: slot " << rt.global_commit_tail
                //               << " not in map (stuck_count=" << stuck_count << ")" << std::endl;
                //     std::cerr.flush();
                // }
            } else {
                last_tail = rt.global_commit_tail;
                stuck_count = 0;
            }
            break;
        }

        const uint32_t mutation_id = it->second;
        auto& ctx = rt.mutations[mutation_id];
        if (!ctx.in_use || !ctx.quorum_done || ctx.applied) {
            if (rt.global_commit_tail == last_tail) {
                stuck_count++;
                // if (stuck_count == 1 || stuck_count % 100000000 == 0) {
                //     std::cerr << "[MuLeader] advance_commit_tail: slot " << rt.global_commit_tail
                //               << " mutation_id=" << mutation_id
                //               << " in_use=" << ctx.in_use
                //               << " quorum_done=" << ctx.quorum_done
                //               << " applied=" << ctx.applied
                //               << " (stuck_count=" << stuck_count << ")" << std::endl;
                //     std::cerr.flush();
                // }
            } else {
                last_tail = rt.global_commit_tail;
                stuck_count = 0;
            }
            break;
        }

        rt.slot_to_mutation.erase(it);
        apply_mutation(rt, mutation_id);
        rt.global_commit_tail++;
        last_tail = rt.global_commit_tail;
        stuck_count = 0;
    }
}

void post_mutation_writes(MuLeaderRuntime& rt, const uint32_t mutation_id) {
    // Replicate one global append-only MU mutation entry to the follower nodes.
    // The leader writes locally first; followers receive the same mutation entry
    // via RDMA writes, and quorum on those writes commits the mutation.
    auto& ctx = rt.mutations[mutation_id];
    auto* entry_ptr = rt.local_buf + mu_global_log_slot_offset(ctx.global_slot);
    const size_t followers = rt.follower_indices.size();
    const size_t quorum_needed = (QUORUM > 0) ? std::min<size_t>(QUORUM - 1, followers) : 0;
    const size_t signaled_to_track = rt.quorum_only_signal ? quorum_needed : followers;
    const size_t signal_start = followers == 0 ? 0 : (rt.repl_signal_cursor % followers);
    if (followers != 0 && signaled_to_track != 0) {
        rt.repl_signal_cursor = (rt.repl_signal_cursor + signaled_to_track) % followers;
    }

    // static uint64_t post_count = 0;
    // const bool debug_post = (post_count < 5) || (post_count % 10000 == 0);
    // if (debug_post) {
    //     std::cerr << "[MuLeader] post_mutation_writes #" << post_count
    //               << " mutation_id=" << mutation_id
    //               << " global_slot=" << ctx.global_slot
    //               << " followers=" << followers << std::endl;
    //     std::cerr.flush();
    // }

    for (size_t follower_pos = 0; follower_pos < rt.follower_indices.size(); ++follower_pos) {
        const size_t follower_idx = rt.follower_indices[follower_pos];
        auto& follower = rt.peers[follower_idx];
        bool should_signal = !rt.quorum_only_signal;
        if (rt.quorum_only_signal && followers != 0) {
            should_signal = false;
            for (size_t i = 0; i < signaled_to_track; ++i) {
                if (follower_pos == ((signal_start + i) % followers)) {
                    should_signal = true;
                    break;
                }
            }
        }

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(entry_ptr);
        sge.length = ENTRY_SIZE;
        sge.lkey = rt.local_mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_repl_wr_id(mutation_id, ctx.generation);
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_INLINE | (should_signal ? IBV_SEND_SIGNALED : 0);
        wr.wr.rdma.remote_addr = follower.remote_addr + mu_global_log_slot_offset(ctx.global_slot);
        wr.wr.rdma.rkey = follower.rkey;

        if (ibv_post_send(follower.cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("MuLeader: failed to replicate mutation");
        }

        if (should_signal) {
            ctx.pending_followers++;
        }
    }

    // post_count++;

    // Remove periodic drain - main event loop handles all CQ polling

    if (ctx.pending_followers == 0) {
        ctx.quorum_done = true;
        advance_commit_tail(rt);
        maybe_release_mutation(rt, mutation_id);
    }
}

// Forward declaration
void post_watch_writes(MuLeaderRuntime& rt, const uint32_t mutation_id, const uint32_t object_id, const uint64_t slot);

// Process one watch registration from the queue and replicate it (like start_append_mutation for locks)
void start_register_watch(MuLeaderRuntime& rt, const uint32_t object_id) {
    auto& watch = rt.watch_objects[object_id];
    if (watch.pending_registers.empty() || watch.register_inflight >= MU_MAX_REGISTER_INFLIGHT_PER_OBJECT) {
        return;
    }

    const auto mutation_id_opt = try_alloc_mutation(rt);
    if (!mutation_id_opt.has_value()) {
        enqueue_ready_watch(rt, object_id);
        return;
    }

    MuRequest req = watch.pending_registers.front();
    watch.pending_registers.pop_front();

    // Atomically allocate a watch slot
    auto* counter_ptr = reinterpret_cast<uint64_t*>(
        rt.local_buf + watch_counter_offset(object_id));
    const uint64_t slot = __sync_fetch_and_add(counter_ptr, 1);

    if (slot >= MAX_WATCHERS_PER_OBJECT) {
        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::WatchRegister);
        resp.status = static_cast<uint8_t>(MuRpcStatus::QueueFull);
        resp.client_id = req.client_id;
        resp.lock_id = object_id;
        resp.req_id = req.req_id;
        resp.granted_slot = 0;
        send_response(rt, resp);
        enqueue_ready_watch(rt, object_id);
        return;
    }

    // Assign global slot for MU commit ordering
    const uint32_t global_slot = rt.global_next_append_slot++;

    const uint32_t mutation_id = *mutation_id_opt;
    auto& ctx = rt.mutations[mutation_id];
    ctx.kind = MutationKind::register_watch;
    ctx.lock_id = object_id;
    ctx.granted_slot = static_cast<uint32_t>(slot);
    ctx.client_id = req.client_id;
    ctx.req_id = req.req_id;
    ctx.global_slot = global_slot;
    rt.slot_to_mutation[global_slot] = mutation_id;

    // Write watcher ID locally
    auto* watcher_slot = reinterpret_cast<uint64_t*>(
        rt.local_buf + watch_id_slot_offset(object_id, slot));
    *watcher_slot = req.client_id;

    watch.register_inflight++;
    post_watch_writes(rt, mutation_id, object_id, slot);
}

// Replicate watch registration to followers (same pattern as post_mutation_writes)
void post_watch_writes(MuLeaderRuntime& rt, const uint32_t mutation_id, const uint32_t object_id, const uint64_t slot) {
    // static uint64_t call_count = 0;
    // const bool debug_this = call_count < 10 || call_count % 10000 == 0;
    // if (debug_this) {
    //     std::cerr << "[MuLeader] post_watch_writes #" << call_count
    //               << " mutation_id=" << mutation_id
    //               << " object=" << object_id
    //               << " slot=" << slot << std::endl;
    //     std::cerr.flush();
    // }
    // call_count++;

    auto& ctx = rt.mutations[mutation_id];
    auto* watcher_slot_ptr = rt.local_buf + watch_id_slot_offset(object_id, slot);
    const size_t followers = rt.follower_indices.size();
    const size_t quorum_needed = (QUORUM > 0) ? std::min<size_t>(QUORUM - 1, followers) : 0;
    const size_t signaled_to_track = rt.quorum_only_signal ? quorum_needed : followers;
    const size_t signal_start = followers == 0 ? 0 : (rt.repl_signal_cursor % followers);
    if (followers != 0 && signaled_to_track != 0) {
        rt.repl_signal_cursor = (rt.repl_signal_cursor + signaled_to_track) % followers;
    }

    // if (debug_this) {
    //     std::cerr << "[MuLeader] post_watch_writes: followers=" << followers
    //               << " quorum_needed=" << quorum_needed
    //               << " signaled_to_track=" << signaled_to_track
    //               << " signal_start=" << signal_start
    //               << " quorum_only=" << rt.quorum_only_signal
    //               << " ack_count=" << ctx.ack_count << std::endl;
    //     std::cerr.flush();
    // }

    for (size_t follower_pos = 0; follower_pos < rt.follower_indices.size(); ++follower_pos) {
        const size_t follower_idx = rt.follower_indices[follower_pos];
        auto& follower = rt.peers[follower_idx];
        bool should_signal = !rt.quorum_only_signal;
        if (rt.quorum_only_signal && followers != 0) {
            should_signal = false;
            for (size_t i = 0; i < signaled_to_track; ++i) {
                if (follower_pos == ((signal_start + i) % followers)) {
                    should_signal = true;
                    break;
                }
            }
        }

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(watcher_slot_ptr);
        sge.length = sizeof(uint64_t);
        sge.lkey = rt.local_mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = make_repl_wr_id(mutation_id, ctx.generation);
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        // Use selective signaling like post_mutation_writes (match lock behavior)
        wr.send_flags = IBV_SEND_INLINE | (should_signal ? IBV_SEND_SIGNALED : 0);
        wr.wr.rdma.remote_addr = follower.remote_addr + watch_id_slot_offset(object_id, slot);
        wr.wr.rdma.rkey = follower.rkey;

        if (ibv_post_send(follower.cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("MuLeader: failed to replicate watch registration");
        }

        // Only track completion if signaled (match lock behavior)
        if (should_signal) {
            ctx.pending_followers++;
        }
        // if (debug_this) {
        //     std::cerr << "[MuLeader] post_watch_writes: signaled follower_pos=" << follower_pos
        //               << " follower_idx=" << follower_idx
        //               << " pending_followers=" << ctx.pending_followers << std::endl;
        //     std::cerr.flush();
        // }
    }

    // if (debug_this) {
    //     std::cerr << "[MuLeader] post_watch_writes: DONE pending_followers=" << ctx.pending_followers << std::endl;
    //     std::cerr.flush();
    // }

    if (ctx.pending_followers == 0) {
        ctx.quorum_done = true;
        ctx.applied = true;
        // Respond immediately if no followers
        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::WatchRegister);
        resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
        resp.client_id = ctx.client_id;
        resp.lock_id = ctx.lock_id;
        resp.req_id = ctx.req_id;
        resp.granted_slot = ctx.granted_slot;
        send_response(rt, resp);
        maybe_release_mutation(rt, mutation_id);
    }
}

// Append one lock request to the global mutation log and replicate it.
void start_append_mutation(MuLeaderRuntime& rt, const uint32_t lock_id) {
    auto& lock = rt.locks[lock_id];
    if (lock.pending_locks.empty() || lock.append_inflight >= MU_MAX_APPEND_INFLIGHT_PER_LOCK) return;

    const auto mutation_id_opt = try_alloc_mutation(rt);
    if (!mutation_id_opt.has_value()) {
        enqueue_ready(rt, lock_id);
        return;
    }

    MuRequest req = lock.pending_locks.front();
    lock.pending_locks.pop_front();

    if (rt.global_next_append_slot >= MU_GLOBAL_LOG_CAPACITY) {
        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
        resp.status = static_cast<uint8_t>(MuRpcStatus::QueueFull);
        resp.client_id = req.client_id;
        resp.lock_id = req.lock_id;
        resp.req_id = req.req_id;
        send_response(rt, resp);
        enqueue_ready(rt, lock_id);
        return;
    }

    const uint32_t global_slot = rt.global_next_append_slot++;
    *reinterpret_cast<uint64_t*>(rt.local_buf + mu_global_log_slot_offset(global_slot)) =
        mu_make_log_entry(MuRpcOp::Lock, req.lock_id, req.client_id, req.req_id);

    const uint32_t mutation_id = *mutation_id_opt;
    auto& ctx = rt.mutations[mutation_id];
    ctx.kind = MutationKind::append_lock;
    ctx.global_slot = global_slot;
    ctx.lock_id = lock_id;
    ctx.client_id = req.client_id;
    ctx.req_id = req.req_id;
    ctx.granted_slot = 0;
    rt.slot_to_mutation[global_slot] = mutation_id;

    lock.append_inflight++;
    post_mutation_writes(rt, mutation_id);
}

// Append one unlock request to the global mutation log after validating the
// current in-memory holder state.
void start_unlock_mutation(MuLeaderRuntime& rt, const uint32_t lock_id) {
    auto& lock = rt.locks[lock_id];
    if (!lock.pending_unlock.has_value() || lock.unlock_mutation != -1) return;

    const MuRequest req = *lock.pending_unlock;
    if (!lock.holder_active
        || req.client_id != lock.holder_client_id
        || req.req_id != lock.holder_req_id
        || req.granted_slot != lock.holder_slot) {
        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
        resp.status = static_cast<uint8_t>(MuRpcStatus::InvalidUnlock);
        resp.client_id = req.client_id;
        resp.lock_id = req.lock_id;
        resp.req_id = req.req_id;
        resp.granted_slot = req.granted_slot;
        send_response(rt, resp);
        lock.pending_unlock.reset();
        enqueue_ready(rt, lock_id);
        return;
    }

    const auto mutation_id_opt = try_alloc_mutation(rt);
    if (!mutation_id_opt.has_value()) {
        enqueue_ready(rt, lock_id);
        return;
    }

    if (rt.global_next_append_slot >= MU_GLOBAL_LOG_CAPACITY) {
        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
        resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
        resp.client_id = req.client_id;
        resp.lock_id = req.lock_id;
        resp.req_id = req.req_id;
        resp.granted_slot = req.granted_slot;
        send_response(rt, resp);
        lock.pending_unlock.reset();
        enqueue_ready(rt, lock_id);
        return;
    }

    const uint32_t global_slot = rt.global_next_append_slot++;
    *reinterpret_cast<uint64_t*>(rt.local_buf + mu_global_log_slot_offset(global_slot)) =
        mu_make_log_entry(MuRpcOp::Unlock, req.lock_id, req.client_id, req.req_id);

    const uint32_t mutation_id = *mutation_id_opt;
    auto& ctx = rt.mutations[mutation_id];
    ctx.kind = MutationKind::append_unlock;
    ctx.global_slot = global_slot;
    ctx.lock_id = lock_id;
    ctx.client_id = req.client_id;
    ctx.req_id = req.req_id;
    ctx.granted_slot = req.granted_slot;
    rt.slot_to_mutation[global_slot] = mutation_id;

    lock.pending_unlock.reset();
    lock.unlock_mutation = static_cast<int>(mutation_id);
    post_mutation_writes(rt, mutation_id);
}

// Service one lock's pending ingress queues while append capacity is available.
void service_lock(MuLeaderRuntime& rt, const uint32_t lock_id) {
    auto& lock = rt.locks[lock_id];
    if (lock.pending_unlock.has_value() && lock.unlock_mutation == -1) {
        start_unlock_mutation(rt, lock_id);
    }
    while (!lock.pending_locks.empty() && lock.append_inflight < MU_MAX_APPEND_INFLIGHT_PER_LOCK) {
        start_append_mutation(rt, lock_id);
    }
}

// Service one watch object: process pending registrations keeping pipeline full.
void service_watch(MuLeaderRuntime& rt, const uint32_t object_id) {
    auto& watch = rt.watch_objects[object_id];
    while (!watch.pending_registers.empty() && watch.register_inflight < MU_MAX_REGISTER_INFLIGHT_PER_OBJECT) {
        start_register_watch(rt, object_id);
    }
}

// Post a batch of notification writes (match syndra's batching approach).
// Forward declaration
void post_notify_batch(MuLeaderRuntime& rt);

// Start processing a notification request
void start_notification(MuLeaderRuntime& rt, const MuRequest& req) {
    const uint32_t object_id = req.lock_id;

    // Increment version counter
    auto* version_ptr = reinterpret_cast<uint64_t*>(
        rt.local_buf + watch_version_offset(object_id));
    const uint64_t new_version = __sync_add_and_fetch(version_ptr, 1);

    // Read watcher count
    auto* counter_ptr = reinterpret_cast<uint64_t*>(
        rt.local_buf + watch_counter_offset(object_id));
    const uint64_t num_watchers = *counter_ptr;

    // Initialize notification context
    rt.active_notification = NotificationCtx{};
    rt.active_notification->active = true;
    rt.active_notification->object_id = object_id;
    rt.active_notification->client_id = req.client_id;
    rt.active_notification->req_id = req.req_id;
    rt.active_notification->total_watchers = num_watchers;
    rt.active_notification->notify_sent = 0;
    rt.active_notification->notify_completed = 0;
    rt.active_notification->new_version = new_version;

    // std::cerr << "[MuLeader debug] Starting notification for object " << object_id
    //           << " with " << num_watchers << " watchers" << std::endl;
}

// Process next pending notification from queue
void process_next_notification(MuLeaderRuntime& rt) {
    if (rt.pending_notifications.empty()) return;
    if (rt.active_notification.has_value()) return;  // Already processing one

    MuRequest req = rt.pending_notifications.front();
    rt.pending_notifications.pop_front();
    start_notification(rt, req);
    post_notify_batch(rt);
}

void post_notify_batch(MuLeaderRuntime& rt) {
    if (!rt.active_notification.has_value()) return;

    auto& notif = *rt.active_notification;
    constexpr uint64_t MAX_NOTIFY_BATCH = 1024;  // Match syndra_watch
    const size_t metadata_offset = WATCH_TABLE_SIZE;
    const size_t num_followers = rt.follower_indices.size();

    if (num_followers == 0) {
        // No followers - complete notification immediately
        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::WatchNotify);
        resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
        resp.client_id = notif.client_id;
        resp.lock_id = notif.object_id;
        resp.req_id = notif.req_id;
        resp.granted_slot = static_cast<uint32_t>(notif.total_watchers);
        send_response(rt, resp);
        rt.active_notification.reset();

        // Process next notification from queue
        process_next_notification(rt);
        return;
    }

    const uint64_t watchers_remaining = notif.total_watchers - notif.notify_sent;
    const uint64_t notify_count = std::min(watchers_remaining, MAX_NOTIFY_BATCH);
    notif.notify_completed = 0;  // Reset for this batch

    for (uint64_t i = 0; i < notify_count; ++i) {
        const uint64_t watcher_idx = notif.notify_sent + i;
        const size_t follower_idx = rt.follower_indices[watcher_idx % num_followers];
        auto& follower = rt.peers[follower_idx];

        auto* local_data = reinterpret_cast<uint64_t*>(
            rt.local_buf + metadata_offset + (watcher_idx * sizeof(uint64_t)));
        *local_data = notif.new_version;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(local_data);
        sge.length = sizeof(uint64_t);
        sge.lkey = rt.local_mr->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = (MU_NOTIFY_WR_TAG << MU_WR_TAG_SHIFT);  // Tag for notification completions
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;  // Signal every write (match syndra)

        wr.wr.rdma.remote_addr = follower.remote_addr + metadata_offset + (watcher_idx * sizeof(uint64_t));
        wr.wr.rdma.rkey = follower.rkey;

        if (ibv_post_send(follower.cm_id->qp, &wr, &bad_wr)) {
            // QP overflow - break and wait for completions (match syndra behavior)
            // if (MU_DEBUG) {
            //     std::cerr << "[MuLeader debug] QP overflow at notification " << i << "/" << notify_count
            //               << " for object " << notif.object_id << " (errno=" << errno << ")" << std::endl;
            // }
            break;
        }
        notif.notify_sent++;
    }
}

// Handle notification write completion (match syndra's batch completion logic).
void handle_notify_cqe(MuLeaderRuntime& rt) {
    if (!rt.active_notification.has_value()) return;

    auto& notif = *rt.active_notification;
    notif.notify_completed++;

    if (notif.notify_sent < notif.total_watchers) {
        // More watchers to notify - post next batch when current batch completes
        if (notif.notify_completed >= notif.notify_sent) {
            // std::cerr << "[MuLeader debug] Posting next batch: completed=" << notif.notify_completed
            //           << " sent=" << notif.notify_sent << " total=" << notif.total_watchers << std::endl;
            post_notify_batch(rt);
        }
    } else if (notif.notify_completed >= notif.notify_sent) {
        // All notifications complete - send response to client
        // std::cerr << "[MuLeader debug] All notifications done for object " << notif.object_id
        //           << ": sent=" << notif.notify_sent << " completed=" << notif.notify_completed
        //           << " total_watchers=" << notif.total_watchers << std::endl;
        MuResponse resp{};
        resp.op = static_cast<uint8_t>(MuRpcOp::WatchNotify);
        resp.status = static_cast<uint8_t>(MuRpcStatus::Ok);
        resp.client_id = notif.client_id;
        resp.lock_id = notif.object_id;
        resp.req_id = notif.req_id;
        resp.granted_slot = static_cast<uint32_t>(notif.total_watchers);
        send_response(rt, resp);
        rt.active_notification.reset();

        // Process next notification from queue
        process_next_notification(rt);
    }
}

// Consume one client RPC, validate basic shape, and enqueue lock-specific work.
void handle_recv_cqe(MuLeaderRuntime& rt, const ibv_wc& comp) {
    const uint16_t client_id = recv_client_id(comp.wr_id);
    const uint16_t recv_slot = recv_slot_index(comp.wr_id);
    const MuRequest req = rt.recv_buffers[static_cast<size_t>(client_id) * MU_SERVER_RECV_RING + recv_slot];
    post_recv(rt, client_id, recv_slot);

    // Disable debug logging for performance
    // static uint64_t debug_req_count = 0;
    // static uint64_t debug_watch_reg_count = 0;
    // static uint64_t debug_watch_notify_count = 0;

    if (req.client_id != client_id) {
        MuResponse resp{};
        resp.op = req.op;
        resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
        resp.client_id = client_id;
        resp.lock_id = req.lock_id;
        resp.req_id = req.req_id;
        resp.granted_slot = req.granted_slot;
        send_response(rt, resp);
        return;
    }

    if (req.lock_id < rt.lock_start || req.lock_id >= rt.lock_end) {
        MuResponse resp{};
        resp.op = req.op;
        resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
        resp.client_id = req.client_id;
        resp.lock_id = req.lock_id;
        resp.req_id = req.req_id;
        resp.granted_slot = req.granted_slot;
        send_response(rt, resp);
        return;
    }

    auto& lock = rt.locks[req.lock_id];
    if (req.op == static_cast<uint8_t>(MuRpcOp::Lock)) {
        if (lock.pending_locks.size() >= MU_MAX_PENDING_PER_LOCK) {
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Lock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::QueueFull);
            resp.client_id = req.client_id;
            resp.lock_id = req.lock_id;
            resp.req_id = req.req_id;
            send_response(rt, resp);
            return;
        }

        lock.pending_locks.push_back(req);
        enqueue_ready(rt, req.lock_id);
        return;
    }

    if (req.op == static_cast<uint8_t>(MuRpcOp::Unlock)) {
        if (lock.pending_unlock.has_value()) {
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::Unlock);
            resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
            resp.client_id = req.client_id;
            resp.lock_id = req.lock_id;
            resp.req_id = req.req_id;
            resp.granted_slot = req.granted_slot;
            send_response(rt, resp);
            return;
        }

        lock.pending_unlock = req;
        enqueue_ready(rt, req.lock_id);
        return;
    }

    if (req.op == static_cast<uint8_t>(MuRpcOp::WatchRegister)) {
        // Queue watch registration request (like Lock requests - enables batching!)
        const uint32_t object_id = req.lock_id;
        if (object_id >= MAX_LOCKS) {
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::WatchRegister);
            resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
            resp.client_id = req.client_id;
            resp.lock_id = object_id;
            resp.req_id = req.req_id;
            send_response(rt, resp);
            return;
        }

        auto& watch = rt.watch_objects[object_id];
        if (watch.pending_registers.size() >= MU_MAX_PENDING_PER_LOCK) {
            MuResponse resp{};
            resp.op = static_cast<uint8_t>(MuRpcOp::WatchRegister);
            resp.status = static_cast<uint8_t>(MuRpcStatus::QueueFull);
            resp.client_id = req.client_id;
            resp.lock_id = object_id;
            resp.req_id = req.req_id;
            send_response(rt, resp);
            return;
        }
        watch.pending_registers.push_back(req);
        enqueue_ready_watch(rt, object_id);
        return;
    }

    if (req.op == static_cast<uint8_t>(MuRpcOp::WatchNotify)) {
        // Queue notification request (like Lock requests)
        if (rt.active_notification.has_value()) {
            // Notification already in progress - queue for later processing
            rt.pending_notifications.push_back(req);
            return;
        }

        // Start notification immediately if no active notification
        start_notification(rt, req);
        post_notify_batch(rt);
        return;
    }

    MuResponse resp{};
    resp.op = req.op;
    resp.status = static_cast<uint8_t>(MuRpcStatus::InternalError);
    resp.client_id = req.client_id;
    resp.lock_id = req.lock_id;
    resp.req_id = req.req_id;
    resp.granted_slot = req.granted_slot;
    send_response(rt, resp);
}

// Consume one follower replication completion and commit the mutation once
// quorum is reached.
void handle_repl_cqe(MuLeaderRuntime& rt, const ibv_wc& comp) {
    const uint32_t mutation_id = repl_mutation_id(comp.wr_id);
    if (mutation_id >= rt.mutations.size()) {
        throw std::runtime_error("MuLeader: mutation id out of range");
    }

    auto& ctx = rt.mutations[mutation_id];
    if (!ctx.in_use || ctx.generation != repl_generation(comp.wr_id)) {
        // static uint64_t stale_count = 0;
        // if (stale_count < 10) {
        //     std::cerr << "[MuLeader] handle_repl_cqe: stale completion mutation_id=" << mutation_id
        //               << " in_use=" << ctx.in_use << " gen_match=" << (ctx.generation == repl_generation(comp.wr_id)) << std::endl;
        //     std::cerr.flush();
        // }
        // stale_count++;
        return;
    }

    if (ctx.pending_followers == 0) {
        // static uint64_t zero_pending_count = 0;
        // if (zero_pending_count < 10) {
        //     std::cerr << "[MuLeader] handle_repl_cqe: pending_followers already 0 for mutation_id=" << mutation_id << std::endl;
        //     std::cerr.flush();
        // }
        // zero_pending_count++;
        return;
    }

    ctx.pending_followers--;
    ctx.ack_count++;

    // static uint64_t ack_count = 0;
    // const bool debug_ack = (ack_count < 20) || (ack_count % 10000 == 0);
    // if (debug_ack) {
    //     std::cerr << "[MuLeader] handle_repl_cqe #" << ack_count
    //               << " mutation_id=" << mutation_id
    //               << " ack_count=" << ctx.ack_count
    //               << " pending=" << ctx.pending_followers
    //               << " quorum_done=" << ctx.quorum_done
    //               << " wr_id=0x" << std::hex << comp.wr_id << std::dec
    //               << " qp_num=" << comp.qp_num << std::endl;
    //     std::cerr.flush();
    // }
    // ack_count++;

    if (!ctx.quorum_done && ctx.ack_count >= QUORUM) {
        // static uint64_t quorum_reached_count = 0;
        // if (quorum_reached_count < 10) {
        //     std::cerr << "[MuLeader] handle_repl_cqe: QUORUM REACHED for mutation_id=" << mutation_id
        //               << " ack_count=" << ctx.ack_count << " QUORUM=" << QUORUM << std::endl;
        //     std::cerr.flush();
        // }
        // quorum_reached_count++;
        ctx.quorum_done = true;
        advance_commit_tail(rt);
    }

    maybe_release_mutation(rt, mutation_id);
}

} // namespace

// Main MU leader event loop: build runtime state, post receives, poll the CQ,
// and drain the ready-lock queue.
void MuLeader::run() {
    // Size the global mutation pool from the number of locks this leader owns
    // and the maximum append concurrency we allow per lock.
    // Also account for watch registrations which share the same mutation pool.
    const uint32_t num_clients = expected_clients();
    const size_t handled_locks = static_cast<size_t>(lock_end_ - lock_start_);
    const size_t lock_pool = handled_locks * (MU_MAX_APPEND_INFLIGHT_PER_LOCK + 1);
    const size_t watch_pool = MAX_LOCKS * (MU_MAX_REGISTER_INFLIGHT_PER_OBJECT + 1);
    const size_t mutation_pool_size = std::max<size_t>(
        lock_pool + watch_pool,
        MU_MAX_APPEND_INFLIGHT_PER_LOCK + 1);

    if (mutation_pool_size > MU_REPL_ID_MASK) {
        throw std::runtime_error("MuLeader: mutation pool exceeds wr_id capacity");
    }

    std::cout << "[MuLeader " << node_id_ << "] locks ["
              << lock_start_ << ", " << lock_end_ << ")\n";
    std::cout.flush();

    // Build the explicit runtime object that all helper functions operate on.
    // This replaces the old lambda-captured state and makes the event loop easier
    // to follow and extend.
    MuLeaderRuntime rt{
        .node_id = node_id_,
        .lock_start = lock_start_,
        .lock_end = lock_end_,
        .debug_enabled = MU_DEBUG,
        .quorum_only_signal = MU_REPL_SIGNAL_QUORUM_ONLY,
        .local_buf = static_cast<uint8_t*>(buf_),
        .pd = pd_,
        .cq = cq_,
        .local_mr = mr_,
        .clients = clients_,
        .peers = peers_,
        .num_clients = num_clients,
        .handled_locks = handled_locks,
        .recv_buffers = std::vector<MuRequest>(num_clients * MU_SERVER_RECV_RING),
        .locks = std::vector<LockState>(MAX_LOCKS),
        .mutations = std::vector<MutationCtx>(mutation_pool_size),
        .ready_flags = std::vector<uint8_t>(MAX_LOCKS, 0),
        .client_send_signal_counts = std::vector<uint32_t>(num_clients, 0),
        .watch_objects = std::vector<WatchState>(MAX_LOCKS),
        .ready_watch_flags = std::vector<uint8_t>(MAX_LOCKS, 0),
    };

    // Seed the free mutation-context pool and discover which peer connections
    // are follower targets for replicated MU log writes.
    rt.free_mutations.resize(mutation_pool_size);
    for (uint32_t i = 0; i < mutation_pool_size; ++i) {
        rt.free_mutations[i] = i;
    }
    rt.slot_to_mutation.reserve(mutation_pool_size * 2);
    rt.follower_indices.reserve(rt.peers.size());
    for (size_t i = 0; i < rt.peers.size(); ++i) {
        if (i == rt.node_id || rt.peers[i].cm_id == nullptr) continue;
        rt.follower_indices.push_back(i);
    }

    // Register and pre-post the client receive ring so lock/unlock RPCs can
    // arrive immediately without additional setup in the hot path.
    rt.recv_mr = ibv_reg_mr(
        rt.pd,
        rt.recv_buffers.data(),
        rt.recv_buffers.size() * sizeof(MuRequest),
        IBV_ACCESS_LOCAL_WRITE);
    if (!rt.recv_mr) {
        throw std::runtime_error("MuLeader: failed to register recv buffers");
    }

    for (uint16_t client_id = 0; client_id < rt.num_clients; ++client_id) {
        for (uint16_t recv_slot = 0; recv_slot < MU_SERVER_RECV_RING; ++recv_slot) {
            post_recv(rt, client_id, recv_slot);
        }
    }

    // Main event loop:
    // 1. poll completions,
    // 2. route them to recv/replication handlers,
    // 3. drain the ready-lock queue to append more global-log mutations.
    ibv_wc wc[512];
    while (true) {
        const int n = ibv_poll_cq(rt.cq, 512, wc);
        if (n < 0) {
            throw std::runtime_error("MuLeader: CQ poll failed");
        }

        for (int i = 0; i < n; ++i) {
            const ibv_wc& comp = wc[i];
            if (comp.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(std::string("MuLeader: completion error ") + ibv_wc_status_str(comp.status));
            }

            // Recv completions are client RPCs entering the leader.
            if ((comp.opcode & IBV_WC_RECV) != 0) {
                if (is_recv_wr_id(comp.wr_id)) {
                    handle_recv_cqe(rt, comp);
                }
                continue;
            }

            // Response SEND completions do not change leader state.
            if (is_resp_send_wr_id(comp.wr_id)) {
                continue;
            }

            // Replication completions advance quorum and may commit/apply global-log mutations.
            if (is_repl_wr_id(comp.wr_id)) {
                handle_repl_cqe(rt, comp);
                continue;
            }

            // Notification write completions (batched like syndra)
            if ((comp.wr_id >> MU_WR_TAG_SHIFT) == MU_NOTIFY_WR_TAG) {
                handle_notify_cqe(rt);
                continue;
            }
        }

        // Once completions have updated state, drain every ready lock so the
        // leader keeps as many mutations in flight as capacity allows.
        while (!rt.ready_locks.empty()) {
            const uint32_t lock_id = rt.ready_locks.front();
            rt.ready_locks.pop_front();
            rt.ready_flags[lock_id] = 0;
            service_lock(rt, lock_id);
        }

        // Drain ready watch objects to keep watch registration pipeline full
        while (!rt.ready_watches.empty()) {
            const uint32_t object_id = rt.ready_watches.front();
            rt.ready_watches.pop_front();
            rt.ready_watch_flags[object_id] = 0;
            service_watch(rt, object_id);
        }
    }
}
