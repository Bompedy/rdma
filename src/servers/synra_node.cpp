#include "rdma/servers/synra_node.h"

#include <chrono>
#include <functional>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace {

constexpr uint64_t kCopyWrTag = 0xE100000000000000ULL;
constexpr uint64_t kRecoveryReadWrTag = 0xE200000000000000ULL;
constexpr uint64_t kLogLiveBit = 1ULL << 63;
constexpr uint64_t kLogSeqMask = (1ULL << 31) - 1;

enum class CoordinatorPhase {
    warmup,
    waiting_detection,
    waiting_for_client_quiesce,
    collecting_reports,
    reset_waiting_for_client_quiesce,
    round_gap,
    done,
};

struct FailoverSample {
    uint32_t round = 0;
    uint64_t total_failover_us = 0;
    uint64_t permission_switch_us = 0;
    uint64_t detection_us = 0;
    uint64_t quiesce_us = 0;
    uint64_t detection_delay_us = 0;
};

uint64_t* local_frontier_ptr(void* buf) {
    return reinterpret_cast<uint64_t*>(static_cast<uint8_t*>(buf) + recovery_frontier_control_offset());
}

uint64_t* local_turn_ptr(void* buf) {
    return reinterpret_cast<uint64_t*>(static_cast<uint8_t*>(buf) + recovery_frontier_turn_offset());
}

uint8_t* local_log_ptr(void* buf) {
    return static_cast<uint8_t*>(buf) + recovery_log_region_offset();
}

uint64_t read_local_log_slot(void* buf, const uint64_t slot) {
    return *reinterpret_cast<uint64_t*>(local_log_ptr(buf) + slot * ENTRY_SIZE);
}

void write_local_log_slot(void* buf, const uint64_t slot, const uint64_t value) {
    *reinterpret_cast<uint64_t*>(local_log_ptr(buf) + slot * ENTRY_SIZE) = value;
}

uint64_t cas_frontier_from_log_value(const uint64_t value) {
    if (value == EMPTY_SLOT) {
        return 0;
    }
    const uint64_t seq = (value >> 32) & kLogSeqMask;
    if ((value & kLogLiveBit) != 0) {
        return 2 * seq + 1;
    }
    if (seq >= CAS_LOG_CAPACITY) {
        return 2 * (seq - CAS_LOG_CAPACITY) + 2;
    }
    return 2 * seq + 2;
}

uint64_t ticket_frontier_from_log_value(const uint64_t value) {
    if (value == EMPTY_SLOT) {
        return 0;
    }
    uint64_t ticket = (value >> 32) & kLogSeqMask;
    if ((value & kLogLiveBit) == 0 && ticket >= TICKET_FAA_LOG_CAPACITY) {
        ticket -= TICKET_FAA_LOG_CAPACITY;
    }
    return ticket + 1;
}

uint64_t cas_used_physical_slots(const uint64_t frontier) {
    if (frontier == 0) {
        return 0;
    }
    return std::min<uint64_t>((frontier + 1) / 2, CAS_LOG_CAPACITY);
}

uint64_t ticket_used_physical_slots(const uint64_t frontier) {
    return std::min<uint64_t>(frontier, TICKET_FAA_LOG_CAPACITY);
}

uint64_t scan_local_cas_frontier(void* buf) {
    uint64_t frontier = 0;
    for (uint64_t slot = 0; slot < CAS_LOG_CAPACITY; ++slot) {
        frontier = std::max(frontier, cas_frontier_from_log_value(read_local_log_slot(buf, slot)));
    }
    return frontier;
}

uint64_t scan_local_ticket_frontier(void* buf) {
    uint64_t frontier = 0;
    for (uint64_t slot = 0; slot < TICKET_FAA_LOG_CAPACITY; ++slot) {
        frontier = std::max(frontier, ticket_frontier_from_log_value(read_local_log_slot(buf, slot)));
    }
    return frontier;
}

uint64_t scan_local_turn(void* buf) {
    return scan_local_ticket_frontier(buf);
}

uint64_t surviving_live_mask() {
    return recovery_live_mask_all_nodes() & ~(1ULL << RECOVERY_FAILED_NODE);
}

size_t received_report_count(const std::array<bool, MAX_REPLICAS>& received) {
    size_t count = 0;
    for (size_t i = 0; i < CLUSTER_NODES.size(); ++i) {
        if (received[i]) {
            count++;
        }
    }
    return count;
}

bool is_copy_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == kCopyWrTag;
}

bool is_recovery_read_wr_id(const uint64_t wr_id) {
    return (wr_id & 0xFFFF000000000000ULL) == kRecoveryReadWrTag;
}

} // namespace

void SynraNode::run() {
    constexpr uint32_t num_rounds = RECOVERY_NUM_ROUNDS;
    constexpr uint32_t initial_trigger_ms = RECOVERY_TRIGGER_MS;
    constexpr uint32_t detection_delay_ms = RECOVERY_DETECTION_DELAY_MS;
    constexpr uint32_t reset_quiesce_ms = RECOVERY_RESET_QUIESCE_MS;
    constexpr uint32_t round_gap_ms = RECOVERY_ROUND_GAP_MS;

    auto verbose_log = [&](const std::string& msg) {
        if constexpr (RECOVERY_VERBOSE_LOGS) {
            std::cout << msg << "\n";
        }
    };

    verbose_log(
        "[SynraNode " + std::to_string(node_id_) + "] Recovery prototype active for lock "
        + std::to_string(RECOVERY_TARGET_LOCK) + " rounds=" + std::to_string(num_rounds)
        + " detection_delay_ms=" + std::to_string(detection_delay_ms));

    const auto started_at = std::chrono::steady_clock::now();
    auto phase_started_at = started_at;
    auto failure_injected_at = started_at;
    auto recovery_notice_sent_at = started_at;
    auto permission_switch_started_at = started_at;
    std::vector<FailoverSample> samples;
    samples.reserve(num_rounds);

    CoordinatorPhase phase = node_id_ == RECOVERY_COORD_NODE ? CoordinatorPhase::warmup : CoordinatorPhase::done;
    uint32_t current_round = 0;
    bool summary_printed = false;
    bool experiment_done_broadcast = false;
    bool published_new_creds = false;
    bool sent_recovery_done = false;
    std::optional<std::chrono::steady_clock::time_point> exit_deadline;
    std::array<bool, TOTAL_CLIENTS> client_quiesced{};
    uint64_t next_read_wr_id = 1;
    std::function<void(const RecoveryControlMessage&, uint32_t)> handle_peer_control_message;
    uint64_t recovery_active_mask = 0;

    auto reset_report_state = [&]() {
        recovery_report_received_.fill(false);
        recovery_repair_received_.fill(false);
        recovery_cas_frontiers_.fill(0);
        recovery_ticket_frontiers_.fill(0);
        recovery_ticket_turns_.fill(0);
        for (auto& cred : recovery_log_creds_) {
            cred = {};
        }
    };

    auto reset_client_quiesced = [&]() {
        client_quiesced.fill(false);
    };

    auto all_clients_quiesced = [&]() {
        for (uint32_t i = 0; i < TOTAL_CLIENTS; ++i) {
            if (!client_quiesced[i]) {
                return false;
            }
        }
        return true;
    };

    auto all_live_replicas_repaired = [&](const uint64_t live_mask) {
        for (size_t node = 0; node < CLUSTER_NODES.size(); ++node) {
            if (!recovery_live_mask_contains(live_mask, static_cast<uint32_t>(node))) {
                continue;
            }
            if (!recovery_repair_received_[node]) {
                return false;
            }
        }
        return true;
    };

    auto reported_replica_mask = [&]() {
        uint64_t mask = 0;
        for (size_t node = 0; node < CLUSTER_NODES.size(); ++node) {
            if (recovery_report_received_[node]) {
                mask |= (1ULL << node);
            }
        }
        mask &= ~(1ULL << RECOVERY_FAILED_NODE);
        return mask;
    };

    auto process_control_message = [&](const RecoveryControlMessage& msg, const bool from_client, const uint32_t sender_id) {
        if (from_client) {
            if (msg.type == RecoveryMsgType::client_quiesced
                && sender_id < TOTAL_CLIENTS
                && msg.epoch == recovery_epoch_) {
                client_quiesced[sender_id] = true;
                verbose_log(
                    "[SynraNode " + std::to_string(node_id_) + "] Client "
                    + std::to_string(sender_id) + " quiesced for epoch " + std::to_string(msg.epoch));
            }
            return;
        }
        handle_peer_control_message(msg, sender_id);
    };

    auto read_remote_log_slot = [&](const uint32_t node_id, const uint64_t slot) {
        if (node_id >= peers_.size() || peers_[node_id].cm_id == nullptr) {
            throw std::runtime_error("SynraNode: invalid peer for recovery read");
        }
        const RecoveryRegionCred remote_log = recovery_log_creds_[node_id].addr != 0
            ? recovery_log_creds_[node_id]
            : peers_[node_id].prototype_log;
        if (remote_log.addr == 0 || remote_log.rkey == 0) {
            throw std::runtime_error("SynraNode: missing remote recovery log credentials");
        }
        auto* scratch = reinterpret_cast<uint64_t*>(static_cast<uint8_t*>(buf_) + recovery_metadata_offset());
        *scratch = EMPTY_SLOT;

        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(scratch);
        sge.length = sizeof(uint64_t);
        sge.lkey = mr_->lkey;

        const uint64_t wr_id = kRecoveryReadWrTag | (next_read_wr_id++ & 0x0000FFFFFFFFFFFFULL);
        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = wr_id;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = remote_log.addr + slot * ENTRY_SIZE;
        wr.wr.rdma.rkey = remote_log.rkey;
        if (ibv_post_send(peers_[node_id].cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("SynraNode: failed to post recovery read");
        }

        ibv_wc wc[16];
        while (true) {
            const int polled = ibv_poll_cq(cq_, 16, wc);
            if (polled < 0) {
                throw std::runtime_error("SynraNode: poll failed while waiting for recovery read");
            }
            if (polled == 0) {
                continue;
            }
            for (int i = 0; i < polled; ++i) {
                const ibv_wc& comp = wc[i];
                if ((comp.opcode & IBV_WC_RECV) != 0) {
                    RecoveryControlMessage msg{};
                    bool from_client = false;
                    uint32_t sender_id = 0;
                    if (poll_control_completion(comp, msg, from_client, sender_id)) {
                        process_control_message(msg, from_client, sender_id);
                    }
                    continue;
                }
                if (comp.status != IBV_WC_SUCCESS) {
                    throw std::runtime_error("SynraNode: recovery read completion failed");
                }
                if (is_recovery_read_wr_id(comp.wr_id) && comp.wr_id == wr_id) {
                    return *scratch;
                }
            }
        }
    };

    auto repair_local_log_from_quorum = [&](const uint64_t live_mask, const uint64_t cas_frontier, const uint64_t ticket_frontier) {
        const uint64_t limit = std::min<uint64_t>(
            MAX_LOG_PER_LOCK,
            std::max(cas_used_physical_slots(cas_frontier), ticket_used_physical_slots(ticket_frontier)));
        for (uint64_t slot = 0; slot < limit; ++slot) {
            if (read_local_log_slot(buf_, slot) != EMPTY_SLOT) {
                continue;
            }
            std::unordered_map<uint64_t, size_t> value_counts;
            uint64_t chosen_value = EMPTY_SLOT;
            size_t chosen_count = 0;
            for (size_t node = 0; node < CLUSTER_NODES.size(); ++node) {
                if (static_cast<uint32_t>(node) == RECOVERY_FAILED_NODE
                    || !recovery_live_mask_contains(live_mask, static_cast<uint32_t>(node))
                    || !recovery_report_received_[node]) {
                    continue;
                }
                const uint64_t candidate = (static_cast<uint32_t>(node) == node_id_)
                    ? read_local_log_slot(buf_, slot)
                    : read_remote_log_slot(static_cast<uint32_t>(node), slot);
                if (candidate == EMPTY_SLOT) {
                    continue;
                }
                const size_t count = ++value_counts[candidate];
                if (count > chosen_count) {
                    chosen_count = count;
                    chosen_value = candidate;
                }
            }
            if (chosen_value != EMPTY_SLOT && chosen_count >= QUORUM) {
                write_local_log_slot(buf_, slot, chosen_value);
            }
        }
    };

    auto send_repaired_to_coordinator = [&]() {
        if (node_id_ == RECOVERY_COORD_NODE) {
            return;
        }
        RecoveryControlMessage repaired{};
        repaired.type = RecoveryMsgType::replica_repaired;
        repaired.epoch = recovery_epoch_;
        repaired.lock_id = RECOVERY_TARGET_LOCK;
        repaired.from_node = node_id_;
        repaired.failed_node = RECOVERY_FAILED_NODE;
        repaired.replacement_node = RECOVERY_REPLACEMENT_NODE;
        repaired.frontier_host = RECOVERY_REPLACEMENT_NODE;
        repaired.live_mask = surviving_live_mask();
        repaired.log_creds[node_id_] = server_creds_.prototype_log;
        verbose_log(
            "[SynraNode " + std::to_string(node_id_) + "] Sending replica_repaired epoch="
            + std::to_string(repaired.epoch));
        send_control_message(peers_[RECOVERY_COORD_NODE].cm_id, repaired);
    };

    auto install_local_report = [&]() {
        recovery_report_received_[node_id_] = true;
        recovery_cas_frontiers_[node_id_] = scan_local_cas_frontier(buf_);
        recovery_ticket_frontiers_[node_id_] = scan_local_ticket_frontier(buf_);
        recovery_ticket_turns_[node_id_] = scan_local_turn(buf_);
        recovery_log_creds_[node_id_] = server_creds_.prototype_log;
    };

    auto baseline_log_creds = [&]() {
        std::array<RecoveryRegionCred, MAX_REPLICAS> creds{};
        for (size_t i = 0; i < CLUSTER_NODES.size(); ++i) {
            if (static_cast<uint32_t>(i) == node_id_) {
                creds[i] = server_creds_.prototype_log;
            } else if (static_cast<uint32_t>(i) == RECOVERY_FAILED_NODE) {
                creds[i] = peers_[i].prototype_log;
            } else if (recovery_log_creds_[i].addr != 0) {
                creds[i] = recovery_log_creds_[i];
            } else {
                creds[i] = peers_[i].prototype_log;
            }
        }
        return creds;
    };

    auto send_report_to_coordinator = [&]() {
        if (node_id_ == RECOVERY_COORD_NODE) {
            return;
        }
        RecoveryControlMessage report{};
        report.type = RecoveryMsgType::replica_report;
        report.epoch = recovery_epoch_;
        report.lock_id = RECOVERY_TARGET_LOCK;
        report.from_node = node_id_;
        report.failed_node = RECOVERY_FAILED_NODE;
        report.replacement_node = RECOVERY_REPLACEMENT_NODE;
        report.frontier_host = RECOVERY_REPLACEMENT_NODE;
        report.live_mask = surviving_live_mask();
        report.cas_frontier = scan_local_cas_frontier(buf_);
        report.ticket_frontier = scan_local_ticket_frontier(buf_);
        report.ticket_turn = scan_local_turn(buf_);
        report.log_creds[node_id_] = server_creds_.prototype_log;
        send_control_message(peers_[RECOVERY_COORD_NODE].cm_id, report);
    };

    auto broadcast_reset_start = [&]() {
        recovery_triggered_ = true;
        recovery_epoch_++;
        reset_client_quiesced();
        verbose_log(
            "[SynraNode " + std::to_string(node_id_) + "] Starting baseline reset epoch="
            + std::to_string(recovery_epoch_));
        RecoveryControlMessage reset{};
        reset.type = RecoveryMsgType::baseline_reset_start;
        reset.epoch = recovery_epoch_;
        reset.lock_id = RECOVERY_TARGET_LOCK;
        reset.from_node = node_id_;
        reset.failed_node = RECOVERY_REPLACEMENT_NODE;
        reset.replacement_node = RECOVERY_FAILED_NODE;
        reset.frontier_host = RECOVERY_FAILED_NODE;
        reset.live_mask = recovery_live_mask_all_nodes();
        broadcast_control_message(reset, false);
    };

    auto wait_for_copy_completions = [&](const size_t expected) {
        size_t done = 0;
        ibv_wc wcs[16];
        while (done < expected) {
            const int polled = ibv_poll_cq(cq_, 16, wcs);
            if (polled < 0) {
                throw std::runtime_error("SynraNode: poll failed while waiting for copy completions");
            }
            if (polled == 0) {
                continue;
            }
            for (int i = 0; i < polled; ++i) {
                const ibv_wc& comp = wcs[i];
                if ((comp.opcode & IBV_WC_RECV) != 0) {
                    RecoveryControlMessage msg{};
                    bool from_client = false;
                    uint32_t sender_id = 0;
                    if (poll_control_completion(comp, msg, from_client, sender_id)) {
                        continue;
                    }
                }
                if (comp.status != IBV_WC_SUCCESS) {
                    throw std::runtime_error("SynraNode: copy completion failed");
                }
                if (is_copy_wr_id(comp.wr_id)) {
                    done++;
                }
            }
        }
    };

    auto post_copy_write = [&](const uint64_t wr_index, void* local_addr, const size_t length, const RecoveryRegionCred& remote) {
        if (remote.addr == 0 || remote.rkey == 0) {
            throw std::runtime_error("SynraNode: missing remote baseline recovery credential");
        }
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(local_addr);
        sge.length = static_cast<uint32_t>(length);
        sge.lkey = mr_->lkey;

        ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id = kCopyWrTag | wr_index;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.send_flags = IBV_SEND_SIGNALED;
        if (length <= MAX_INLINE_DEPTH) {
            wr.send_flags |= IBV_SEND_INLINE;
        }
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.wr.rdma.remote_addr = remote.addr;
        wr.wr.rdma.rkey = remote.rkey;
        if (ibv_post_send(peers_[RECOVERY_FAILED_NODE].cm_id->qp, &wr, &bad_wr)) {
            throw std::runtime_error("SynraNode: failed to post baseline copy write");
        }
    };

    auto reset_to_baseline = [&]() {
        if (node_id_ != RECOVERY_COORD_NODE) {
            return;
        }

        post_copy_write(1, local_frontier_ptr(buf_), sizeof(uint64_t), peers_[RECOVERY_FAILED_NODE].prototype_frontier);
        post_copy_write(2, local_turn_ptr(buf_), sizeof(uint64_t), peers_[RECOVERY_FAILED_NODE].prototype_turn);
        post_copy_write(3, local_log_ptr(buf_), RECOVERY_LOG_REGION_SIZE, peers_[RECOVERY_FAILED_NODE].prototype_log);
        wait_for_copy_completions(3);

        RecoveryControlMessage creds{};
        creds.type = RecoveryMsgType::new_creds;
        creds.epoch = recovery_epoch_;
        creds.lock_id = RECOVERY_TARGET_LOCK;
        creds.from_node = node_id_;
        creds.failed_node = RECOVERY_REPLACEMENT_NODE;
        creds.replacement_node = RECOVERY_FAILED_NODE;
        creds.frontier_host = RECOVERY_FAILED_NODE;
        creds.live_mask = recovery_live_mask_all_nodes();
        creds.cas_frontier = *local_frontier_ptr(buf_);
        creds.ticket_frontier = *local_frontier_ptr(buf_);
        creds.ticket_turn = *local_turn_ptr(buf_);
        creds.frontier_cred = peers_[RECOVERY_FAILED_NODE].prototype_frontier;
        creds.turn_cred = peers_[RECOVERY_FAILED_NODE].prototype_turn;
        creds.log_creds = baseline_log_creds();
        broadcast_control_message(creds, false);

        RecoveryControlMessage done = creds;
        done.type = RecoveryMsgType::recovery_done;
        broadcast_control_message(done, false);

        recovery_triggered_ = false;
        published_new_creds = false;
        sent_recovery_done = false;
    };

    auto begin_failover_round = [&]() {
        if (node_id_ != RECOVERY_COORD_NODE || recovery_triggered_) {
            return;
        }
        recovery_triggered_ = true;
        recovery_epoch_++;
        published_new_creds = false;
        sent_recovery_done = false;
        recovery_active_mask = 0;
        reset_report_state();
        reset_client_quiesced();
        verbose_log(
            "[SynraNode " + std::to_string(node_id_) + "] Starting failover round "
            + std::to_string(current_round + 1) + " epoch=" + std::to_string(recovery_epoch_));

        RecoveryControlMessage start{};
        start.type = RecoveryMsgType::recovery_start;
        start.epoch = recovery_epoch_;
        start.lock_id = RECOVERY_TARGET_LOCK;
        start.from_node = node_id_;
        start.failed_node = RECOVERY_FAILED_NODE;
        start.replacement_node = RECOVERY_REPLACEMENT_NODE;
        start.frontier_host = RECOVERY_REPLACEMENT_NODE;
        start.live_mask = surviving_live_mask();
        broadcast_control_message(start, false);
    };

    auto begin_permission_switch = [&]() {
        if (node_id_ != RECOVERY_COORD_NODE) {
            return;
        }
        verbose_log(
            "[SynraNode " + std::to_string(node_id_) + "] All clients quiesced for epoch "
            + std::to_string(recovery_epoch_) + ", switching permissions");
        reregister_recovery_log_readonly();
        install_local_report();
        reregister_recovery_frontiers_writable();
        recovery_log_creds_[node_id_] = server_creds_.prototype_log;

        RecoveryControlMessage switch_msg{};
        switch_msg.type = RecoveryMsgType::recovery_switch;
        switch_msg.epoch = recovery_epoch_;
        switch_msg.lock_id = RECOVERY_TARGET_LOCK;
        switch_msg.from_node = node_id_;
        switch_msg.failed_node = RECOVERY_FAILED_NODE;
        switch_msg.replacement_node = RECOVERY_REPLACEMENT_NODE;
        switch_msg.frontier_host = RECOVERY_REPLACEMENT_NODE;
        switch_msg.live_mask = surviving_live_mask();
        broadcast_control_message(switch_msg, false);
    };

    auto install_recovered_frontiers = [&](const uint64_t cas_frontier, const uint64_t ticket_frontier, const uint64_t turn) {
        *local_frontier_ptr(buf_) = cas_frontier;
        *local_turn_ptr(buf_) = turn;
        *reinterpret_cast<uint64_t*>(static_cast<uint8_t*>(buf_) + lock_control_offset(RECOVERY_TARGET_LOCK)) = cas_frontier;
        *reinterpret_cast<uint64_t*>(static_cast<uint8_t*>(buf_) + lock_turn_offset(RECOVERY_TARGET_LOCK)) = turn;
        (void)ticket_frontier;
    };

    auto maybe_publish_failover_creds = [&]() {
        if (node_id_ != RECOVERY_COORD_NODE || !recovery_triggered_ || published_new_creds) {
            return false;
        }
        if (received_report_count(recovery_report_received_) < QUORUM) {
            return false;
        }

        recovery_active_mask = reported_replica_mask();

        std::vector<uint64_t> cas_values;
        std::vector<uint64_t> ticket_values;
        std::vector<uint64_t> turn_values;
        for (size_t i = 0; i < CLUSTER_NODES.size(); ++i) {
            if (!recovery_live_mask_contains(recovery_active_mask, static_cast<uint32_t>(i))) {
                continue;
            }
            cas_values.push_back(recovery_cas_frontiers_[i]);
            ticket_values.push_back(recovery_ticket_frontiers_[i]);
            turn_values.push_back(recovery_ticket_turns_[i]);
        }

        const uint64_t recovered_cas_frontier = quorum_median(cas_values);
        const uint64_t recovered_ticket_frontier = quorum_median(ticket_values);
        const uint64_t recovered_turn = quorum_median(turn_values);
        install_recovered_frontiers(recovered_cas_frontier, recovered_ticket_frontier, recovered_turn);
        repair_local_log_from_quorum(recovery_active_mask, recovered_cas_frontier, recovered_ticket_frontier);
        reregister_recovery_log_writable();
        recovery_log_creds_[node_id_] = server_creds_.prototype_log;
        recovery_repair_received_[node_id_] = true;

        RecoveryControlMessage creds{};
        creds.type = RecoveryMsgType::new_creds;
        creds.epoch = recovery_epoch_;
        creds.lock_id = RECOVERY_TARGET_LOCK;
        creds.from_node = node_id_;
        creds.failed_node = RECOVERY_FAILED_NODE;
        creds.replacement_node = RECOVERY_REPLACEMENT_NODE;
        creds.frontier_host = RECOVERY_REPLACEMENT_NODE;
        creds.live_mask = recovery_active_mask;
        creds.cas_frontier = recovered_cas_frontier;
        creds.ticket_frontier = recovered_ticket_frontier;
        creds.ticket_turn = recovered_turn;
        creds.frontier_cred = server_creds_.prototype_frontier;
        creds.turn_cred = server_creds_.prototype_turn;
        creds.log_creds = recovery_log_creds_;
        broadcast_control_message(creds, false);
        published_new_creds = true;
        return true;
    };

    auto finish_failover_round = [&]() {
        if (node_id_ != RECOVERY_COORD_NODE || !published_new_creds || sent_recovery_done) {
            return false;
        }
        if (!all_live_replicas_repaired(recovery_active_mask)) {
            return false;
        }
        RecoveryControlMessage done{};
        done.type = RecoveryMsgType::recovery_done;
        done.epoch = recovery_epoch_;
        done.lock_id = RECOVERY_TARGET_LOCK;
        done.from_node = node_id_;
        done.failed_node = RECOVERY_FAILED_NODE;
        done.replacement_node = RECOVERY_REPLACEMENT_NODE;
        done.frontier_host = RECOVERY_REPLACEMENT_NODE;
        done.live_mask = recovery_active_mask;
        done.frontier_cred = server_creds_.prototype_frontier;
        done.turn_cred = server_creds_.prototype_turn;
        done.log_creds = recovery_log_creds_;
        broadcast_control_message(done, false);

        sent_recovery_done = true;
        recovery_triggered_ = false;

        const auto recovery_done_at = std::chrono::steady_clock::now();
        FailoverSample sample{};
        sample.round = current_round + 1;
        sample.total_failover_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            recovery_done_at - failure_injected_at).count());
        sample.permission_switch_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            recovery_done_at - permission_switch_started_at).count());
        sample.detection_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            permission_switch_started_at - failure_injected_at).count());
        sample.quiesce_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            permission_switch_started_at - recovery_notice_sent_at).count());
        sample.detection_delay_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            recovery_notice_sent_at - failure_injected_at).count());
        samples.push_back(sample);

        return true;
    };

    handle_peer_control_message = [&](const RecoveryControlMessage& msg, const uint32_t sender_id) {
        switch (msg.type) {
        case RecoveryMsgType::recovery_start:
            if (node_id_ == RECOVERY_COORD_NODE) {
                break;
            }
            recovery_triggered_ = true;
            recovery_epoch_ = std::max(recovery_epoch_, msg.epoch);
            break;
        case RecoveryMsgType::recovery_switch:
            if (node_id_ == RECOVERY_COORD_NODE) {
                break;
            }
            verbose_log(
                "[SynraNode " + std::to_string(node_id_) + "] Received recovery_switch epoch="
                + std::to_string(msg.epoch));
            reregister_recovery_log_readonly();
            install_local_report();
            reregister_recovery_frontiers_writable();
            recovery_log_creds_[node_id_] = server_creds_.prototype_log;
            send_report_to_coordinator();
            break;
        case RecoveryMsgType::replica_report:
            if (node_id_ == RECOVERY_COORD_NODE && sender_id < MAX_REPLICAS) {
                recovery_report_received_[sender_id] = true;
                recovery_cas_frontiers_[sender_id] = msg.cas_frontier;
                recovery_ticket_frontiers_[sender_id] = msg.ticket_frontier;
                recovery_ticket_turns_[sender_id] = msg.ticket_turn;
                recovery_log_creds_[sender_id] = msg.log_creds[sender_id];
                verbose_log(
                    "[SynraNode " + std::to_string(node_id_) + "] Received replica_report from node "
                    + std::to_string(sender_id) + " epoch=" + std::to_string(msg.epoch));
            }
            break;
        case RecoveryMsgType::replica_repaired:
            if (node_id_ == RECOVERY_COORD_NODE && sender_id < MAX_REPLICAS) {
                recovery_repair_received_[sender_id] = true;
                recovery_log_creds_[sender_id] = msg.log_creds[sender_id];
                verbose_log(
                    "[SynraNode " + std::to_string(node_id_) + "] Received replica_repaired from node "
                    + std::to_string(sender_id) + " epoch=" + std::to_string(msg.epoch));
            }
            break;
        case RecoveryMsgType::baseline_reset_start:
            recovery_triggered_ = true;
            recovery_epoch_ = std::max(recovery_epoch_, msg.epoch);
            break;
        case RecoveryMsgType::new_creds:
            recovery_epoch_ = std::max(recovery_epoch_, msg.epoch);
            recovery_log_creds_ = msg.log_creds;
            verbose_log(
                "[SynraNode " + std::to_string(node_id_) + "] Received new_creds epoch="
                + std::to_string(msg.epoch));
            install_recovered_frontiers(msg.cas_frontier, msg.ticket_frontier, msg.ticket_turn);
            repair_local_log_from_quorum(msg.live_mask, msg.cas_frontier, msg.ticket_frontier);
            reregister_recovery_log_writable();
            recovery_log_creds_[node_id_] = server_creds_.prototype_log;
            recovery_repair_received_[node_id_] = true;
            send_repaired_to_coordinator();
            break;
        case RecoveryMsgType::recovery_done:
            recovery_epoch_ = std::max(recovery_epoch_, msg.epoch);
            recovery_triggered_ = false;
            published_new_creds = false;
            sent_recovery_done = false;
            break;
        case RecoveryMsgType::experiment_done:
            exit_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(RECOVERY_EXPERIMENT_DONE_GRACE_MS);
            break;
        case RecoveryMsgType::client_quiesced:
        case RecoveryMsgType::go:
        case RecoveryMsgType::invalid:
            break;
        }
    };

    ibv_wc wc[64];
    while (true) {
        if (node_id_ == RECOVERY_COORD_NODE) {
            const auto now = std::chrono::steady_clock::now();
            switch (phase) {
            case CoordinatorPhase::warmup:
                if (std::chrono::duration_cast<std::chrono::milliseconds>(now - started_at).count() >= initial_trigger_ms) {
                    failure_injected_at = now;
                    phase = CoordinatorPhase::waiting_detection;
                    phase_started_at = now;
                }
                break;
            case CoordinatorPhase::waiting_detection:
                if (std::chrono::duration_cast<std::chrono::milliseconds>(now - failure_injected_at).count() >= detection_delay_ms) {
                    recovery_notice_sent_at = now;
                    begin_failover_round();
                    phase = CoordinatorPhase::waiting_for_client_quiesce;
                    phase_started_at = now;
                }
                break;
            case CoordinatorPhase::waiting_for_client_quiesce:
                if (all_clients_quiesced()) {
                    permission_switch_started_at = now;
                    begin_permission_switch();
                    phase = CoordinatorPhase::collecting_reports;
                    phase_started_at = std::chrono::steady_clock::now();
                }
                break;
            case CoordinatorPhase::collecting_reports:
                if (!published_new_creds) {
                    maybe_publish_failover_creds();
                }
                if (finish_failover_round()) {
                    broadcast_reset_start();
                    phase = CoordinatorPhase::reset_waiting_for_client_quiesce;
                    phase_started_at = std::chrono::steady_clock::now();
                }
                break;
            case CoordinatorPhase::reset_waiting_for_client_quiesce:
                if (all_clients_quiesced()
                    && std::chrono::duration_cast<std::chrono::milliseconds>(now - phase_started_at).count() >= reset_quiesce_ms) {
                    reset_to_baseline();
                    current_round++;
                    phase = current_round >= num_rounds ? CoordinatorPhase::done : CoordinatorPhase::round_gap;
                    phase_started_at = std::chrono::steady_clock::now();
                }
                break;
            case CoordinatorPhase::round_gap:
                if (std::chrono::duration_cast<std::chrono::milliseconds>(now - phase_started_at).count() >= round_gap_ms) {
                    failure_injected_at = now;
                    phase = CoordinatorPhase::waiting_detection;
                    phase_started_at = now;
                }
                break;
            case CoordinatorPhase::done:
                if (!experiment_done_broadcast) {
                    RecoveryControlMessage done{};
                    done.type = RecoveryMsgType::experiment_done;
                    done.epoch = recovery_epoch_;
                    done.lock_id = RECOVERY_TARGET_LOCK;
                    done.from_node = node_id_;
                    done.failed_node = RECOVERY_FAILED_NODE;
                    done.replacement_node = RECOVERY_REPLACEMENT_NODE;
                    done.frontier_host = RECOVERY_FAILED_NODE;
                    done.live_mask = recovery_live_mask_all_nodes();
                    broadcast_control_message(done, true);
                    experiment_done_broadcast = true;
                    exit_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(RECOVERY_EXPERIMENT_DONE_GRACE_MS);
                }
                if (!summary_printed) {
                    std::cout << "RECOVERY_HDR: round,total_failover_us,permission_switch_us,detection_us,quiesce_us,detection_delay_us\n";
                    for (const auto& sample : samples) {
                        std::cout << "RECOVERY_CSV: "
                                  << sample.round << ","
                                  << sample.total_failover_us << ","
                                  << sample.permission_switch_us << ","
                                  << sample.detection_us << ","
                                  << sample.quiesce_us << ","
                                  << sample.detection_delay_us << "\n";
                    }
                    std::cout << "[SynraNode " << node_id_ << "] Completed " << samples.size()
                              << " measured failover rounds\n";
                    summary_printed = true;
                }
                break;
            }
        }

        if (exit_deadline.has_value()) {
            if (std::chrono::steady_clock::now() >= *exit_deadline) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        const int n = ibv_poll_cq(cq_, 64, wc);
        if (n < 0) {
            throw std::runtime_error("SynraNode: ibv_poll_cq failed");
        }

        for (int i = 0; i < n; ++i) {
            const ibv_wc& comp = wc[i];
            if ((comp.opcode & IBV_WC_RECV) != 0) {
                if (comp.status == IBV_WC_WR_FLUSH_ERR && exit_deadline.has_value()) {
                    continue;
                }
                RecoveryControlMessage msg{};
                bool from_client = false;
                uint32_t sender_id = 0;
                if (!poll_control_completion(comp, msg, from_client, sender_id)) {
                    continue;
                }
                if (from_client) {
                    if (msg.type == RecoveryMsgType::client_quiesced
                        && sender_id < TOTAL_CLIENTS
                        && msg.epoch == recovery_epoch_) {
                        client_quiesced[sender_id] = true;
                        verbose_log(
                            "[SynraNode " + std::to_string(node_id_) + "] Client "
                            + std::to_string(sender_id) + " quiesced for epoch " + std::to_string(msg.epoch));
                    }
                } else {
                    handle_peer_control_message(msg, sender_id);
                }
                continue;
            }

            if (comp.status == IBV_WC_WR_FLUSH_ERR && exit_deadline.has_value()) {
                continue;
            }
            if (comp.status != IBV_WC_SUCCESS) {
                std::cerr << "[SynraNode] WC error: "
                          << ibv_wc_status_str(comp.status)
                          << " opcode: " << comp.opcode << "\n";
            }
        }
    }
}
