#pragma once

#include "rdma/common.h"

#include <cstdint>
#include <vector>
#include <array>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

class Server {
public:
    explicit Server(uint32_t node_id);
    virtual ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void start(uint16_t port);

    [[nodiscard]] uint32_t id() const { return node_id_; }

protected:
    [[nodiscard]] virtual uint32_t expected_clients() const = 0;
    virtual void run() = 0;

    void init_recovery_regions();
    void register_recovery_regions();
    void reregister_recovery_frontiers_writable();
    void reregister_recovery_log_readonly();
    void reregister_recovery_log_writable();
    void reregister_recovery_regions_writable();
    void post_control_recvs();
    void send_control_message(rdma_cm_id* cm_id, const RecoveryControlMessage& msg, uint64_t wr_id = 0);
    void broadcast_control_message(const RecoveryControlMessage& msg, bool include_failed_peer = false);
    bool poll_control_completion(const ibv_wc& wc, RecoveryControlMessage& out_msg, bool& from_client, uint32_t& sender_id);
    void signal_clients_ready();

    uint32_t node_id_;

    rdma_event_channel* ec_ = nullptr;
    rdma_cm_id* listener_ = nullptr;
    ibv_pd* pd_ = nullptr;
    ibv_cq* cq_ = nullptr;
    void* buf_ = nullptr;
    ibv_mr* mr_ = nullptr;
    ibv_mr* control_mr_ = nullptr;
    ibv_mr* control_send_mr_ = nullptr;
    ibv_mr* recovery_frontier_mr_ = nullptr;
    ibv_mr* recovery_turn_mr_ = nullptr;
    ibv_mr* recovery_log_mr_ = nullptr;

    std::vector<RemoteConnection> peers_;
    std::vector<RemoteConnection> clients_;
    std::vector<RecoveryControlMessage> control_recv_buffers_;
    std::vector<RecoveryControlMessage> control_send_buffers_;
    std::vector<uint16_t> control_send_slots_;
    ConnPrivateData server_creds_{};
    uint32_t recovery_epoch_ = 0;
    bool recovery_triggered_ = false;
    std::array<bool, MAX_REPLICAS> recovery_report_received_{};
    std::array<bool, MAX_REPLICAS> recovery_repair_received_{};
    std::array<uint64_t, MAX_REPLICAS> recovery_cas_frontiers_{};
    std::array<uint64_t, MAX_REPLICAS> recovery_ticket_frontiers_{};
    std::array<uint64_t, MAX_REPLICAS> recovery_ticket_turns_{};
    std::array<RecoveryRegionCred, MAX_REPLICAS> recovery_log_creds_{};

private:
    RemoteConnection connect_to_node(const std::string& ip, uint16_t port);
};
