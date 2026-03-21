#pragma once

#include <cstdint>
#include <cstddef>
#include <optional>
#include <vector>
#include <string>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include "common.h"

struct RemoteNode;

class Client {
public:
    explicit Client(uint32_t id, size_t buffer_size = CLIENT_ALIGNED_SIZE);
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    void connect(const std::vector<std::string>& node_ips, uint16_t port);

    [[nodiscard]] uint32_t id() const { return id_; }
    [[nodiscard]] ibv_cq* cq() const { return cq_; }
    [[nodiscard]] ibv_mr* mr() const { return mr_; }
    [[nodiscard]] void* buffer() const { return buf_; }
    [[nodiscard]] size_t buffer_size() const { return buffer_size_; }
    [[nodiscard]] const std::vector<RemoteNode>& connections() const { return connections_; }
    [[nodiscard]] const RecoveryRoute& recovery_route() const { return recovery_route_; }
    [[nodiscard]] uint32_t go_messages() const { return go_messages_; }
    [[nodiscard]] bool recovery_active() const { return recovery_route_.recovering; }

    bool handle_control_completion(const ibv_wc& wc);
    void mark_recovery_retry_pending();

private:
    void init_recovery_route();
    void post_control_recvs(size_t conn_index);
    void handle_control_message(const RecoveryControlMessage& msg, uint32_t remote_node);
    void apply_new_creds(const RecoveryControlMessage& msg);

    uint32_t id_;

    rdma_event_channel* ec_ = nullptr;
    ibv_pd* pd_ = nullptr;
    ibv_cq* cq_ = nullptr;
    ibv_mr* mr_ = nullptr;
    ibv_mr* control_mr_ = nullptr;
    void* buf_ = nullptr;
    size_t buffer_size_;
    std::vector<RemoteNode> connections_;
    std::vector<RecoveryControlMessage> control_recv_buffers_;
    RecoveryRoute recovery_route_{};
    uint32_t go_messages_ = 0;
    bool recovery_retry_pending_ = false;
};
