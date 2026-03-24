#pragma once

#include "rdma/common.h"

#include <cstdint>
#include <vector>
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

    void signal_clients_ready();

    uint32_t node_id_;

    rdma_event_channel* ec_ = nullptr;
    rdma_cm_id* listener_ = nullptr;
    ibv_pd* pd_ = nullptr;
    ibv_cq* cq_ = nullptr;
    void* buf_ = nullptr;
    ibv_mr* mr_ = nullptr;

    std::vector<RemoteConnection> peers_;
    std::vector<RemoteConnection> clients_;
    ConnPrivateData server_creds_{};

private:
    RemoteConnection connect_to_node(const std::string& ip, uint16_t port);
    bool try_process_incoming_request(
        size_t num_nodes,
        uint32_t num_clients,
        uint32_t& higher_connected,
        uint32_t& clients_connected);
};