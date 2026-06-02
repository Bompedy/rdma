#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <infiniband/verbs.h>

#include "rdma/config.h"

class Transport {
public:
    Transport(uint32_t node_id,
              const std::vector<std::string>& all_ips,
              uint32_t num_threads,
              uint16_t tcp_port = 9400);
    ~Transport();

    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;

    uint32_t node_id() const { return node_id_; }
    uint32_t num_nodes() const { return num_nodes_; }
    uint32_t num_threads() const { return num_threads_; }
    void* buffer() const { return buf_; }
    uint64_t buffer_addr() const { return reinterpret_cast<uint64_t>(buf_); }

    int32_t faa(
        uint32_t thread, uint32_t remote_node,
        uint64_t local_addr, uint64_t remote_offset,
        uint64_t add_value, uint64_t wr_id,
        uint32_t flags = IBV_SEND_SIGNALED
    ) const;

    int32_t cas(
        uint32_t thread, uint32_t remote_node,
        uint64_t local_addr, uint64_t remote_offset,
        uint64_t compare, uint64_t swap, uint64_t wr_id,
        uint32_t flags = IBV_SEND_SIGNALED
    ) const;

    int32_t read(
        uint32_t thread, uint32_t remote_node,
        uint64_t local_addr, uint64_t remote_offset,
        uint32_t length, uint64_t wr_id,
        uint32_t flags = IBV_SEND_SIGNALED
    ) const;

    int32_t write(
        uint32_t thread, uint32_t remote_node,
        uint64_t local_addr, uint64_t remote_offset,
        uint32_t length, uint64_t wr_id,
        uint32_t flags = IBV_SEND_SIGNALED
    ) const;

    int32_t send(
        uint32_t thread, uint32_t remote_node,
        uint64_t local_addr, uint32_t length, uint64_t wr_id,
        uint32_t flags = IBV_SEND_SIGNALED
    ) const;

    int32_t post_recv(
        uint32_t thread, uint32_t remote_node,
        uint64_t local_addr, uint32_t length, uint64_t wr_id
    ) const;

    int32_t poll(uint32_t thread, ibv_wc* wc, int32_t max = 1) const;
    void poll_one(uint32_t thread, ibv_wc* wc) const;

private:
    const uint32_t node_id_;
    const uint32_t num_nodes_;
    const uint32_t num_threads_;

    ibv_context* ctx_ = nullptr;
    ibv_pd* pd_ = nullptr;
    void* buf_ = nullptr;
    ibv_mr* mr_ = nullptr;

    ibv_cq* cqs_[MAX_THREADS] = {};
    ibv_qp* qps_[MAX_THREADS][MAX_REPLICAS] = {};

    struct RemoteNode {
        uint32_t rkey;
        uint64_t addr;
    };
    RemoteNode remotes_[MAX_REPLICAS] = {};
};
