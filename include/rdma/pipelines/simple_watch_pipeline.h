#pragma once

// Client-side non-fault-tolerant one-sided watch registration baseline.
// - Register on single owner node (no replication)
// - Notify by reading from single owner node
// - Fast but no fault tolerance

#include <cstddef>
#include <cstdint>

class Client;

struct SimpleWatchPipelineConfig {
    size_t active_window = 0;
    size_t cq_batch = 0;
    double zipf_skew = 0.0;
    bool shard_owner = true;
};

[[nodiscard]] SimpleWatchPipelineConfig load_simple_watch_pipeline_config();
[[nodiscard]] size_t simple_watch_pipeline_client_buffer_size(const SimpleWatchPipelineConfig& config);

void run_simple_watch_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* object_counts,
    const SimpleWatchPipelineConfig& config
);
