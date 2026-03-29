#pragma once

// Client-side Mu watch pipeline:
// - Register and notify via leader using 2-sided SEND/RECV
// - Leader serializes all operations (bottleneck for comparison)

#include <cstddef>
#include <cstdint>

class Client;

struct MuWatchPipelineConfig {
    size_t active_window;
    size_t cq_batch;
    uint32_t client_send_signal_every;
    double zipf_skew;
};

[[nodiscard]] MuWatchPipelineConfig load_mu_watch_pipeline_config();
[[nodiscard]] size_t mu_watch_pipeline_client_buffer_size(const MuWatchPipelineConfig& config);

void run_mu_watch_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* object_counts,
    const MuWatchPipelineConfig& config
);
