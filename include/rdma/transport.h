#pragma once

#include <cstdint>
#include <string>
#include <vector>

class Transport {
public:
    Transport(uint32_t node_id,
              const std::vector<std::string>& all_ips,
              uint32_t num_threads,
              uint16_t tcp_port = 9400);
    ~Transport();

    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;
};
