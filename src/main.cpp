#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "rdma/transport.h"

static std::vector<std::string> split(const char *s, char delim) {
    std::vector<std::string> out;
    std::istringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, delim))
        if (!tok.empty()) out.push_back(tok);
    return out;
}

int main() {
    const char *servers_env = std::getenv("SERVERS");
    if (!servers_env) {
        std::cerr << "SERVERS not set\n";
        return 1;
    }

    auto all_ips = split(servers_env, ',');

    const char *node_id_env = std::getenv("NODE_ID");
    if (!node_id_env) {
        std::cerr << "NODE_ID not set\n";
        return 1;
    }
    uint32_t node_id = static_cast<uint32_t>(std::stoul(node_id_env));

    uint32_t num_threads = 1; // just test mesh for now

    std::cerr << "[Node " << node_id << "] Starting transport...\n";
    Transport transport(node_id, all_ips, num_threads);

    std::cerr << "[Node " << node_id << "] Ready.\n";

    return 0;
}
