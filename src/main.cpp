#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include "rdma/transport.h"

static std::vector<std::string> split(const char* s, const char delim) {
    std::vector<std::string> out;
    std::istringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, delim))
        if (!tok.empty()) out.push_back(tok);
    return out;
}

int main() {
    const char* const servers_env = std::getenv("SERVERS");
    if (!servers_env) {
        std::cerr << "SERVERS not set\n";
        return 1;
    }

    const auto all_ips = split(servers_env, ',');

    const char* const node_id_env = std::getenv("NODE_ID");
    if (!node_id_env) {
        std::cerr << "NODE_ID not set\n";
        return 1;
    }
    const uint32_t node_id = static_cast<uint32_t>(std::stoul(node_id_env));
    const uint32_t num_threads = 1;

    Transport transport(node_id, all_ips, num_threads);
    std::cerr << "[Node " << node_id << "] Ready. Buffer at "
              << transport.buffer() << "\n";

    if (all_ips.size() >= 2) {
        auto* const buf = static_cast<uint64_t*>(transport.buffer());
        ibv_wc wc{};

        if (node_id == 0) {
            // FAA: increment node 1's first 8 bytes
            buf[0] = 0;
            transport.faa(0, 1, reinterpret_cast<uint64_t>(&buf[0]), 0, 1, 1);
            transport.poll_one(0, &wc);

            if (wc.status == IBV_WC_SUCCESS)
                std::cerr << "[Node 0] FAA old value: " << buf[0] << "\n";
            else
                std::cerr << "[Node 0] FAA failed: " << ibv_wc_status_str(wc.status) << "\n";

            // READ: read back node 1 word 0
            buf[1] = 0xDEAD;
            transport.read(0, 1, reinterpret_cast<uint64_t>(&buf[1]), 0, 8, 2);
            transport.poll_one(0, &wc);

            if (wc.status == IBV_WC_SUCCESS)
                std::cerr << "[Node 0] READ node 1 word 0 = " << buf[1] << "\n";
            else
                std::cerr << "[Node 0] READ failed: " << ibv_wc_status_str(wc.status) << "\n";

            // CAS: swap node 1's word 0 from 1 -> 42
            buf[2] = 0xDEAD;
            transport.cas(0, 1, reinterpret_cast<uint64_t>(&buf[2]), 0, 1, 42, 3);
            transport.poll_one(0, &wc);

            if (wc.status == IBV_WC_SUCCESS)
                std::cerr << "[Node 0] CAS old value: " << buf[2]
                          << (buf[2] == 1 ? " (swapped)" : " (failed)") << "\n";

            // WRITE: write 0xFF to node 1 offset 16
            buf[3] = 0xFF;
            transport.write(0, 1, reinterpret_cast<uint64_t>(&buf[3]), 16, 8, 4);
            transport.poll_one(0, &wc);

            if (wc.status == IBV_WC_SUCCESS)
                std::cerr << "[Node 0] WRITE to node 1 offset 16 OK\n";

            // READ back the write
            buf[4] = 0;
            transport.read(0, 1, reinterpret_cast<uint64_t>(&buf[4]), 16, 8, 5);
            transport.poll_one(0, &wc);

            if (wc.status == IBV_WC_SUCCESS)
                std::cerr << "[Node 0] READ back = " << buf[4] << "\n";

            // SEND: send 8 bytes to node 1 (node 1 posts recv first via sleep ordering)
            sleep(1);
            buf[5] = 0xCAFE;
            transport.send(0, 1, reinterpret_cast<uint64_t>(&buf[5]), 8, 6);
            transport.poll_one(0, &wc);

            if (wc.status == IBV_WC_SUCCESS)
                std::cerr << "[Node 0] SEND to node 1 OK\n";
            else
                std::cerr << "[Node 0] SEND failed: " << ibv_wc_status_str(wc.status) << "\n";

        } else if (node_id == 1) {
            // Post a receive buffer for the send from node 0
            buf[10] = 0;
            transport.post_recv(0, 0, reinterpret_cast<uint64_t>(&buf[10]), 8, 100);
            transport.poll_one(0, &wc);

            if (wc.status == IBV_WC_SUCCESS)
                std::cerr << "[Node 1] RECV from node 0: " << buf[10] << "\n";
            else
                std::cerr << "[Node 1] RECV failed: " << ibv_wc_status_str(wc.status) << "\n";

        } else {
            sleep(3);
        }
    }

    return 0;
}
