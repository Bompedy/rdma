#include <chrono>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <ranges>
#include <thread>
#include <vector>
#include <netinet/in.h>
#include <sys/mman.h>
#include "temp.h"
#include "leader.h"
#include "follower.h"
#include "client.h"
#include "synra.h"


int main() {
    try {
        if (get_uint_env("IS_CLIENT") != 0) {
            // run_mu_clients();
            run_synra_clients();
        } else {
            pin_thread_to_cpu(1);
            const uint32_t node_id = get_uint_env("NODE_ID");
            run_synra_node(node_id);
            // if (node_id == 0) run_leader(node_id);
            // else run_follower_mu(node_id);
        }
    } catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
