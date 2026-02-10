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


int main() {
    try {
        if (get_uint_env("IS_CLIENT") != 0) {
            std::cout << "Starting as a client!" << std::endl;
            run_clients();
        } else {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(1, &cpuset);
            pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

            const uint32_t node_id = get_uint_env("NODE_ID");
            if (node_id == 0) run_leader(node_id);
            else run_follower(node_id);
        }
    } catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
