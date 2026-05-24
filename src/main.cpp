#include <cstdlib>
#include <iostream>
#include <infiniband/verbs.h>

#include "rdma/config.h"

int main() {
    int num_devices = 0;
    ibv_device** dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list || num_devices == 0) {
        std::cerr << "No RDMA devices found.\n";
        return 1;
    }

    std::cout << "Found " << num_devices << " RDMA device(s):\n";
    for (int i = 0; i < num_devices; ++i) {
        ibv_context* ctx = ibv_open_device(dev_list[i]);
        if (!ctx) {
            std::cerr << "  [" << i << "] Failed to open "
                      << ibv_get_device_name(dev_list[i]) << "\n";
            continue;
        }

        ibv_device_attr attr{};
        if (ibv_query_device(ctx, &attr) == 0) {
            std::cout << "  [" << i << "] " << ibv_get_device_name(dev_list[i])
                      << "  ports=" << static_cast<int>(attr.phys_port_cnt)
                      << "  max_qp=" << attr.max_qp
                      << "  max_mr=" << attr.max_mr
                      << "\n";
        }

        ibv_close_device(ctx);
    }
    ibv_free_device_list(dev_list);

    // Env vars passed by the Makefile
    auto env = [](const char* name) -> const char* {
        const char* v = std::getenv(name);
        return v ? v : "unset";
    };

    std::cout << "\nEnvironment:\n"
              << "  NODE_ID    = " << env("NODE_ID") << "\n"
              << "  IS_CLIENT  = " << env("IS_CLIENT") << "\n"
              << "  MACHINE_ID = " << env("MACHINE_ID") << "\n"
              << "  SERVERS    = " << env("SERVERS") << "\n"
              << "  RDMA_PORT  = " << env("RDMA_PORT") << "\n";

	std::cout << "Completed it all works" << std::endl;

    return 0;
}
