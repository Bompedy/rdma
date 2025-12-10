#include <iostream>
#include <infiniband/verbs.h>



int main() {
    int num_devices = 0;

    // Get the list of available RDMA devices
    ibv_device **dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        std::cerr << "Failed to get IB devices list" << std::endl;
        return 1;
    }

    std::cout << "Found " << num_devices << " RDMA device(s)" << std::endl;

    if (num_devices == 0) {
        ibv_free_device_list(dev_list);
        return 0;
    }

    // Just open the first device
    ibv_context *ctx = ibv_open_device(dev_list[0]);
    if (!ctx) {
        std::cerr << "Failed to open the device" << std::endl;
        ibv_free_device_list(dev_list);
        return 1;
    }

    std::cout << "Opened device: " << ibv_get_device_name(dev_list[0]) << std::endl;

    // Query the device attributes
    ibv_device_attr dev_attr{};
    if (ibv_query_device(ctx, &dev_attr)) {
        std::cerr << "Failed to query device" << std::endl;
        ibv_close_device(ctx);
        ibv_free_device_list(dev_list);
        return 1;
    }

    std::cout << "Max QP: " << dev_attr.max_qp << std::endl;
    std::cout << "Max MR size: " << dev_attr.max_mr_size << std::endl;

    // Query port info
    for (int port = 1; port <= dev_attr.phys_port_cnt; port++) {
        ibv_port_attr port_attr{};
        if (!ibv_query_port(ctx, port, &port_attr)) {
            std::cout << "Port " << port
                      << " state: " << port_attr.state
                      << " link_layer: " << (int)port_attr.link_layer
                      << " max_mtu: " << port_attr.max_mtu << std::endl;
        }
    }

    // Clean up
    ibv_close_device(ctx);
    ibv_free_device_list(dev_list);

    return 0;
}