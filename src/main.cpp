#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <ranges>
#include <unordered_map>
#include <unistd.h>
#include <vector>
#include <netinet/in.h>

const std::vector<std::string> CLUSTER_NODES = {
    "10.10.1.1",
    "10.10.1.2",
    "10.10.1.3",
};

unsigned int get_node_id() {
    const char* id = std::getenv("NODE_ID");
    if (!id) throw std::runtime_error("NODE_ID not set");
    return std::stoi(id);
}

struct Peer {
    uint32_t node_id;
    rdma_cm_id* id;
    ibv_qp* qp;
};

constexpr unsigned short RDMA_PORT = 6969;

void run_leader_mu(unsigned int node_id, const std::vector<Peer>& peers) {
    pause();
}

void run_follower_mu(unsigned int node_id, const rdma_cm_id* id) {
    pause();
}

void run_leader(const unsigned int node_id) {
    std::cout << "[leader] starting\n";

    std::vector<Peer> peers(CLUSTER_NODES.size());
    const unsigned int expected = CLUSTER_NODES.size() - 1;
    rdma_event_channel* ec = rdma_create_event_channel();
    rdma_cm_id* listener = nullptr;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(RDMA_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (!ec) throw std::runtime_error("rdma_create_event_channel failed");
    if (rdma_create_id(ec, &listener, nullptr, RDMA_PS_TCP)) throw std::runtime_error("rdma_create_id failed");
    if (rdma_bind_addr(listener, reinterpret_cast<sockaddr*>(&addr))) throw std::runtime_error("rdma_bind_addr failed");
    if (rdma_listen(listener, 16)) throw std::runtime_error("rdma_listen failed");

    std::cout << "[leader] waiting for " << expected << " nodes\n";

    size_t connected = 0;
    while (connected < expected) {
        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(ec, &event)) {
            perror("rdma_get_cm_event");
            break;
        }

        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_ack_cm_event(event);
            continue;
        }

        rdma_cm_id* id = event->id;

        if (!event->param.conn.private_data ||
            event->param.conn.private_data_len != sizeof(uint32_t)) {
            rdma_reject(id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        uint32_t remote = 0;
        std::memcpy(&remote,event->param.conn.private_data, sizeof(uint32_t));

        if (remote >= peers.size() || remote == node_id) {
            rdma_reject(id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        if (peers[remote].id != nullptr) {
            rdma_reject(id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        ibv_qp_init_attr qp_attr{};
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.cap.max_send_wr = 128;
        qp_attr.cap.max_recv_wr = 128;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;

        if (rdma_create_qp(id, nullptr, &qp_attr)) {
            rdma_reject(id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        rdma_conn_param accept{};
        if (rdma_accept(id, &accept)) {
            rdma_destroy_qp(id);
            rdma_reject(id, nullptr, 0);
            rdma_ack_cm_event(event);
            continue;
        }

        peers[remote] = Peer{remote, id};
        connected++;

        std::cout << "[leader] connected node " << remote << "\n";

        rdma_ack_cm_event(event);
    }

    std::cout << "[leader] all nodes connected\n";
    run_leader_mu(node_id, peers);
}


void run_follower(const unsigned int node_id) {
    std::cout << "[follower " << node_id << "] starting\n";

    rdma_event_channel* ec = rdma_create_event_channel();
    rdma_cm_id* id = nullptr;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(RDMA_PORT);
    inet_pton(AF_INET, CLUSTER_NODES[0].c_str(), &addr.sin_addr);
    if (!ec) throw std::runtime_error("rdma_create_event_channel failed");
    if (rdma_create_id(ec, &id, nullptr, RDMA_PS_TCP)) throw std::runtime_error("rdma_create_id failed");

    rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr*>(&addr), 2000);
    rdma_cm_event* event = nullptr;
    rdma_get_cm_event(ec, &event);
    rdma_ack_cm_event(event);
    rdma_resolve_route(id, 2000);
    rdma_get_cm_event(ec, &event);
    rdma_ack_cm_event(event);

    ibv_qp_init_attr qp_attr{};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 128;
    qp_attr.cap.max_recv_wr = 128;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    if (rdma_create_qp(id, nullptr, &qp_attr)) throw std::runtime_error("rdma_create_qp failed");

    const auto nid = static_cast<uint32_t>(node_id);

    rdma_conn_param param{};
    param.private_data = &nid;
    param.private_data_len = sizeof(uint32_t);

    if (rdma_connect(id, &param)) {
        perror("rdma_connect");
        std::exit(1);
    }

    if (rdma_get_cm_event(ec, &event)) {
        perror("rdma_get_cm_event(connect)");
        std::exit(1);
    }

    std::cout << "[follower " << node_id << "] connect event: " << event->event << "\n";
    rdma_ack_cm_event(event);
    std::cout << "[follower " << node_id << "] connected to leader\n";
    run_follower_mu(node_id, id);
}

int main() {
    try {
        const unsigned int node_id = get_node_id();
        if (node_id == 0) run_leader(node_id);
        else run_follower(node_id);
    }
    catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
