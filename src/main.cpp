#include <chrono>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <ranges>
#include <thread>
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
};

constexpr unsigned short RDMA_PORT = 6969;

void run_leader_mu(unsigned int node_id, const std::vector<Peer>& peers) {

    auto die_if_null = [&](const char* name, const void* p) {
        if (!p) {
            std::cerr << "[mu] " << name << " is null\n";
            std::abort();
        }
    };

    die_if_null("id", peers[1].id);
    die_if_null("id->verbs", peers[1].id->verbs);
    die_if_null("id->qp", peers[1].id->qp);
    die_if_null("id->pd", peers[1].id->pd);
    // die_if_null("id->recv_cq", id->recv_cq);
    // die_if_null("id->send_cq", id->send_cq);
    die_if_null("id->qp->recv_cq", peers[1].id->qp->recv_cq);
    die_if_null("id->qp->send_cq", peers[1].id->qp->send_cq);



    const auto any = peers[1].id;
    static char buf[4096];


    const ibv_mr* mr = ibv_reg_mr(
        peers[1].id->pd,
        buf,
        sizeof(buf),
        IBV_ACCESS_LOCAL_WRITE
    );
    if (!mr) throw std::runtime_error("ibv_reg_mr failed");

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(buf);
    sge.length = sizeof(buf);
    sge.lkey = mr->lkey;

    for (const auto& peer : peers) {
        if (!peer.id) continue;

        ibv_recv_wr rwr{};
        rwr.wr_id = peer.node_id;
        rwr.sg_list = &sge;
        rwr.num_sge = 1;

        ibv_recv_wr* bad = nullptr;
        if (ibv_post_recv(peer.id->qp, &rwr, &bad)) {
            throw std::runtime_error("ibv_post_recv failed");
        }
    }

    while (true) {
        ibv_wc wc{};
        while (ibv_poll_cq(any->recv_cq, 1, &wc) == 0) {}

        if (wc.status != IBV_WC_SUCCESS) {
            std::cerr << "[leader] WC error " << wc.status << "\n";
            break;
        }

        if (wc.opcode != IBV_WC_RECV) continue;
        std::cout << "[leader] received " << wc.byte_len << " bytes from node " << wc.wr_id << "\n";

        for (const auto& peer : peers) {
            if (peer.id && peer.node_id == wc.wr_id) {
                ibv_recv_wr rwr{};
                rwr.wr_id = peer.node_id;
                rwr.sg_list = &sge;
                rwr.num_sge = 1;

                ibv_recv_wr* bad = nullptr;
                if (ibv_post_recv(peer.id->qp, &rwr, &bad)) throw std::runtime_error("ibv_post_recv failed");
                break;
            }
        }
    }
}

void run_follower_mu(const unsigned int node_id, const rdma_cm_id* id) {
    std::this_thread::sleep_for(std::chrono::seconds(5));

    const char msg[] = "hello from follower";

    const ibv_mr* send_mr = ibv_reg_mr(
        id->pd,
        (void*)msg,
        sizeof(msg),
        IBV_ACCESS_LOCAL_WRITE
    );
    if (!send_mr) throw std::runtime_error("ibv_reg_mr(send) failed");

    ibv_sge send_sge{};
    send_sge.addr   = reinterpret_cast<uintptr_t>(msg);
    send_sge.length = sizeof(msg);
    send_sge.lkey   = send_mr->lkey;

    ibv_send_wr swr{};
    swr.wr_id      = node_id;
    swr.sg_list    = &send_sge;
    swr.num_sge    = 1;
    swr.opcode     = IBV_WR_SEND;
    swr.send_flags = IBV_SEND_SIGNALED;

    ibv_send_wr* sbad = nullptr;
    if (ibv_post_send(id->qp, &swr, &sbad))
        throw std::runtime_error("ibv_post_send failed");

    ibv_wc swc{};
    while (ibv_poll_cq(id->qp->send_cq, 1, &swc) == 0) {}

    if (swc.status != IBV_WC_SUCCESS)
        throw std::runtime_error("send failed");

    std::cout << "[follower " << node_id << "] SEND completed\n";

    static char buf[4096];
    const ibv_mr* mr = ibv_reg_mr(
        id->pd,
        buf,
        sizeof(buf),
        IBV_ACCESS_LOCAL_WRITE
    );
    if (!mr) throw std::runtime_error("ibv_reg_mr failed");

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(buf);
    sge.length = sizeof(buf);
    sge.lkey = mr->lkey;

    ibv_recv_wr rwr{};
    rwr.wr_id = 1;
    rwr.sg_list = &sge;
    rwr.num_sge = 1;

    ibv_recv_wr* bad = nullptr;
    if (ibv_post_recv(id->qp, &rwr, &bad)) throw std::runtime_error("ibv_post_recv failed");

    while (true) {
        ibv_wc wc{};
        while (ibv_poll_cq(id->qp->recv_cq, 1, &wc) == 0) {}

        if (wc.status != IBV_WC_SUCCESS) {
            std::cerr << "[follower] WC error " << wc.status << "\n";
            break;
        }

        if (wc.opcode != IBV_WC_RECV) continue;
        std::cout << "[follower " << node_id << "] received " << wc.byte_len << " bytes\n";
        if (ibv_post_recv(id->qp, &rwr, &bad)) throw std::runtime_error("ibv_post_recv failed");
    }
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

    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;

    int connected = 0;
    while (connected < expected) {
        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(ec, &event)) {
            perror("rdma_get_cm_event");
            break;
        }

        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_cm_id* id = event->id;

            uint32_t remote = 0;
            if (event->param.conn.private_data) {
                uint64_t tmp = 0;
                std::memcpy(&tmp, event->param.conn.private_data, 8);
                remote = static_cast<uint32_t>(tmp);
            } else {
                std::cout << "[leader] CONNECT_REQUEST missing private_data, rejecting" << std::endl;
                rdma_reject(event->id, nullptr, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            if (!pd) {
                pd = ibv_alloc_pd(id->verbs);
                if (!pd) throw std::runtime_error("ibv_alloc_pd failed");

                cq = ibv_create_cq(id->verbs, 4096, nullptr, nullptr, 0);
                if (!cq) throw std::runtime_error("ibv_create_cq failed");
            }

            ibv_qp_init_attr qp_attr{};
            qp_attr.qp_type = IBV_QPT_RC;
            qp_attr.send_cq = cq;
            qp_attr.recv_cq = cq;
            qp_attr.cap.max_send_wr  = 128;
            qp_attr.cap.max_recv_wr  = 128;
            qp_attr.cap.max_send_sge = 1;
            qp_attr.cap.max_recv_sge = 1;

            if (rdma_create_qp(id, pd, &qp_attr)) {
                rdma_reject(id, nullptr, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            rdma_conn_param accept{};
            if (rdma_accept(id, &accept)) {
                perror("rdma_accept");
                rdma_destroy_qp(id);
                rdma_reject(id, nullptr, 0);
                rdma_ack_cm_event(event);
                continue;
            }
            peers[remote] = Peer{remote, id};

            ++connected;
            std::cout << "[leader] connected node " << remote << "\n";
        }

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

    ibv_pd* pd = ibv_alloc_pd(id->verbs);
    if (!pd) throw std::runtime_error("ibv_alloc_pd failed");
    ibv_cq* cq = ibv_create_cq(id->verbs, 256, nullptr, nullptr, 0);
    if (!cq) throw std::runtime_error("ibv_create_cq failed");

    ibv_qp_init_attr qp_attr{};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.cap.max_send_wr  = 128;
    qp_attr.cap.max_recv_wr  = 128;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    if (rdma_create_qp(id, pd, &qp_attr)) throw std::runtime_error("rdma_create_qp failed");

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
    if (id->pd == nullptr) {
        std::cout << "The pd is null somehow!" << std::endl;
    }
    run_follower_mu(node_id, id);
}

int main() {
    try {
        const unsigned int node_id = get_node_id();
        if (node_id == 0) run_leader(node_id);
        else run_follower(node_id);
    } catch (const std::exception& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
