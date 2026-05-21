#include <memory>

#include <grpcpp/grpcpp.h>
#include "raft_node.hpp"

constexpr int32_t FIRST_PORT = 50000;

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "please provide: <id> <cluster_size>" << '\n';
        return 1;
    }

    uint32_t id = std::stoul(argv[1]);
    uint32_t port = FIRST_PORT + id;
    uint32_t cluster_size = std::stoul(argv[2]);

    auto node = std::make_unique<RaftNode>(port, id);
    std::vector<PeerInfo> peers;
    for (uint32_t idx = 0; idx < cluster_size; ++idx) {
        if (idx == id) continue;
        peers.push_back(PeerInfo {
            .id = idx,
            .address = "localhost:" + std::to_string(FIRST_PORT + idx)
        });
    }

    node->set_other_nodes(peers);
    node->setup_rpc_server();

    return 0;
}
