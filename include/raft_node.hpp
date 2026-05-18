#pragma once

#include <grpcpp/channel.h>
#include <stdint.h>
#include <vector>
#include <memory>

#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
#include "raft.pb.h"

using namespace raft::v1;

struct Log {
    uint32_t term;
    std::string command;
};

struct Peer {
    uint32_t id;
    std::string address;
    std::unique_ptr<RaftService::Stub> stub;
};

class RaftNode;

class RaftServiceImpl final : public raft::v1::RaftService::Service {
public:
    explicit RaftServiceImpl(RaftNode& raft_node)
    : node_{raft_node}
    {};

    grpc::Status AppendEntries(
        grpc::ServerContext* context,
        const AppendEntriesRequest* request,
        AppendEntriesResponse* response
    ) override;

    grpc::Status RequestVote(
        grpc::ServerContext* context,
        const RequestVoteRequest* request,
        RequestVoteResponse* response
    ) override;

private:
    RaftNode& node_;
};

class RaftNode {
public:
    RaftNode(uint32_t port, uint32_t id)
    : port_{port},
      id_{id},
      address_{"localhost:" + std::to_string(port_)},
      service_{*this}
    {};

    void set_other_nodes(std::vector<Peer> nodes) {
        peers_ = std::move(nodes);

        next_index_.resize(nodes.size() + 1); // + 1 for current node too
        match_index_.resize(nodes.size() + 1); // + 1 for current node too
    };

    void setup_rpc_server() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
        builder.RegisterService(&service_);

        server_ = builder.BuildAndStart();
        server_->Wait();
    };

    std::shared_ptr<grpc::Channel> get_channel() {
        return grpc::CreateChannel(
            address_,
            grpc::InsecureChannelCredentials()
        );
    };

    Peer get_peer_obj() {
        return Peer {
            .id = id_,
            .address = address_,
            .stub = RaftService::NewStub(
                get_channel()
            )
        };
    };

private:
    uint32_t port_;
    uint32_t current_term_ = 0; // TODO: make this non volatile (write to disk)
    std::optional<uint32_t> voted_for_; // TODO: make this non volatile (write to disk)
    std::string address_;

    std::vector<Log> log_; // TODO: make this non volatile (write to disk)

    uint32_t commit_idx_ = 0;
    uint32_t last_applied_ = 0;
    uint32_t id_;

    std::vector<uint32_t> next_index_;
    std::vector<uint32_t> match_index_;

    std::vector<Peer> peers_;
    RaftServiceImpl service_;
    std::unique_ptr<grpc::Server> server_;
};
