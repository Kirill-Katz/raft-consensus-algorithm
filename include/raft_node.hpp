#pragma once

#include <chrono>
#include <ctime>
#include <grpcpp/channel.h>
#include <grpcpp/support/status.h>
#include <iostream>
#include <stdint.h>
#include <vector>
#include <memory>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
#include "raft.pb.h"

using namespace raft::v1;

struct Log {
    uint32_t term;
    std::string command;
};

struct PeerInfo {
    uint32_t id;
    std::string address;
};

struct Peer {
    PeerInfo info;
    std::unique_ptr<RaftService::Stub> stub;
};

enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
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
    friend class RaftServiceImpl;

public:
    RaftNode(uint32_t port, uint32_t id, uint32_t election_timeout_ms)
    : port_{port},
      id_{id},
      address_{"localhost:" + std::to_string(port_)},
      service_{*this},
      election_timeout_{election_timeout_ms}
    {};

    void set_other_nodes(const std::vector<PeerInfo>& peer_info) {
        peers_.clear();

        for (auto& info : peer_info) {
            if (info.id == id_) continue;

            peers_.push_back(Peer {
                .info = info,
                .stub = get_peer_stub(info.address)
            });
        }

        next_index_.resize(peer_info.size());
        match_index_.resize(peer_info.size());
    };

    void setup_rpc_server() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
        builder.RegisterService(&service_);

        server_ = builder.BuildAndStart();
        std::cout << "Starting raft node " << id_ << " on port " << port_ << '\n';

        auto reply_thread = std::thread([this]() {
            reply_loop();
        });

        server_->Wait();
        reply_thread.join();
    };

    void reply_loop() {
        while (true) {
            if (state_ == NodeState::LEADER) {
                send_heartbeats();
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            } else {
                auto now = std::chrono::steady_clock::now();
                if (now - last_heartbeat_ > election_timeout_) {
                    start_election();
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    void start_election() {
        state_ = NodeState::CANDIDATE;
        current_term_++;
        voted_for_ = id_;
        votes_received_ = 1;

        for (auto& peer : peers_) {
            grpc::ClientContext context;

            RequestVoteRequest request;
            RequestVoteResponse response;

            request.set_candidate_id(id_);
            request.set_term(current_term_);
            request.set_last_log_term(!log_.empty() ? log_.back().term : 0);
            request.set_last_log_index(log_.size());

            grpc::Status status = peer.stub->RequestVote(
                &context,
                request,
                &response
            );

            if (!status.ok()) {
                continue;
            }

            if (response.term() > current_term_) {
                current_term_ = response.term();
                state_ = NodeState::FOLLOWER;
                voted_for_.reset();
                return;
            }

            if (response.vote_granted()) {
                votes_received_++;
            }

            if (votes_received_ > peers_.size() / 2) {
                state_ = NodeState::LEADER;
                return;
            }
        }
    }

    void send_heartbeats() {
        for (auto& peer : peers_) {
            std::cout << "Sent heartbeat from " << id_ << " to " << peer.info.id << '\n';
            grpc::ClientContext context;

            AppendEntriesRequest request;
            AppendEntriesResponse response;

            request.set_term(current_term_);
            request.set_leader_id(id_);

            grpc::Status status = peer.stub->AppendEntries(
                &context,
                request,
                &response
            );
        }
    }

    std::unique_ptr<RaftService::Stub> get_peer_stub(const std::string& address) const {
        return RaftService::NewStub(
            grpc::CreateChannel(
                address,
                grpc::InsecureChannelCredentials()
            )
        );
    };

    PeerInfo get_info() const {
        return PeerInfo {
            .id = id_,
            .address = address_,
        };
    };

private:
    uint32_t port_;
    uint32_t current_term_ = 0; // TODO: make this non volatile (write to disk)
    std::optional<uint32_t> voted_for_; // TODO: make this non volatile (write to disk)
    std::string address_;

    uint32_t votes_received_ = 0;

    std::vector<Log> log_; // TODO: make this non volatile (write to disk)

    uint32_t commit_idx_ = 0;
    uint32_t last_applied_ = 0;
    uint32_t id_;

    std::vector<uint32_t> next_index_;
    std::vector<uint32_t> match_index_;

    std::vector<Peer> peers_;

    RaftServiceImpl service_;
    std::unique_ptr<grpc::Server> server_;

    std::chrono::steady_clock::time_point last_heartbeat_;
    std::chrono::milliseconds election_timeout_{250};

    NodeState state_ = NodeState::FOLLOWER;
};
