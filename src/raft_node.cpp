#include "raft_node.hpp"
#include <grpcpp/support/status.h>

grpc::Status RaftServiceImpl::AppendEntries(
    grpc::ServerContext* context,
    const AppendEntriesRequest* request,
    AppendEntriesResponse* response
) {
    std::cout << "Received heartbeat from " << request->leader_id() << " to " << node_.id_ << '\n';

    node_.last_heartbeat_ = std::chrono::steady_clock::now();
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::RequestVote(
    grpc::ServerContext* context,
    const RequestVoteRequest* request,
    RequestVoteResponse* response
) {
    response->set_vote_granted(false);

    if (request->term() < node_.current_term_) {
        response->set_term(node_.current_term_);
        return grpc::Status::OK;
    }

    if (request->term() > node_.current_term_) {
        node_.current_term_ = request->term();
        node_.voted_for_.reset();
        node_.state_ = NodeState::FOLLOWER;
    }

    bool can_vote =
        !node_.voted_for_.has_value() ||
        node_.voted_for_.value() == request->candidate_id();

    uint32_t last_term = !node_.log_.empty() ? node_.log_.back().term : 0;
    uint32_t last_idx = node_.log_.size();

    std::pair<uint32_t, uint32_t> receiver_log = {last_term, last_idx};
    std::pair<uint32_t, uint32_t> candidate_log = {request->last_log_term(), request->last_log_index()};

    bool log_ok = candidate_log >= receiver_log;

    if (can_vote && log_ok) {
        response->set_vote_granted(true);
        node_.voted_for_ = request->candidate_id();
    }

    response->set_term(node_.current_term_);
    return grpc::Status::OK;
}
