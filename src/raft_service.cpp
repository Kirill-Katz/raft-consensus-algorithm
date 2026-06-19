#include "raft_node.hpp"
#include <grpcpp/support/status.h>

grpc::Status RaftServiceImpl::AppendEntries(
    grpc::ServerContext* context,
    const AppendEntriesRequest* request,
    AppendEntriesResponse* response
) {
    std::lock_guard<std::mutex> lock(node_.m_);

    if (request->term() < node_.current_term_) {
        response->set_term(node_.current_term_);
        response->set_success(false);
        return grpc::Status::OK;
    }

    if (request->term() > node_.current_term_) {
        node_.current_term_ = request->term();
        node_.voted_for_.reset();
    }

    node_.state_ = NodeState::FOLLOWER;
    node_.last_heartbeat_ = std::chrono::steady_clock::now();

    response->set_term(node_.current_term_);
    response->set_success(true);

    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::RequestVote(
    grpc::ServerContext* context,
    const RequestVoteRequest* request,
    RequestVoteResponse* response
) {
    std::lock_guard<std::mutex> lock(node_.m_);

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
        node_.last_heartbeat_ = std::chrono::steady_clock::now();
    }

    response->set_term(node_.current_term_);

    return grpc::Status::OK;
}
