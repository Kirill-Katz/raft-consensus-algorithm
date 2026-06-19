#include "raft_node.hpp"
#include <grpcpp/support/status.h>


void RaftNode::start_election() {
    uint32_t election_term;
    uint32_t last_log_term;
    uint32_t last_log_index;

    {
        std::lock_guard<std::mutex> lock(m_);
        state_ = NodeState::CANDIDATE;
        current_term_++;

        voted_for_ = id_;
        votes_received_ = 1;

        election_term = current_term_;
        last_log_term = !log_.empty() ? log_.back().term : 0;
        last_log_index = log_.size();
    }

    for (auto& peer : peers_) {
        grpc::ClientContext context;

        RequestVoteRequest request;
        RequestVoteResponse response;

        request.set_candidate_id(id_);
        request.set_term(election_term);
        request.set_last_log_term(last_log_term);
        request.set_last_log_index(last_log_index);

        grpc::Status status = peer.stub->RequestVote(
            &context,
            request,
            &response
        );

        if (!status.ok()) {
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(m_);

            if (state_ != NodeState::CANDIDATE || current_term_ != election_term) {
                return;
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

            uint32_t cluster_size = peers_.size() + 1;
            if (votes_received_ > cluster_size / 2) {
                state_ = NodeState::LEADER;
                std::cout << "Node: " << id_ << " just became leader in term " << current_term_ << '\n';
                return;
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(m_);
        last_heartbeat_ = std::chrono::steady_clock::now();
        election_timeout_ = std::chrono::milliseconds{get_election_timeout_ms()};
    }
}

void RaftNode::send_heartbeats() {
    uint32_t hb_term;

    {
        std::lock_guard<std::mutex> lock(m_);
        hb_term = current_term_;
    }

    for (auto& peer : peers_) {
        {
            std::lock_guard<std::mutex> lock(m_);
            if (state_ != NodeState::LEADER || current_term_ != hb_term) {
                break;
            }
        }

        grpc::ClientContext context;

        AppendEntriesRequest request;
        AppendEntriesResponse response;

        request.set_term(hb_term);
        request.set_leader_id(id_);

        grpc::Status status = peer.stub->AppendEntries(
            &context,
            request,
            &response
        );

        if (status.ok()) {
            std::lock_guard<std::mutex> lock(m_);

            if (response.term() > current_term_) {
                state_ = NodeState::FOLLOWER;
                voted_for_.reset();
                current_term_ = response.term();
                return;
            }
        }
    }
}

void RaftNode::reply_loop() {
    while (true) {
        bool is_leader;
        bool election_timed_out;

        {
            std::lock_guard<std::mutex> lock(m_);
            auto now = std::chrono::steady_clock::now();
            is_leader = state_ == NodeState::LEADER;
            election_timed_out = now - last_heartbeat_ > election_timeout_;
        }

        if (is_leader) {
            send_heartbeats();

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } else {
            if (election_timed_out) {
                start_election();
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}
