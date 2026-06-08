#include "replication_manager.hpp"

#include <algorithm>
#include <iomanip>
#include <iostream>

ReplicationManager::ReplicationManager(std::size_t num_blocks,
                                        std::size_t num_replicas)
{
    primary_ = std::make_shared<StorageNode>("node-primary", num_blocks);
    primary_->set_role(NodeRole::PRIMARY);

    for (std::size_t i = 0; i < num_replicas; ++i) {
        auto r = std::make_shared<StorageNode>(
            "node-replica-" + std::to_string(i), num_blocks);
        r->set_role(NodeRole::REPLICA);
        replicas_.push_back(std::move(r));
    }

    hb_thread_ = std::thread(&ReplicationManager::heartbeat_loop, this);

    std::cout << "[Cluster] Started: 1 primary + "
              << num_replicas << " replicas  ("
              << (1 + num_replicas) << " nodes total)\n";
}

ReplicationManager::~ReplicationManager() {
    running_ = false;
    if (hb_thread_.joinable()) hb_thread_.join();
}

// ─── Write ────────────────────────────────────────────────────────────────────

bool ReplicationManager::write(uint64_t lba, uint32_t num_blocks,
                                const std::vector<uint8_t>& data)
{
    std::lock_guard<std::mutex> lock(cluster_mutex_);

    // ── Failover: if primary is dead, promote the best available replica ──
    if (!primary_ || !primary_->is_healthy()) {
        std::cout << "[Cluster] Primary unhealthy — triggering failover...\n";
        auto candidate = find_healthy_replica();
        if (!candidate) {
            std::cerr << "[Cluster] No healthy replicas. Write FAILED.\n";
            return false;
        }
        promote_replica(candidate);
    }

    // ── Write to primary first ────────────────────────────────────────────
    if (!primary_->device().write(lba, num_blocks, data)) return false;
    std::size_t acks = 1; // primary counts

    // ── Replicate synchronously to all healthy replicas ───────────────────
    // (Sync replication = strong consistency; async = eventual consistency)
    for (auto& replica : replicas_) {
        if (replica->is_healthy()) {
            if (replica->device().write(lba, num_blocks, data)) ++acks;
        }
    }

    // ── Quorum check: majority must ack ──────────────────────────────────
    const std::size_t total  = 1 + replicas_.size();
    const std::size_t quorum = (total / 2) + 1;
    if (acks < quorum) {
        std::cerr << "[Cluster] Quorum not reached ("
                  << acks << "/" << quorum << "). Write FAILED.\n";
        return false;
    }
    return true;
}

// ─── Read ─────────────────────────────────────────────────────────────────────

bool ReplicationManager::read(uint64_t lba, uint32_t num_blocks,
                               std::vector<uint8_t>& buffer)
{
    std::lock_guard<std::mutex> lock(cluster_mutex_);

    if (primary_ && primary_->is_healthy()) {
        return primary_->device().read(lba, num_blocks, buffer);
    }
    // Fallback: read from any healthy replica
    auto r = find_healthy_replica();
    if (!r) return false;
    return r->device().read(lba, num_blocks, buffer);
}

// ─── Failure simulation ───────────────────────────────────────────────────────

void ReplicationManager::simulate_failure(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(cluster_mutex_);
    failed_nodes_.insert(node_id);
    std::cout << "[Cluster] Failure injected: " << node_id
              << " (heartbeats stopped; timeout in ~3 s)\n";
}

// ─── Internals ────────────────────────────────────────────────────────────────

void ReplicationManager::heartbeat_loop() {
    while (running_) {
        {
            std::lock_guard<std::mutex> lock(cluster_mutex_);
            // Only heartbeat nodes that have not been manually failed
            if (primary_ &&
                failed_nodes_.find(primary_->node_id()) == failed_nodes_.end())
            {
                primary_->receive_heartbeat();
            }
            for (auto& r : replicas_) {
                if (failed_nodes_.find(r->node_id()) == failed_nodes_.end()) {
                    r->receive_heartbeat();
                }
            }
        }
        std::this_thread::sleep_for(kHeartbeatInterval);
    }
}

void ReplicationManager::promote_replica(
    std::shared_ptr<StorageNode> new_primary)
{
    // Called while holding cluster_mutex_
    std::cout << "[Cluster] FAILOVER → " << new_primary->node_id()
              << " promoted to PRIMARY\n";

    new_primary->set_role(NodeRole::PRIMARY);

    // Remove the new primary from the replica list
    replicas_.erase(
        std::remove(replicas_.begin(), replicas_.end(), new_primary),
        replicas_.end());

    // Demote old primary to replica (it may recover later)
    if (primary_) {
        primary_->set_role(NodeRole::REPLICA);
        replicas_.push_back(primary_);
    }

    primary_ = std::move(new_primary);
}

std::shared_ptr<StorageNode>
ReplicationManager::find_healthy_replica() const
{
    // Called while holding cluster_mutex_
    for (auto& r : replicas_) {
        if (r->is_healthy()) return r;
    }
    return nullptr;
}

// ─── Status ───────────────────────────────────────────────────────────────────

void ReplicationManager::print_status() const {
    std::lock_guard<std::mutex> lock(cluster_mutex_);
    std::cout << "\n┌─────────────────────────── Cluster Status ───────────────────────────┐\n";

    auto print_node = [](const StorageNode& n, const std::string& role_label) {
        auto info = n.device().identify();
        std::cout << "│ " << std::left << std::setw(18) << n.node_id()
                  << " [" << std::setw(8) << role_label << "]  "
                  << (n.is_healthy() ? "\033[32mHEALTHY\033[0m" : "\033[31mFAILED \033[0m")
                  << "   reads=" << std::setw(6) << info.read_ops
                  << "  writes=" << std::setw(6) << info.write_ops << " │\n";
    };

    if (primary_) print_node(*primary_, "PRIMARY");
    for (auto& r : replicas_) print_node(*r, "REPLICA");

    std::cout << "└──────────────────────────────────────────────────────────────────────┘\n\n";
}
