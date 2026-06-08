#pragma once
/**
 * replication_manager.hpp — StoreMesh
 *
 * Manages a cluster of StorageNodes with primary-backup replication.
 *
 * REPLICATION MODEL:
 *   Write path : primary + all healthy replicas must acknowledge.
 *                A write succeeds only if a quorum (majority) acks.
 *   Read path  : primary only (strong consistency / no stale reads).
 *                Falls back to a healthy replica if primary is down.
 *
 * FAILURE DETECTION:
 *   A background heartbeat thread calls receive_heartbeat() on every node
 *   every 500 ms.  Nodes that are "manually failed" via simulate_failure()
 *   stop receiving heartbeats; after 3 s they fail is_healthy().
 *
 * FAILOVER:
 *   When write() detects an unhealthy primary it calls promote_replica(),
 *   which atomically swaps the best healthy replica into the primary slot.
 *   The old primary is demoted to replica (it might recover later).
 *
 * INTERVIEW COMPARISON:
 *   This is a simplified version of what Raft / Paxos does:
 *     - Raft uses log replication + commit index; we use direct device writes.
 *     - Raft uses election timeouts; we use heartbeat timeouts.
 *     - Raft guarantees no two leaders; we guarantee no two primaries
 *       because promotion is a single-threaded operation inside cluster_mutex_.
 */

#include "storage_node.hpp"

#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

class ReplicationManager {
public:
    // num_blocks: capacity of each node's block device
    // num_replicas: number of backup nodes (default 2 → 3-node cluster)
    ReplicationManager(std::size_t num_blocks, std::size_t num_replicas = 2);
    ~ReplicationManager();

    ReplicationManager(const ReplicationManager&) = delete;
    ReplicationManager& operator=(const ReplicationManager&) = delete;

    // Replicated write.  Returns true iff quorum acknowledges.
    bool write(uint64_t lba, uint32_t num_blocks,
               const std::vector<uint8_t>& data);

    // Read from primary (or best available replica).
    bool read(uint64_t lba, uint32_t num_blocks,
              std::vector<uint8_t>& buffer);

    // Inject a failure for testing: stops heartbeats to that node_id.
    void simulate_failure(const std::string& node_id);

    void print_status() const;

private:
    void heartbeat_loop();
    void promote_replica(std::shared_ptr<StorageNode> new_primary);
    std::shared_ptr<StorageNode> find_healthy_replica() const;

    static constexpr auto kHeartbeatInterval = std::chrono::milliseconds(500);

    std::shared_ptr<StorageNode>              primary_;
    std::vector<std::shared_ptr<StorageNode>> replicas_;
    std::set<std::string>                     failed_nodes_; // no heartbeat sent

    mutable std::mutex cluster_mutex_;
    std::thread        hb_thread_;
    std::atomic<bool>  running_{true};
};
