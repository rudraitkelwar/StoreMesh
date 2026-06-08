#pragma once
/**
 * storage_node.hpp — StoreMesh
 *
 * A StorageNode ties together:
 *   1. A BlockDevice   (the simulated NVMe drive)
 *   2. A ThreadPool    (I/O worker threads)
 *   3. A ConcurrentQueue<IOCommand>    (Submission Queue)
 *   4. A ConcurrentQueue<IOCompletion> (Completion Queue)
 *   5. A dispatch thread that drains the SQ and fans work to the pool
 *   6. A heartbeat timestamp for failure detection
 *
 * I/O FLOW:
 *   Caller → submit_io() → SQ → dispatch thread → ThreadPool worker
 *            → BlockDevice → CQ → poll_completion() → Caller
 *
 * This mirrors the actual NVMe driver flow:
 *   Application → write to SQ doorbell → NVMe controller → DMA → CQ interrupt
 *
 * HEARTBEAT:
 *   The ReplicationManager calls receive_heartbeat() every 500 ms.
 *   If no heartbeat arrives for >3 s, is_healthy() returns false and
 *   the manager promotes a replica.  This is analogous to ZooKeeper
 *   session timeouts or etcd lease TTLs.
 */

#include "block_device.hpp"
#include "concurrent_queue.hpp"
#include "io_types.hpp"
#include "thread_pool.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

enum class NodeRole  { PRIMARY, REPLICA };
enum class NodeState { HEALTHY, FAILED  };

class StorageNode {
public:
    StorageNode(std::string node_id,
                std::size_t num_blocks,
                std::size_t num_io_threads = 4);
    ~StorageNode();

    StorageNode(const StorageNode&) = delete;
    StorageNode& operator=(const StorageNode&) = delete;

    // ── I/O interface ─────────────────────────────────────────────────────
    bool submit_io(IOCommand cmd);
    std::optional<IOCompletion> poll_completion();

    // ── Heartbeat ─────────────────────────────────────────────────────────
    void receive_heartbeat();
    bool is_healthy() const;

    // ── Direct device access (used by ReplicationManager for sync writes) ─
    BlockDevice&       device()       { return *device_; }
    const BlockDevice& device() const { return *device_; }

    // ── Identity ──────────────────────────────────────────────────────────
    const std::string& node_id()   const { return node_id_; }
    NodeRole           role()      const { return role_; }
    void               set_role(NodeRole r) { role_ = r; }
    NodeState          state()     const { return state_.load(); }

private:
    void dispatch_loop(); // Drains SQ → dispatches to ThreadPool → fills CQ

    static constexpr auto kHeartbeatTimeout = std::chrono::seconds(3);

    std::string node_id_;
    NodeRole    role_{NodeRole::REPLICA};
    std::atomic<NodeState> state_{NodeState::HEALTHY};

    std::unique_ptr<BlockDevice> device_;
    std::unique_ptr<ThreadPool>  thread_pool_;

    ConcurrentQueue<IOCommand>    sq_; // Submission Queue
    ConcurrentQueue<IOCompletion> cq_; // Completion Queue

    std::thread      dispatch_thread_;
    std::atomic<bool> running_{true};

    mutable std::mutex                        hb_mutex_;
    std::chrono::steady_clock::time_point     last_heartbeat_;
};
