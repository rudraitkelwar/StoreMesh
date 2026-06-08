#include "storage_node.hpp"

#include <chrono>
#include <thread>

StorageNode::StorageNode(std::string node_id,
                         std::size_t num_blocks,
                         std::size_t num_io_threads)
    : node_id_(std::move(node_id))
    , device_(std::make_unique<BlockDevice>(num_blocks, node_id_))
    , thread_pool_(std::make_unique<ThreadPool>(num_io_threads))
    , last_heartbeat_(std::chrono::steady_clock::now())
{
    // Launch the dispatch thread that drains the SQ
    dispatch_thread_ = std::thread(&StorageNode::dispatch_loop, this);
}

StorageNode::~StorageNode() {
    running_ = false;
    // Push a sentinel so the blocking wait_pop unblocks
    IOCommand sentinel{};
    sentinel.opcode = IOOpcode::FLUSH;
    sq_.push(std::move(sentinel));

    if (dispatch_thread_.joinable()) dispatch_thread_.join();
}

bool StorageNode::submit_io(IOCommand cmd) {
    sq_.push(std::move(cmd));
    return true;
}

std::optional<IOCompletion> StorageNode::poll_completion() {
    return cq_.try_pop();
}

void StorageNode::receive_heartbeat() {
    std::lock_guard<std::mutex> lock(hb_mutex_);
    last_heartbeat_ = std::chrono::steady_clock::now();
    state_.store(NodeState::HEALTHY);
}

bool StorageNode::is_healthy() const {
    std::lock_guard<std::mutex> lock(hb_mutex_);
    const auto elapsed = std::chrono::steady_clock::now() - last_heartbeat_;
    return elapsed < kHeartbeatTimeout &&
           state_.load() != NodeState::FAILED;
}

void StorageNode::dispatch_loop() {
    while (running_) {
        // Block until a command arrives in the SQ
        IOCommand cmd = sq_.wait_pop();

        if (!running_) break; // sentinel was the wakeup

        // Capture fields before moving cmd into the lambda
        const uint16_t  cid        = cmd.command_id;
        const IOOpcode  opcode     = cmd.opcode;
        const uint64_t  lba        = cmd.lba;
        const uint32_t  num_blocks = cmd.num_blocks;

        // Fan the command out to the thread pool for async execution.
        // The lambda captures cmd by move so the data buffer is not copied.
        thread_pool_->enqueue([this, cmd = std::move(cmd)]() mutable {
            const auto t0 = std::chrono::steady_clock::now();
            bool success = false;

            switch (cmd.opcode) {
                case IOOpcode::READ: {
                    std::vector<uint8_t> buf;
                    success = device_->read(cmd.lba, cmd.num_blocks, buf);
                    if (success) {
                        cmd.data = std::move(buf); // caller can inspect via callback
                    }
                    break;
                }
                case IOOpcode::WRITE:
                    success = device_->write(cmd.lba, cmd.num_blocks, cmd.data);
                    break;
                case IOOpcode::FLUSH:
                    success = device_->flush();
                    break;
            }

            const uint64_t latency_ns =
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t0).count());

            // Fire optional callback (on the worker thread)
            if (cmd.callback) {
                cmd.callback(success, success ? 0u : 1u);
            }

            // Post completion entry to the CQ
            cq_.push(IOCompletion{
                cmd.command_id,
                success,
                success ? 0u : 1u,
                latency_ns
            });
        });

        (void)cid; (void)opcode; (void)lba; (void)num_blocks; // suppress warnings
    }
}
