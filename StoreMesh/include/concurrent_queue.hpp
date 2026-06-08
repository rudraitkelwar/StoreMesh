#pragma once
/**
 * concurrent_queue.hpp — StoreMesh
 *
 * A thread-safe Multi-Producer Multi-Consumer (MPMC) queue backed by
 * std::queue + std::mutex + std::condition_variable.
 *
 * Used for the I/O Completion Queue inside StorageNode: multiple thread-pool
 * workers push completions, while the polling thread (or caller) pops them.
 * This is MPMC, so the lock-free SPSC RingBuffer cannot be used here.
 *
 * WHY NOT LOCK-FREE MPMC?
 *   A correct lock-free MPMC queue (e.g., Michael-Scott queue) requires
 *   careful handling of ABA problems and memory reclamation.  For a project
 *   that teaches concepts, the mutex version is correct, readable, and
 *   performs well enough for thousands of ops/sec.  In production NVMe
 *   drivers the CQ really is SPSC (one device, one host ring), so the
 *   RingBuffer is the right model for that layer.
 */

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

template <typename T>
class ConcurrentQueue {
public:
    void push(T item) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::move(item));
        }
        cv_.notify_one();
    }

    // Non-blocking pop: returns nullopt immediately if empty
    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return std::nullopt;
        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    // Blocking pop: waits until an item is available
    T wait_pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty(); });
        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    mutable std::mutex      mutex_;
    std::condition_variable cv_;
    std::queue<T>           queue_;
};
