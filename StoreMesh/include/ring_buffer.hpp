#pragma once
/**
 * ring_buffer.hpp — StoreMesh
 *
 * A lock-free Single-Producer Single-Consumer (SPSC) ring buffer.
 * This directly models how NVMe Submission Queues (SQ) and Completion
 * Queues (CQ) work in hardware:
 *
 *   Host writes to SQ head, device reads from SQ tail.
 *   Device writes to CQ head, host reads from CQ tail.
 *   No locking needed because only one side touches each pointer.
 *
 * MEMORY ORDER RATIONALE (interview-ready explanation):
 *   - head_ store uses release  → makes the written data visible to the consumer
 *   - tail_ load  uses acquire  → consumer sees all data written before the release
 *   - The relaxed loads/stores on the "same side" are safe because only one
 *     thread ever touches that index.
 *
 * CONSTRAINT: Capacity MUST be a power of two so we can use bitwise AND
 *             instead of modulo (no branch, no division).
 */

#include <array>
#include <atomic>
#include <cstddef>
#include <optional>
#include <type_traits>

template <typename T, std::size_t Capacity>
class RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two");
    static_assert(std::is_default_constructible<T>::value,
                  "T must be default constructible");

public:
    RingBuffer() : head_(0), tail_(0) {}

    // Producer: returns false if the queue is full
    bool push(T item) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next_head = (head + 1) & kMask;

        // Full: next slot would collide with the tail
        if (next_head == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        buffer_[head] = std::move(item);
        head_.store(next_head, std::memory_order_release); // publish
        return true;
    }

    // Consumer: returns nullopt if the queue is empty
    std::optional<T> pop() {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);

        // Empty: tail has caught up to head
        if (tail == head_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }

        T item = std::move(buffer_[tail]);
        tail_.store((tail + 1) & kMask, std::memory_order_release);
        return item;
    }

    bool empty() const {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

    std::size_t size() const {
        const std::size_t h = head_.load(std::memory_order_acquire);
        const std::size_t t = tail_.load(std::memory_order_acquire);
        return (h - t) & kMask;
    }

    static constexpr std::size_t capacity() { return Capacity; }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    std::array<T, Capacity> buffer_{};

    // Cache-line aligned to prevent false sharing between producer and consumer
    alignas(64) std::atomic<std::size_t> head_; // producer writes
    alignas(64) std::atomic<std::size_t> tail_; // consumer reads
};
