#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <optional>

template <typename T, std::size_t Capacity>

class RingBuffer
{
    static_assert((Capacity & (Capacity - 1)) == 0, "capacity must be a power of two");

    bool push(T item);
    std::optional<T> pop();
    bool empty() const;

    static constexpr std::size_t kMask = capacity - 1;

    std::array<T, Capacity> buffer_{};

    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};

    bool push(T item)
    {
        // Step 1: read current head (only producer touches head, so relaxed is fine)
        std::size_t current_head = head_.load(std::memory_order_relaxed);

        // Step 2: calculate where head would be after this push
        std::size_t next_head = (current_head + 1) & kMask;

        // Step 3: check if buffer is full by comparing next_head to tail
        // acquire because we need to see the latest tail the consumer wrote
        if (next_head == tail_.load(std::memory_order_acquire)) {
            return false; // full, no room
        }

        // Step 4: write the item into the current head slot
        buffer_[current_head] = std::move(item);

        // Step 5: publish the new head so consumer can see the item
        // release so the consumer sees the data we wrote in step 4
        head_.store(next_head, std::memory_order_release);

        return true;
    }

    std::optional<T> pop()
    {
        std::size_t curren_tail = tail_.load(std::memory_order_realxed);

        if(current_tail == head_.load(std::memory_order_acquire))
        {
            return std::nullopt;
        }

        T item = std::move(buffer_[current_tail]);

        tail_.store((current_tail + 1) & kMask, std::memory_order_release);

        return item;
    }
};