#pragma once
/**
 * block_device.hpp — StoreMesh
 *
 * An in-memory block device that simulates an NVMe SSD.
 *
 * KEY DESIGN DECISION — std::shared_mutex:
 *   Reads are concurrent (shared_lock): multiple threads can read different
 *   LBA ranges simultaneously, just like an NVMe drive handles parallel
 *   read commands from different submission queue pairs.
 *
 *   Writes are exclusive (unique_lock): a write must not race with any read
 *   or other write.  On a real NVMe SSD, the controller serialises writes to
 *   the same LBA internally; here we model that with the exclusive lock.
 *
 * INTERVIEW POINT:
 *   "Why not just use a regular mutex?"
 *   → With shared_mutex, a read-heavy workload (typical storage read path)
 *     can have many concurrent readers without head-of-line blocking.  This
 *     is the same reason ZFS uses rwlocks in its ARC eviction path.
 */

#include "io_types.hpp"

#include <atomic>
#include <shared_mutex>
#include <string>
#include <vector>

class BlockDevice {
public:
    explicit BlockDevice(std::size_t num_blocks,
                         std::string device_id = "nvme0n1");

    // Read [lba, lba+num_blocks) into buffer.
    // Thread-safe: multiple concurrent reads allowed.
    bool read(uint64_t lba, uint32_t num_blocks,
              std::vector<uint8_t>& buffer);

    // Write data to [lba, lba+num_blocks).
    // Thread-safe: exclusive; no concurrent reads or writes.
    bool write(uint64_t lba, uint32_t num_blocks,
               const std::vector<uint8_t>& data);

    // Flush: no-op for in-memory device; models the NVMe FLUSH command
    // that drains write caches on a real SSD.
    bool flush();

    // Mirrors NVMe Identify Controller / Identify Namespace responses
    struct IdentifyInfo {
        std::string device_id;
        std::size_t num_blocks;
        std::size_t block_size;
        std::size_t total_bytes;
        uint64_t    read_ops;
        uint64_t    write_ops;
    };
    IdentifyInfo identify() const;  // const: safe to call on const BlockDevice&

    std::size_t capacity_blocks() const { return num_blocks_; }

private:
    bool is_valid_range(uint64_t lba, uint32_t count) const;

    std::string                          device_id_;
    std::size_t                          num_blocks_;
    std::vector<std::vector<uint8_t>>    blocks_;      // blocks_[lba][byte]
    mutable std::shared_mutex            rw_mutex_;
    std::atomic<uint64_t>                read_ops_{0};
    std::atomic<uint64_t>                write_ops_{0};
};
