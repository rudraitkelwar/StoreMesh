#include "block_device.hpp"

#include <cstring>   // memcpy
#include <mutex>
#include <stdexcept>

BlockDevice::BlockDevice(std::size_t num_blocks, std::string device_id)
    : device_id_(std::move(device_id))
    , num_blocks_(num_blocks)
    // Pre-allocate all blocks, zero-initialised (mimics freshly erased NAND)
    , blocks_(num_blocks, std::vector<uint8_t>(kBlockSize, 0x00))
{}

bool BlockDevice::is_valid_range(uint64_t lba, uint32_t count) const {
    return count > 0 && (lba + count) <= num_blocks_;
}

bool BlockDevice::read(uint64_t lba, uint32_t num_blocks,
                       std::vector<uint8_t>& buffer)
{
    // Shared lock: concurrent reads are allowed — no writer can be active
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);

    if (!is_valid_range(lba, num_blocks)) return false;

    buffer.resize(static_cast<std::size_t>(num_blocks) * kBlockSize);
    for (uint32_t i = 0; i < num_blocks; ++i) {
        std::memcpy(buffer.data() + i * kBlockSize,
                    blocks_[lba + i].data(),
                    kBlockSize);
    }
    read_ops_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool BlockDevice::write(uint64_t lba, uint32_t num_blocks,
                        const std::vector<uint8_t>& data)
{
    if (data.size() < static_cast<std::size_t>(num_blocks) * kBlockSize)
        return false;

    // Exclusive lock: no concurrent reads or writes during a write
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    if (!is_valid_range(lba, num_blocks)) return false;

    for (uint32_t i = 0; i < num_blocks; ++i) {
        std::memcpy(blocks_[lba + i].data(),
                    data.data() + i * kBlockSize,
                    kBlockSize);
    }
    write_ops_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool BlockDevice::flush() {
    // In-memory: no persistent write cache to flush.
    // On a real NVMe SSD, FLUSH (opcode 0x00) drains the volatile write
    // cache to persistent media.  We model the interface without the work.
    return true;
}

BlockDevice::IdentifyInfo BlockDevice::identify() const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    return IdentifyInfo{
        device_id_,
        num_blocks_,
        kBlockSize,
        num_blocks_ * kBlockSize,
        read_ops_.load(std::memory_order_relaxed),
        write_ops_.load(std::memory_order_relaxed)
    };
}
