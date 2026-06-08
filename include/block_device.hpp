#pragma once
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <string>
#include <cstdint>
#include <cstring>

inline constexpr std::size_t  kBlockSize = 512;

class BlockDevice
{
    public:
        explicit BlockDevice(std::size_t num_blocks, std::string device_id = "nvme0n1");


        bool read(uint64_t lba, uint32_t num_blocks, std::vector<uint8_t>& buffer);

        bool write(uint64_t lba, uint32_t num_blocks, const std::vector<uint8_t>& data);

        bool flush();

        
    private:

        bool is_valid_range(uint64_t lba, uint32_t count) const;
        std::string                       device_id_;
        std::size_t                       num_blocks_;
        std::vector<std::vector<uint8_t>> blocks_;
        mutable std::shared_mutex         rw_mutex_;
        std::atomic<uint64_t>             read_ops_{0};
        std::atomic<uint64_t>             write_ops_{0};

};