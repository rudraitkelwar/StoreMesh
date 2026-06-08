#pragma once
/**
 * io_types.hpp — StoreMesh
 *
 * NVMe-inspired I/O command and completion structures.
 *
 * REAL NVMe PARALLEL:
 *   NVMe Command Dword 0 contains: Opcode (READ=0x02, WRITE=0x01, FLUSH=0x00),
 *   PSDT, FUSE, CID (command ID).  SLBA (Starting LBA) lives in Dword 10-11.
 *   NLB (Number of Logical Blocks) is in Dword 12.
 *
 *   Our IOCommand maps directly to these concepts, minus the PCIe register
 *   layout.  This is the level of abstraction SPDK works at when it bypasses
 *   the kernel and submits commands directly to the NVMe SQ doorbell.
 */

#include <cstdint>
#include <functional>
#include <vector>

// Block size: 512 bytes (one NVMe logical block at default LBA format)
inline constexpr std::size_t kBlockSize = 512;

// I/O opcodes — matches NVMe Admin/IO Command Set opcodes
enum class IOOpcode : uint8_t {
    FLUSH = 0x00,
    WRITE = 0x01,
    READ  = 0x02,
};

// Command submitted by the host to the Submission Queue
struct IOCommand {
    uint16_t  command_id;   // Unique ID; echoed back in the completion
    IOOpcode  opcode;
    uint64_t  lba;          // Starting Logical Block Address
    uint32_t  num_blocks;   // Number of 512-byte blocks to transfer

    // Payload for WRITE commands (must be num_blocks * kBlockSize bytes)
    std::vector<uint8_t> data;

    // Optional async callback: (success, status_code)
    // Called on the worker thread that processed the command.
    std::function<void(bool, uint32_t)> callback;
};

// Completion entry posted by the device to the Completion Queue
struct IOCompletion {
    uint16_t  command_id;   // Matches the originating IOCommand
    bool      success;
    uint32_t  status_code;  // 0 = success; non-zero = error
    uint64_t  latency_ns;   // Wall-clock time from dispatch to completion
};
