/**
 * main.cpp — StoreMesh Demonstration
 *
 * Runs four progressive demos that exercise every layer of the engine:
 *
 *   Demo 1 — Basic replicated R/W  (correctness)
 *   Demo 2 — Concurrent writes     (multi-threading + data integrity)
 *   Demo 3 — Throughput benchmark  (performance measurement)
 *   Demo 4 — Primary failure + failover  (distributed fault-tolerance)
 *
 * Each demo is self-contained and prints results you can talk through in
 * a Pure Storage interview.
 */

#include "replication_manager.hpp"
#include "ring_buffer.hpp"        // demonstrate the lock-free SPSC separately

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

// ─── Helpers ──────────────────────────────────────────────────────────────────

static std::vector<uint8_t> make_data(std::size_t num_blocks, uint8_t fill) {
    return std::vector<uint8_t>(num_blocks * kBlockSize, fill);
}

static bool verify(const std::vector<uint8_t>& buf,
                   std::size_t num_blocks, uint8_t expected) {
    for (std::size_t i = 0; i < num_blocks * kBlockSize; ++i) {
        if (buf[i] != expected) return false;
    }
    return true;
}

static void separator(const std::string& title) {
    std::cout << "\n\033[1;34m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n";
    std::cout << "\033[1m  " << title << "\033[0m\n";
    std::cout << "\033[1;34m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n\n";
}

// ─── Demo 1: Basic Read/Write ─────────────────────────────────────────────────

void demo_basic_rw(ReplicationManager& rm) {
    separator("Demo 1 — Basic Replicated Read / Write");

    // Write 4 blocks of 0xAB at LBA 0
    auto wr = make_data(4, 0xAB);
    bool ok = rm.write(0, 4, wr);
    std::cout << "  write(lba=0, 4 blocks, 0xAB): " << (ok ? "OK" : "FAIL") << "\n";

    // Read back and verify
    std::vector<uint8_t> rd;
    ok = rm.read(0, 4, rd);
    bool pass = verify(rd, 4, 0xAB);
    std::cout << "  read (lba=0, 4 blocks):        " << (ok ? "OK" : "FAIL")
              << "  verify: " << (pass ? "\033[32mPASS\033[0m" : "\033[31mFAIL\033[0m") << "\n";

    // Write to a different LBA
    auto wr2 = make_data(2, 0xFF);
    ok = rm.write(200, 2, wr2);
    std::cout << "  write(lba=200, 2 blocks, 0xFF):" << (ok ? "OK" : "FAIL") << "\n";

    std::vector<uint8_t> rd2;
    ok = rm.read(200, 2, rd2);
    pass = verify(rd2, 2, 0xFF);
    std::cout << "  read (lba=200, 2 blocks):       " << (ok ? "OK" : "FAIL")
              << "  verify: " << (pass ? "\033[32mPASS\033[0m" : "\033[31mFAIL\033[0m") << "\n";
}

// ─── Demo 2: Concurrent Writes ────────────────────────────────────────────────

void demo_concurrent_writes(ReplicationManager& rm) {
    separator("Demo 2 — Concurrent Multi-Threaded Writes");

    constexpr int kThreads       = 8;
    constexpr int kWritesPerThread = 50;

    std::atomic<int> success_count{0};
    std::atomic<int> verify_fail{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    const auto t0 = std::chrono::steady_clock::now();

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kWritesPerThread; ++i) {
                // Each thread owns a non-overlapping LBA range
                uint64_t lba = static_cast<uint64_t>(
                    (t * kWritesPerThread + i) * 2);
                uint8_t fill = static_cast<uint8_t>(t + 1);
                auto data = make_data(2, fill);

                if (rm.write(lba, 2, data)) {
                    success_count.fetch_add(1, std::memory_order_relaxed);

                    // Read back immediately to verify no data corruption
                    std::vector<uint8_t> rd;
                    if (rm.read(lba, 2, rd)) {
                        if (!verify(rd, 2, fill)) {
                            verify_fail.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }
            }
        });
    }
    for (auto& th : threads) th.join();

    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    const int total = kThreads * kWritesPerThread;
    std::cout << "  Threads:       " << kThreads << "\n";
    std::cout << "  Writes each:   " << kWritesPerThread << "\n";
    std::cout << "  Total writes:  " << total << "\n";
    std::cout << "  Successful:    " << success_count.load() << "\n";
    std::cout << "  Verify fails:  " << verify_fail.load()
              << (verify_fail.load() == 0 ? "  \033[32m(no corruption)\033[0m" : "  \033[31m(DATA CORRUPTED!)\033[0m") << "\n";
    std::cout << "  Wall time:     " << elapsed_ms << " ms\n";
    const long throughput = (elapsed_ms > 0)
        ? (success_count.load() * 1000L / elapsed_ms) : 0;
    std::cout << "  Throughput:    " << throughput << " writes/sec\n";
}

// ─── Demo 3: Throughput Benchmark ─────────────────────────────────────────────

void demo_benchmark(ReplicationManager& rm) {
    separator("Demo 3 — Sequential Write Throughput Benchmark");

    constexpr int kOps = 2000;
    auto data = make_data(1, 0x42);

    const auto t0 = std::chrono::steady_clock::now();
    int ok_count = 0;
    for (int i = 0; i < kOps; ++i) {
        if (rm.write(static_cast<uint64_t>(i % 1000), 1, data)) ++ok_count;
    }
    const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();

    std::cout << "  Total writes:  " << kOps << "\n";
    std::cout << "  Successful:    " << ok_count << "\n";
    std::cout << "  Total time:    " << elapsed_us << " µs\n";
    if (ok_count > 0) {
        std::cout << "  Avg latency:   " << (elapsed_us / ok_count) << " µs/write\n";
        const long tp = (elapsed_us > 0)
            ? (ok_count * 1'000'000L / elapsed_us) : 0;
        std::cout << "  Throughput:    " << tp << " writes/sec\n";
    }
}

// ─── Demo 4: Primary Failure + Failover ───────────────────────────────────────

void demo_failover(ReplicationManager& rm) {
    separator("Demo 4 — Primary Node Failure & Automatic Failover");

    // 1. Baseline write before any failure
    auto pre = make_data(1, 0xAA);
    bool ok = rm.write(5000, 1, pre);
    std::cout << "  Pre-failure write (lba=5000):   " << (ok ? "OK" : "FAIL") << "\n";

    // 2. Inject failure — heartbeats to primary stop
    std::cout << "\n  Injecting primary failure...\n";
    rm.simulate_failure("node-primary");

    std::cout << "  Waiting for heartbeat timeout (≈3 s)...\n";
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // 3. Write after timeout — should auto-failover to replica
    auto post = make_data(1, 0xBB);
    ok = rm.write(5001, 1, post);
    std::cout << "  Post-failover write (lba=5001):  " << (ok ? "\033[32mOK (failover succeeded)\033[0m"
                                                                : "\033[31mFAIL\033[0m") << "\n";

    // 4. Read back to verify data integrity across failover
    std::vector<uint8_t> rd;
    ok = rm.read(5001, 1, rd);
    bool pass = verify(rd, 1, 0xBB);
    std::cout << "  Read after failover (lba=5001):  " << (ok ? "OK" : "FAIL")
              << "  verify: " << (pass ? "\033[32mPASS\033[0m" : "\033[31mFAIL\033[0m") << "\n";

    rm.print_status();
}

// ─── Demo 5: Lock-free Ring Buffer (SPSC) ────────────────────────────────────

void demo_ring_buffer() {
    separator("Demo 5 — Lock-Free SPSC Ring Buffer (NVMe SQ/CQ model)");

    RingBuffer<int, 16> rb; // 16-entry ring, like a small NVMe SQ
    std::atomic<int> sum_produced{0};
    std::atomic<int> sum_consumed{0};
    constexpr int N = 10'000;

    // Producer thread: pushes integers 0..N-1
    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            // Spin until there is space (back-pressure, like NVMe SQ full)
            while (!rb.push(i)) {
                std::this_thread::yield();
            }
            sum_produced.fetch_add(i, std::memory_order_relaxed);
        }
    });

    // Consumer thread: pops and sums
    std::thread consumer([&] {
        int consumed = 0;
        while (consumed < N) {
            auto val = rb.pop();
            if (val) {
                sum_consumed.fetch_add(*val, std::memory_order_relaxed);
                ++consumed;
            } else {
                std::this_thread::yield();
            }
        }
    });

    producer.join();
    consumer.join();

    const bool match = (sum_produced.load() == sum_consumed.load());
    std::cout << "  Items transferred: " << N << "\n";
    std::cout << "  Sum produced:  " << sum_produced.load() << "\n";
    std::cout << "  Sum consumed:  " << sum_consumed.load() << "\n";
    std::cout << "  Integrity:     "
              << (match ? "\033[32mPASS (no lost/duplicate items)\033[0m"
                        : "\033[31mFAIL\033[0m") << "\n";
}

// ─── Main ─────────────────────────────────────────────────────────────────────

int main() {
    std::cout << "\033[1m";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║         StoreMesh — Distributed Block Storage Engine         ║\n";
    std::cout << "║   Multi-Threaded  ·  NVMe-Inspired  ·  Replicated           ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << "\033[0m\n";

    // 3-node cluster (1 primary + 2 replicas), 10 000 blocks ≈ 5 MB each
    ReplicationManager cluster(10'000, 2);
    cluster.print_status();

    demo_basic_rw(cluster);
    cluster.print_status();

    demo_concurrent_writes(cluster);
    cluster.print_status();

    demo_benchmark(cluster);
    cluster.print_status();

    demo_ring_buffer();

    demo_failover(cluster); // last: includes a 4-second sleep

    std::cout << "\033[1;32m\nAll demos complete.\033[0m\n\n";
    return 0;
}
