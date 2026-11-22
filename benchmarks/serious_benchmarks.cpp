#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <mach/mach_time.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "conveyor_ring.hpp"

struct PacedPkt {
    uint64_t scheduled_ticks;
    uint64_t seq;
};

struct Order32 {
    uint64_t seq;
    uint64_t tag;
    double price;
    uint64_t qty;
};
static_assert(sizeof(Order32) == 32);

static inline uint64_t mix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

// ============================================================================
// BENCHMARK 1: PACED ONE-WAY TAIL LATENCY (COORDINATED-OMISSION FREE)
// Measures (t_recv - t_scheduled) across realistic injection rates.
// ============================================================================
static void run_paced_latency(double target_mops, size_t samples = 200'000) {
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    const double ns_per_tick = static_cast<double>(tb.numer) / static_cast<double>(tb.denom);
    const double ticks_per_msg = (1e9 / (target_mops * 1e6)) / ns_per_tick;

    using Ring = ConveyorRing<PacedPkt, 4096>;
    auto* ring = static_cast<Ring*>(mmap(nullptr, sizeof(Ring), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    auto* lats = static_cast<double*>(mmap(nullptr, sizeof(double) * samples, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    new (ring) Ring();

    constexpr size_t WARMUP = 10'000;
    pid_t pid = fork();
    if (pid == 0) {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        auto cons = ring->make_consumer();
        for (size_t i = 0; i < WARMUP; ++i) {
            (void)cons.pop();
        }
        for (size_t i = 0; i < samples; ++i) {
            PacedPkt p = cons.pop();
            uint64_t t_recv = mach_absolute_time();
            int64_t diff = static_cast<int64_t>(t_recv) - static_cast<int64_t>(p.scheduled_ticks);
            lats[i] = diff > 0 ? diff * ns_per_tick : 0.0;
        }
        _exit(0);
    }

    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto prod = ring->make_producer();

    uint64_t base = mach_absolute_time();
    for (size_t i = 0; i < WARMUP + samples; ++i) {
        uint64_t sched = base + static_cast<uint64_t>(i * ticks_per_msg);
        while (mach_absolute_time() < sched) CPU_RELAX();
        prod.push(PacedPkt{sched, i});
    }
    waitpid(pid, nullptr, 0);

    std::sort(lats, lats + samples);
    auto pct = [&](double p) { return lats[std::min(samples - 1, static_cast<size_t>(samples * p))]; };
    std::cout << " " << std::right << std::setw(6) << std::fixed << std::setprecision(1) << target_mops << " M/s"
              << " | " << std::setw(8) << pct(0.50) << " ns"
              << " | " << std::setw(8) << pct(0.90) << " ns"
              << " | " << std::setw(8) << pct(0.99) << " ns"
              << " | " << std::setw(9) << pct(0.999) << " ns"
              << " | " << std::setw(10) << pct(0.9999) << " ns"
              << " | " << std::setw(10) << lats[samples - 1] << " ns\n";

    munmap(ring, sizeof(Ring));
    munmap(lats, sizeof(double) * samples);
}

// ============================================================================
// BENCHMARK 2: MULTI-STAGE PIPELINE SCALING (2, 3, 4 CHAINED OS PROCESSES)
// P0 -> P1 (-> P2 -> P3), 20M x 32-byte messages with keyed-hash verification
// ============================================================================
static void run_pipeline(int stages, uint64_t count = 20'000'000) {
    using Ring = ConveyorRing<Order32, 4096>;
    const int hops = stages - 1;
    auto* rings = static_cast<Ring*>(mmap(nullptr, sizeof(Ring) * hops, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    auto* mismatches = static_cast<uint64_t*>(mmap(nullptr, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    for (int i = 0; i < hops; ++i) new (&rings[i]) Ring();
    *mismatches = 0;

    constexpr uint64_t SEED = 0x517cc1b727220a95ULL;
    std::vector<pid_t> pids;

    // Spawn intermediate relay stages and final consumer stage
    for (int s = 1; s < stages; ++s) {
        pid_t pid = fork();
        if (pid == 0) {
            pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
            auto cons = rings[s - 1].make_consumer();
            if (s < stages - 1) {
                auto prod = rings[s].make_producer();
                for (uint64_t i = 0; i < count; ++i) {
                    Order32 o = cons.pop();
                    o.qty += 1;  // lightweight per-stage work
                    prod.push(o);
                }
            } else {
                uint64_t bad = 0;
                for (uint64_t i = 0; i < count; ++i) {
                    Order32 o = cons.pop();
                    bad += (o.seq != i) | (o.tag != mix64(i ^ SEED)) | (o.qty != static_cast<uint64_t>(stages - 2));
                }
                *mismatches = bad;
            }
            _exit(0);
        }
        pids.push_back(pid);
    }

    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    auto prod = rings[0].make_producer();
    auto t0 = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < count; ++i) {
        prod.push(Order32{i, mix64(i ^ SEED), 199.5, 0});
    }
    for (pid_t p : pids) waitpid(p, nullptr, 0);
    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    double mops = (count / sec) / 1e6;
    double gbps = (count * sizeof(Order32) * hops) / (sec * 1024.0 * 1024.0 * 1024.0);

    std::cout << " " << stages << " processes (" << hops << " hop" << (hops > 1 ? "s" : " ") << ")"
              << " | " << std::right << std::setw(8) << std::fixed << std::setprecision(2) << mops << " Mmsg/s"
              << " | " << std::setw(7) << (sec * 1e9 / count) << " ns/msg"
              << " | " << std::setw(7) << gbps << " GiB/s agg"
              << " | mismatches=" << *mismatches << "\n";

    munmap(rings, sizeof(Ring) * hops);
    munmap(mismatches, sizeof(uint64_t));
}

int main() {
    std::cout << "=======================================================================================\n";
    std::cout << "  1. PACED ONE-WAY TAIL LATENCY (COORDINATED-OMISSION FREE, 200k SAMPLES/RATE)\n";
    std::cout << "=======================================================================================\n";
    std::cout << " Rate       |      p50    |      p90    |      p99    |      p99.9   |     p99.99    |        max\n";
    std::cout << "------------|-------------|-------------|-------------|--------------|---------------|-------------\n";
    for (double rate : {0.5, 1.0, 5.0, 10.0, 20.0}) {
        run_paced_latency(rate);
    }

    std::cout << "\n=======================================================================================\n";
    std::cout << "  2. MULTI-STAGE PIPELINE SCALING (20M x 32B ORDERS, KEYED-HASH VERIFIED)\n";
    std::cout << "=======================================================================================\n";
    for (int stages : {2, 3, 4}) {
        run_pipeline(stages);
    }
    return 0;
}
