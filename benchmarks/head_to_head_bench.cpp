#include "conveyor_ring.hpp"
#include "contenders.hpp"
#include <boost/lockfree/spsc_queue.hpp>

#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>
#include <numeric>
#include <iomanip>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <mach/mach_time.h>

static constexpr size_t QUEUE_CAP = 2048;

// Wrapper for Boost SPSC queue to match push/pop API
template <typename T, size_t Cap>
class BoostSPSCWrapper {
    boost::lockfree::spsc_queue<T, boost::lockfree::capacity<Cap>> queue_;
public:
    inline void push(const T& val) {
        while (!queue_.push(val)) {
            CPU_RELAX();
        }
    }
    inline T pop() {
        T val;
        while (!queue_.pop(val)) {
            CPU_RELAX();
        }
        return val;
    }
};

// Wrapper for Conveyor Ring to match push/pop API
template <typename T, size_t Cap>
class ConveyorWrapper {
    ConveyorRing<T, Cap> ring_;
public:
    auto make_producer() { return ring_.make_producer(); }
    auto make_consumer() { return ring_.make_consumer(); }
};

struct FinancialOrder {
    uint64_t order_id;
    uint64_t timestamp;
    double price;
    uint32_t qty;
    char symbol[4];
};

struct LatencyPkt {
    uint64_t t0;
    uint64_t seq;
};

void print_banner(const std::string& title) {
    std::cout << "\n=========================================================================================" << std::endl;
    std::cout << "  " << title << std::endl;
    std::cout << "=========================================================================================" << std::endl;
}

// ============================================================================
// BENCHMARK 1: SUSTAINED THROUGHPUT BLAST (10,000,000 FINANCIAL ORDERS)
// ============================================================================
template <typename QueueType>
void bench_throughput(const char* name, uint64_t total_orders = 10'000'000) {
    QueueType* q = (QueueType*)mmap(nullptr, sizeof(QueueType), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    new (q) QueueType();

    pid_t pid = fork();
    if (pid == 0) {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        uint64_t checksum = 0;

        if constexpr (std::is_same_v<QueueType, ConveyorWrapper<FinancialOrder, QUEUE_CAP>>) {
            auto consumer = q->make_consumer();
            for (uint64_t i = 0; i < total_orders; ++i) {
                checksum += consumer.pop().order_id;
            }
        } else {
            for (uint64_t i = 0; i < total_orders; ++i) {
                checksum += q->pop().order_id;
            }
        }
        asm volatile("" : : "r"(checksum) : "memory");
        _exit(0);
    } else {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        auto start = std::chrono::steady_clock::now();

        if constexpr (std::is_same_v<QueueType, ConveyorWrapper<FinancialOrder, QUEUE_CAP>>) {
            auto producer = q->make_producer();
            for (uint64_t i = 0; i < total_orders; ++i) {
                producer.push({ i, 1234567ULL, 199.50, 100, {'A','A','P','L'} });
            }
        } else {
            for (uint64_t i = 0; i < total_orders; ++i) {
                q->push({ i, 1234567ULL, 199.50, 100, {'A','A','P','L'} });
            }
        }

        waitpid(pid, nullptr, 0);
        auto end = std::chrono::steady_clock::now();

        double sec = std::chrono::duration<double>(end - start).count();
        double mops = (total_orders / sec) / 1e6;
        double ns_op = (sec * 1e9) / total_orders;

        std::cout << " " << std::left << std::setw(34) << name 
                  << " | " << std::right << std::setw(10) << std::fixed << std::setprecision(2) << mops << " Mmsgs/s"
                  << " | " << std::right << std::setw(8) << std::fixed << std::setprecision(2) << ns_op << " ns/op"
                  << " | " << std::right << std::setw(8) << std::fixed << std::setprecision(3) << sec << " s"
                  << std::endl;
    }
    munmap(q, sizeof(QueueType));
}

// ============================================================================
// BENCHMARK 2: STOP-AND-WAIT PING-PONG LATENCY (100,000 ROUNDS)
// ============================================================================
template <typename QueueType>
void bench_pingpong_latency(const char* name, size_t rounds = 100'000) {
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);


    QueueType* p2c = (QueueType*)mmap(nullptr, sizeof(QueueType), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    QueueType* c2p = (QueueType*)mmap(nullptr, sizeof(QueueType), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    new (p2c) QueueType();
    new (c2p) QueueType();

    double* rtts = (double*)mmap(nullptr, sizeof(double) * rounds, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);

    static constexpr size_t WARMUP = 5'000;
    size_t total = WARMUP + rounds;

    pid_t pid = fork();
    if (pid == 0) {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);

        if constexpr (std::is_same_v<QueueType, ConveyorWrapper<LatencyPkt, 64>>) {
            auto consumer = p2c->make_consumer();
            auto producer = c2p->make_producer();
            for (size_t i = 0; i < total; ++i) {
                LatencyPkt pkt = consumer.pop();
                producer.push(pkt);
            }
        } else {
            for (size_t i = 0; i < total; ++i) {
                LatencyPkt pkt = p2c->pop();
                c2p->push(pkt);
            }
        }
        _exit(0);
    } else {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        if constexpr (std::is_same_v<QueueType, ConveyorWrapper<LatencyPkt, 64>>) {
            auto producer = p2c->make_producer();
            auto consumer = c2p->make_consumer();
            for (size_t i = 0; i < WARMUP; ++i) {
                producer.push({ mach_absolute_time(), i });
                consumer.pop();
            }
            for (size_t i = 0; i < rounds; ++i) {
                uint64_t t0 = mach_absolute_time();
                producer.push({ t0, i });
                LatencyPkt echo = consumer.pop();
                uint64_t t1 = mach_absolute_time();
                rtts[i] = ((t1 - t0) * tb.numer) / (double)tb.denom;
            }
        } else {
            for (size_t i = 0; i < WARMUP; ++i) {
                p2c->push({ mach_absolute_time(), i });
                c2p->pop();
            }
            for (size_t i = 0; i < rounds; ++i) {
                uint64_t t0 = mach_absolute_time();
                p2c->push({ t0, i });
                LatencyPkt echo = c2p->pop();
                uint64_t t1 = mach_absolute_time();
                rtts[i] = ((t1 - t0) * tb.numer) / (double)tb.denom;
            }
        }

        waitpid(pid, nullptr, 0);

        std::sort(rtts, rtts + rounds);
        double min_rtt = rtts[0];
        double p50_rtt = rtts[(size_t)(rounds * 0.50)];
        double p99_rtt = rtts[(size_t)(rounds * 0.99)];
        double mean_rtt = std::accumulate(rtts, rtts + rounds, 0.0) / rounds;

        std::cout << " " << std::left << std::setw(34) << name 
                  << " | " << std::right << std::setw(9) << std::fixed << std::setprecision(1) << min_rtt << " ns"
                  << " | " << std::right << std::setw(9) << std::fixed << std::setprecision(1) << p50_rtt << " ns"
                  << " | " << std::right << std::setw(9) << std::fixed << std::setprecision(1) << p99_rtt << " ns"
                  << " | " << std::right << std::setw(9) << std::fixed << std::setprecision(1) << mean_rtt << " ns"
                  << " | " << std::right << std::setw(10) << std::fixed << std::setprecision(1) << (p50_rtt / 2.0) << " ns"
                  << std::endl;
    }
    munmap(p2c, sizeof(QueueType));
    munmap(c2p, sizeof(QueueType));
    munmap(rtts, sizeof(double) * rounds);
}

// ============================================================================
// BENCHMARK 3: THROUGHPUT UNDER BACKPRESSURE (100ns STALL EVERY 1,000 ORDERS)
// ============================================================================
template <typename QueueType>
void bench_backpressure(const char* name, uint64_t total_orders = 5'000'000) {
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    uint64_t stall_ticks = (100 * tb.denom) / tb.numer;

    QueueType* q = (QueueType*)mmap(nullptr, sizeof(QueueType), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    new (q) QueueType();

    pid_t pid = fork();
    if (pid == 0) {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        uint64_t checksum = 0;

        if constexpr (std::is_same_v<QueueType, ConveyorWrapper<FinancialOrder, QUEUE_CAP>>) {
            auto consumer = q->make_consumer();
            for (uint64_t i = 0; i < total_orders; ++i) {
                checksum += consumer.pop().order_id;
                if ((i % 1000) == 0) {
                    uint64_t st = mach_absolute_time();
                    while ((mach_absolute_time() - st) < stall_ticks) CPU_RELAX();
                }
            }
        } else {
            for (uint64_t i = 0; i < total_orders; ++i) {
                checksum += q->pop().order_id;
                if ((i % 1000) == 0) {
                    uint64_t st = mach_absolute_time();
                    while ((mach_absolute_time() - st) < stall_ticks) CPU_RELAX();
                }
            }
        }
        asm volatile("" : : "r"(checksum) : "memory");
        _exit(0);
    } else {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        auto start = std::chrono::steady_clock::now();

        if constexpr (std::is_same_v<QueueType, ConveyorWrapper<FinancialOrder, QUEUE_CAP>>) {
            auto producer = q->make_producer();
            for (uint64_t i = 0; i < total_orders; ++i) {
                producer.push({ i, 1234567ULL, 199.50, 100, {'A','A','P','L'} });
            }
        } else {
            for (uint64_t i = 0; i < total_orders; ++i) {
                q->push({ i, 1234567ULL, 199.50, 100, {'A','A','P','L'} });
            }
        }

        waitpid(pid, nullptr, 0);
        auto end = std::chrono::steady_clock::now();

        double sec = std::chrono::duration<double>(end - start).count();
        double mops = (total_orders / sec) / 1e6;
        double ns_op = (sec * 1e9) / total_orders;

        std::cout << " " << std::left << std::setw(34) << name 
                  << " | " << std::right << std::setw(10) << std::fixed << std::setprecision(2) << mops << " Mmsgs/s"
                  << " | " << std::right << std::setw(8) << std::fixed << std::setprecision(2) << ns_op << " ns/op"
                  << std::endl;
    }
    munmap(q, sizeof(QueueType));
}

int main() {
    std::cout << "#########################################################################################" << std::endl;
    std::cout << "   HEAD-TO-HEAD AUDIT: IN-SLOT CONVEYOR RING VS INDUSTRY-STANDARD LOCK-FREE QUEUES      " << std::endl;
    std::cout << "   Platform: Apple Silicon M2 (4 Avalanche P-cores @ 3.5 GHz, 128B Cache Lines)          " << std::endl;
    std::cout << "#########################################################################################" << std::endl;

    // =========================================================================
    // TEST 1: STREAMING THROUGHPUT
    // =========================================================================
    print_banner("1. SUSTAINED STREAMING THROUGHPUT (10,000,000 Financial Orders, 32-Byte Payload)");
    std::cout << " " << std::left << std::setw(34) << "Queue Implementation" 
              << " | " << std::right << std::setw(13) << "Throughput" 
              << " | " << std::right << std::setw(11) << "Amortized" 
              << " | " << std::right << std::setw(10) << "Wall Time" 
              << std::endl;
    std::cout << "-----------------------------------|---------------|-------------|------------" << std::endl;

    bench_throughput<ClassicLamportSPSC<FinancialOrder, QUEUE_CAP>>("1. Classic Lamport SPSC (Padded)");
    bench_throughput<BoostSPSCWrapper<FinancialOrder, QUEUE_CAP>>("2. Boost.Lockfree SPSC (1.88)");
    bench_throughput<RigtorpSPSC<FinancialOrder, QUEUE_CAP>>("3. Rigtorp SPSC (Cached Pointers)");
    bench_throughput<VyukovBoundedQueue<FinancialOrder, QUEUE_CAP>>("4. Dmitry Vyukov Bounded Queue");
    bench_throughput<ConveyorWrapper<FinancialOrder, QUEUE_CAP>>("5. In-Slot Conveyor Ring (Ours)");

    // =========================================================================
    // TEST 2: STOP-AND-WAIT PING-PONG LATENCY
    // =========================================================================
    print_banner("2. PING-PONG TRANSIT LATENCY (100,000 Strict Round-Trips)");
    std::cout << " " << std::left << std::setw(34) << "Queue Implementation" 
              << " | " << std::right << std::setw(12) << "Min RTT" 
              << " | " << std::right << std::setw(12) << "p50 RTT" 
              << " | " << std::right << std::setw(12) << "p99 RTT" 
              << " | " << std::right << std::setw(12) << "Mean RTT" 
              << " | " << std::right << std::setw(13) << "Est. One-Way" 
              << std::endl;
    std::cout << "-----------------------------------|--------------|--------------|--------------|--------------|--------------" << std::endl;

    bench_pingpong_latency<ClassicLamportSPSC<LatencyPkt, 64>>("1. Classic Lamport SPSC");
    bench_pingpong_latency<BoostSPSCWrapper<LatencyPkt, 64>>("2. Boost.Lockfree SPSC (1.88)");
    bench_pingpong_latency<RigtorpSPSC<LatencyPkt, 64>>("3. Rigtorp SPSC (Cached Pointers)");
    bench_pingpong_latency<VyukovBoundedQueue<LatencyPkt, 64>>("4. Dmitry Vyukov Bounded Queue");
    bench_pingpong_latency<ConveyorWrapper<LatencyPkt, 64>>("5. In-Slot Conveyor Ring (Ours)");

    // =========================================================================
    // TEST 3: BACKPRESSURE TOLERANCE
    // =========================================================================
    print_banner("3. SUSTAINED THROUGHPUT UNDER BACKPRESSURE (100ns Consumer Stall / 1k Orders)");
    std::cout << " " << std::left << std::setw(34) << "Queue Implementation" 
              << " | " << std::right << std::setw(13) << "Throughput" 
              << " | " << std::right << std::setw(11) << "Amortized" 
              << std::endl;
    std::cout << "-----------------------------------|---------------|-------------" << std::endl;

    bench_backpressure<ClassicLamportSPSC<FinancialOrder, QUEUE_CAP>>("1. Classic Lamport SPSC (Padded)");
    bench_backpressure<BoostSPSCWrapper<FinancialOrder, QUEUE_CAP>>("2. Boost.Lockfree SPSC (1.88)");
    bench_backpressure<RigtorpSPSC<FinancialOrder, QUEUE_CAP>>("3. Rigtorp SPSC (Cached Pointers)");
    bench_backpressure<VyukovBoundedQueue<FinancialOrder, QUEUE_CAP>>("4. Dmitry Vyukov Bounded Queue");
    bench_backpressure<ConveyorWrapper<FinancialOrder, QUEUE_CAP>>("5. In-Slot Conveyor Ring (Ours)");

    std::cout << "\n#########################################################################################" << std::endl;
    std::cout << "   BENCHMARK COMPLETE: All contenders tested under identical OS and hardware conditions. " << std::endl;
    std::cout << "#########################################################################################\n" << std::endl;
    return 0;
}
