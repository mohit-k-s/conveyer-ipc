// 5 Cross-Domain Real-World Experiments: ConveyorRing vs Standard OS IPC (AF_UNIX / pipe)
// Domains:
//   1. Pro Audio / DAW Out-of-Process DSP Plugin (zero-xrun 192kHz frames)
//   2. Autonomous Drone / Rocket GNC Safety Kill-Switch (abort reaction time)
//   3. Quant Finance Out-of-Process Pre-Trade Risk Firewall (64B orders)
//   4. Cybersecurity Inline DDoS Packet-Header Filter (Mpps)
//   5. AI Speculative Decoding Draft -> Verifier 1-by-1 Token Stream
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <mach/mach_time.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#include "conveyor_ring.hpp"

static constexpr uint64_t N = 1'000'000;

struct AudioFrame { uint64_t sample_idx; float left; float right; float gain; uint32_t flags; };
struct ImuFrame   { uint64_t ts_ticks; uint64_t seq; float pitch; float roll; float yaw; float accel_z; uint64_t pad[4]; };
struct Order64    { uint64_t id; uint64_t ts; double price; uint32_t qty; uint32_t side; char sym[8]; uint64_t pad[3]; };
struct PktHdr64   { uint64_t seq; uint32_t src_ip; uint32_t dst_ip; uint16_t src_port; uint16_t dst_port; uint32_t tcp_flags; uint64_t pad[5]; };
struct DraftTok   { uint64_t req_id; uint32_t token_id; uint32_t pos; float logprob; uint32_t flags; };

static_assert(sizeof(Order64) == 64 && sizeof(PktHdr64) == 64 && sizeof(ImuFrame) == 64);

template <typename T, typename WorkFn>
static double bench_conveyor(uint64_t count, WorkFn&& work) {
    using Ring = ConveyorRing<T, 4096>;
    auto* ring = static_cast<Ring*>(mmap(nullptr, sizeof(Ring), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    auto* acc = static_cast<uint64_t*>(mmap(nullptr, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    new (ring) Ring();
    *acc = 0;

    pid_t pid = fork();
    if (pid == 0) {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        auto c = ring->make_consumer();
        uint64_t sum = 0;
        for (uint64_t i = 0; i < count; ++i) sum += work(c.pop());
        *acc = sum;
        _exit(0);
    }
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    auto p = ring->make_producer();
    auto t0 = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < count; ++i) {
        T item{};
        *reinterpret_cast<uint64_t*>(&item) = i;
        p.push(item);
    }
    waitpid(pid, nullptr, 0);
    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    munmap(ring, sizeof(Ring));
    munmap(acc, sizeof(uint64_t));
    return sec;
}

template <typename T, typename WorkFn>
static double bench_unix_socket(uint64_t count, WorkFn&& work) {
    int fds[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    auto* acc = static_cast<uint64_t*>(mmap(nullptr, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    *acc = 0;

    pid_t pid = fork();
    if (pid == 0) {
        close(fds[0]);
        uint64_t sum = 0;
        T item{};
        for (uint64_t i = 0; i < count; ++i) {
            size_t got = 0;
            while (got < sizeof(T)) {
                ssize_t r = read(fds[1], reinterpret_cast<char*>(&item) + got, sizeof(T) - got);
                if (r <= 0) break;
                got += static_cast<size_t>(r);
            }
            sum += work(item);
        }
        *acc = sum;
        close(fds[1]);
        _exit(0);
    }
    close(fds[1]);
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    auto t0 = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < count; ++i) {
        T item{};
        *reinterpret_cast<uint64_t*>(&item) = i;
        size_t sent = 0;
        while (sent < sizeof(T)) {
            ssize_t w = write(fds[0], reinterpret_cast<const char*>(&item) + sent, sizeof(T) - sent);
            if (w <= 0) break;
            sent += static_cast<size_t>(w);
        }
    }
    waitpid(pid, nullptr, 0);
    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    close(fds[0]);
    munmap(acc, sizeof(uint64_t));
    return sec;
}

// Domain 2: Aerospace GNC Kill-Switch Reaction Time (ConveyorRing vs POSIX pipe)
static void exp2_rocket_gnc_killswitch() {
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    const double ns_per_tick = static_cast<double>(tb.numer) / static_cast<double>(tb.denom);

    // A. ConveyorRing
    struct SharedGnc {
        ConveyorRing<ImuFrame, 1024> ring;
        alignas(128) std::atomic<bool> abort_tripped{false};
    };
    auto* sh = static_cast<SharedGnc*>(mmap(nullptr, sizeof(SharedGnc), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    new (sh) SharedGnc();
    pid_t pid = fork();
    if (pid == 0) {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        auto c = sh->ring.make_consumer();
        while (true) {
            ImuFrame f = c.pop();
            if (f.pitch > 45.0f) {
                sh->abort_tripped.store(true, std::memory_order_release);
                break;
            }
        }
        _exit(0);
    }
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    auto p = sh->ring.make_producer();
    for (uint64_t i = 0; i < 10000; ++i) p.push(ImuFrame{0, i, 2.0f, 0.0f, 0.0f, 9.8f, {}});
    uint64_t t0 = mach_absolute_time();
    p.push(ImuFrame{t0, 10000, 62.5f, 0.0f, 0.0f, 9.8f, {}});
    while (!sh->abort_tripped.load(std::memory_order_acquire)) CPU_RELAX();
    double conv_ns = (mach_absolute_time() - t0) * ns_per_tick;
    waitpid(pid, nullptr, 0);
    munmap(sh, sizeof(SharedGnc));

    // B. POSIX pipe
    int p2c[2], c2p[2];
    pipe(p2c); pipe(c2p);
    pid = fork();
    if (pid == 0) {
        close(p2c[1]); close(c2p[0]);
        ImuFrame f{};
        while (read(p2c[0], &f, sizeof(f)) == sizeof(f)) {
            if (f.pitch > 45.0f) {
                uint8_t abort_byte = 1;
                (void)write(c2p[1], &abort_byte, 1);
                break;
            }
        }
        _exit(0);
    }
    close(p2c[0]); close(c2p[1]);
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    for (uint64_t i = 0; i < 10000; ++i) {
        ImuFrame f{0, i, 2.0f, 0.0f, 0.0f, 9.8f, {}};
        (void)write(p2c[1], &f, sizeof(f));
    }
    uint64_t tp0 = mach_absolute_time();
    ImuFrame bad{tp0, 10000, 62.5f, 0.0f, 0.0f, 9.8f, {}};
    (void)write(p2c[1], &bad, sizeof(bad));
    uint8_t ack = 0;
    (void)read(c2p[0], &ack, 1);
    double pipe_ns = (mach_absolute_time() - tp0) * ns_per_tick;
    waitpid(pid, nullptr, 0);
    close(p2c[1]); close(c2p[0]);

    std::cout << "2. Aerospace / Drone GNC Kill-Switch (64B IMU -> Abort Trip RTT):\n"
              << "   ConveyorRing : " << std::fixed << std::setprecision(1) << std::setw(8) << conv_ns << " ns\n"
              << "   POSIX pipe   : " << std::setw(8) << pipe_ns << " ns\n"
              << "   Speedup      : " << std::setprecision(1) << (pipe_ns / conv_ns) << "x faster abort reaction\n\n";
}

int main() {
    std::cout << "================================================================================\n";
    std::cout << "  5 CROSS-DOMAIN REAL-WORLD EXPERIMENTS: ConveyorRing vs OS IPC (1M msgs each)\n";
    std::cout << "================================================================================\n\n";

    // 1. Pro Audio / DAW DSP Plugin
    {
        auto work = [](const AudioFrame& a) -> uint64_t { return a.sample_idx + static_cast<uint64_t>(a.left > 0.5f); };
        double c = bench_conveyor<AudioFrame>(N, work);
        double u = bench_unix_socket<AudioFrame>(N, work);
        std::cout << "1. Pro Audio / DAW Out-of-Process DSP Plugin (24B stereo frames):\n"
                  << "   ConveyorRing : " << std::fixed << std::setprecision(2) << (N / c) / 1e6 << " M frames/s (" << c << "s)\n"
                  << "   AF_UNIX sock : " << (N / u) / 1e6 << " M frames/s (" << u << "s)\n"
                  << "   Speedup      : " << std::setprecision(1) << (u / c) << "x faster\n\n";
    }

    // 2. Aerospace / Drone GNC Kill-Switch
    exp2_rocket_gnc_killswitch();

    // 3. Quant Finance Pre-Trade Risk Firewall
    {
        auto work = [](const Order64& o) -> uint64_t { return (o.price * o.qty < 1e7) ? o.id : 0; };
        double c = bench_conveyor<Order64>(N, work);
        double u = bench_unix_socket<Order64>(N, work);
        std::cout << "3. Quant Finance Out-of-Process Pre-Trade Risk Firewall (64B orders):\n"
                  << "   ConveyorRing : " << std::fixed << std::setprecision(2) << (N / c) / 1e6 << " M orders/s (" << c << "s)\n"
                  << "   AF_UNIX sock : " << (N / u) / 1e6 << " M orders/s (" << u << "s)\n"
                  << "   Speedup      : " << std::setprecision(1) << (u / c) << "x faster\n\n";
    }

    // 4. Cybersecurity Inline DDoS Packet-Header Filter
    {
        auto work = [](const PktHdr64& p) -> uint64_t { return p.seq ^ p.src_ip ^ p.tcp_flags; };
        double c = bench_conveyor<PktHdr64>(N, work);
        double u = bench_unix_socket<PktHdr64>(N, work);
        std::cout << "4. Cybersecurity Inline DDoS Packet-Header Filter (64B pkt descriptors):\n"
                  << "   ConveyorRing : " << std::fixed << std::setprecision(2) << (N / c) / 1e6 << " Mpps (" << c << "s)\n"
                  << "   AF_UNIX sock : " << (N / u) / 1e6 << " Mpps (" << u << "s)\n"
                  << "   Speedup      : " << std::setprecision(1) << (u / c) << "x faster\n\n";
    }

    // 5. AI Speculative Decoding Draft -> Verifier 1-by-1 Token Stream
    {
        auto work = [](const DraftTok& t) -> uint64_t { return t.req_id + t.token_id; };
        double c = bench_conveyor<DraftTok>(N, work);
        double u = bench_unix_socket<DraftTok>(N, work);
        std::cout << "5. AI Speculative Decoding (Draft -> Verifier 1-by-1 24B tokens):\n"
                  << "   ConveyorRing : " << std::fixed << std::setprecision(2) << (N / c) / 1e6 << " M tok/s (" << c << "s)\n"
                  << "   AF_UNIX sock : " << (N / u) / 1e6 << " M tok/s (" << u << "s)\n"
                  << "   Speedup      : " << std::setprecision(1) << (u / c) << "x faster\n";
    }
    return 0;
}
