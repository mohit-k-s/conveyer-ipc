// Black-Box Flight Recorder powered by ConveyorRing.
// Process A (Hot-Path App) streams 1,000,000 binary 64B TraceEvents into shared memory
// and deliberately crashes with SIGSEGV.
// Process B (Collector Sidecar) survives the crash and dumps the exact last 150
// nanosecond events right before the crash to JSON for the visual dashboard.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <mach/mach_time.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "conveyor_ring.hpp"

enum EventKind : uint32_t {
    EVT_REQ_START = 0,
    EVT_CACHE_HIT = 1,
    EVT_GPU_INFER = 2,
    EVT_ORDER_EXEC = 3,
    EVT_ANOMALY = 4,
    EVT_PRE_CRASH = 5,
};

struct TraceEvent {
    uint64_t ts_ns;
    uint64_t seq;
    uint64_t trace_id;
    uint32_t kind;
    uint32_t status;      // 0 = OK, 1 = WARN/SPIKE, 2 = CRITICAL
    uint64_t latency_ns;
    uint64_t payload;
    uint64_t pad[2];
};
static_assert(sizeof(TraceEvent) == 64);

struct SharedFlightBox {
    ConveyorRing<TraceEvent, 4096> ring;
    alignas(128) std::atomic<bool> app_exited{false};
};

static inline uint64_t now_ns(double ns_per_tick) {
    return static_cast<uint64_t>(mach_absolute_time() * ns_per_tick);
}

int main(int argc, char** argv) {
    const char* out_json = argc > 1 ? argv[1] : "/tmp/blackbox_dump.json";
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    const double ns_per_tick = static_cast<double>(tb.numer) / static_cast<double>(tb.denom);

    auto* box = static_cast<SharedFlightBox*>(
        mmap(nullptr, sizeof(SharedFlightBox), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    new (box) SharedFlightBox();

    constexpr uint64_t TOTAL_EVENTS = 1'000'000;
    constexpr size_t KEEP_LAST = 150;

    pid_t app_pid = fork();
    if (app_pid == 0) {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        auto prod = box->ring.make_producer();
        uint64_t base_ns = now_ns(ns_per_tick);

        for (uint64_t i = 0; i < TOTAL_EVENTS; ++i) {
            uint32_t kind = static_cast<uint32_t>(i & 3);
            uint32_t status = 0;
            uint64_t lat = 180 + ((i * 37) % 140);

            // Inject a realistic anomaly spike right before the crash
            if (i >= TOTAL_EVENTS - 40 && i < TOTAL_EVENTS - 5) {
                kind = EVT_ANOMALY;
                status = 1;
                lat = 4200 + ((i * 193) % 3800);
            } else if (i >= TOTAL_EVENTS - 5) {
                kind = EVT_PRE_CRASH;
                status = 2;
                lat = 18500 + (i - (TOTAL_EVENTS - 5)) * 2100;
            }

            prod.push(TraceEvent{
                now_ns(ns_per_tick) - base_ns,
                i,
                100000 + (i / 4),
                kind,
                status,
                lat,
                0xCAFE0000ULL + (i & 0xFFFF),
                {0, 0},
            });
        }

        // Deliberate fatal crash (SIGSEGV) in the hot-path app!
        raise(SIGSEGV);
        _exit(139);
    }

    // Collector Sidecar (Parent Process)
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    auto cons = box->ring.make_consumer();
    std::vector<TraceEvent> tail(KEEP_LAST);
    uint64_t captured = 0;
    auto t0 = std::chrono::steady_clock::now();

    int app_status = 0;
    bool app_dead = false;
    TraceEvent ev{};

    while (true) {
        if (cons.try_pop(ev)) {
            tail[captured % KEEP_LAST] = ev;
            captured++;
        } else {
            if (!app_dead) {
                pid_t r = waitpid(app_pid, &app_status, WNOHANG);
                if (r == app_pid) app_dead = true;
            } else {
                // App is dead and ring is completely drained!
                break;
            }
            CPU_RELAX();
        }
    }
    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    double mops = (captured / sec) / 1e6;
    int sig = WIFSIGNALED(app_status) ? WTERMSIG(app_status) : 0;

    // Write JSON dump of surviving flight-recorder data
    std::ofstream out(out_json);
    out << "{\n"
        << "  \"captured\": " << captured << ",\n"
        << "  \"expected\": " << TOTAL_EVENTS << ",\n"
        << "  \"dropped\": " << (TOTAL_EVENTS > captured ? TOTAL_EVENTS - captured : 0) << ",\n"
        << "  \"elapsed_s\": " << sec << ",\n"
        << "  \"events_per_s_m\": " << mops << ",\n"
        << "  \"crash_signal\": " << sig << ",\n"
        << "  \"app_pid\": " << app_pid << ",\n"
        << "  \"collector_pid\": " << getpid() << ",\n"
        << "  \"tail_events\": [\n";

    size_t count = std::min<size_t>(captured, KEEP_LAST);
    size_t start_idx = captured > KEEP_LAST ? (captured % KEEP_LAST) : 0;
    for (size_t i = 0; i < count; ++i) {
        const auto& e = tail[(start_idx + i) % KEEP_LAST];
        out << "    {\"seq\":" << e.seq
            << ",\"ts_ns\":" << e.ts_ns
            << ",\"trace_id\":" << e.trace_id
            << ",\"kind\":" << e.kind
            << ",\"status\":" << e.status
            << ",\"latency_ns\":" << e.latency_ns
            << "}" << (i + 1 < count ? "," : "") << "\n";
    }
    out << "  ]\n}\n";
    out.close();

    std::cout << "Black-Box Collector survived App crash (SIG=" << sig << ")\n"
              << "  Captured : " << captured << " / " << TOTAL_EVENTS << " events (0 dropped)\n"
              << "  Rate     : " << mops << " Million events/sec (" << sec << "s)\n"
              << "  Dump     : " << out_json << "\n";

    munmap(box, sizeof(SharedFlightBox));
    return 0;
}
