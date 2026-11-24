// Live Black-Box Flight Recorder backend (stdout JSON stream at ~5 Hz).
// Reads single-char commands from stdin:
//   'c' -> send SIGSEGV to Hot-Path App (Collector survives & freezes pre-crash ring!)
//   'r' -> restart Hot-Path App
//   'q' -> quit cleanly
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <mach/mach_time.h>
#include <optional>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "conveyor_ring.hpp"

struct TraceEvent {
    uint64_t ts_ns;
    uint64_t seq;
    uint64_t trace_id;
    uint32_t kind;
    uint32_t status;
    uint64_t latency_ns;
    uint64_t payload;
    uint64_t pad[2];
};
static_assert(sizeof(TraceEvent) == 64);

struct SharedLive {
    ConveyorRing<TraceEvent, 4096> ring;
    alignas(128) std::atomic<bool> crash_now{false};
};

static inline uint64_t now_ns(double ns_per_tick) {
    return static_cast<uint64_t>(mach_absolute_time() * ns_per_tick);
}

static pid_t spawn_app(SharedLive* box, double ns_per_tick) {
    box->crash_now.store(false, std::memory_order_release);
    new (&box->ring) ConveyorRing<TraceEvent, 4096>();
    pid_t pid = fork();
    if (pid == 0) {
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        auto prod = box->ring.make_producer();
        uint64_t base = now_ns(ns_per_tick);
        uint64_t i = 0;
        while (true) {
            bool crashing = box->crash_now.load(std::memory_order_relaxed);
            uint32_t kind = crashing ? 5u : static_cast<uint32_t>(i & 3);
            uint32_t status = crashing ? 2u : ((i % 25000 == 0) ? 1u : 0u);
            uint64_t lat = crashing ? 22000 : (status == 1 ? 6500 : (160 + ((i * 37) % 140)));

            prod.push(TraceEvent{
                now_ns(ns_per_tick) - base,
                i,
                100000 + (i / 4),
                kind,
                status,
                lat,
                i,
                {0, 0},
            });
            ++i;
            if (crashing) {
                // Emit 4 more fatal pre-crash breadcrumbs then crash!
                for (int k = 1; k <= 4; ++k) {
                    prod.push(TraceEvent{now_ns(ns_per_tick) - base, i++, 999999, 5, 2, static_cast<uint64_t>(24000 + k * 1500), 0, {0, 0}});
                }
                raise(SIGSEGV);
                _exit(139);
            }
            // Light pacing (~2M-4M events/sec) so laptop stays cool & quiet while live
            if ((i & 0x3FF) == 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(250));
            }
        }
    }
    return pid;
}

int main() {
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    const double ns_per_tick = static_cast<double>(tb.numer) / static_cast<double>(tb.denom);

    // Non-blocking stdin for live control commands ('c', 'r', 'q')
    fcntl(STDIN_FILENO, F_SETFL, fcntl(STDIN_FILENO, F_GETFL, 0) | O_NONBLOCK);

    auto* box = static_cast<SharedLive*>(
        mmap(nullptr, sizeof(SharedLive), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    new (box) SharedLive();

    constexpr size_t KEEP_LAST = 80;
    std::vector<TraceEvent> tail(KEEP_LAST);
    pid_t app_pid = spawn_app(box, ns_per_tick);
    std::optional<ConveyorRing<TraceEvent, 4096>::Consumer> cons;
    cons.emplace(box->ring.make_consumer());

    uint64_t captured = 0, last_captured = 0;
    bool app_alive = true;
    int crash_sig = 0;
    auto t_last = std::chrono::steady_clock::now();

    while (true) {
        // Drain available events
        TraceEvent ev{};
        int batch = 0;
        while (batch < 4096 && cons->try_pop(ev)) {
            tail[captured % KEEP_LAST] = ev;
            captured++;
            batch++;
        }

        // Check if app died
        if (app_alive) {
            int st = 0;
            if (waitpid(app_pid, &st, WNOHANG) == app_pid) {
                app_alive = false;
                crash_sig = WIFSIGNALED(st) ? WTERMSIG(st) : 0;
            }
        }

        // Check stdin commands
        char cmd = 0;
        if (read(STDIN_FILENO, &cmd, 1) == 1) {
            if (cmd == 'c' && app_alive) {
                box->crash_now.store(true, std::memory_order_release);
            } else if (cmd == 'r' && !app_alive) {
                captured = 0;
                last_captured = 0;
                crash_sig = 0;
                app_pid = spawn_app(box, ns_per_tick);
                cons.emplace(box->ring.make_consumer());
                app_alive = true;
            } else if (cmd == 'q') {
                if (app_alive) { kill(app_pid, SIGKILL); waitpid(app_pid, nullptr, 0); }
                break;
            }
        }

        auto now = std::chrono::steady_clock::now();
        double dt = std::chrono::duration<double>(now - t_last).count();
        if (dt >= 0.20) {
            double mops = ((captured - last_captured) / dt) / 1e6;
            last_captured = captured;
            t_last = now;

            size_t cnt = std::min<size_t>(captured, KEEP_LAST);
            size_t start = captured > KEEP_LAST ? (captured % KEEP_LAST) : 0;
            std::cout << "{\"alive\":" << (app_alive ? "true" : "false")
                      << ",\"crash_sig\":" << crash_sig
                      << ",\"app_pid\":" << app_pid
                      << ",\"collector_pid\":" << getpid()
                      << ",\"captured\":" << captured
                      << ",\"rate_m\":" << mops
                      << ",\"tail\":[";
            for (size_t i = 0; i < cnt; ++i) {
                const auto& e = tail[(start + i) % KEEP_LAST];
                std::cout << "{\"seq\":" << e.seq << ",\"ts_ns\":" << e.ts_ns
                          << ",\"kind\":" << e.kind << ",\"status\":" << e.status
                          << ",\"lat\":" << e.latency_ns << "}" << (i + 1 < cnt ? "," : "");
            }
            std::cout << "]}\n" << std::flush;
        }
        if (batch == 0) std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    munmap(box, sizeof(SharedLive));
    return 0;
}
