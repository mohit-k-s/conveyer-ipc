// conveyor-agent: Sub-microsecond anomaly detector + shared-memory circuit breaker
// powered by ConveyorRing. Measures exact Time-to-Detect & Trip Circuit Breaker (ns)
// when a latency spike hits the hot path, and outputs an OTLP-compatible JSON trace.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mach/mach_time.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#include "conveyor_ring.hpp"

struct SpanEvent {
    uint64_t ts_ticks;
    uint64_t seq;
    uint64_t trace_id;
    uint64_t latency_ns;
    uint32_t kind;
    uint32_t flags;
};
static_assert(sizeof(SpanEvent) == 40);

struct SharedNervousSystem {
    ConveyorRing<SpanEvent, 4096> ring;
    alignas(128) std::atomic<bool> circuit_open{false};
    alignas(128) std::atomic<uint64_t> trip_ticks{0};
    alignas(128) std::atomic<uint64_t> spike_seq{0};
    alignas(128) std::atomic<uint64_t> total_seen{0};
};

int main(int argc, char** argv) {
    const char* otlp_out = argc > 1 ? argv[1] : "/tmp/conveyor_otlp_trace.json";
    mach_timebase_info_data_t tb;
    mach_timebase_info(&tb);
    const double ns_per_tick = static_cast<double>(tb.numer) / static_cast<double>(tb.denom);

    auto* sys = static_cast<SharedNervousSystem*>(
        mmap(nullptr, sizeof(SharedNervousSystem), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    new (sys) SharedNervousSystem();

    constexpr uint64_t WARMUP_EVENTS = 100'000;
    constexpr uint64_t SPIKE_THRESHOLD_NS = 5'000;

    pid_t pid = fork();
    if (pid == 0) {
        // Sidecar Agent: watches 100% of spans in real time and trips circuit breaker on spike
        pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
        auto cons = sys->ring.make_consumer();
        uint64_t seen = 0;
        while (true) {
            SpanEvent ev = cons.pop();
            if (ev.seq == UINT64_MAX) break;
            seen++;
            if (ev.latency_ns >= SPIKE_THRESHOLD_NS && !sys->circuit_open.load(std::memory_order_relaxed)) {
                sys->trip_ticks.store(mach_absolute_time(), std::memory_order_relaxed);
                sys->spike_seq.store(ev.seq, std::memory_order_relaxed);
                sys->circuit_open.store(true, std::memory_order_release);
            }
        }
        sys->total_seen.store(seen, std::memory_order_release);
        _exit(0);
    }

    // Hot-Path App
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto prod = sys->ring.make_producer();

    // 1. Normal traffic
    for (uint64_t i = 0; i < WARMUP_EVENTS; ++i) {
        prod.push(SpanEvent{mach_absolute_time(), i, 500000 + i, 180 + (i & 63), 0, 0});
    }

    // 2. Inject sudden critical latency spike (25,000 ns) and measure exact reaction time!
    uint64_t spike_injected_ticks = mach_absolute_time();
    prod.push(SpanEvent{spike_injected_ticks, WARMUP_EVENTS, 999999, 25'000, 1, 2});

    // Wait until sidecar trips circuit breaker
    uint64_t slipped = 0;
    while (!sys->circuit_open.load(std::memory_order_acquire)) {
        slipped++;
        CPU_RELAX();
    }
    uint64_t app_observed_trip_ticks = mach_absolute_time();
    uint64_t sidecar_trip_ticks = sys->trip_ticks.load(std::memory_order_acquire);

    // Shutdown sidecar cleanly
    prod.push(SpanEvent{0, UINT64_MAX, 0, 0, 0, 0});
    waitpid(pid, nullptr, 0);

    double detect_ns = (sidecar_trip_ticks - spike_injected_ticks) * ns_per_tick;
    double roundtrip_trip_ns = (app_observed_trip_ticks - spike_injected_ticks) * ns_per_tick;

    // Emit valid OTLP/JSON trace payload (Grafana Tempo / OTel Collector format)
    std::ofstream out(otlp_out);
    out << "{\n"
        << "  \"resourceSpans\": [{\n"
        << "    \"resource\": {\"attributes\": [{\"key\": \"service.name\", \"value\": {\"stringValue\": \"conveyor-hotpath\"}}]},\n"
        << "    \"scopeSpans\": [{\n"
        << "      \"scope\": {\"name\": \"conveyor-agent\", \"version\": \"1.0.0\"},\n"
        << "      \"spans\": [{\n"
        << "        \"traceId\": \"000000000000000000000000000f423f\",\n"
        << "        \"spanId\": \"00000000000186a0\",\n"
        << "        \"name\": \"anomaly.circuit_breaker_trip\",\n"
        << "        \"attributes\": [\n"
        << "          {\"key\": \"anomaly.latency_ns\", \"value\": {\"intValue\": \"25000\"}},\n"
        << "          {\"key\": \"conveyor.detect_ns\", \"value\": {\"doubleValue\": " << detect_ns << "}},\n"
        << "          {\"key\": \"conveyor.trip_rtt_ns\", \"value\": {\"doubleValue\": " << roundtrip_trip_ns << "}}\n"
        << "        ]\n"
        << "      }]\n"
        << "    }]\n"
        << "  }]\n"
        << "}\n";
    out.close();

    std::cout << "CONVEYOR_DETECT_NS=" << std::fixed << std::setprecision(1) << detect_ns << "\n"
              << "CONVEYOR_TRIP_RTT_NS=" << roundtrip_trip_ns << "\n"
              << "CONVEYOR_SEEN=" << sys->total_seen.load(std::memory_order_acquire) << "\n";

    munmap(sys, sizeof(SharedNervousSystem));
    return 0;
}
