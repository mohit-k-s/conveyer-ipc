// Zero-copy, zero-pickling PyTorch DataLoader backend powered by ConveyorRing.
// Compiles on both Linux x86_64 (Google Colab) and macOS ARM64 (Apple Silicon).
#include <atomic>
#include <cstdint>
#include <cstring>
#include <new>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include "conveyor_ring.hpp"

struct BatchDesc {
    uint64_t batch_id;
    uint32_t slot_idx;
    uint32_t num_bytes;
};

static constexpr size_t kNumSlabs = 64;
static constexpr size_t kRingCap = 128;

struct SharedArena {
    ConveyorRing<BatchDesc, kRingCap> ready_ring;
    ConveyorRing<uint32_t, kRingCap> free_ring;
    alignas(kDefaultCacheLine) std::atomic<bool> stop{false};
};

struct LoaderHandle {
    SharedArena* arena;
    uint8_t* slabs;
    size_t slab_bytes;
    size_t total_slab_bytes;
    pid_t worker_pid;
    ConveyorRing<BatchDesc, kRingCap>::Consumer ready_cons;
    ConveyorRing<uint32_t, kRingCap>::Producer free_prod;

    LoaderHandle(SharedArena* a, uint8_t* s, size_t sb, size_t tsb, pid_t pid)
        : arena(a),
          slabs(s),
          slab_bytes(sb),
          total_slab_bytes(tsb),
          worker_pid(pid),
          ready_cons(a->ready_ring.make_consumer()),
          free_prod(a->free_ring.make_producer()) {}
};

extern "C" {

void* loader_start(size_t floats_per_batch, uint64_t total_batches) {
    const size_t slab_bytes = floats_per_batch * sizeof(float);
    const size_t total_slab_bytes = slab_bytes * kNumSlabs;

    auto* arena = static_cast<SharedArena*>(
        mmap(nullptr, sizeof(SharedArena), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    auto* slabs = static_cast<uint8_t*>(
        mmap(nullptr, total_slab_bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    if (arena == MAP_FAILED || slabs == MAP_FAILED) return nullptr;

    new (arena) SharedArena();

    pid_t pid = fork();
    if (pid == 0) {
        auto ready_prod = arena->ready_ring.make_producer();
        auto free_cons = arena->free_ring.make_consumer();

        for (uint64_t b = 0; b < total_batches; ++b) {
            if (arena->stop.load(std::memory_order_relaxed)) break;
            uint32_t slot = (b < kNumSlabs) ? static_cast<uint32_t>(b) : free_cons.pop();

            // Simulate lightweight preprocessing directly into shared-memory slab
            float* dst = reinterpret_cast<float*>(slabs + static_cast<size_t>(slot) * slab_bytes);
            const float val = static_cast<float>(b & 0xFF) * 0.01f;
            for (size_t i = 0; i < floats_per_batch; ++i) {
                dst[i] = val;
            }

            ready_prod.push(BatchDesc{b, slot, static_cast<uint32_t>(slab_bytes)});
        }
        // Sentinel
        ready_prod.push(BatchDesc{UINT64_MAX, 0, 0});
        _exit(0);
    }

    return new LoaderHandle(arena, slabs, slab_bytes, total_slab_bytes, pid);
}

// Returns pointer to shared-memory batch buffer (zero-copy!), or nullptr when done.
const float* loader_next_batch(void* handle_ptr, uint32_t* out_slot_idx) {
    auto* h = static_cast<LoaderHandle*>(handle_ptr);
    BatchDesc d = h->ready_cons.pop();
    if (d.batch_id == UINT64_MAX) return nullptr;
    *out_slot_idx = d.slot_idx;
    return reinterpret_cast<const float*>(h->slabs + static_cast<size_t>(d.slot_idx) * h->slab_bytes);
}

void loader_release_slot(void* handle_ptr, uint32_t slot_idx) {
    auto* h = static_cast<LoaderHandle*>(handle_ptr);
    h->free_prod.push(slot_idx);
}

void loader_stop(void* handle_ptr) {
    auto* h = static_cast<LoaderHandle*>(handle_ptr);
    h->arena->stop.store(true, std::memory_order_release);
    waitpid(h->worker_pid, nullptr, 0);
    munmap(h->slabs, h->total_slab_bytes);
    munmap(h->arena, sizeof(SharedArena));
    delete h;
}

}  // extern "C"
