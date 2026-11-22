// Zero-copy C++20 token-shard streamer powered by ConveyorRing.
// Reads a flat uint16_t token file via mmap and streams (B, T+1) int64 batches
// directly into shared-memory slabs for PyTorch GPU training.
#include <atomic>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "conveyor_ring.hpp"

struct TokenBatchDesc {
    uint64_t step;
    uint32_t slot_idx;
    uint32_t reserved;
};

static constexpr size_t kNumSlabs = 32;
static constexpr size_t kRingCap = 64;

struct SharedTokenArena {
    ConveyorRing<TokenBatchDesc, kRingCap> ready_ring;
    ConveyorRing<uint32_t, kRingCap> free_ring;
    alignas(kDefaultCacheLine) std::atomic<bool> stop{false};
};

struct TokenLoaderHandle {
    SharedTokenArena* arena;
    int64_t* slabs;
    size_t tokens_per_batch;
    size_t total_slab_bytes;
    pid_t worker_pid;
    ConveyorRing<TokenBatchDesc, kRingCap>::Consumer ready_cons;
    ConveyorRing<uint32_t, kRingCap>::Producer free_prod;

    TokenLoaderHandle(SharedTokenArena* a, int64_t* s, size_t tpb, size_t tsb, pid_t pid)
        : arena(a),
          slabs(s),
          tokens_per_batch(tpb),
          total_slab_bytes(tsb),
          worker_pid(pid),
          ready_cons(a->ready_ring.make_consumer()),
          free_prod(a->free_ring.make_producer()) {}
};

static inline uint64_t xorshift64(uint64_t& s) {
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    return s;
}

extern "C" {

void* token_loader_start(const char* bin_path, size_t batch_size, size_t seq_len, uint64_t total_steps, uint64_t seed) {
    int fd = open(bin_path, O_RDONLY);
    if (fd < 0) return nullptr;
    struct stat st{};
    if (fstat(fd, &st) != 0 || st.st_size < static_cast<off_t>((seq_len + 2) * sizeof(uint16_t))) {
        close(fd);
        return nullptr;
    }
    const size_t file_bytes = static_cast<size_t>(st.st_size);
    const size_t num_tokens = file_bytes / sizeof(uint16_t);
    auto* corpus = static_cast<const uint16_t*>(mmap(nullptr, file_bytes, PROT_READ, MAP_SHARED, fd, 0));
    close(fd);
    if (corpus == MAP_FAILED) return nullptr;

    const size_t row_len = seq_len + 1;
    const size_t tokens_per_batch = batch_size * row_len;
    const size_t slab_bytes = tokens_per_batch * sizeof(int64_t);
    const size_t total_slab_bytes = slab_bytes * kNumSlabs;

    auto* arena = static_cast<SharedTokenArena*>(
        mmap(nullptr, sizeof(SharedTokenArena), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    auto* slabs = static_cast<int64_t*>(
        mmap(nullptr, total_slab_bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0));
    if (arena == MAP_FAILED || slabs == MAP_FAILED) return nullptr;
    new (arena) SharedTokenArena();

    pid_t pid = fork();
    if (pid == 0) {
        auto ready_prod = arena->ready_ring.make_producer();
        auto free_cons = arena->free_ring.make_consumer();
        const size_t max_start = num_tokens - row_len;
        uint64_t rng = seed ? seed : 0x9e3779b97f4a7c15ULL;

        for (uint64_t step = 0; step < total_steps; ++step) {
            if (arena->stop.load(std::memory_order_relaxed)) break;
            uint32_t slot = (step < kNumSlabs) ? static_cast<uint32_t>(step) : free_cons.pop();
            int64_t* dst = slabs + static_cast<size_t>(slot) * tokens_per_batch;

            for (size_t b = 0; b < batch_size; ++b) {
                size_t start = static_cast<size_t>(xorshift64(rng) % max_start);
                const uint16_t* src = corpus + start;
                int64_t* row = dst + b * row_len;
                for (size_t t = 0; t < row_len; ++t) {
                    row[t] = static_cast<int64_t>(src[t]);
                }
            }
            ready_prod.push(TokenBatchDesc{step, slot, 0});
        }
        ready_prod.push(TokenBatchDesc{UINT64_MAX, 0, 0});
        munmap(const_cast<uint16_t*>(corpus), file_bytes);
        _exit(0);
    }

    munmap(const_cast<uint16_t*>(corpus), file_bytes);
    return new TokenLoaderHandle(arena, slabs, tokens_per_batch, total_slab_bytes, pid);
}

const int64_t* token_loader_next(void* handle_ptr, uint32_t* out_slot_idx) {
    auto* h = static_cast<TokenLoaderHandle*>(handle_ptr);
    TokenBatchDesc d = h->ready_cons.pop();
    if (d.step == UINT64_MAX) return nullptr;
    *out_slot_idx = d.slot_idx;
    return h->slabs + static_cast<size_t>(d.slot_idx) * h->tokens_per_batch;
}

void token_loader_release(void* handle_ptr, uint32_t slot_idx) {
    auto* h = static_cast<TokenLoaderHandle*>(handle_ptr);
    h->free_prod.push(slot_idx);
}

void token_loader_stop(void* handle_ptr) {
    auto* h = static_cast<TokenLoaderHandle*>(handle_ptr);
    h->arena->stop.store(true, std::memory_order_release);
    waitpid(h->worker_pid, nullptr, 0);
    munmap(h->slabs, h->total_slab_bytes);
    munmap(h->arena, sizeof(SharedTokenArena));
    delete h;
}

}  // extern "C"
