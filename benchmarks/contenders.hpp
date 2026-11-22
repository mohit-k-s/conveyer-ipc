#pragma once
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <new>

#if defined(__aarch64__) || defined(__arm64__)
#define CPU_RELAX() asm volatile("yield" ::: "memory")
#else
#define CPU_RELAX() std::this_thread::yield()
#endif

// ============================================================================
// CONTENDER 1: CLASSIC LAMPORT SPSC (UNCACHED ATOMIC HEAD & TAIL)
// ============================================================================
template <typename T, size_t Capacity = 2048>
class ClassicLamportSPSC {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static constexpr size_t MASK = Capacity - 1;
    static constexpr size_t kCacheLine = 128; // Apple Silicon M2 128B

    alignas(kCacheLine) std::atomic<size_t> head_{0};
    alignas(kCacheLine) std::atomic<size_t> tail_{0};
    alignas(kCacheLine) T buffer_[Capacity];

public:
    ClassicLamportSPSC() = default;

    inline void push(const T& val) {
        size_t current_head = head_.load(std::memory_order_relaxed);
        while (current_head - tail_.load(std::memory_order_acquire) == Capacity) {
            CPU_RELAX();
        }
        buffer_[current_head & MASK] = val;
        head_.store(current_head + 1, std::memory_order_release);
    }

    inline T pop() {
        size_t current_tail = tail_.load(std::memory_order_relaxed);
        while (current_tail == head_.load(std::memory_order_acquire)) {
            CPU_RELAX();
        }
        T val = buffer_[current_tail & MASK];
        tail_.store(current_tail + 1, std::memory_order_release);
        return val;
    }
};

// ============================================================================
// CONTENDER 3: RIGTORP CACHED-POINTERS SPSC QUEUE
// ============================================================================
// Erik Rigtorp's design: caches head and tail locally to amortize cross-core reads.
template <typename T, size_t Capacity = 2048>
class RigtorpSPSC {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static constexpr size_t MASK = Capacity - 1;
    static constexpr size_t kCacheLine = 128;

    // Tail written by consumer
    alignas(kCacheLine) std::atomic<size_t> tail_{0};

    // Cached head: private to consumer
    alignas(kCacheLine) size_t cached_head_{0};

    // Head written by producer
    alignas(kCacheLine) std::atomic<size_t> head_{0};

    // Cached tail: private to producer
    alignas(kCacheLine) size_t cached_tail_{0};

    // Circular data slots
    alignas(kCacheLine) T buffer_[Capacity];

public:
    RigtorpSPSC() = default;

    inline void push(const T& val) {
        size_t h = head_.load(std::memory_order_relaxed);
        if (h - cached_tail_ == Capacity) {
            while (h - (cached_tail_ = tail_.load(std::memory_order_acquire)) == Capacity) {
                CPU_RELAX();
            }
        }
        buffer_[h & MASK] = val;
        head_.store(h + 1, std::memory_order_release);
    }

    inline T pop() {
        size_t t = tail_.load(std::memory_order_relaxed);
        if (t == cached_head_) {
            while (t == (cached_head_ = head_.load(std::memory_order_acquire))) {
                CPU_RELAX();
            }
        }
        T val = buffer_[t & MASK];
        tail_.store(t + 1, std::memory_order_release);
        return val;
    }
};

// ============================================================================
// CONTENDER 4: DMITRY VYUKOV BOUNDED QUEUE (WITH SLOT SEQUENCES & SHARED POS)
// ============================================================================
template <typename T, size_t Capacity = 2048>
class VyukovBoundedQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static constexpr size_t MASK = Capacity - 1;
    static constexpr size_t kCacheLine = 128;

    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    alignas(kCacheLine) Cell buffer_[Capacity];
    alignas(kCacheLine) std::atomic<size_t> enqueue_pos_{0};
    alignas(kCacheLine) std::atomic<size_t> dequeue_pos_{0};

public:
    VyukovBoundedQueue() {
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    inline void push(const T& val) {
        Cell* cell;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[pos & MASK];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = (intptr_t)seq - (intptr_t)pos;
            if (dif == 0) {
                enqueue_pos_.store(pos + 1, std::memory_order_relaxed);
                break;
            }
            CPU_RELAX();
            pos = enqueue_pos_.load(std::memory_order_relaxed);
        }
        cell->data = val;
        cell->sequence.store(pos + 1, std::memory_order_release);
    }

    inline T pop() {
        Cell* cell;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[pos & MASK];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
            if (dif == 0) {
                dequeue_pos_.store(pos + 1, std::memory_order_relaxed);
                break;
            }
            CPU_RELAX();
            pos = dequeue_pos_.load(std::memory_order_relaxed);
        }
        T val = cell->data;
        cell->sequence.store(pos + MASK + 1, std::memory_order_release);
        return val;
    }
};
