#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>

#if defined(__aarch64__) || defined(__arm64__)
#define CPU_RELAX() asm volatile("yield" ::: "memory")
#elif defined(__x86_64__) || defined(_M_X64)
#define CPU_RELAX() __builtin_ia32_pause()
#else
#define CPU_RELAX() std::this_thread::yield()
#endif

// ============================================================================
// CONVEYOR-BELT SPSC RING BUFFER (APPLE SILICON 128-BYTE CACHE-FUSED)
//
// Novelty:
// 1. Zero shared index variables. Neither 'head' nor 'tail' exists in shared memory!
// 2. Each slot is aligned and padded to 128 bytes (1 Apple Silicon Cache Line).
// 3. The 64-bit sequence counter is fused directly into the data cache line:
//    - Even sequence: Owned by Producer (ready to write)
//    - Odd sequence : Owned by Consumer (ready to read)
// 4. Hardware prefetcher friendly: strictly linear forward stride.
// ============================================================================

template <typename T, size_t Capacity = 2048>
class ConveyorRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable for IPC");
    static_assert(std::atomic<uint64_t>::is_always_lock_free, "64-bit atomics must be lock-free");
    static constexpr size_t MASK = Capacity - 1;

public:
    struct alignas(128) Slot {
        std::atomic<uint64_t> sequence{0};
        T data;
    };
    static_assert(sizeof(Slot) % 128 == 0, "Slot must occupy an exact multiple of 128B cache lines");

private:
    Slot slots_[Capacity];

public:
    ConveyorRing() {
        for (size_t i = 0; i < Capacity; ++i) {
            slots_[i].sequence.store(0, std::memory_order_relaxed);
        }
    }

    // Producer side: zero shared head/tail memory accesses
    class Producer {
        ConveyorRing& ring_;
        uint64_t index_{0};

    public:
        explicit Producer(ConveyorRing& ring) : ring_(ring), index_(0) {}
        Producer(const Producer&) = delete;
        Producer& operator=(const Producer&) = delete;
        Producer(Producer&&) = default;
        Producer& operator=(Producer&&) = delete;

        template <typename... Args>
        inline void emplace(Args&&... args) {
            size_t slot_idx = index_ & MASK;
            uint64_t lap = index_ / Capacity;
            uint64_t expected_seq = 2 * lap; // Even number = owned by producer

            // Spin until consumer has consumed previous lap
            while (ring_.slots_[slot_idx].sequence.load(std::memory_order_acquire) != expected_seq) {
                CPU_RELAX();
            }

            // Write data directly into slot
            new (&ring_.slots_[slot_idx].data) T(std::forward<Args>(args)...);

            // Hand off to consumer by advancing to odd sequence (STLR on ARM64)
            ring_.slots_[slot_idx].sequence.store(2 * lap + 1, std::memory_order_release);
            index_++;
        }

        inline void push(const T& item) {
            emplace(item);
        }

        inline bool try_push(const T& item) {
            size_t slot_idx = index_ & MASK;
            uint64_t lap = index_ / Capacity;
            uint64_t expected_seq = 2 * lap;

            if (ring_.slots_[slot_idx].sequence.load(std::memory_order_acquire) != expected_seq) {
                return false;
            }

            ring_.slots_[slot_idx].data = item;
            ring_.slots_[slot_idx].sequence.store(2 * lap + 1, std::memory_order_release);
            index_++;
            return true;
        }
    };

    // Consumer side: zero shared head/tail memory accesses
    class Consumer {
        ConveyorRing& ring_;
        uint64_t index_{0};

    public:
        explicit Consumer(ConveyorRing& ring) : ring_(ring), index_(0) {}
        Consumer(const Consumer&) = delete;
        Consumer& operator=(const Consumer&) = delete;
        Consumer(Consumer&&) = default;
        Consumer& operator=(Consumer&&) = delete;

        inline T pop() {
            size_t slot_idx = index_ & MASK;
            uint64_t lap = index_ / Capacity;
            uint64_t expected_seq = 2 * lap + 1; // Odd number = owned by consumer

            // Spin until producer has published this slot (LDAR on ARM64)
            while (ring_.slots_[slot_idx].sequence.load(std::memory_order_acquire) != expected_seq) {
                CPU_RELAX();
            }

            // Read data directly from slot
            T item = ring_.slots_[slot_idx].data;

            // Release slot back to producer for next lap (STLR on ARM64)
            ring_.slots_[slot_idx].sequence.store(2 * (lap + 1), std::memory_order_release);
            index_++;
            return item;
        }

        inline bool try_pop(T& out) {
            size_t slot_idx = index_ & MASK;
            uint64_t lap = index_ / Capacity;
            uint64_t expected_seq = 2 * lap + 1;

            if (ring_.slots_[slot_idx].sequence.load(std::memory_order_acquire) != expected_seq) {
                return false;
            }

            out = ring_.slots_[slot_idx].data;
            ring_.slots_[slot_idx].sequence.store(2 * (lap + 1), std::memory_order_release);
            index_++;
            return true;
        }
    };

    Producer make_producer() { return Producer(*this); }
    Consumer make_consumer() { return Consumer(*this); }
};
