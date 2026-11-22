# In-Slot Conveyor Ring: Beating Aeron & FastIPC by 3.4×–4.7× on Apple Silicon SPSC Streaming

**Platform:** Apple Silicon M2 (4 Avalanche P-cores @ 3.5 GHz, 4 Blizzard E-cores @ 2.4 GHz, 128-byte cache lines, 16 MB shared P-cluster L2, no SMT)  
**Competitors Tested:** Aeron 1.53.3 (IPC), FastIPC 1.0.0, Boost.Lockfree 1.88, Rigtorp SPSC, Dmitry Vyukov Bounded Queue

---

## Executive Summary

For fixed-size, single-producer single-consumer (SPSC) inter-process streaming on Apple Silicon, our **In-Slot Conveyor Ring** (`conveyor_ring.hpp`) sustains **462.35 Million messages/sec** (16-byte payloads, 100M exact-delivery verified messages) across separate OS processes:

* **3.9× faster than Aeron 1.53.3** unbatched (`118.59 Mmsg/s`)
* **4.7× faster than FastIPC 1.0.0** unbatched (`99.20 Mmsg/s`)
* **3.4× faster than Aeron & FastIPC even with batch-64** (`~137.6 Mmsg/s` and `~136.0 Mmsg/s`)
* **16× faster than unbatched Boost.Lockfree & Rigtorp SPSC** (`~24.5 Mmsg/s` on 32-byte orders)

Every throughput number below is backed by **exact-delivery verification** (every message checked against a keyed 64-bit challenge seed across distinct producer/consumer PIDs) and a **1-bit corruption negative control** that all transports caught and rejected.

---

## 1. Head-to-Head vs. Aeron 1.53.3 & FastIPC 1.0.0

### Exact-Delivery Native Comparison (100,000,000 × 16-Byte Messages, 7 Randomized Rounds)

To eliminate benchmark artifacts (dead-code elimination, dropped frames, or single-process illusions), we ran 7 randomized rounds of 100M 16-byte messages between two separate processes using a keyed 64-bit challenge seed (`0x5689550e6800ed85`), verifying every sequence index and payload word, plus a negative control that flips 1 bit in flight.

| Transport | Mode | Median Throughput | Amortized Interval | Speedup of Conveyor | Corruption Control |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **In-Slot Conveyor Ring** (`conveyor-direct`) | 16B unbatched, 128B slot | **462.35 Mmsg/s** | **2.16 ns/msg** | **1.0×** | Rejected (1/1) |
| **Aeron 1.53.3** (`aeron-direct-offer`) | 16B unbatched (`fragment_limit=1`) | 118.59 Mmsg/s | 8.43 ns/msg | **3.9× faster** | Rejected (1/1) |
| **FastIPC 1.0.0** (`fastipc-direct-copy-wait`) | 16B unbatched | 99.20 Mmsg/s | 10.08 ns/msg | **4.7× faster** | Rejected (1/1) |

### What About Batching?

Even when Aeron and FastIPC are given **batch-64** amortizations (1s sustained runs after 300ms warmup, 0 lost messages), the unbatched Conveyor Ring remains **3.4× faster**:

| Transport | Mode | Median Throughput | Speedup of Conveyor |
| :--- | :--- | :---: | :---: |
| **In-Slot Conveyor Ring** (`conveyor-direct`) | Unbatched (`batch=1`) | **~442–462 Mmsg/s** | **1.0×** |
| **Aeron 1.53.3** (`aeron-batch-64-poll`) | `batch=64` | ~137.6 Mmsg/s | **3.4× faster** |
| **FastIPC 1.0.0** (`fastipc-batch-64-wait`) | `batch=64` | ~136.0 Mmsg/s | **3.4× faster** |
| **Aeron 1.53.3** (`aeron-batch-16-poll`) | `batch=16` | ~124.2 Mmsg/s | **3.7× faster** |
| **FastIPC 1.0.0** (`fastipc-batch-16-poll`) | `batch=16` | ~122.3 Mmsg/s | **3.8× faster** |

### 2-Minute Continuous Soak Test (86.26 Billion Verified Messages, 20s Cool-Off Between Runs)

To rule out short-burst artifacts and thermal throttling, each transport ran continuously for 2 full minutes across two separate OS processes, verifying every single 16-byte message against a keyed 64-bit hash:

| Transport | 30s | 60s | 90s | 120s | **2-Min Avg** | **Total Verified** | Speedup of Conveyor | Mismatches |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`ConveyorRing`** | 516.45 M/s | 507.65 M/s | 504.66 M/s | 518.68 M/s | **511.86 Mmsg/s** | **61.42 Billion** | **1.0×** | **0** |
| **Aeron IPC** (`fragment_limit=10`) | 143.68 M/s | 143.90 M/s | 143.38 M/s | 143.89 M/s | **143.68 Mmsg/s** | **17.25 Billion** | **3.56× faster** | **0** |
| **FastIPC 1.0.0** (`fipc_send`/`recv`) | 72.44 M/s | 56.90 M/s | 55.76 M/s | 67.98 M/s | **63.27 Mmsg/s** | **7.59 Billion** | **8.09× faster** | **0** |

---

## 2. Why Conveyor Ring Beats Aeron & FastIPC Here

Aeron and FastIPC are general-purpose IPC transports; Conveyor Ring is co-designed specifically for **fixed-size SPSC streams on 128-byte cache-line silicon**:

1. **Zero Shared `head` / `tail` Metadata:** Neither producer nor consumer maintains a shared head or tail pointer in RAM. Each side keeps its monotonic `index_` in a private CPU register and coordinates ownership via an in-slot parity-lap sequence (`2*lap` = producer, `2*lap + 1` = consumer).
2. **128-Byte Cache-Line Fusion (`alignas(128)`):** Apple Silicon uses 128-byte cache lines (`hw.cachelinesize = 128`). Fusing the 8-byte `std::atomic<uint64_t> sequence` and the inline payload `T` into a single 128-byte slot means each message hand-off touches **exactly one cache line** with zero false sharing between adjacent slots.
3. **Zero Framing or Media-Driver Overhead:** Aeron's IPC path includes frame headers, term-buffer management, and media-driver coordination; FastIPC includes general-purpose ring framing and synchronization. For fixed-size structs (e.g., market ticks, telemetry frames, financial orders), Conveyor Ring writes `T` directly into shared `mmap` memory with ARMv8.5-A `LDAR`/`STLR` acquire/release instructions.
4. **Hardware Stream Prefetcher Sympathy:** Strictly linear 128-byte forward strides let Apple Silicon's hardware prefetcher stream lines through the shared 16 MB L2 cache without pointer-chasing stalls.

---

## 3. Additional Hardware Verification

Beyond the Aeron/FastIPC runs, the local verification suite (`cache_ipc/industry_grade_verification` and `cache_ipc/head_to_head_bench`) confirms:

* **50M-Message Cryptographic Integrity Chain (SplitMix64 + FNV-1a):** Sustained **478.55 Mmsg/s (2.09 ns/op)** with bit-for-bit hash identity (`0xF9CE0CDECE5F7CAF`) and 0 torn/out-of-order reads.
* **L2-to-DRAM Physical Cliff:** Sweeping ring capacity proves the throughput comes from shared L2 cache residence:
  * **4,096 slots (512 KB, inside 16 MB L2):** **1,045.36 Mmsg/s (0.96 ns/op)**
  * **65,536 slots (8 MB, inside 16 MB L2):** **924.89 Mmsg/s (1.08 ns/op)**
  * **262,144 slots (32 MB, spills to LPDDR5 DRAM):** **202.48 Mmsg/s (4.94 ns/op)** — a 5.2× drop right at the L2 boundary.
* **Noisy-Neighbor Resilience:** Under 2 background processes thrashing 64 MB of cache lines, throughput held at **275.10 Mmsg/s (3.64 ns/op)** with 0 drops.
* **Unbatched Lock-Free SPSC Contenders (10M × 32-Byte Financial Orders):**
  * Classic Lamport SPSC: `23.82 Mmsg/s`
  * Boost.Lockfree 1.88 SPSC: `24.49 Mmsg/s`
  * Rigtorp SPSC (unbatched): `24.57 Mmsg/s`
  * Dmitry Vyukov Bounded Queue: `129.84 Mmsg/s`
  * **In-Slot Conveyor Ring:** **`405.19 Mmsg/s`**

---

## 4. Honest Engineering Caveats (When *Not* to Use Conveyor Ring)

A credible systems benchmark should state its boundaries clearly:

1. **Specialized vs. General-Purpose IPC:** Aeron and FastIPC support variable-length framing, flow control, disconnection handling, and (for Aeron) multi-subscriber fan-out and network transport. Conveyor Ring is strictly a **fixed-size, single-producer, single-consumer** busy-spinning ring buffer.
2. **Throughput Interval ≠ One-Way Latency:** The **2.1–2.5 ns/msg** numbers are *amortized throughput intervals* (`1 / throughput`) in a pipelined stream. True one-way cross-core transit latency on Apple M2 is **41.7–62.5 ns** (1–1.5 ticks of the 24 MHz ARM generic timer). In strict stop-and-wait ping-pong (1 message in flight), all queues converge near the crossbar floor, and Rigtorp SPSC actually measured lower RTT (**83.3 ns p50**) than Conveyor Ring (**125.0 ns p50**).
3. **Tuned Shared-Cursor Queues Can Beat It:** While Conveyor Ring beats *unbatched immediate-retry* shared-cursor queues by ~16×, our follow-up validation (`cursor_variance_summary.txt`) showed that a shared-cursor SPSC ring with **empty-backoff (`1 µs`)** (`742 Mmsg/s`) or **batched publication (`publish-16`)** (`701 Mmsg/s`) outperforms Conveyor Ring (`584 Mmsg/s`) in sustained throughput. Conveyor Ring's advantage is delivering **400M–460M+ msgs/s unbatched with immediate visibility** and zero backoff tuning.
4. **Memory Footprint:** Padding every slot to 128 bytes trades RAM for cache-line isolation (256 KB for 2,048 slots regardless of whether `T` is 8 bytes or 120 bytes).

---

## 5. Repository Map

* **Core Header:** [`include/conveyor_ring.hpp`](include/conveyor_ring.hpp)
* **Lock-Free SPSC Head-to-Head:** [`benchmarks/head_to_head_bench.cpp`](benchmarks/head_to_head_bench.cpp)
* **Verification Suite (Crypto Chain, L2/DRAM Cliff, Latency, Noisy Neighbor):** [`benchmarks/industry_grade_verification.cpp`](benchmarks/industry_grade_verification.cpp)
* **Aeron & FastIPC Native Harness:** `Documents/ChatGPT/ipc/compare_native_ipc.py` & `ipc_exact_delivery.hpp`
