// GenMC C11 weak-memory model verification harness for ConveyorRing.
// Run with: genmc -- formal/conveyor_genmc.c
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>

#define CAP 2
#define MASK (CAP - 1)
#define MSGS 4

typedef struct {
    _Atomic uint64_t seq;
    uint64_t data;
} Slot;

static Slot slots[CAP];

static void* producer(void* arg) {
    (void)arg;
    for (uint64_t idx = 0; idx < MSGS; ++idx) {
        size_t s = idx & MASK;
        uint64_t lap = idx / CAP;
        uint64_t expected = 2 * lap;
        while (atomic_load_explicit(&slots[s].seq, memory_order_acquire) != expected) {}
        slots[s].data = idx + 1;
        atomic_store_explicit(&slots[s].seq, 2 * lap + 1, memory_order_release);
    }
    return NULL;
}

static void* consumer(void* arg) {
    (void)arg;
    for (uint64_t idx = 0; idx < MSGS; ++idx) {
        size_t s = idx & MASK;
        uint64_t lap = idx / CAP;
        uint64_t expected = 2 * lap + 1;
        while (atomic_load_explicit(&slots[s].seq, memory_order_acquire) != expected) {}
        uint64_t val = slots[s].data;
        assert(val == idx + 1);
        atomic_store_explicit(&slots[s].seq, 2 * (lap + 1), memory_order_release);
    }
    return NULL;
}

int main(void) {
    for (size_t i = 0; i < CAP; ++i) atomic_init(&slots[i].seq, 0);
    pthread_t tp, tc;
    pthread_create(&tp, NULL, producer, NULL);
    pthread_create(&tc, NULL, consumer, NULL);
    pthread_join(tp, NULL);
    pthread_join(tc, NULL);
    return 0;
}
