// Minimal C++20 C-API wrapper around ConveyorRing<TokenMsg, 4096> for Python IPC
// (vLLM / SGLang-style token streaming & speculative-decoding draft<->verifier).
#include <cstdint>
#include <fcntl.h>
#include <new>
#include <sys/mman.h>
#include <unistd.h>

#include "conveyor_ring.hpp"

struct TokenMsg {
    uint64_t req_id;
    uint32_t token_id;
    uint32_t pos;
    float logprob;
    uint32_t flags;
};
static_assert(sizeof(TokenMsg) == 24);

struct DualTokenRings {
    ConveyorRing<TokenMsg, 4096> fwd;
    ConveyorRing<TokenMsg, 4096> rev;
};

struct Endpoint {
    DualTokenRings* rings;
    ConveyorRing<TokenMsg, 4096>::Producer prod;
    ConveyorRing<TokenMsg, 4096>::Consumer cons;
    Endpoint(DualTokenRings* r, bool is_server)
        : rings(r),
          prod((is_server ? r->rev : r->fwd).make_producer()),
          cons((is_server ? r->fwd : r->rev).make_consumer()) {}
};

extern "C" {

void* tok_ipc_create(const char* path, int is_server) {
    int fd = open(path, O_CREAT | O_RDWR, 0600);
    if (fd < 0) return nullptr;
    if (is_server) {
        if (ftruncate(fd, sizeof(DualTokenRings)) != 0) { close(fd); return nullptr; }
    }
    void* mem = mmap(nullptr, sizeof(DualTokenRings), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (mem == MAP_FAILED) return nullptr;
    auto* rings = static_cast<DualTokenRings*>(mem);
    if (is_server) new (rings) DualTokenRings();
    return new Endpoint(rings, is_server != 0);
}

void tok_ipc_push(void* ep_ptr, uint64_t req_id, uint32_t token_id, uint32_t pos, float logprob, uint32_t flags) {
    auto* ep = static_cast<Endpoint*>(ep_ptr);
    ep->prod.push(TokenMsg{req_id, token_id, pos, logprob, flags});
}

void tok_ipc_pop(void* ep_ptr, TokenMsg* out) {
    auto* ep = static_cast<Endpoint*>(ep_ptr);
    *out = ep->cons.pop();
}

// High-speed C++ batch drain / burst helper for throughput comparison
uint64_t tok_ipc_stream_send(void* ep_ptr, uint64_t count) {
    auto* ep = static_cast<Endpoint*>(ep_ptr);
    for (uint64_t i = 0; i < count; ++i) {
        ep->prod.push(TokenMsg{1, static_cast<uint32_t>(i), static_cast<uint32_t>(i), -0.1f, 0});
    }
    return count;
}

uint64_t tok_ipc_stream_recv(void* ep_ptr, uint64_t count) {
    auto* ep = static_cast<Endpoint*>(ep_ptr);
    uint64_t sum = 0;
    for (uint64_t i = 0; i < count; ++i) {
        sum += ep->cons.pop().token_id;
    }
    return sum;
}

void tok_ipc_close(void* ep_ptr) {
    auto* ep = static_cast<Endpoint*>(ep_ptr);
    munmap(ep->rings, sizeof(DualTokenRings));
    delete ep;
}

}  // extern "C"
