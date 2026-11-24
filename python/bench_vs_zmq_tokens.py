#!/usr/bin/env python3
"""Experiment 5: AI Inference Control Plane — ConveyorRing vs ZeroMQ (ipc://).

ZeroMQ over ipc:// is the standard transport used by vLLM and SGLang between the
GPU worker process and the API/tokenizer frontend. This benchmark compares:
  1. Sustained 1-by-1 24B token streaming (200,000 tokens across 2 OS processes)
  2. Speculative-decoding Draft <-> Verifier round-trip latency (20,000 strict RTT rounds)
"""
import ctypes
import multiprocessing as mp
import os
from pathlib import Path
import platform
import struct
import subprocess
import time

import zmq

ROOT = Path(__file__).resolve().parent.parent
SHM_PATH = "/tmp/conveyor_tok_ipc.shm"
ZMQ_ADDR = "ipc:///tmp/conveyor_zmq_bench.ipc"
ZMQ_RTT_ADDR = "ipc:///tmp/conveyor_zmq_rtt.ipc"
TOK_FMT = "<QIIfI"  # 24 bytes: req_id, token_id, pos, logprob, flags


class TokenMsg(ctypes.Structure):
    _fields_ = [
        ("req_id", ctypes.c_uint64),
        ("token_id", ctypes.c_uint32),
        ("pos", ctypes.c_uint32),
        ("logprob", ctypes.c_float),
        ("flags", ctypes.c_uint32),
    ]


def build_lib() -> Path:
    src = ROOT / "python" / "token_ring_ipc.cpp"
    ext = ".dylib" if platform.system() == "Darwin" else ".so"
    out = ROOT / "python" / f"libtoken_ring_ipc{ext}"
    subprocess.run(
        ["c++", "-O3", "-DNDEBUG", "-std=c++20", "-shared", "-fPIC",
         "-I", str(ROOT / "include"), str(src), "-o", str(out)],
        check=True,
    )
    return out


def load_lib(path: Path):
    lib = ctypes.CDLL(str(path))
    lib.tok_ipc_create.argtypes = [ctypes.c_char_p, ctypes.c_int]
    lib.tok_ipc_create.restype = ctypes.c_void_p
    lib.tok_ipc_push.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_float, ctypes.c_uint32]
    lib.tok_ipc_pop.argtypes = [ctypes.c_void_p, ctypes.POINTER(TokenMsg)]
    lib.tok_ipc_stream_send.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.tok_ipc_stream_send.restype = ctypes.c_uint64
    lib.tok_ipc_stream_recv.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
    lib.tok_ipc_stream_recv.restype = ctypes.c_uint64
    lib.tok_ipc_close.argtypes = [ctypes.c_void_p]
    return lib


# --- 1. ZMQ Streaming ---
def _zmq_stream_worker(addr: str, count: int, ready_ev):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    sock.bind(addr)
    ready_ev.set()
    checksum = 0
    for _ in range(count):
        msg = sock.recv()
        checksum += struct.unpack(TOK_FMT, msg)[1]
    sock.close()
    ctx.term()


def bench_zmq_stream(count: int):
    ctx_mp = mp.get_context("spawn")
    ready = ctx_mp.Event()
    p = ctx_mp.Process(target=_zmq_stream_worker, args=(ZMQ_ADDR, count, ready))
    p.start()
    ready.wait()
    zctx = zmq.Context()
    sock = zctx.socket(zmq.PUSH)
    sock.connect(ZMQ_ADDR)
    pack = struct.pack
    t0 = time.perf_counter()
    for i in range(count):
        sock.send(pack(TOK_FMT, 1, i, i, -0.1, 0))
    p.join()
    sec = time.perf_counter() - t0
    sock.close()
    zctx.term()
    return count / sec, sec


# --- 2. ConveyorRing Streaming (called from Python) ---
def _conv_stream_worker(lib_path: str, shm_path: str, count: int, ready_ev):
    lib = load_lib(Path(lib_path))
    ep = lib.tok_ipc_create(shm_path.encode(), 1)
    ready_ev.set()
    lib.tok_ipc_stream_recv(ep, count)
    lib.tok_ipc_close(ep)


def bench_conveyor_stream(lib_path: Path, count: int):
    ctx_mp = mp.get_context("spawn")
    ready = ctx_mp.Event()
    p = ctx_mp.Process(target=_conv_stream_worker, args=(str(lib_path), SHM_PATH, count, ready))
    p.start()
    ready.wait()
    lib = load_lib(lib_path)
    ep = lib.tok_ipc_create(SHM_PATH.encode(), 0)
    t0 = time.perf_counter()
    lib.tok_ipc_stream_send(ep, count)
    p.join()
    sec = time.perf_counter() - t0
    lib.tok_ipc_close(ep)
    return count / sec, sec


# --- 3. ZMQ Speculative-Decoding RTT ---
def _zmq_rtt_worker(addr: str, rounds: int, ready_ev):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.bind(addr)
    ready_ev.set()
    for _ in range(rounds):
        msg = sock.recv()
        sock.send(msg)
    sock.close()
    ctx.term()


def bench_zmq_rtt(rounds: int):
    ctx_mp = mp.get_context("spawn")
    ready = ctx_mp.Event()
    p = ctx_mp.Process(target=_zmq_rtt_worker, args=(ZMQ_RTT_ADDR, rounds, ready))
    p.start()
    ready.wait()
    zctx = zmq.Context()
    sock = zctx.socket(zmq.REQ)
    sock.connect(ZMQ_RTT_ADDR)
    payload = struct.pack(TOK_FMT, 1, 42, 0, -0.1, 0)
    # Warmup
    for _ in range(200):
        sock.send(payload); sock.recv()
    rtts = []
    for _ in range(rounds - 200):
        t0 = time.perf_counter_ns()
        sock.send(payload)
        sock.recv()
        rtts.append((time.perf_counter_ns() - t0) / 1e3)  # us
    p.join()
    sock.close()
    zctx.term()
    rtts.sort()
    return rtts[len(rtts) // 2], rtts[int(len(rtts) * 0.99)]


# --- 4. ConveyorRing Speculative-Decoding RTT (from Python ctypes!) ---
def _conv_rtt_worker(lib_path: str, shm_path: str, rounds: int, ready_ev):
    lib = load_lib(Path(lib_path))
    ep = lib.tok_ipc_create(shm_path.encode(), 1)
    ready_ev.set()
    msg = TokenMsg()
    for _ in range(rounds):
        lib.tok_ipc_pop(ep, ctypes.byref(msg))
        lib.tok_ipc_push(ep, msg.req_id, msg.token_id, msg.pos, msg.logprob, msg.flags)
    lib.tok_ipc_close(ep)


def bench_conveyor_rtt(lib_path: Path, rounds: int):
    ctx_mp = mp.get_context("spawn")
    ready = ctx_mp.Event()
    p = ctx_mp.Process(target=_conv_rtt_worker, args=(str(lib_path), SHM_PATH, rounds, ready))
    p.start()
    ready.wait()
    lib = load_lib(lib_path)
    ep = lib.tok_ipc_create(SHM_PATH.encode(), 0)
    msg = TokenMsg()
    for _ in range(200):
        lib.tok_ipc_push(ep, 1, 42, 0, -0.1, 0)
        lib.tok_ipc_pop(ep, ctypes.byref(msg))
    rtts = []
    for _ in range(rounds - 200):
        t0 = time.perf_counter_ns()
        lib.tok_ipc_push(ep, 1, 42, 0, -0.1, 0)
        lib.tok_ipc_pop(ep, ctypes.byref(msg))
        rtts.append((time.perf_counter_ns() - t0) / 1e3)  # us
    p.join()
    lib.tok_ipc_close(ep)
    rtts.sort()
    return rtts[len(rtts) // 2], rtts[int(len(rtts) * 0.99)]


def main():
    lib_path = build_lib()
    stream_n = 200_000
    rtt_n = 10_000

    print("Experiment 5: AI Inference Control Plane — ConveyorRing vs ZeroMQ (vLLM/SGLang ipc://)")
    print("=" * 86)

    z_rate, z_sec = bench_zmq_stream(stream_n)
    c_rate, c_sec = bench_conveyor_stream(lib_path, stream_n)
    print(f"1. Sustained 1-by-1 24B Token Streaming ({stream_n:,} tokens across 2 OS processes):")
    print(f"   ZeroMQ (ipc://)       : {z_rate/1e6:8.3f} M tok/s ({z_sec:.4f}s)")
    print(f"   ConveyorRing (shm)    : {c_rate/1e6:8.2f} M tok/s ({c_sec:.4f}s)")
    print(f"   Speedup               : {c_rate / z_rate:,.1f}x faster\n")

    z_p50, z_p99 = bench_zmq_rtt(rtt_n)
    c_p50, c_p99 = bench_conveyor_rtt(lib_path, rtt_n)
    print(f"2. Speculative-Decoding Draft <-> Verifier RTT ({rtt_n:,} strict rounds, from Python):")
    print(f"   ZeroMQ (ipc://)       : p50 = {z_p50:6.2f} us | p99 = {z_p99:6.2f} us")
    print(f"   ConveyorRing (ctypes) : p50 = {c_p50:6.2f} us | p99 = {c_p99:6.2f} us")
    print(f"   Latency Reduction     : {z_p50 / c_p50:,.1f}x lower p50 RTT")

    for f in (SHM_PATH, "/tmp/conveyor_zmq_bench.ipc", "/tmp/conveyor_zmq_rtt.ipc"):
        if os.path.exists(f):
            os.unlink(f)


if __name__ == "__main__":
    main()
