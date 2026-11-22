#!/usr/bin/env python3
"""Benchmark: ConveyorRing Zero-Pickling DataLoader vs PyTorch DataLoader.

Works on both Google Colab (Linux x86_64 + CUDA GPU) and macOS (Apple Silicon).
Usage:
    python3 python/bench_colab_dataloader.py
"""
import argparse
import ctypes
from pathlib import Path
import platform
import subprocess
import time

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import torch
    from torch.utils.data import DataLoader, Dataset
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


def build_shared_lib() -> Path:
    root = Path(__file__).resolve().parent.parent
    src = root / "python" / "conveyor_loader.cpp"
    ext = ".dylib" if platform.system() == "Darwin" else ".so"
    out = root / "python" / f"libconveyor_loader{ext}"
    cmd = [
        "c++", "-O3", "-DNDEBUG", "-std=c++20", "-shared", "-fPIC",
        "-I", str(root / "include"), str(src), "-o", str(out),
    ]
    subprocess.run(cmd, check=True)
    return out


def bench_conveyor(lib_path: Path, batch_size: int, feature_dim: int, total_batches: int, device: str):
    lib = ctypes.CDLL(str(lib_path))
    lib.loader_start.argtypes = [ctypes.c_size_t, ctypes.c_uint64]
    lib.loader_start.restype = ctypes.c_void_p
    lib.loader_next_batch.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    lib.loader_next_batch.restype = ctypes.POINTER(ctypes.c_float)
    lib.loader_release_slot.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    lib.loader_stop.argtypes = [ctypes.c_void_p]

    floats_per_batch = batch_size * feature_dim
    slot_idx = ctypes.c_uint32()

    t0 = time.perf_counter()
    handle = lib.loader_start(floats_per_batch, total_batches)
    checksum = 0.0
    gpu_acc = torch.zeros((), device="cuda", dtype=torch.float64) if (HAS_TORCH and device == "cuda") else None
    count = 0
    while True:
        ptr = lib.loader_next_batch(handle, ctypes.byref(slot_idx))
        if not ptr:
            break
        # Zero-copy view directly over shared memory slab
        if HAS_NUMPY:
            arr = np.ctypeslib.as_array(ptr, shape=(batch_size, feature_dim))
            if HAS_TORCH:
                t = torch.from_numpy(arr)
                if device == "cuda":
                    t = t.to("cuda", non_blocking=True)
                    gpu_acc += t[0, 0]
                else:
                    checksum += float(t[0, 0])
            else:
                checksum += float(arr[0, 0])
        else:
            checksum += float(ptr[0])
        lib.loader_release_slot(handle, slot_idx.value)
        count += 1
    if gpu_acc is not None:
        torch.cuda.synchronize()
        checksum = float(gpu_acc.item())
    t1 = time.perf_counter()
    lib.loader_stop(handle)
    return count, t1 - t0, checksum


if HAS_TORCH:
    import torch.multiprocessing as mp
    from torch.utils.data import IterableDataset

    class SyntheticDataset(Dataset):
        def __init__(self, batch_size: int, feature_dim: int, total_batches: int):
            self.batch_size = batch_size
            self.feature_dim = feature_dim
            self.total_batches = total_batches

        def __len__(self):
            return self.total_batches * self.batch_size

        def __getitem__(self, idx):
            val = ((idx // self.batch_size) & 0xFF) * 0.01
            return torch.full((self.feature_dim,), val, dtype=torch.float32)

    class PrebatchedIterableDataset(IterableDataset):
        def __init__(self, batch_size: int, feature_dim: int, total_batches: int):
            self.batch_size = batch_size
            self.feature_dim = feature_dim
            self.total_batches = total_batches

        def __iter__(self):
            for b in range(self.total_batches):
                val = (b & 0xFF) * 0.01
                yield torch.full((self.batch_size, self.feature_dim), val, dtype=torch.float32)

    def _torch_shm_worker(q_ready, q_free, slabs, total_batches: int):
        for b in range(total_batches):
            slot = b if b < len(slabs) else q_free.get()
            slabs[slot].fill_((b & 0xFF) * 0.01)
            q_ready.put(slot)
        q_ready.put(-1)


def bench_pytorch_shm_queue(batch_size: int, feature_dim: int, total_batches: int, device: str):
    num_slabs = 64
    slabs = [torch.empty((batch_size, feature_dim), dtype=torch.float32).share_memory_() for _ in range(num_slabs)]
    ctx = mp.get_context("spawn")
    q_ready = ctx.Queue(maxsize=num_slabs)
    q_free = ctx.Queue(maxsize=num_slabs)
    p = ctx.Process(target=_torch_shm_worker, args=(q_ready, q_free, slabs, total_batches))
    t0 = time.perf_counter()
    p.start()
    checksum = 0.0
    count = 0
    while True:
        slot = q_ready.get()
        if slot == -1:
            break
        t = slabs[slot]
        if device == "cuda":
            t = t.to("cuda", non_blocking=True)
        checksum += float(t[0, 0])
        q_free.put(slot)
        count += 1
    if device == "cuda":
        torch.cuda.synchronize()
    p.join()
    t1 = time.perf_counter()
    return count, t1 - t0, checksum


def bench_pytorch_prebatched(batch_size: int, feature_dim: int, total_batches: int, device: str):
    loader = DataLoader(
        PrebatchedIterableDataset(batch_size, feature_dim, total_batches),
        batch_size=None,
        num_workers=1,
        pin_memory=(device == "cuda"),
    )
    t0 = time.perf_counter()
    checksum = 0.0
    count = 0
    for batch in loader:
        if device == "cuda":
            batch = batch.to("cuda", non_blocking=True)
        checksum += float(batch[0, 0])
        count += 1
    if device == "cuda":
        torch.cuda.synchronize()
    t1 = time.perf_counter()
    return count, t1 - t0, checksum


def bench_pytorch_dataloader(batch_size: int, feature_dim: int, total_batches: int, device: str):
    loader = DataLoader(
        SyntheticDataset(batch_size, feature_dim, total_batches),
        batch_size=batch_size,
        num_workers=1,
        pin_memory=(device == "cuda"),
    )
    t0 = time.perf_counter()
    checksum = 0.0
    count = 0
    for batch in loader:
        if device == "cuda":
            batch = batch.to("cuda", non_blocking=True)
        checksum += float(batch[0, 0])
        count += 1
    if device == "cuda":
        torch.cuda.synchronize()
    t1 = time.perf_counter()
    return count, t1 - t0, checksum


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batches", type=int, default=2000)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--dim", type=int, default=128)
    args = parser.parse_args()

    lib_path = build_shared_lib()
    batch_size = args.batch_size
    feature_dim = args.dim
    total_batches = args.batches
    total_gb = (total_batches * batch_size * feature_dim * 4) / (1024 ** 3)

    device = "cuda" if (HAS_TORCH and torch.cuda.is_available()) else "cpu"
    print(f"Platform: {platform.system()} {platform.machine()} | Device: {device}")
    print(f"Workload: {total_batches} batches x ({batch_size}, {feature_dim}) float32 ({total_gb:.2f} GiB total)")
    print("-" * 70)

    c_cnt, c_sec, c_sum = bench_conveyor(lib_path, batch_size, feature_dim, total_batches, device)
    print(f"ConveyorLoader (zero-copy shm): {c_cnt/c_sec:10.1f} batches/s | {total_gb/c_sec:6.2f} GiB/s | {c_sec:.4f}s | checksum={c_sum:.2f}")

    if HAS_TORCH:
        s_cnt, s_sec, s_sum = bench_pytorch_shm_queue(batch_size, feature_dim, total_batches, device)
        print(f"PyTorch share_memory_ + Queue : {s_cnt/s_sec:10.1f} batches/s | {total_gb/s_sec:6.2f} GiB/s | {s_sec:.4f}s | checksum={s_sum:.2f}")
        b_cnt, b_sec, b_sum = bench_pytorch_prebatched(batch_size, feature_dim, total_batches, device)
        print(f"PyTorch Iterable (pre-batched): {b_cnt/b_sec:10.1f} batches/s | {total_gb/b_sec:6.2f} GiB/s | {b_sec:.4f}s | checksum={b_sum:.2f}")
        p_cnt, p_sec, p_sum = bench_pytorch_dataloader(batch_size, feature_dim, total_batches, device)
        print(f"PyTorch DataLoader (standard) : {p_cnt/p_sec:10.1f} batches/s | {total_gb/p_sec:6.2f} GiB/s | {p_sec:.4f}s | checksum={p_sum:.2f}")
        print("-" * 70)
        print(f"Speedup vs share_memory_+Queue: {s_sec / c_sec:.2f}x faster")
        print(f"Speedup vs Pre-batched Loader : {b_sec / c_sec:.2f}x faster")
        print(f"Speedup vs Standard DataLoader: {p_sec / c_sec:.2f}x faster")
    else:
        print("(Install PyTorch or run on Google Colab to compare against torch.utils.data.DataLoader)")


if __name__ == "__main__":
    main()
