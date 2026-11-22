#!/usr/bin/env python3
"""Benchmark: ConveyorRing Zero-Pickling DataLoader vs PyTorch DataLoader.

Works on both Google Colab (Linux x86_64 + CUDA GPU) and macOS (Apple Silicon).
Usage:
    python3 python/bench_colab_dataloader.py
"""
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
                checksum += float(t[0, 0])
            else:
                checksum += float(arr[0, 0])
        else:
            checksum += float(ptr[0])
        lib.loader_release_slot(handle, slot_idx.value)
        count += 1
    if HAS_TORCH and device == "cuda":
        torch.cuda.synchronize()
    t1 = time.perf_counter()
    lib.loader_stop(handle)
    return count, t1 - t0, checksum


def bench_pytorch_dataloader(batch_size: int, feature_dim: int, total_batches: int, device: str):
    class SyntheticDataset(Dataset):
        def __len__(self):
            return total_batches * batch_size

        def __getitem__(self, idx):
            val = ((idx // batch_size) & 0xFF) * 0.01
            return torch.full((feature_dim,), val, dtype=torch.float32)

    loader = DataLoader(
        SyntheticDataset(),
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
    lib_path = build_shared_lib()
    batch_size = 256
    feature_dim = 128
    total_batches = 2000

    device = "cuda" if (HAS_TORCH and torch.cuda.is_available()) else "cpu"
    print(f"Platform: {platform.system()} {platform.machine()} | Device: {device}")
    print(f"Workload: {total_batches} batches x ({batch_size}, {feature_dim}) float32")
    print("-" * 70)

    c_cnt, c_sec, c_sum = bench_conveyor(lib_path, batch_size, feature_dim, total_batches, device)
    print(f"ConveyorLoader (zero-copy shm): {c_cnt/c_sec:10.1f} batches/s | {c_sec:.4f}s | checksum={c_sum:.2f}")

    if HAS_TORCH:
        p_cnt, p_sec, p_sum = bench_pytorch_dataloader(batch_size, feature_dim, total_batches, device)
        print(f"PyTorch DataLoader (1 worker) : {p_cnt/p_sec:10.1f} batches/s | {p_sec:.4f}s | checksum={p_sum:.2f}")
        print("-" * 70)
        print(f"Speedup: {p_sec / c_sec:.2f}x faster")
    else:
        print("(Install PyTorch or run on Google Colab to compare against torch.utils.data.DataLoader)")


if __name__ == "__main__":
    main()
