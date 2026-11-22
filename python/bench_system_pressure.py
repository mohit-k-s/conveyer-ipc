#!/usr/bin/env python3
"""System Pressure Benchmark: ConveyorLoader vs PyTorch DataLoader.

Measures not just throughput, but OS/machine pressure:
  - Wall time & batches/sec
  - Kernel (system) CPU time vs User CPU time (getrusage)
  - Voluntary & involuntary OS context switches (getrusage)
  - Peak RSS memory (MB)
  - Data-wait time per batch (us)

Works on both macOS (Apple Silicon) and Linux (Google Colab GPU).
"""
import ctypes
from pathlib import Path
import platform
import resource
import subprocess
import time

import numpy as np
import torch
from torch.utils.data import DataLoader, Dataset


def build_shared_lib() -> Path:
    root = Path(__file__).resolve().parent.parent
    src = root / "python" / "conveyor_loader.cpp"
    ext = ".dylib" if platform.system() == "Darwin" else ".so"
    out = root / "python" / f"libconveyor_loader{ext}"
    subprocess.run(
        ["c++", "-O3", "-DNDEBUG", "-std=c++20", "-shared", "-fPIC",
         "-I", str(root / "include"), str(src), "-o", str(out)],
        check=True,
    )
    return out


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


def rusage_snapshot():
    r = resource.getrusage(resource.RUSAGE_SELF)
    # macOS ru_maxrss is bytes; Linux ru_maxrss is KiB
    rss_mb = r.ru_maxrss / (1024 * 1024) if platform.system() == "Darwin" else r.ru_maxrss / 1024.0
    return {
        "utime": r.ru_utime,
        "stime": r.ru_stime,
        "nvcsw": r.ru_nvcsw,
        "nivcsw": r.ru_nivcsw,
        "rss_mb": rss_mb,
    }


def measure(name: str, fn):
    before = rusage_snapshot()
    t0 = time.perf_counter()
    batches, wait_sec = fn()
    wall = time.perf_counter() - t0
    after = rusage_snapshot()
    return {
        "name": name,
        "batches": batches,
        "wall_s": wall,
        "batches_s": batches / wall,
        "wait_us": (wait_sec / batches) * 1e6,
        "user_s": after["utime"] - before["utime"],
        "sys_s": after["stime"] - before["stime"],
        "vcsw": after["nvcsw"] - before["nvcsw"],
        "ivcsw": after["nivcsw"] - before["nivcsw"],
        "rss_mb": after["rss_mb"],
    }


def main():
    lib_path = build_shared_lib()
    batch_size, feature_dim, total_batches = 256, 128, 2000
    device = "cuda" if torch.cuda.is_available() else "cpu"

    lib = ctypes.CDLL(str(lib_path))
    lib.loader_start.argtypes = [ctypes.c_size_t, ctypes.c_uint64]
    lib.loader_start.restype = ctypes.c_void_p
    lib.loader_next_batch.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    lib.loader_next_batch.restype = ctypes.POINTER(ctypes.c_float)
    lib.loader_release_slot.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    lib.loader_stop.argtypes = [ctypes.c_void_p]

    def run_conveyor():
        slot = ctypes.c_uint32()
        handle = lib.loader_start(batch_size * feature_dim, total_batches)
        wait_s = 0.0
        cnt = 0
        while True:
            tw0 = time.perf_counter()
            ptr = lib.loader_next_batch(handle, ctypes.byref(slot))
            wait_s += time.perf_counter() - tw0
            if not ptr:
                break
            arr = np.ctypeslib.as_array(ptr, shape=(batch_size, feature_dim))
            t = torch.from_numpy(arr)
            if device == "cuda":
                t = t.to("cuda", non_blocking=True)
            lib.loader_release_slot(handle, slot.value)
            cnt += 1
        if device == "cuda":
            torch.cuda.synchronize()
        lib.loader_stop(handle)
        return cnt, wait_s

    def run_pytorch():
        loader = DataLoader(
            SyntheticDataset(batch_size, feature_dim, total_batches),
            batch_size=batch_size, num_workers=1, pin_memory=(device == "cuda"),
        )
        it = iter(loader)
        wait_s = 0.0
        cnt = 0
        while True:
            tw0 = time.perf_counter()
            try:
                batch = next(it)
            except StopIteration:
                break
            wait_s += time.perf_counter() - tw0
            if device == "cuda":
                batch = batch.to("cuda", non_blocking=True)
            cnt += 1
        if device == "cuda":
            torch.cuda.synchronize()
        return cnt, wait_s

    print(f"System Pressure Audit | {platform.system()} {platform.machine()} | Device: {device}")
    print(f"Workload: {total_batches} batches x ({batch_size}, {feature_dim}) float32")
    print("=" * 98)
    print(f"{'Loader':26} | {'Wall':>7} | {'Wait/b':>8} | {'Kernel CPU':>10} | {'Vol CSW':>8} | {'Invol CSW':>9} | {'Peak RSS':>9}")
    print("-" * 98)
    for res in (measure("ConveyorLoader (C++ shm)", run_conveyor), measure("PyTorch DataLoader (1w)", run_pytorch)):
        print(
            f"{res['name']:26} | {res['wall_s']:6.3f}s | {res['wait_us']:6.1f}us | "
            f"{res['sys_s']:9.4f}s | {res['vcsw']:8d} | {res['ivcsw']:9d} | {res['rss_mb']:7.1f}MB"
        )
    print("=" * 98)


if __name__ == "__main__":
    main()
