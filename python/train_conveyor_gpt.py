#!/usr/bin/env python3
"""Train a mini GPT from scratch using ConveyorRing zero-copy token streaming.

Usage (Google Colab GPU or local Mac):
    python3 python/train_conveyor_gpt.py --steps 500
"""
import argparse
import ctypes
from pathlib import Path
import platform
import subprocess
import time
import urllib.request

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset


class TokenShardDataset(Dataset):
    def __init__(self, bin_path: Path, seq_len: int, total_samples: int):
        self.data = np.fromfile(bin_path, dtype=np.uint16)
        self.seq_len = seq_len
        self.total_samples = total_samples
        self.max_start = len(self.data) - (seq_len + 1)

    def __len__(self):
        return self.total_samples

    def __getitem__(self, idx):
        start = (idx * 2654435761) % self.max_start
        return torch.from_numpy(self.data[start : start + self.seq_len + 1].astype(np.int64))


def build_token_lib() -> Path:
    root = Path(__file__).resolve().parent.parent
    src = root / "python" / "token_conveyor.cpp"
    ext = ".dylib" if platform.system() == "Darwin" else ".so"
    out = root / "python" / f"libtoken_conveyor{ext}"
    subprocess.run(
        ["c++", "-O3", "-DNDEBUG", "-std=c++20", "-shared", "-fPIC",
         "-I", str(root / "include"), str(src), "-o", str(out)],
        check=True,
    )
    return out


def prepare_dataset(data_dir: Path) -> tuple[Path, dict, dict]:
    data_dir.mkdir(parents=True, exist_ok=True)
    txt_path = data_dir / "tinyshakespeare.txt"
    bin_path = data_dir / "tinyshakespeare.bin"
    if not txt_path.exists():
        url = "https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt"
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, txt_path)
    text = txt_path.read_text(encoding="utf-8")
    chars = sorted(set(text))
    stoi = {ch: i for i, ch in enumerate(chars)}
    itos = {i: ch for i, ch in enumerate(chars)}
    if not bin_path.exists():
        arr = np.array([stoi[c] for c in text], dtype=np.uint16)
        arr.tofile(bin_path)
    return bin_path, stoi, itos


class MiniGPT(nn.Module):
    def __init__(self, vocab_size: int, seq_len: int, d_model: int = 256, n_heads: int = 4, n_layers: int = 4):
        super().__init__()
        self.seq_len = seq_len
        self.tok_emb = nn.Embedding(vocab_size, d_model)
        self.pos_emb = nn.Embedding(seq_len, d_model)
        layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=n_heads, dim_feedforward=4 * d_model,
            dropout=0.0, activation="gelu", batch_first=True, norm_first=True,
        )
        self.blocks = nn.TransformerEncoder(layer, num_layers=n_layers)
        self.ln_f = nn.LayerNorm(d_model)
        self.head = nn.Linear(d_model, vocab_size, bias=False)
        self.register_buffer("causal_mask", torch.triu(torch.ones(seq_len, seq_len) * float("-inf"), diagonal=1))

    def forward(self, idx: torch.Tensor) -> torch.Tensor:
        b, t = idx.shape
        pos = torch.arange(t, device=idx.device)
        x = self.tok_emb(idx) + self.pos_emb(pos)
        x = self.blocks(x, mask=self.causal_mask[:t, :t])
        return self.head(self.ln_f(x))

    @torch.no_grad()
    def generate(self, idx: torch.Tensor, max_new_tokens: int = 200) -> torch.Tensor:
        for _ in range(max_new_tokens):
            logits = self(idx[:, -self.seq_len:])[:, -1, :]
            probs = F.softmax(logits, dim=-1)
            idx = torch.cat([idx, torch.multinomial(probs, num_samples=1)], dim=1)
        return idx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--seq-len", type=int, default=128)
    parser.add_argument("--compare", action="store_true", help="Also train with PyTorch DataLoader and compare")
    args = parser.parse_args()

    device = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")
    lib_path = build_token_lib()
    bin_path, stoi, itos = prepare_dataset(Path("/tmp/conveyor_gpt_data"))

    lib = ctypes.CDLL(str(lib_path))
    lib.token_loader_start.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_uint64, ctypes.c_uint64]
    lib.token_loader_start.restype = ctypes.c_void_p
    lib.token_loader_next.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    lib.token_loader_next.restype = ctypes.POINTER(ctypes.c_int64)
    lib.token_loader_release.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    lib.token_loader_stop.argtypes = [ctypes.c_void_p]

    torch.manual_seed(42)
    model = MiniGPT(len(stoi), args.seq_len).to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=3e-3)
    params_m = sum(p.numel() for p in model.parameters()) / 1e6
    print(f"Training MiniGPT ({params_m:.2f}M params) on {device} | ConveyorRing zero-copy token stream")

    handle = lib.token_loader_start(str(bin_path).encode(), args.batch_size, args.seq_len, args.steps, 42)
    slot = ctypes.c_uint32()
    if device == "cuda":
        torch.cuda.synchronize()
    t0 = time.perf_counter()

    for step in range(1, args.steps + 1):
        ptr = lib.token_loader_next(handle, ctypes.byref(slot))
        if not ptr:
            break
        arr = np.ctypeslib.as_array(ptr, shape=(args.batch_size, args.seq_len + 1))
        batch = torch.from_numpy(arr).to(device, non_blocking=(device == "cuda"))
        x, y = batch[:, :-1], batch[:, 1:]
        logits = model(x)
        loss = F.cross_entropy(logits.reshape(-1, logits.size(-1)), y.reshape(-1))
        opt.zero_grad(set_to_none=True)
        loss.backward()
        opt.step()
        lib.token_loader_release(handle, slot.value)

        if step % max(1, args.steps // 5) == 0 or step == 1:
            dt = time.perf_counter() - t0
            tok_s = (step * args.batch_size * args.seq_len) / dt
            print(f"  step {step:4d}/{args.steps} | loss={loss.item():.4f} | {tok_s/1e3:7.1f}k tok/s")

    if device == "cuda":
        torch.cuda.synchronize()
    conv_sec = time.perf_counter() - t0
    lib.token_loader_stop(handle)
    conv_tok_s = (args.steps * args.batch_size * args.seq_len) / conv_sec

    if args.compare:
        print(f"\nTraining MiniGPT ({params_m:.2f}M params) on {device} | PyTorch DataLoader (num_workers=1)")
        torch.manual_seed(42)
        model_pt = MiniGPT(len(stoi), args.seq_len).to(device)
        opt_pt = torch.optim.AdamW(model_pt.parameters(), lr=3e-3)
        ds = TokenShardDataset(bin_path, args.seq_len, args.steps * args.batch_size)
        loader = DataLoader(ds, batch_size=args.batch_size, num_workers=1, pin_memory=(device == "cuda"))
        if device == "cuda":
            torch.cuda.synchronize()
        t1 = time.perf_counter()
        for step, batch in enumerate(loader, 1):
            batch = batch.to(device, non_blocking=(device == "cuda"))
            x, y = batch[:, :-1], batch[:, 1:]
            logits = model_pt(x)
            loss = F.cross_entropy(logits.reshape(-1, logits.size(-1)), y.reshape(-1))
            opt_pt.zero_grad(set_to_none=True)
            loss.backward()
            opt_pt.step()
            if step % max(1, args.steps // 5) == 0 or step == 1:
                dt = time.perf_counter() - t1
                tok_s = (step * args.batch_size * args.seq_len) / dt
                print(f"  step {step:4d}/{args.steps} | loss={loss.item():.4f} | {tok_s/1e3:7.1f}k tok/s")
        if device == "cuda":
            torch.cuda.synchronize()
        pt_sec = time.perf_counter() - t1
        pt_tok_s = (args.steps * args.batch_size * args.seq_len) / pt_sec
        print("\n=== End-to-End Training Comparison ===")
        print(f"  ConveyorRing : {conv_sec:.2f}s ({conv_tok_s/1e3:.1f}k tok/s)")
        print(f"  PyTorch DL   : {pt_sec:.2f}s ({pt_tok_s/1e3:.1f}k tok/s)")
        print(f"  End-to-End   : {pt_sec / conv_sec:.2f}x faster")

    prompt = torch.tensor([[stoi["\n"]]], dtype=torch.long, device=device)
    out = model.generate(prompt, max_new_tokens=200)[0].tolist()
    print("\n--- Generated Sample ---")
    print("".join(itos[i] for i in out))


if __name__ == "__main__":
    main()
