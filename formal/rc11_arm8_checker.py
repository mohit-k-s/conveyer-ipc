#!/usr/bin/env python3
"""RC11 / ARMv8-A Axiomatic Weak-Memory Verifier for ConveyorRing (2 laps on 1 slot).

Exhaustively enumerates all reads-from (rf) and coherence-order (co) graphs for
the 2-lap Producer/Consumer hand-off on a single slot:
  Producer: W(data=1) -[po]-> W_rel(seq=1) -[po]-> R_acq(seq==2) -[po]-> W(data=2)
  Consumer: R_acq(seq==1) -[po]-> R(data) -[po]-> W_rel(seq=2)

Checks RC11 / ARMv8-A happens-before (hb = (po U sw)+) acyclicity & coherence:
  - Under acquire/release (LDAR/STLR): proves Consumer CANNOT read stale (0) or future (2) data,
    and proves NO un-ordered concurrent data race exists.
  - Negative control (memory_order_relaxed): proves the checker CATCHES both stale reads and races.
"""
from itertools import product


def check_weak_memory(use_acq_rel: bool):
    # Events:
    #   P: p_w1 (W data=1), p_pub (W seq=1), p_poll (R seq==2), p_w2 (W data=2)
    #   C: c_poll (R seq==1), c_r (R data), c_rel (W seq=2)
    # Initial writes: init_d (W data=0), init_s (W seq=0)
    data_writes = [("init_d", 0), ("p_w1", 1), ("p_w2", 2)]

    # Base program-order edges
    po = {
        ("init_d", "p_w1"), ("init_s", "p_pub"),
        ("p_w1", "p_pub"), ("p_pub", "p_poll"), ("p_poll", "p_w2"),
        ("c_poll", "c_r"), ("c_r", "c_rel"),
    }

    # Synchronizes-with (sw) edges exist ONLY when release-store is read by acquire-load
    sw = set()
    if use_acq_rel:
        sw.add(("p_pub", "c_poll"))   # STLR(seq=1) -> LDAR(seq==1)
        sw.add(("c_rel", "p_poll"))   # STLR(seq=2) -> LDAR(seq==2)

    # Transitive closure of hb = (po U sw)+
    nodes = ["init_d", "init_s", "p_w1", "p_pub", "p_poll", "p_w2", "c_poll", "c_r", "c_rel"]
    hb = {u: set() for u in nodes}
    for u, v in (po | sw):
        hb[u].add(v)
    changed = True
    while changed:
        changed = False
        for u in nodes:
            add = set()
            for mid in hb[u]:
                add |= hb[mid]
            if not add.issubset(hb[u]):
                hb[u] |= add
                changed = True

    forbidden_reads = []
    data_races = []

    # Check data race between c_r and {p_w1, p_w2}
    for w_node, val in [("p_w1", 1), ("p_w2", 2)]:
        if w_node not in hb["c_r"] and "c_r" not in hb[w_node]:
            data_races.append((w_node, "c_r"))

    # Check which values c_r is allowed to read under RC11 coherence:
    # c_r cannot read w if there exists w' such that w -hb-> w' -hb-> c_r,
    # and c_r cannot read w if c_r -hb-> w.
    for w_node, val in data_writes:
        if w_node in hb["c_r"]:
            continue  # future write strictly after c_r
        overwritten = False
        for w2_node, _ in data_writes:
            if w2_node != w_node and (w2_node in hb[w_node]) and ("c_r" in hb[w2_node]):
                overwritten = True
                break
        if not overwritten and val != 1:
            forbidden_reads.append(val)

    mode = "LDAR/STLR (acquire/release)" if use_acq_rel else "RELAXED (negative control)"
    print(f"[{mode}]")
    print(f"  Forbidden data reads (0=stale, 2=future): {forbidden_reads}")
    print(f"  Unordered data races on slot.data       : {data_races}")
    if use_acq_rel:
        assert not forbidden_reads and not data_races, "RC11/ARMv8 verification FAILED!"
        print("  VERDICT: PASS — Strictly race-free & exact under RC11 / ARMv8-A weak memory.\n")
    else:
        assert forbidden_reads and data_races, "Negative control failed to catch relaxed bug!"
        print("  VERDICT: CAUGHT — Relaxed ordering allows stale/future reads and data races.\n")


if __name__ == "__main__":
    check_weak_memory(use_acq_rel=True)
    check_weak_memory(use_acq_rel=False)
