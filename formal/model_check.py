#!/usr/bin/env python3
"""Exhaustive BFS state-space model checker for ConveyorRing.tla.

Explores 100% of reachable interleavings of Producer and Consumer fine-grained
steps (poll -> data access -> release-store) and verifies:
  1. NoDataRace: Producer and Consumer never access the same slot's data concurrently
  2. FifoExactness: Every delivered item equals its exact 1-based FIFO sequence number
  3. BoundedOccupancy: 0 <= p_idx - c_idx <= Capacity at every state
  4. DeadlockFreedom: Every non-terminal state has at least one enabled transition
  5. Termination: Terminal state (p_idx == c_idx == MaxMsgs) is reached with all items delivered
"""
from collections import deque


def model_check(capacity: int, max_msgs: int):
    init = (
        (0,) * capacity,   # seq
        (0,) * capacity,   # data
        0, "poll",         # p_idx, p_pc
        0, "poll", 0,      # c_idx, c_pc, c_val
        0,                 # delivered_count (verified exact 1..delivered_count)
    )
    visited = {init}
    q = deque([init])
    transitions = 0
    terminal_states = 0

    while q:
        state = q.popleft()
        seq, data, p_idx, p_pc, c_idx, c_pc, c_val, deliv = state

        # Invariant 1: NoDataRace
        if p_pc in ("write", "publish") and c_pc in ("read", "release"):
            assert (p_idx % capacity) != (c_idx % capacity), f"DATA RACE in state {state}"

        # Invariant 2: BoundedOccupancy
        assert 0 <= p_idx - c_idx <= capacity, f"OCCUPANCY VIOLATION in state {state}"

        # Terminal state check
        if p_idx == max_msgs and c_idx == max_msgs and p_pc == "poll" and c_pc == "poll":
            assert deliv == max_msgs, f"INCOMPLETE DELIVERY in terminal state {state}"
            terminal_states += 1
            continue

        next_states = []
        p_slot, p_lap = p_idx % capacity, p_idx // capacity
        c_slot, c_lap = c_idx % capacity, c_idx // capacity

        # Producer transitions
        if p_pc == "poll" and p_idx < max_msgs and seq[p_slot] == 2 * p_lap:
            next_states.append((seq, data, p_idx, "write", c_idx, c_pc, c_val, deliv))
        elif p_pc == "write":
            d2 = list(data); d2[p_slot] = p_idx + 1
            next_states.append((seq, tuple(d2), p_idx, "publish", c_idx, c_pc, c_val, deliv))
        elif p_pc == "publish":
            s2 = list(seq); s2[p_slot] = 2 * p_lap + 1
            next_states.append((tuple(s2), data, p_idx + 1, "poll", c_idx, c_pc, c_val, deliv))

        # Consumer transitions
        if c_pc == "poll" and c_idx < max_msgs and seq[c_slot] == 2 * c_lap + 1:
            next_states.append((seq, data, p_idx, p_pc, c_idx, "read", c_val, deliv))
        elif c_pc == "read":
            next_states.append((seq, data, p_idx, p_pc, c_idx, "release", data[c_slot], deliv))
        elif c_pc == "release":
            # Invariant 3: FifoExactness
            assert c_val == deliv + 1, f"FIFO VIOLATION: expected {deliv + 1}, got {c_val}"
            s2 = list(seq); s2[c_slot] = 2 * (c_lap + 1)
            next_states.append((tuple(s2), data, p_idx, p_pc, c_idx + 1, "poll", c_val, deliv + 1))

        # Invariant 4: DeadlockFreedom
        assert next_states, f"DEADLOCK in non-terminal state {state}"

        for ns in next_states:
            transitions += 1
            if ns not in visited:
                visited.add(ns)
                q.append(ns)

    assert terminal_states == 1, f"Expected 1 terminal state, got {terminal_states}"
    print(f"PASS [Cap={capacity}, MaxMsgs={max_msgs}]: {len(visited):,} distinct states | {transitions:,} transitions | 0 races | 0 deadlocks | 100% FIFO")


if __name__ == "__main__":
    for cap, msgs in [(1, 6), (2, 8), (4, 12)]:
        model_check(cap, msgs)
