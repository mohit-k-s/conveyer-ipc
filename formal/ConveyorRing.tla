--------------------------- MODULE ConveyorRing ---------------------------
EXTENDS Naturals, Sequences

CONSTANTS Capacity, MaxMsgs
ASSUME Capacity \in Nat \ {0}

VARIABLES
    seq,        \* Array [0..Capacity-1] of atomic sequence numbers
    data,       \* Array [0..Capacity-1] of slot payloads
    p_idx,      \* Producer private register index
    p_pc,       \* Producer program counter: "poll" | "write" | "publish"
    c_idx,      \* Consumer private register index
    c_pc,       \* Consumer program counter: "poll" | "read" | "release"
    c_val,      \* Consumer local register holding popped value
    delivered   \* Sequence of all delivered values

vars == <<seq, data, p_idx, p_pc, c_idx, c_pc, c_val, delivered>>

Slot(i) == i % Capacity
Lap(i)  == i \div Capacity

Init ==
    /\ seq = [s \in 0..(Capacity - 1) |-> 0]
    /\ data = [s \in 0..(Capacity - 1) |-> 0]
    /\ p_idx = 0
    /\ p_pc = "poll"
    /\ c_idx = 0
    /\ c_pc = "poll"
    /\ c_val = 0
    /\ delivered = <<>>

\* Producer steps
ProdPoll ==
    /\ p_pc = "poll"
    /\ p_idx < MaxMsgs
    /\ seq[Slot(p_idx)] = 2 * Lap(p_idx)
    /\ p_pc' = "write"
    /\ UNCHANGED <<seq, data, p_idx, c_idx, c_pc, c_val, delivered>>

ProdWrite ==
    /\ p_pc = "write"
    /\ data' = [data EXCEPT ![Slot(p_idx)] = p_idx + 1]
    /\ p_pc' = "publish"
    /\ UNCHANGED <<seq, p_idx, c_idx, c_pc, c_val, delivered>>

ProdPublish ==
    /\ p_pc = "publish"
    /\ seq' = [seq EXCEPT ![Slot(p_idx)] = 2 * Lap(p_idx) + 1]
    /\ p_idx' = p_idx + 1
    /\ p_pc' = "poll"
    /\ UNCHANGED <<data, c_idx, c_pc, c_val, delivered>>

\* Consumer steps
ConsPoll ==
    /\ c_pc = "poll"
    /\ c_idx < MaxMsgs
    /\ seq[Slot(c_idx)] = 2 * Lap(c_idx) + 1
    /\ c_pc' = "read"
    /\ UNCHANGED <<seq, data, p_idx, p_pc, c_idx, c_val, delivered>>

ConsRead ==
    /\ c_pc = "read"
    /\ c_val' = data[Slot(c_idx)]
    /\ c_pc' = "release"
    /\ UNCHANGED <<seq, data, p_idx, p_pc, c_idx, delivered>>

ConsRelease ==
    /\ c_pc = "release"
    /\ seq' = [seq EXCEPT ![Slot(c_idx)] = 2 * (Lap(c_idx) + 1)]
    /\ delivered' = Append(delivered, c_val)
    /\ c_idx' = c_idx + 1
    /\ c_pc' = "poll"
    /\ UNCHANGED <<data, p_idx, p_pc, c_val>>

Next ==
    \/ ProdPoll \/ ProdWrite \/ ProdPublish
    \/ ConsPoll \/ ConsRead  \/ ConsRelease

\* Safety Invariants
NoDataRace ==
    ~(/\ p_pc \in {"write", "publish"}
      /\ c_pc \in {"read", "release"}
      /\ Slot(p_idx) = Slot(c_idx))

FifoExactness ==
    \A k \in 1..Len(delivered) : delivered[k] = k

BoundedOccupancy ==
    /\ p_idx >= c_idx
    /\ p_idx - c_idx <= Capacity

TypeOK ==
    /\ p_idx \in 0..MaxMsgs
    /\ c_idx \in 0..MaxMsgs

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)
=============================================================================
