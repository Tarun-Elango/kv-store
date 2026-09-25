# Node startup and replication

The role is chosen by `-role` when `cmd/server/main.go` starts. The two paths share the same client protocol, but only the follower opens a separate replication listener. These diagrams show the current configured leader/follower behavior; startup does not elect a leader.

## Starting as leader

```mermaid
flowchart TD
    A[Start with role leader] --> B[Create empty in-memory store]
    B --> C[Open leader WAL]
    C --> D[Replay WAL records in index order]
    D --> E[Apply each SET or DEL to store<br/>and rebuild in-memory replication entries]
    E --> F[Validate recovered entries are contiguous<br/>from index 1]
    F --> G[Set next client-write index<br/>to WAL last index + 1]
    G --> H[Start one replication worker<br/>per configured follower]
    H --> I[Start client TCP listener]
    H --> J[Each worker starts at NextIndex = 1<br/>and catches its follower up]
    J --> K[Send PrevIndex, previous entry,<br/>and a batch of missing entries]
    K --> L{Follower response}
    L -->|success| M[Set NextIndex = LastIndex + 1]
    M --> N{More entries?}
    N -->|yes| K
    N -->|no| O[Wait for a new write]
    O --> K
    L -->|gap| P[Set NextIndex to follower LastIndex + 1]
    P --> R[Retry after backoff]
    L -->|previous entry conflict| Q[Move NextIndex back by one]
    Q --> R
    L -->|network or other error| R
    R --> K
```

The leader checks that its recovered log is contiguous. Each worker then uses the follower's reported index to catch up; a conflicting previous entry makes it search backward for a shared prefix.

## Starting as follower

```mermaid
flowchart TD
    A[Start with role follower] --> B[Require expected leader ID]
    B --> C[Create empty in-memory store]
    C --> D[Open follower WAL and replay records in index order]
    D --> E[Apply records to store<br/>and restore entry map and LastApplied]
    E --> F[Start replication TCP listener]
    E --> G[Start client TCP listener]
    F --> H[Receive AppendRequest from leader]
    H --> I{Leader ID matches?}
    I -->|no| X[Reject request with current LastIndex]
    I -->|yes| J{PrevIndex exists and<br/>previous entry matches?}
    J -->|gap or conflict| X
    J -->|yes| K[Validate incoming indexes are consecutive<br/>and commands are valid]
    K -->|invalid| X
    K -->|valid| L{Incoming entry differs<br/>from an existing entry?}
    L -->|yes| M[Truncate divergent WAL suffix<br/>and rebuild store from retained prefix]
    L -->|no| N[Skip matching entries already applied]
    M --> N
    N --> O[Append new entries to WAL first]
    O --> P[Apply to store and update<br/>entry map and LastApplied]
    P --> Q[Reply success with LastIndex]
    Q --> H
    X --> H
```

The follower's client listener serves local reads. It rejects SET and DEL with `StatusNotLeader` and the configured leader address. It does not forward the request itself.
