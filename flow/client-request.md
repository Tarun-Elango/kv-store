# Client request path

This shows a client `SET` or `DEL`. The client connects to a node's **client** TCP port; the leader's replication workers connect separately to each follower's **replication** TCP port.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant FollowerClient as Follower client listener
    participant Leader as Leader client listener
    participant LeaderWAL as Leader WAL and store
    participant Worker as Leader replication worker
    participant FollowerPeer as Follower replication listener
    participant FollowerWAL as Follower WAL and store

    opt Client first connects to a follower
        Client->>FollowerClient: SET or DEL
        FollowerClient-->>Client: NotLeader + configured leader address
        Note over Client: Client must reconnect to leader manually
    end

    Client->>Leader: SET or DEL
    Leader->>Leader: Serialize writes and assign next index
    Leader->>LeaderWAL: Append indexed record and sync
    LeaderWAL-->>Leader: Durable locally
    Leader->>LeaderWAL: Apply command to in-memory store
    Leader->>Leader: Advance next client-write index
    Leader->>Leader: Check replication entry index equals log length + 1
    Leader->>Worker: Queue entry and wake worker
    Leader-->>Client: OK after local write without waiting for followers

    loop Independently for each configured follower
        Worker->>FollowerPeer: AppendRequest(leader ID, PrevIndex, previous entry, entries)
        FollowerPeer->>FollowerPeer: Check leader ID, previous entry, indexes, and commands
        alt Gap or conflicting previous entry
            FollowerPeer-->>Worker: Reject with LastIndex / conflict
            Worker->>Worker: Adjust NextIndex and retry after backoff
        else Entries accepted
            opt Divergent existing entry
                FollowerPeer->>FollowerWAL: Truncate divergent suffix and rebuild store
            end
            FollowerPeer->>FollowerWAL: Append new entries and sync, then apply
            FollowerPeer-->>Worker: Success + LastIndex
            Worker->>Worker: Advance NextIndex = LastIndex + 1
        end
    end
```

`GET`, `LEN`, and `PING` use the connected node's client listener and do not enter the write replication path. A follower serves `GET` and `LEN` from its local store, so a read can lag behind a recently acknowledged leader write. The sample CLI prints the `NotLeader` address but does not automatically reconnect.
