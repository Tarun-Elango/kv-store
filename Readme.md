  ## Key value store

- distributed key-value store with durable writes, 
- leader–follower replication
- Raft-based consensus for fault-tolerant 
- communicating over a lightweight custom binary TCP protocol.


To run leader :
cd cmd/server
go run main.go \
  -node-id=leader-1 \
  -role=leader \
  -client-addr=:9000 \
  -wal=data/leader.wal \
  -followers=127.0.0.1:9002,127.0.0.1:9003

To run followers :
cd cmd/server
go run main.go \
  -node-id=follower-1 \
  -role=follower \
  -client-addr=:9010 \
  -replication-addr=:9002 \
  -wal=data/follower.wal \
  -leader-id=leader-1 \
  -leader-addr=127.0.0.1:9000

compare data : cmp cmd/server/data/leader.wal cmd/server/data/follower.wal
check bytes: 
xxd cmd/server/data/leader.wal
xxd cmd/server/data/follower.wal