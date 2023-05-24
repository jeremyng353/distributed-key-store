# Distributed Key-Value Store
Distributed key-value store that is hosted on an AWS EC2 instance. Features the following components:
- Key-value store: Uses a hashmap to store key-value pairs.
- Internodal communications: Uses Google protobufs to serialize data to communicate between nodes.
- At-most-once semantics: Ensures that each message is received at most once. Done using Google's Guava caches.
- Consistent hash ring: Balances server load across all nodes by allocating certain hash ranges to each node. 
- Member monitor: Periodically checks each node to check whether they're alive.
- Key transferer: Distributes key-value pairs from a dead node to alive nodes.
- Replication chain: Replicates key-value pairs atomically to ensure that the system can still return data even if a node goes down.

## Launching the system
`./start.sh <NUM_NODES>` where `<NUM_NODES>` is the number of nodes to launch.
