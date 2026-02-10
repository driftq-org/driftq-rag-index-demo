# Distributed Systems Notes

A distributed system is a bunch of independent machines that try to act like one system.
The tricky parts are partial failures, time, concurrency, and coordination.

Common tools you will see in the wild:

- Consensus (Raft/Paxos): agree on the current truth
- Replication + quorum reads/writes: keep data available and safe
- Message queues and logs: decouple producers and consumers
- Idempotency + retries: survive duplicate messages safely

This demo uses a queue and retries to show how real systems keep going even when a worker fails.
