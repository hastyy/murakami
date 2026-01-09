- New protocol iteration more akin to Redis Streams (ID instead of TIMESTAMP , no explicit option for strategy)
- Padding to IDs https://claude.ai/chat/495a7978-8fc1-486d-a8bb-ab0a638244ca
- TCP improvements https://claude.ai/chat/956517f3-75f3-4d08-9784-61a053aaf370 , https://jewelhuq.medium.com/mastering-high-performance-tcp-udp-socket-programming-in-go-996dc85f5de1 , https://dev.to/jones_charles_ad50858dbc0/build-a-blazing-fast-tcp-server-in-go-a-practical-guide-29d , https://www.youtube.com/watch?v=LI1YTFMi8W4 , https://www.reddit.com/r/golang/comments/1c174z7/ways_to_improve_performance_on_thousands_of/

REFACTOR:

- Turn the client main code into a benchmarking harness

- Introduce structured logging (for later tracing and debugging) https://claude.ai/chat/e89d7de0-ffa6-46d3-9a50-2d4b4984534b
- Add Promtail (log shipper) to ship logs to Loki (log store) to then visualize in Grafana
- Add prometheus client and server to expose logs to Prometheus to then visualize in Grafana

- Raft implementation

- Introduce config (move static values)

- A more advanced version of the protocol should inform the client of version and configured limits upon connection

- Client-side protocol components (command encoder, reply decoder) improvements (implementation + tests) since they were the most AI-involved components

- I was using a bufferpool as a means to limit the memory used by the system and avoid OOM (safety), but it can cause service unavailability if we exhaust all the buffers in the pool (liveness); I then explored other techniques, such as using different pool sizes and splitting the bigger sizes in small if needed, but if demand is more towards the bigger sizes then we can still be unavailable in the same way; there were then other concerns around pooling the same buffer twice by accident, which would likely cause data loss by concurrent writes to the same buffer (was solving through the poison method but there were still possible race conditions there). Finally, I explored the idea of replenishing the pool once it went below a threshold, but realised it would just cause the same problem as the naive approach of allocating until we OOM. Overall, the only safe approach is to try and offload older records from memory and do it at least as fast as memory is demanded; if memory usage grows faster we need to intentionally cause service unavailability at some point or else we'll OOM. The simplest scenario is to think about a hashmap behind an API which is allowed to grow undefinitely. A more real-world scenario is to think about LSM-trees whose memtables need to get flushed to disk in order to release memory. A bufferpool makes more sense in scenarios where we are continuously uing the same buffers and each usage is very short (e.g. moving data from point A to B). But in our case we write the records to a buffer and then hog to it for the lifetime of the record, so there's the pontial of rapidly exhausting the pool and then either having to allocate more (potential OMM risk) or stop serving requests (service unavailability).
- We currently even have a bug where trimmed records' buffers are not returned to the pool, nor records from a deleted stream

Next steps:

- add tcp server middleware
- add the logging package, with a logging middleware for wideEvents
- start instrumenting the code to populate the wide event
- add a text logger to each major component, mostly for debug logs
- create scripts and benchmarks with the client library to test our current implementation
- start replication work

RAFT IMPROVEMENT:

```
If desired, the protocol can be optimized to reduce the
number of rejected AppendEntries RPCs. For example,
when rejecting an AppendEntries request, the follower
can include the term of the conflicting entry and the first
index it stores for that term. With this information, the
leader can decrement nextIndex to bypass all of the conflicting entries in that term; one AppendEntries RPC will
be required for each term with conflicting entries, rather
than one RPC per entry. In practice, we doubt this optimization is necessary, since failures happen infrequently
and it is unlikely that there will be many inconsistent entries.
```
