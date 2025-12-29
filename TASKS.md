- New protocol iteration more akin to Redis Streams (ID instead of TIMESTAMP , no explicit option for strategy)
- Padding to IDs https://claude.ai/chat/495a7978-8fc1-486d-a8bb-ab0a638244ca
- TCP improvements https://claude.ai/chat/956517f3-75f3-4d08-9784-61a053aaf370 , https://jewelhuq.medium.com/mastering-high-performance-tcp-udp-socket-programming-in-go-996dc85f5de1 , https://dev.to/jones_charles_ad50858dbc0/build-a-blazing-fast-tcp-server-in-go-a-practical-guide-29d , https://www.youtube.com/watch?v=LI1YTFMi8W4 , https://www.reddit.com/r/golang/comments/1c174z7/ways_to_improve_performance_on_thousands_of/

REFACTOR:

- Refactor: Delayer + Listener interfaces for tcp.Server (instead of passing it functions); Delayer.Bakcoff(), Delayer.Reset(), Listener.Listen()
- Refactor: Decode bulk bytes in multiple steps to get the length first, then pass the length to BufferProvider so it can make better decisions about the size of the buffer it provides, e.g. it can allocate exactly those many bytes or match the length to the closest buffer class size to return one of those

- Turn the client main code into a benchmarking harness

- Introduce structured logging (for later tracing and debugging) https://claude.ai/chat/e89d7de0-ffa6-46d3-9a50-2d4b4984534b
- Add Promtail (log shipper) to ship logs to Loki (log store) to then visualize in Grafana
- Add prometheus client and server to expose logs to Prometheus to then visualize in Grafana

- Raft implementation

- Introduce config (move static values)

- A more advanced version of the protocol should inform the client of version and configured limits upon connection

- Client-side protocol components (command encoder, reply decoder) improvements (implementation + tests) since they were the most AI-involved components
