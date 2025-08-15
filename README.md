# ob3

A small in-memory level‑3 orderbook library with built‑in ingress buffering.

TODO:
- Use rayon for parallel processing the data query
- Sync over exchange snapshot (for the initial load or restart)

## Quick start

You can play around with this using the [demo](/examples/demo.rs).

This demo aims to simulate a real world scenario where we have a stream of order events coming in at various speed, and we want to maintain a level-3 orderbook in memory.

You can configure the ingress traffic to simulate different scenarios, use the interval parameter to test if your strategy is the bottleneck here (by comparing the speed with 0 interval and some interval).

You can configure the event generator to generate new tests (we use a simple deterministic random generation logic here to produce comparable outcomes).

You can configure the strategy to test out different combinations (different buffer size, backpressure policy, batch size etc.).

You can simply run:

```zsh
cargo run --example demo
```

Example output (processing 10M events in 11s, not bad!):

```text
Starting to process 10000000 operations...
Total time taken: 11.378316209s
Throughput: 878864.66 ops/s
Orderbook snapshot: OrderbookSnapshot { total_orders: 721043, total_bids: 360189, total_asks: 360854, checksum: 3618553793530 }
```