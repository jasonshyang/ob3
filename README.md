# ob3

A small in-memory level‑3 orderbook library with built‑in ingress buffering.

## Quick start

You can play around with this library using the [demo](/examples/demo.rs).

This demo aims to simulate a real world scenario where we have a stream of order events coming in at various speed, and we want to maintain a level-3 orderbook in memory, and we want to access the orderbook snapshot periodically.

### Configuration
You can configure the ingress traffic to simulate different scenarios, use the interval parameter to test if your strategy is the bottleneck here (by comparing the speed with 0 interval and some interval).

You can configure the event generator to generate new tests (we use a simple deterministic random generation logic here to produce comparable outcomes).

You can configure the strategy to test out different combinations (different buffer size, backpressure policy, batch size etc.).

You can configure how frequent we access the orderbook (here we use a simple example where we want to access a summary of top N level of the book).

### Run it!

```zsh
cargo run --example demo
```

Example output (processing 10M events in 11s, not bad!):

```text
Starting to process 10000000 operations...
(Querying top 1000 levels every 500 ms)
Total time taken: 11.474647334s
Throughput: 871486.48 ops/s
Total orders in the book: 721043
Total bids: 360189
Total asks: 360854
Checksum: 3618553793530
```