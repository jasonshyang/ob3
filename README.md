# ob3

A small in-memory level‑3 orderbook library with built‑in ingress buffering mainly for research purposes.

Currently two data structures are explored:
#### V1 - uses a combination of HashMap, BTreeSet and BTreeMap
```rust
pub struct Orderbook {
    pub next_idx: usize,
    pub idx_map: HashMap<usize, Order>,       // idx => Order
    pub oid_map: HashMap<String, usize>,      // oid => idx
    pub bids: BTreeMap<u64, BTreeSet<usize>>, // price => level(indices)
    pub asks: BTreeMap<u64, BTreeSet<usize>>, // price => level(indices)
}
```
#### V2 - uses Slab with pointers
```rust
pub struct OrderbookV2 {
    pub slab: Slab<Slot<Order>>,
    pub map: HashMap<String, usize>,   // Oid -> Slab key
    pub bids: BTreeMap<u64, LevelInfo>, // Price -> LevelInfo
    pub asks: BTreeMap<u64, LevelInfo>, // Price -> LevelInfo
}
```

## Quick start

You can run the two [demos](/src/bin/)
```zsh
cargo run --bin v1 --release
cargo run --bin v2 --release
```

These demo aims to simulate a real world scenario where we have a stream of order events coming in at various speed, and we want to maintain a level-3 orderbook in memory, and we want to access the orderbook snapshot periodically.

Example output (processing 10M events in 3s, not bad!):
#### V1:
```
Starting to process 10000000 operations...
(6667958 adds, 1998011 removes, 1334031 modifies)
(Querying top 1000 levels every 500 ms)
/
Shutdown signal received, stopping query task.
Total time taken: 4.079288625s
Throughput: 2451407.81 ops/s
Total orders in the book: 1519929
Total bids: 199951
Total asks: 1319978
Checksum: 761178567166227
Processor thread shutdown complete.
```

#### V2:
```
Starting to process 10000000 operations...
(6667958 adds, 1998011 removes, 1334031 modifies)
(Querying top 1000 levels every 500 ms)
/
Shutdown signal received, stopping query task.
Total time taken: 3.703655292s
Throughput: 2700035.29 ops/s
Total orders in the book: 1519929
Total bids: 199951
Total asks: 1319978
Checksum: 761178567166227
Processor thread shutdown complete.
```

### Configuration
You can configure how these demo simulations are ran directly on [demos](/src/bin/).

- configure the ingress traffic to simulate different scenarios, use the interval parameter to test if your strategy is the bottleneck here (by comparing the speed with 0 interval and some interval).
- configure the event generator to generate new tests (we use a simple deterministic random generation logic here to produce comparable outcomes).
- configure the strategy to test out different combinations (different buffer size, backpressure policy, batch size etc.).
- configure how frequent we access the orderbook (here we use a simple example where we want to access a summary of top N level of the book).