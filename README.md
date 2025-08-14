# ob3

A small in-memory level‑3 orderbook library with built‑in ingress buffering.

TODO:
- Add command to access the orderbook data
- Use rayon for parallel processing the data query

## Quick start
Run the demo:

```zsh
cargo run
```

Example output (processing 10M events):

```text
Starting to process 10000000 operations...
Total time taken: 7.753756292s
Throughput: 1289697.49 ops/s
```

You can change the setting (e.g. using the `BackPressureStrategy::Drop`) to get different performance results.