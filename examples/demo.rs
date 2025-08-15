use std::io::Write as _;

use ob3::{
    ingress::spawn_processor_thread,
    orderbook::Orderbook,
    types::{BackPressureStrategy, Command, Op, Order, Side},
};

// Our nice looking spinner
const SPINNER: &[&str] = &["|", "/", "-", "\\"];

// ---- Config for ob3 (change this to test different strategies) ----
// The query channel size
const QUERY_CHANNEL_SIZE: usize = 100;
// The maximum delay in milliseconds for processing commands
const MAX_DELAY_MS: u64 = 50;
// The size of the ingress command buffer channel.
const BUFFER_SIZE: usize = 1_000;
// The maximum number of operations to process in a single batch.
const BATCH_SIZE: usize = 100;
// The strategy to use when the command buffer is full.
const BACK_PRESSURE_STRATEGY: BackPressureStrategy = BackPressureStrategy::Block;

// ---- Config for volume (change this to simulate different scenarios) ----
// Total number of operations to process
const INGRESS_NUM_TOTAL: usize = 10_000_000;
// The range of batch sizes for the ingress, this simulates real-world scenarios where volume can vary
const INGRESS_BATCH_RANGE: (u64, u64) = (1_000, 100_000);
// Interval between batches in milliseconds
const INGRESS_BATCH_INTERVAL_MS: u64 = 5;
// Seed for the batch size random number generator
const INGRESS_BATCH_SIZE_SEED: u64 = 1042;

// ---- Config for random operation generation (change this to generate new order test data) ----
// Seed to use for our homemade random number generator to output deterministic results
const OP_SEED: u64 = 42;
// The range of price values for orders
const PRICE_RANGE: (u64, u64) = (1_000, 1_000_000);
// The range of size values for orders
const SIZE_RANGE: (u64, u64) = (1_000, 1_000_000_000);

#[tokio::main]
async fn main() {
    let ops_len = INGRESS_NUM_TOTAL;
    // Generate a large number of random operations
    let mut ops = generate_random_ops(OP_SEED, ops_len);

    // Create a channel for sending commands
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Command<Op>>(INGRESS_BATCH_RANGE.1 as usize);

    let orderbook = Orderbook::default();
    let config = ob3::ingress::IngressConfig {
        query_channel_size: QUERY_CHANNEL_SIZE,
        max_delay_ms: std::time::Duration::from_millis(MAX_DELAY_MS),
        buffer_size: BUFFER_SIZE,
        batch_size: BATCH_SIZE,
        back_pressure_strategy: BACK_PRESSURE_STRATEGY,
    };
    let (producer, mut controller, query_sender) = spawn_processor_thread(orderbook, config);

    let start = std::time::Instant::now();
    println!("Starting to process {} operations...", ops.len());

    // Spawn a task to send commands to the processor thread for consumption
    let producer_task = tokio::spawn(async move {
        while let Some(op) = rx.recv().await {
            producer.send(op.into()).unwrap();
        }
    });

    // Spawn a task to send the operations (to simulate network ingress)
    let sender_task = tokio::spawn(async move {
        let mut lcg = Lcg {
            seed: INGRESS_BATCH_SIZE_SEED,
        };

        for i in 0..ops_len {
            let batch_size = lcg.next_range(INGRESS_BATCH_RANGE.0, INGRESS_BATCH_RANGE.1) as usize;

            print!(
                "\r{} Sending {} operations...",
                SPINNER[i % SPINNER.len()],
                batch_size
            );
            std::io::stdout().flush().unwrap();
            let split = ops.len().saturating_sub(batch_size);
            let batch = ops.split_off(split);

            for op in batch {
                tx.send(Command::Operation(op)).await.unwrap();
            }

            if ops.is_empty() {
                break;
            }

            if INGRESS_BATCH_INTERVAL_MS > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(INGRESS_BATCH_INTERVAL_MS))
                    .await;
            }
        }
    });

    // Wait for the producer and sender tasks to complete
    let _ = tokio::join!(producer_task, sender_task);

    let elapsed = start.elapsed();
    println!("\rTotal time taken: {:?}", elapsed);
    println!(
        "Throughput: {:.2} ops/s",
        ops_len as f64 / elapsed.as_secs_f64()
    );

    let (oneshot_tx, oneshot_rx) = crossbeam_channel::bounded(1);
    query_sender
        .send(ob3::types::Query::GetSnapshot(oneshot_tx))
        .unwrap();

    let snapshot = oneshot_rx.recv().unwrap();
    println!("Orderbook snapshot: {:?}", snapshot);
    // Shutdown the processor thread
    controller.shutdown().unwrap();
    println!("Processor thread shutdown complete.");
}

struct Lcg {
    pub seed: u64,
}

impl Lcg {
    fn next(&mut self) -> u64 {
        // Linear Congruential Generator parameters
        const A: u64 = 6364136223846793005;
        const C: u64 = 1;
        const M: u64 = 1 << 48; // 2^48

        self.seed = (A.wrapping_mul(self.seed).wrapping_add(C)) % M;
        self.seed
    }

    fn next_range(&mut self, min: u64, max: u64) -> u64 {
        let range = max - min;
        min + (self.next() % range)
    }
}

fn generate_random_ops(seed: u64, count: usize) -> Vec<Op> {
    let mut ops = Vec::with_capacity(count);
    let mut lcg = Lcg { seed };
    for i in 0..count {
        let op_type = lcg.next_range(0, 3); // 0: Add, 1: Remove, 2: Modify

        match op_type {
            0 => {
                // Add
                let order = Order {
                    oid: i as u64,
                    price: lcg.next_range(PRICE_RANGE.0, PRICE_RANGE.1),
                    size: lcg.next_range(SIZE_RANGE.0, SIZE_RANGE.1),
                    side: if lcg.next() % 2 == 0 {
                        Side::Bid
                    } else {
                        Side::Ask
                    },
                    timestamp: lcg.next(),
                };
                ops.push(Op::Add(order));
            }
            1 => {
                // Remove
                let oid = lcg.next_range(0, i as u64);
                ops.push(Op::Remove(oid));
            }
            2 => {
                // Modify
                let oid = lcg.next_range(0, i as u64);
                let size = lcg.next_range(SIZE_RANGE.0, SIZE_RANGE.1);
                ops.push(Op::Modify { oid, size });
            }
            _ => unreachable!(),
        }
    }

    ops
}
