use std::io::Write as _;

use ob3::{
    ingress::spawn_processor_thread,
    lcg::{Lcg, generate_random_ops},
    orderbook::Orderbook,
    types::{BackPressureStrategy, Command, Op, Query},
};
use tokio_util::sync::CancellationToken;

// Our nice looking spinner
const SPINNER: &[&str] = &["|", "/", "-", "\\"];

// ---- Config for ob3 (change this to test different strategies) ----
// The maximum delay in milliseconds for processing commands
const MAX_DELAY_MS: u64 = 50;
// The size of the ingress command buffer channel.
const BUFFER_SIZE: usize = 1_000;
// The maximum number of operations to process in a single batch.
const BATCH_SIZE: usize = 100;
// The strategy to use when the command buffer is full.
const BACK_PRESSURE_STRATEGY: BackPressureStrategy = BackPressureStrategy::Block;

// ---- Config for query (change this to test different query frequency) ----
// The query channel size
const QUERY_CHANNEL_SIZE: usize = 10;
// The frequency of queries in milliseconds
const QUERY_FREQUENCY_MS: u64 = 500;
// The level of book to query
const QUERY_LEVEL: usize = 1000;

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
    let mut ops = generate_random_ops(OP_SEED, ops_len, PRICE_RANGE, SIZE_RANGE);
    let total_adds = ops.iter().filter(|op| matches!(op, Op::Add(_))).count();
    let total_removes = ops.iter().filter(|op| matches!(op, Op::Remove(_))).count();
    let total_modifies = ops
        .iter()
        .filter(|op| matches!(op, Op::Modify { .. }))
        .count();

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
    println!(
        "({} adds, {} removes, {} modifies)",
        total_adds, total_removes, total_modifies
    );
    println!(
        "(Querying top {} levels every {} ms)",
        QUERY_LEVEL, QUERY_FREQUENCY_MS
    );
    // Spawn a task to send commands to the processor thread for consumption
    let producer_task = tokio::spawn(async move {
        while let Some(op) = rx.recv().await {
            producer.send(op).unwrap();
        }
    });

    // Spawn a task to send the operations (to simulate network ingress)
    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    let sender_task = tokio::spawn(async move {
        let mut lcg = Lcg {
            seed: INGRESS_BATCH_SIZE_SEED,
        };

        for i in 0..ops_len {
            let batch_size =
                lcg.next_rand_in_range(INGRESS_BATCH_RANGE.0, INGRESS_BATCH_RANGE.1) as usize;

            print!("\r{}", SPINNER[i % SPINNER.len()]);
            std::io::stdout().flush().unwrap();
            let split = ops.len().min(batch_size);
            for op in ops.drain(..split) {
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

        shutdown_signal.cancel();
    });

    // Spawn a task to periodically query the orderbook
    let sender = query_sender.clone();
    let query_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    println!("\nShutdown signal received, stopping query task.");
                    break;
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(QUERY_FREQUENCY_MS)) => {
                    let (query_tx, query_rx) = crossbeam_channel::unbounded();
                    let query = Query::GetTopNLevels {
                        n: QUERY_LEVEL,
                        sender: query_tx,
                    };

                    // End the task loop after the processor thread is done (which closes the channel)
                    if sender.send(query).is_err() {
                        break;
                    }

                    // Block until we receive the snapshot
                    let _ = query_rx.recv().expect("Failed to receive query response");
                }
            }
        }
    });

    // Wait for the tasks to complete
    let _ = tokio::join!(producer_task, sender_task, query_task);

    let elapsed = start.elapsed();
    println!("\rTotal time taken: {:?}", elapsed);
    println!(
        "Throughput: {:.2} ops/s",
        ops_len as f64 / elapsed.as_secs_f64()
    );

    // Shutdown the processor thread
    let orderbook = controller.shutdown().unwrap();

    // Print the final snapshot
    let final_snapshot = orderbook.generate_simple_snapshot();
    println!("Total orders in the book: {}", final_snapshot.total_orders);
    println!("Total bids: {}", final_snapshot.total_bids);
    println!("Total asks: {}", final_snapshot.total_asks);
    println!("Checksum: {}", final_snapshot.checksum);
    println!("Processor thread shutdown complete.");
}
