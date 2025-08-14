use std::io::Write as _;

use ob3::{
    ingress::spawn_processor_thread,
    orderbook::Orderbook,
    types::{Command, Op, Order, Side},
};

const SPINNER: &[&str] = &["|", "/", "-", "\\"];

#[tokio::main]
async fn main() {
    let ops_len = 10_000_000;
    // Generate a large number of random operations
    let mut ops = generate_random_ops(42, ops_len);

    // Create a channel for sending commands
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Command<Op>>(10_000);

    let orderbook = Orderbook::default();
    let config = ob3::ingress::IngressConfig {
        max_delay_ms: std::time::Duration::from_millis(50),
        buffer_size: 1_000,
        batch_size: 100,
        back_pressure_strategy: ob3::types::BackPressureStrategy::Block,
    };
    let (producer, mut controller) = spawn_processor_thread(orderbook, config);

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
        let mut lcg = Lcg { seed: 1042 };

        for i in 0..ops_len {
            let batch_size = lcg.next_range(1, 10_000) as usize; // Random batch size

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
                    price: lcg.next_range(100, 1000),
                    size: lcg.next_range(1, 100),
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
                let size = lcg.next_range(1, 100);
                ops.push(Op::Modify { oid, size });
            }
            _ => unreachable!(),
        }
    }

    ops
}
