use crate::types::{Op, Order, Side};

/*
    To generate some pseudo-random ops.
*/

pub struct Lcg {
    pub seed: u64,
}

impl Lcg {
    pub fn next_rand(&mut self) -> u64 {
        // Linear Congruential Generator parameters
        const A: u64 = 6364136223846793005;
        const C: u64 = 1;
        const M: u64 = 1 << 48; // 2^48

        self.seed = (A.wrapping_mul(self.seed).wrapping_add(C)) % M;
        self.seed
    }

    pub fn next_rand_in_range(&mut self, min: u64, max: u64) -> u64 {
        let range = max - min;
        min + (self.next_rand() % range)
    }
}

pub fn generate_random_ops(
    seed: u64,
    count: usize,
    price_range: (u64, u64),
    size_range: (u64, u64),
) -> Vec<Op> {
    let mut ops = Vec::with_capacity(count);
    let mut lcg = Lcg { seed };
    for i in 0..count {
        let op_type = lcg.next_rand_in_range(0, 6); // 0 - 3: Add, 4: Remove, 5: Modify

        match op_type {
            0..=3 => {
                // Add
                let order = Order {
                    oid: i.to_string(),
                    price: lcg.next_rand_in_range(price_range.0, price_range.1),
                    size: lcg.next_rand_in_range(size_range.0, size_range.1),
                    side: if lcg.next_rand() % 2 == 0 {
                        Side::Bid
                    } else {
                        Side::Ask
                    },
                    timestamp: lcg.next_rand(),
                };
                ops.push(Op::Add(order));
            }
            4 => {
                // Remove
                let oid = lcg.next_rand_in_range(0, i as u64).to_string();
                ops.push(Op::Remove(oid));
            }
            5 => {
                // Modify
                let oid = lcg.next_rand_in_range(0, i as u64).to_string();
                let size = lcg.next_rand_in_range(size_range.0, size_range.1);
                ops.push(Op::Modify { oid, size });
            }
            _ => unreachable!(),
        }
    }

    ops
}

impl std::iter::Iterator for Lcg {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next_rand())
    }
}
