use std::collections::{BTreeMap, BTreeSet, HashMap};

use rayon::{
    iter::{IntoParallelRefIterator, ParallelIterator as _},
    slice::ParallelSlice as _,
};

use crate::{
    error::Error,
    types::{
        BatchProcessor, Either, Level, Op, Order, OrderbookSnapshot, Query, Side, SimpleSnapshot,
        TopNLevels,
    },
};

/*
    Core Orderbook implementation.
*/

// here we assume our oid is sequential, so we can use it as FIFO index
#[derive(Debug, Clone, Default)]
pub struct Orderbook {
    pub map: HashMap<u64, Order>,           // oid => order
    pub bids: BTreeMap<u64, BTreeSet<u64>>, // price => level
    pub asks: BTreeMap<u64, BTreeSet<u64>>, // price => level
}

impl BatchProcessor for Orderbook {
    type Operation = Op;
    type Snapshot = OrderbookSnapshot;

    // TODO: right now it's a very naive implementation, we can optimize it later
    fn process_ops(&mut self, ops: Vec<Op>) {
        for op in ops {
            self.process_op(op);
        }
    }

    fn process_query(&self, query: Query<OrderbookSnapshot>) -> Result<(), Error> {
        match query {
            Query::GetSimpleSnapshot(sender) => {
                sender.send(OrderbookSnapshot::Simple(self.generate_simple_snapshot()))?;
            }
            Query::GetTopNLevels { n, sender } => {
                sender.send(OrderbookSnapshot::TopNLevels(
                    self.generate_level_snapshot(n),
                ))?;
            }
        }

        Ok(())
    }
}

impl Orderbook {
    const CHUNK: usize = 1024;

    pub fn generate_simple_snapshot(&self) -> SimpleSnapshot {
        let total_orders = self.map.len();
        let total_bids = self.bids.values().map(|levels| levels.len()).sum();
        let total_asks = self.asks.values().map(|levels| levels.len()).sum();
        let checksum = self
            .map
            .values()
            .fold(0u64, |acc, order| acc.wrapping_add(order.oid));

        SimpleSnapshot {
            total_orders,
            total_bids,
            total_asks,
            checksum,
        }
    }

    pub fn generate_level_snapshot(&self, n: usize) -> TopNLevels {
        let (bids, asks) = rayon::join(
            || self.generate_level_snapshot_for_side(Side::Bid, n),
            || self.generate_level_snapshot_for_side(Side::Ask, n),
        );

        TopNLevels { n, bids, asks }
    }

    pub fn generate_level_snapshot_for_side(&self, side: Side, n: usize) -> Vec<Level> {
        // We want to give the response in best price order
        let iter = match side {
            Side::Ask => Either::Ascending(self.asks.iter()),
            Side::Bid => Either::Descending(self.bids.iter().rev()),
        };

        let top_levels_ref: Vec<(u64, &BTreeSet<u64>)> =
            iter.take(n).map(|(p, s)| (*p, s)).collect();

        top_levels_ref
            .par_iter()
            .map(|(price, oids_set)| {
                let oids: Vec<u64> = oids_set.iter().cloned().collect();

                let (total_size, total_count) = if oids.len() < Self::CHUNK {
                    self.summarize_level(&oids)
                } else {
                    oids.par_chunks(Self::CHUNK)
                        .map(|c| self.summarize_level(c))
                        .reduce_with(|a, b| (a.0 + b.0, a.1 + b.1))
                        .unwrap_or((0, 0))
                };

                Level {
                    price: *price,
                    total_size,
                    total_count,
                }
            })
            .collect()
    }

    pub fn get(&self, oid: u64) -> Option<&Order> {
        self.map.get(&oid)
    }

    pub fn depth(&self, side: Side) -> usize {
        match side {
            Side::Bid => self.bids.len(),
            Side::Ask => self.asks.len(),
        }
    }

    pub fn contains(&self, oid: u64) -> bool {
        self.map.contains_key(&oid)
    }

    pub fn process_op(&mut self, op: Op) {
        match op {
            Op::Add(order) => self.add(order),
            Op::Remove(oid) => self.remove(oid),
            Op::Modify { oid, size } => self.modify(oid, size),
        }
    }

    pub fn add(&mut self, mut order: Order) {
        // Check if the order already exists
        if self.contains(order.oid) {
            return;
        }

        // Fill order
        let mut filled = Vec::new();

        // Determine the iteration direction based on the order side
        let iter = match order.side {
            Side::Bid => Either::Ascending(self.asks.range(..=order.price)),
            Side::Ask => Either::Descending(self.bids.range(order.price..).rev()),
        };

        'outer: for (_, levels) in iter {
            for &oid in levels {
                if let Some(other) = self.map.get_mut(&oid) {
                    order.fill(other);
                    // If the order is fully filled, add to filled list for removal
                    if other.size == 0 {
                        filled.push(oid);
                    }

                    // If the order is fully filled, break out of the outer loop
                    if order.size == 0 {
                        break 'outer;
                    }
                }
            }
        }

        // Handle the filled orders
        for oid in filled {
            let order = self.map.remove(&oid).expect("Order should exist in map");
            let book = match order.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            book.get_mut(&order.price)
                .expect("Price level should exist")
                .remove(&oid);
            if book[&order.price].is_empty() {
                book.remove(&order.price);
            }
        }

        // If new order is not fully filled, add it to the orderbook
        if order.size > 0 {
            let book = match order.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            book.entry(order.price).or_default().insert(order.oid);

            self.map.insert(order.oid, order);
        }
    }

    pub fn remove(&mut self, oid: u64) {
        // Remove the order from the map
        if let Some(order) = self.map.remove(&oid) {
            // Remove the order from the corresponding book
            let book = match order.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };

            if let Some(level) = book.get_mut(&order.price) {
                level.remove(&oid);
                if level.is_empty() {
                    book.remove(&order.price);
                }
            }
        }
    }

    pub fn modify(&mut self, oid: u64, size: u64) {
        if size == 0 {
            self.remove(oid);
            return;
        }

        if let Some(order) = self.map.get_mut(&oid) {
            order.size = size;
        }
    }

    fn summarize_level(&self, oids: &[u64]) -> (u64, usize) {
        let total_size = oids.iter().fold(0, |acc, oid| {
            if let Some(order) = self.map.get(oid) {
                acc + order.size
            } else {
                acc
            }
        });
        (total_size, oids.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Order, Side};

    #[test]
    fn test_orderbook_add() {
        let mut orderbook = Orderbook::default();

        let orders = vec![
            Order {
                oid: 1,
                price: 100,
                size: 15,
                side: Side::Bid,
                timestamp: 0,
            },
            Order {
                oid: 2,
                price: 101,
                size: 5,
                side: Side::Ask,
                timestamp: 1,
            },
            Order {
                oid: 3,
                price: 98,
                size: 15,
                side: Side::Bid,
                timestamp: 2,
            },
        ];

        for order in orders {
            orderbook.add(order);
        }

        assert!(orderbook.contains(1));
        assert!(orderbook.contains(2));
        assert!(orderbook.contains(3));
        assert_eq!(orderbook.depth(Side::Bid), 2); // 100 and 98
        assert_eq!(orderbook.depth(Side::Ask), 1); // 101

        let new_order = Order {
            oid: 4,
            price: 99,
            size: 5,
            side: Side::Ask,
            timestamp: 3,
        };
        orderbook.add(new_order);

        // We expect the new order should match oid 1 and half fill it.
        // bids: {98: {3}, 100: {1}}, asks: {101: {2}}
        assert!(orderbook.contains(3));
        assert_eq!(orderbook.get(1).unwrap().size, 10); // oid 1 should be partially filled
        assert_eq!(orderbook.depth(Side::Ask), 1);

        let new_order = Order {
            oid: 5,
            price: 98,
            size: 10,
            side: Side::Ask,
            timestamp: 4,
        };
        orderbook.add(new_order);

        // We expect oid 1 to be fully filled now
        // bids: {98: {3}}, asks: {101: {2}}
        assert!(!orderbook.contains(1)); // oid 1 should be fully filled and removed

        let new_order = Order {
            oid: 6,
            price: 102,
            size: 15,
            side: Side::Bid,
            timestamp: 5,
        };
        orderbook.add(new_order);

        // We expect oid 2 to be fully filled and remaining of oid 6 is added to bids
        // bids: {98: {3}, 102: {6}}, asks: {}
        assert!(!orderbook.contains(2)); // oid 2 should be fully filled and removed
        assert!(orderbook.contains(6)); // oid 6 should be added to bids
        assert_eq!(orderbook.get(6).unwrap().size, 10); // oid 6 should have 10 left
    }

    #[test]
    fn test_top_n_level_summary() {
        let mut orderbook = Orderbook::default();

        let orders = vec![
            Order {
                oid: 1,
                price: 100,
                size: 15,
                side: Side::Bid,
                timestamp: 0,
            },
            Order {
                oid: 2,
                price: 101,
                size: 5,
                side: Side::Ask,
                timestamp: 1,
            },
            Order {
                oid: 3,
                price: 98,
                size: 15,
                side: Side::Bid,
                timestamp: 2,
            },
            Order {
                oid: 4,
                price: 102,
                size: 10,
                side: Side::Ask,
                timestamp: 3,
            },
            Order {
                oid: 5,
                price: 100,
                size: 20,
                side: Side::Bid,
                timestamp: 4,
            },
        ];

        for order in orders {
            orderbook.add(order);
        }

        /*
        Orderbook state (none of the orders are crossing so the orders should remain same)
        bids: {98: {3}, 100: {1, 5}}, asks: {101: {2}, 102: {4}}
        */

        let snapshot = orderbook.generate_level_snapshot(3);
        assert_eq!(snapshot.n, 3);
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);

        // Check bids
        // First level should be the highest bid, total size 15, total count 1
        assert_eq!(snapshot.bids[0].price, 100);
        assert_eq!(snapshot.bids[0].total_size, 35);
        assert_eq!(snapshot.bids[0].total_count, 2);
        // Second level should be the second highest bid, total size 15, total count 1
        assert_eq!(snapshot.bids[1].price, 98);
        assert_eq!(snapshot.bids[1].total_size, 15);
        assert_eq!(snapshot.bids[1].total_count, 1);

        // Check asks
        // First level should be the lowest ask, total size 5, total count 1
        assert_eq!(snapshot.asks[0].price, 101);
        assert_eq!(snapshot.asks[0].total_size, 5);
        assert_eq!(snapshot.asks[0].total_count, 1);
        // Second level should be the second lowest ask, total size 10, total count 1
        assert_eq!(snapshot.asks[1].price, 102);
        assert_eq!(snapshot.asks[1].total_size, 10);
        assert_eq!(snapshot.asks[1].total_count, 1);
    }
}
