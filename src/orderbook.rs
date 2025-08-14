use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::types::{BatchProcessor, Op, Order, Side};

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

#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    pub total_orders: usize,
    pub total_bids: usize,
    pub total_asks: usize,
    pub checksum: u64,
}

impl BatchProcessor for Orderbook {
    type Operation = Op;
    type Snapshot = OrderbookSnapshot;

    // TODO: right now it's a very naive implementation, we can optimize it later
    fn process_ops(&mut self, ops: Vec<Self::Operation>) {
        for op in ops {
            self.process_op(op);
        }
    }

    // TODO: this is a placeholder
    fn produce_snapshot(&self) -> Self::Snapshot {
        self.clone().into()
    }
}

impl Orderbook {
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

        match order.side {
            Side::Bid => {
                // Iterate through lowest ask price first
                'outer: for (_, levels) in self.asks.range(..=order.price) {
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
            }
            Side::Ask => {
                // Iterate through highest bid price first
                'outer: for (_, levels) in self.bids.range(order.price..).rev() {
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
            }
        };

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
}

impl From<Orderbook> for OrderbookSnapshot {
    fn from(orderbook: Orderbook) -> Self {
        let total_orders = orderbook.map.len();
        let total_bids = orderbook.bids.len();
        let total_asks = orderbook.asks.len();
        let checksum = orderbook
            .map
            .values()
            .fold(0u64, |acc, order| acc.wrapping_add(order.oid));

        OrderbookSnapshot {
            total_orders,
            total_bids,
            total_asks,
            checksum,
        }
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
}
