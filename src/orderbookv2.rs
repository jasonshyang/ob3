use std::collections::{BTreeMap, HashMap};

use slab::Slab;

use crate::{
    error::Error,
    types::{
        BatchProcessor, Either, Level, Op, Order, OrderbookSnapshot, Query, Side, SimpleSnapshot,
        TopNLevels,
    },
};

#[derive(Debug, Clone)]
pub struct Slot<T> {
    pub v: T,
    pub next: Option<usize>,
    pub prev: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct LevelInfo {
    pub head: Option<usize>,
    pub tail: Option<usize>,
    pub total_size: u64,
    pub total_count: usize,
}

#[derive(Debug, Default)]
pub struct OrderbookV2 {
    pub slab: Slab<Slot<Order>>,
    pub map: HashMap<String, usize>,    // Oid -> Slab key
    pub bids: BTreeMap<u64, LevelInfo>, // Price -> LevelInfo
    pub asks: BTreeMap<u64, LevelInfo>, // Price -> LevelInfo
}

impl BatchProcessor for OrderbookV2 {
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

impl OrderbookV2 {
    pub fn get(&self, oid: &str) -> Option<&Order> {
        self.map
            .get(oid)
            .and_then(|key| self.slab.get(*key))
            .map(|slot| &slot.v)
    }

    pub fn get_level(&self, side: Side, price: u64) -> Option<&LevelInfo> {
        match side {
            Side::Bid => self.bids.get(&price),
            Side::Ask => self.asks.get(&price),
        }
    }

    pub fn get_nth_level(&self, side: Side, n: usize) -> Option<&LevelInfo> {
        match side {
            Side::Bid => self.bids.values().nth(n),
            Side::Ask => self.asks.values().nth(n),
        }
    }

    pub fn depth(&self, side: Side) -> usize {
        match side {
            Side::Bid => self.bids.len(),
            Side::Ask => self.asks.len(),
        }
    }

    pub fn contains(&self, oid: &str) -> bool {
        self.map.contains_key(oid)
    }

    pub fn process_op(&mut self, op: Op) {
        match op {
            Op::Add(order) => self.add(order),
            Op::Remove(oid) => self.remove(&oid),
            Op::Modify { oid, size } => self.modify(&oid, size),
        }
    }

    pub fn add(&mut self, mut order: Order) {
        // Check if the order already exists
        if self.contains(&order.oid) {
            return;
        }

        // Fill order
        let mut filled = Vec::new();

        // Determine the iteration direction based on the order side
        let iter = match order.side {
            Side::Bid => Either::Ascending(self.asks.range(..=order.price)),
            Side::Ask => Either::Descending(self.bids.range(order.price..).rev()),
        };

        'outer: for (_, level) in iter {
            // Traverse the level
            if let Some(head) = level.head {
                let mut curr = head;

                while let Some(other) = self.slab.get_mut(curr) {
                    order.fill(&mut other.v);

                    // If the order is fully filled, add to filled list for removal
                    if other.v.size == 0 {
                        filled.push(other.v.oid.clone());
                    }

                    // If the order is fully filled, break out of the outer loop
                    if order.size == 0 {
                        break 'outer;
                    }

                    // Move the pointer
                    match other.next {
                        Some(next) => curr = next,
                        None => break, // End of the level
                    }
                }
            }
        }

        // Handle the filled orders
        for oid in filled {
            self.remove(&oid);
        }

        // If the order is still not fully filled, add it to the orderbook
        if order.size > 0 {
            self.add_unchecked(order);
        }
    }

    // This method adds an order without checking for duplicates or crossing.
    pub fn add_unchecked(&mut self, order: Order) {
        let oid = order.oid.clone();

        // Insert the order into the slab and map
        let key = self.slab.insert(Slot::new(order));
        self.map.insert(oid.clone(), key);

        // We need a mutable reference to the slot to update pointers
        let slot = self.slab.get_mut(key).unwrap();

        // Insert the order into the appropriate book
        let book = match slot.v.side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        let level = book.entry(slot.v.price).or_default();

        match level.tail {
            Some(tail) => {
                level.total_count += 1;
                level.total_size += slot.v.size;

                // Point new slot's prev to current tail
                slot.prev = Some(tail);
                // Point current tail's next to new slot
                self.slab[tail].next = Some(key);
                // Update the tail to the new slot
                level.tail = Some(key);
            }
            None => {
                // If the level has no tail, it's empty
                level.head = Some(key);
                level.tail = Some(key);
                level.total_count = 1;
                level.total_size = slot.v.size;
            }
        }
    }

    pub fn remove(&mut self, oid: &str) {
        if let Some(key) = self.map.remove(oid) {
            let slot = self.slab.remove(key);

            // Update adjacency pointers to detach from the slot
            if let Some(prev) = slot.prev {
                self.slab[prev].next = slot.next;
            }

            if let Some(next) = slot.next {
                self.slab[next].prev = slot.prev;
            }

            // Remove the order from the corresponding book
            let book = match slot.v.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let level = book.get_mut(&slot.v.price).unwrap();

            // If the slot is currently head, update the head pointer
            if level.head == Some(key) {
                level.head = slot.next;
            }
            // If the slot is currently tail, update the tail pointer
            if level.tail == Some(key) {
                level.tail = slot.prev;
            }

            // Update the level
            level.total_count -= 1;
            level.total_size -= slot.v.size;

            // If the level is empty, remove the level from the book
            if level.is_empty() {
                book.remove(&slot.v.price);
            }
        }
    }

    pub fn modify(&mut self, oid: &str, size: u64) {
        if let Some(key) = self.map.get(oid) {
            // If the size is zero, remove the order
            if size == 0 {
                self.remove(oid);
            } else {
                // Otherwise, we update our record
                let slot = self.slab.get_mut(*key).unwrap();

                let original_size = slot.v.size;
                slot.v.size = size;

                // We also need to update the book
                let level = match slot.v.side {
                    Side::Bid => self.bids.get_mut(&slot.v.price).unwrap(),
                    Side::Ask => self.asks.get_mut(&slot.v.price).unwrap(),
                };

                match original_size.cmp(&size) {
                    // Increase
                    std::cmp::Ordering::Less => level.total_size += size - original_size,
                    // Decrease
                    std::cmp::Ordering::Greater => level.total_size -= original_size - size,
                    std::cmp::Ordering::Equal => {}
                }
            }
        }
    }

    pub fn generate_simple_snapshot(&self) -> SimpleSnapshot {
        let total_orders = self.map.len();
        let total_bids = self.bids.values().map(|levels| levels.total_count).sum();
        let total_asks = self.asks.values().map(|levels| levels.total_count).sum();
        let checksum = self.slab.iter().fold(0u64, |acc, (_, slot)| {
            acc.wrapping_add(slot.v.price).wrapping_add(slot.v.size)
        });

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
        let iter = match side {
            Side::Ask => Either::Ascending(self.asks.iter()),
            Side::Bid => Either::Descending(self.bids.iter().rev()),
        };

        iter.take(n)
            .map(|(p, l)| Level {
                price: *p,
                total_size: l.total_size,
                total_count: l.total_count,
            })
            .collect()
    }
}

impl<T> Slot<T> {
    pub fn new(v: T) -> Self {
        Slot {
            v,
            next: None,
            prev: None,
        }
    }
}

impl LevelInfo {
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    pub fn summary(&self) -> (u64, usize) {
        (self.total_size, self.total_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Side;

    fn order(oid: u64, price: u64, size: u64, side: Side, timestamp: u64) -> Order {
        Order {
            oid: oid.to_string(),
            price,
            size,
            side,
            timestamp,
        }
    }

    #[test]
    fn test_orderbookv2_add() {
        let orders = vec![
            order(1, 100, 15, Side::Bid, 0),
            order(2, 101, 5, Side::Ask, 1),
            order(3, 98, 15, Side::Bid, 2),
        ];

        let mut orderbook = OrderbookV2::default();

        for o in orders {
            orderbook.add(o);
        }

        assert!(orderbook.contains("1"));
        assert!(orderbook.contains("2"));
        assert!(orderbook.contains("3"));
        assert_eq!(orderbook.bids.len(), 2); // 100 and 98
        assert_eq!(orderbook.asks.len(), 1); // 101

        let new_order = order(4, 99, 5, Side::Ask, 3);
        orderbook.add(new_order);

        // We expect the new order should match oid 1 and half fill it.
        assert!(orderbook.contains("3"));
        let key1 = orderbook.map.get("1").unwrap();
        assert_eq!(orderbook.slab[*key1].v.size, 10); // oid 1 should be partially filled

        assert_eq!(orderbook.asks.len(), 1);

        let new_order = order(5, 98, 10, Side::Ask, 4);
        orderbook.add(new_order);

        // We expect oid 1 to be fully filled now
        assert!(!orderbook.contains("1")); // oid 1 should be fully filled and removed

        let new_order = order(6, 102, 15, Side::Bid, 5);
        orderbook.add(new_order);

        // We expect oid 2 to be fully filled and remaining of oid 6 is added to bids
        assert!(!orderbook.contains("2")); // oid 2 should be fully filled and removed
        assert!(orderbook.contains("6")); // oid 6 should be added to bids
        let key6 = orderbook.map.get("6").unwrap();
        assert_eq!(orderbook.slab[*key6].v.size, 10); // oid 6 should have 10 left
    }
}
