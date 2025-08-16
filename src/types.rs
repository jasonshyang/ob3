use std::fmt::Debug;

use crate::error::Error;

pub trait BatchProcessor {
    type Operation;
    type Snapshot: Debug + Clone + Send;

    fn process_ops(&mut self, ops: Vec<Self::Operation>);
    fn process_query(&self, query: Query<Self::Snapshot>) -> Result<(), Error>;
}

#[derive(Debug, Clone)]
pub struct Order {
    pub oid: u64,
    pub price: u64,
    pub size: u64,
    pub side: Side,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}
pub enum Either<Iter1, Iter2> {
    Ascending(Iter1),
    Descending(Iter2),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackPressureStrategy {
    Block,
    Drop,
}

#[derive(Debug, Clone)]
pub enum Command<T> {
    Operation(T),
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum Query<T> {
    GetSnapshot(crossbeam_channel::Sender<T>),
}

#[derive(Debug, Clone)]
pub enum Op {
    Add(Order),
    Remove(u64),
    Modify { oid: u64, size: u64 },
}

impl Order {
    pub fn fill(&mut self, other: &mut Order) -> u64 {
        let fill_size = self.size.min(other.size);
        self.size -= fill_size;
        other.size -= fill_size;
        fill_size
    }
}

impl<T> From<T> for Command<T> {
    fn from(op: T) -> Self {
        Command::Operation(op)
    }
}

impl<'a, Iter1, Iter2, K: 'a, V: 'a> Iterator for Either<Iter1, Iter2>
where
    Iter1: Iterator<Item = (&'a K, &'a V)>,
    Iter2: Iterator<Item = (&'a K, &'a V)>,
{
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Either::Ascending(iter) => iter.next(),
            Either::Descending(iter) => iter.next(),
        }
    }
}
