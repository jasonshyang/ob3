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

impl Order {
    pub fn fill(&mut self, other: &mut Order) -> u64 {
        let fill_size = self.size.min(other.size);
        self.size -= fill_size;
        other.size -= fill_size;
        fill_size
    }
}
