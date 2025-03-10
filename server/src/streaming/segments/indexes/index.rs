#[derive(Debug, Eq, Clone, Copy, Default)]
pub struct Index {
    pub offset: u32,
    pub position: u32,
    pub timestamp: u64,
}

impl PartialEq<Self> for Index {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.position == other.position
            && self.timestamp == other.timestamp
    }
}
