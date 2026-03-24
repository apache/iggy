pub const IGGY_INDEX_SIZE: usize = std::mem::size_of::<u64>() * 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IggyIndex {
    pub offset: u64,
    pub timestamp: u64,
    pub position: u64,
}

impl IggyIndex {
    #[must_use]
    pub const fn new(offset: u64, timestamp: u64, position: u64) -> Self {
        Self {
            offset,
            timestamp,
            position,
        }
    }

    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.offset.to_le_bytes());
        buffer.extend_from_slice(&self.timestamp.to_le_bytes());
        buffer.extend_from_slice(&self.position.to_le_bytes());
    }
}

#[derive(Debug, Clone, Default)]
pub struct IggyIndexCache {
    entries: Vec<IggyIndex>,
}

impl IggyIndexCache {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn insert(&mut self, offset: u64, timestamp: u64, position: u64) {
        self.entries
            .push(IggyIndex::new(offset, timestamp, position));
    }

    #[must_use]
    pub fn serialize(indexes: &IggyIndex) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(IGGY_INDEX_SIZE);
        indexes.write_to(&mut buffer);
        buffer
    }

    #[must_use]
    pub fn offset_lower_bound(&self, offset: u64) -> Option<&IggyIndex> {
        match self
            .entries
            .binary_search_by_key(&offset, |entry| entry.offset)
        {
            Ok(idx) => self.entries.get(idx),
            Err(0) => self.entries.first(),
            Err(idx) => self.entries.get(idx - 1),
        }
    }

    #[must_use]
    pub fn timestamp_lower_bound(&self, timestamp: u64) -> Option<&IggyIndex> {
        match self
            .entries
            .binary_search_by_key(&timestamp, |entry| entry.timestamp)
        {
            Ok(idx) => self.entries.get(idx),
            Err(0) => self.entries.first(),
            Err(idx) => self.entries.get(idx - 1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IggyIndex, IggyIndexCache};

    fn cache() -> IggyIndexCache {
        let mut cache = IggyIndexCache::empty();
        cache.insert(10, 100, 1);
        cache.insert(20, 200, 2);
        cache.insert(30, 300, 3);
        cache
    }

    #[test]
    fn offset_lookup_returns_predecessor() {
        let cache = cache();

        assert_eq!(
            cache.offset_lower_bound(25),
            Some(&IggyIndex::new(20, 200, 2))
        );
        assert_eq!(
            cache.offset_lower_bound(20),
            Some(&IggyIndex::new(20, 200, 2))
        );
        assert_eq!(
            cache.offset_lower_bound(5),
            Some(&IggyIndex::new(10, 100, 1))
        );
        assert_eq!(
            cache.offset_lower_bound(35),
            Some(&IggyIndex::new(30, 300, 3))
        );
    }

    #[test]
    fn timestamp_lookup_returns_predecessor() {
        let cache = cache();

        assert_eq!(
            cache.timestamp_lower_bound(250),
            Some(&IggyIndex::new(20, 200, 2))
        );
        assert_eq!(
            cache.timestamp_lower_bound(200),
            Some(&IggyIndex::new(20, 200, 2))
        );
        assert_eq!(
            cache.timestamp_lower_bound(50),
            Some(&IggyIndex::new(10, 100, 1))
        );
        assert_eq!(
            cache.timestamp_lower_bound(350),
            Some(&IggyIndex::new(30, 300, 3))
        );
    }
}
