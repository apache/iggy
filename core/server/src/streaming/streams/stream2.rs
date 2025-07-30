use bytes::{BufMut, BytesMut};
use iggy_common::IggyTimestamp;

use crate::slab::{IndexedSlab, Keyed, topics::Topics};

#[derive(Default)]
pub struct Stream {
    id: usize,
    name: String,
    created_at: IggyTimestamp,
    topics: Topics,
}

impl Keyed for Stream {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}

impl Stream {
    pub fn new(name: String) -> Self {
        let now = IggyTimestamp::now();
        Self {
            id: 0,
            name,
            created_at: now,
            topics: Topics::default(),
        }
    }

    pub fn insert_into(self, container: &mut IndexedSlab<Self>) -> usize {
        let idx = container.insert(self);
        let stream = &mut container[idx];
        stream.id = idx;
        idx
    }

    pub fn topics(&self) -> &Topics {
        &self.topics
    }
}
