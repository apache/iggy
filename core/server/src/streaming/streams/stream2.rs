use bytes::{BufMut, BytesMut};
use iggy_common::IggyTimestamp;

use crate::slab::{IndexedSlab, Keyed, topics::Topics};

#[derive(Debug)]
pub struct Stream {
    id: usize,
    name: String,
    created_at: IggyTimestamp,
    topics: Topics,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            created_at: IggyTimestamp::now(),
            topics: Topics::init(),
        }
    }
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
            topics: Topics::init(),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn topics_count(&self) -> usize {
        self.topics.len()
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

    pub fn topics_mut(&mut self) -> &mut Topics {
        &mut self.topics
    }
}
