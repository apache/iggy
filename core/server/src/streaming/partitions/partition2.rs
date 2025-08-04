use slab::Slab;

use crate::slab::IndexedSlab;

#[derive(Default, Debug)]
pub struct Partition {
    id: usize,
}

impl Partition {
    pub fn new() -> Self {
        Self { id: 0 }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn insert_into(self, container: &mut Slab<Self>) -> usize {
        let idx = container.insert(self);
        let partition = &mut container[idx];
        partition.id = idx;
        idx
    }
}
