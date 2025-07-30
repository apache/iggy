use crate::slab::Keyed;

#[derive(Default)]
pub struct ConsumerGroup {
    name: String,
}

impl Keyed for ConsumerGroup {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}
