use monoio::{Buildable, Driver, Runtime};

use crate::shard::{connector::ShardConnector, frame::ShardFrame};
use std::ops::Range;

pub fn create_shard_connections(shards_set: Range<usize>) -> Vec<ShardConnector<ShardFrame>> {
    let shards_count = shards_set.len();
    let connectors = shards_set
        .into_iter()
        .map(|id| ShardConnector::new(id as u16, shards_count))
        .collect();

    connectors
}

pub async fn create_directories() {
    todo!();
}

pub async fn create_root_user() {
    todo!();
}

pub fn create_default_executor<D>() -> Runtime<D>
where
    D: Driver + Buildable,
{
    let builder = monoio::RuntimeBuilder::<D>::new();
    let rt = Buildable::build(builder).expect("Failed to create default runtime");
    rt
}
