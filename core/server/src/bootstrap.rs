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

}

pub async fn create_root_user() {

}
