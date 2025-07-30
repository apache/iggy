use compio::fs::create_dir_all;
use iggy_common::{Identifier, IggyError};
use slab::Slab;
use std::{
    cell::{RefCell, UnsafeCell},
    ops::Index,
    path::Path,
};
use tracing::info;

use crate::{
    configs::system::SystemConfig,
    io::storage::Storage,
    shard_info,
    slab::{IndexedSlab, partitions::Partitions, topics::Topics},
    streaming::{partitions::partition2, streams::stream2, topics::topic2},
};

const CAPACITY: usize = 1024;

pub struct Streams {
    container: RefCell<IndexedSlab<stream2::Stream>>,
    stats: (),
}

impl Streams {
    pub fn init() -> Self {
        Self {
            container: RefCell::new(IndexedSlab::with_capacity(CAPACITY)),
            stats: (),
        }
    }

    pub async fn create_stream_file_hierarchy(
        &self,
        id: usize,
        config: &SystemConfig,
    ) -> Result<(), IggyError> {
        let id = id as u32;
        let path = config.get_stream_path(id);
        let topics_path = config.get_topics_path(id);

        if !Path::new(&path).exists() && create_dir_all(&path).await.is_err() {
            return Err(IggyError::CannotCreateStreamDirectory(id, path.clone()));
        }

        if !Path::new(&topics_path).exists() && create_dir_all(&topics_path).await.is_err() {
            return Err(IggyError::CannotCreateTopicsDirectory(
                id,
                topics_path.clone(),
            ));
        }

        info!("Saved stream with ID: {id}.");
        Ok(())
    }

    pub async fn with_async(&self, f: impl AsyncFnOnce(&IndexedSlab<stream2::Stream>)) {
        let container = self.container.borrow();
        f(&container).await;
    }

    pub fn with<T>(&self, f: impl FnOnce(&IndexedSlab<stream2::Stream>) -> T) -> T {
        let container = self.container.borrow();
        f(&container)
    }

    pub fn with_mut<T>(&self, f: impl FnOnce(&mut IndexedSlab<stream2::Stream>) -> T) -> T {
        let mut container = self.container.borrow_mut();
        f(&mut container)
    }

    pub fn with_stream_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(&stream2::Stream) -> T,
    ) -> T {
        self.with(|streams| {
            let stream = match id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = id.get_u32_value().unwrap() as usize;
                    &streams[id]
                }
                iggy_common::IdKind::String => {
                    let key = id.get_string_value().unwrap();
                    unsafe { streams.get_by_key_unchecked(&key) }
                }
            };
            f(stream)
        })
    }

    pub fn with_stream_by_id_mut(&self, id: &Identifier, mut f: impl FnMut(&mut stream2::Stream)) {
        self.with_mut(|streams| {
            let stream = match id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = id.get_u32_value().unwrap() as usize;
                    &mut streams[id]
                }
                iggy_common::IdKind::String => {
                    let key = id.get_string_value().unwrap();
                    unsafe { streams.get_by_key_mut_unchecked(&key) }
                }
            };
            f(stream)
        });
    }

    pub fn with_topics(&self, stream_id: &Identifier, f: impl FnOnce(&Topics)) {
        self.with(|streams| {
            let stream = match stream_id.kind {
                iggy_common::IdKind::Numeric => {
                    let id = stream_id.get_u32_value().unwrap() as usize;
                    &streams[id]
                }
                iggy_common::IdKind::String => {
                    let key = stream_id.get_string_value().unwrap();
                    unsafe { streams.get_by_key_unchecked(&key) }
                }
            };
            f(stream.topics())
        });
    }

    pub fn with_topic_by_id(
        &self,
        id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&topic2::Topic),
    ) {
        self.with_topics(id, |topics| {
            topics.with_topic_by_id(topic_id, f);
        });
    }

    pub fn with_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&Partitions),
    ) {
        self.with_topics(stream_id, |topics| {
            topics.with_partitions(topic_id, f);
        });
    }

    pub fn with_partition_by_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        f: impl FnOnce(&partition2::Partition),
    ) {
        self.with_partitions(stream_id, topic_id, |partitions| {
            partitions.with_partition_id(partition_id, f);
        });
    }
}
