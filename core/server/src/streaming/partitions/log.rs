use crate::{
    configs::system::SystemConfig,
    slab::partitions::{self},
    streaming::{
        partitions::journal::Journal,
        segments::{IggyIndexesMut, Segment2, storage::Storage},
    },
};
use iggy_common::INDEX_SIZE;
use ringbuffer::AllocRingBuffer;
use std::fmt::Debug;

const SEGMENTS_CAPACITY: usize = 1024;
const ACCESS_MAP_CAPACITY: usize = 8;
const SIZE_16MB: usize = 16 * 1024 * 1024;
#[derive(Debug)]
pub struct SegmentedLog<J>
where
    J: Journal + Debug,
{
    journal: J,
    // Ring buffer tracking recently accessed segment indices for cleanup optimization.
    // A background task uses this to identify and close file descriptors for unused segments.
    access_map: AllocRingBuffer<usize>,
    cache: (),
    segments: Vec<Segment2>,
    indexes: Option<IggyIndexesMut>,
    storage: Vec<Storage>,
}

impl<J> Clone for SegmentedLog<J>
where
    J: Journal + Default + Debug + Clone,
{
    fn clone(&self) -> Self {
        Default::default()
    }
}

impl<J> Default for SegmentedLog<J>
where
    J: Journal + Debug + Default,
{
    fn default() -> Self {
        Self {
            journal: J::default(),
            access_map: AllocRingBuffer::with_capacity_power_of_2(ACCESS_MAP_CAPACITY),
            cache: (),
            segments: Vec::with_capacity(SEGMENTS_CAPACITY),
            storage: Vec::with_capacity(SEGMENTS_CAPACITY),
            indexes: None,
        }
    }
}

impl<J> SegmentedLog<J>
where
    J: Journal + Debug,
{
    pub fn active_segment(&self) -> &Segment2 {
        self.segments
            .last()
            .expect("active segment called on empty log")
    }

    pub fn active_segment_mut(&mut self) -> &mut Segment2 {
        self.segments
            .last_mut()
            .expect("active segment called on empty log")
    }

    pub fn active_storage(&self) -> &Storage {
        self.storage
            .last()
            .expect("active storage called on empty log")
    }

    pub fn active_storage_mut(&mut self) -> &mut Storage {
        self.storage
            .last_mut()
            .expect("active storage called on empty log")
    }

    pub fn indexes(&self) -> Option<&IggyIndexesMut> {
        self.indexes.as_ref()
    }

    pub fn indexes_mut(&mut self) -> Option<&mut IggyIndexesMut> {
        self.indexes.as_mut()
    }

    pub fn clear_indexes(&mut self) {
        self.indexes = None;
    }

    pub fn ensure_indexes(&mut self) {
        if self.indexes.is_none() {
            let capacity = SIZE_16MB / INDEX_SIZE;
            self.indexes = Some(IggyIndexesMut::with_capacity(capacity, 0));
        }
    }

    pub fn add_persisted_segment(&mut self, segment: Segment2, storage: Storage) {
        self.segments.push(segment);
        self.storage.push(storage);
    }
}

impl<J> SegmentedLog<J>
where
    J: Journal + Debug,
{
    pub fn journal_mut(&mut self) -> &mut J {
        &mut self.journal
    }
}

impl<J> Log for SegmentedLog<J> where J: Journal + Debug {}
pub trait Log {}
