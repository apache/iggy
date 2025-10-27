use crate::streaming::{
    partitions::journal::Journal,
    segments::{IggyIndexesMut, Segment, storage::Storage},
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
    _access_map: AllocRingBuffer<usize>,
    _cache: (),
    segments: Vec<Segment>,
    indexes: Vec<Option<IggyIndexesMut>>,
    storage: Vec<Storage>,
}

impl<J> Default for SegmentedLog<J>
where
    J: Journal + Debug + Default,
{
    fn default() -> Self {
        Self {
            journal: J::default(),
            _access_map: AllocRingBuffer::with_capacity_power_of_2(ACCESS_MAP_CAPACITY),
            _cache: (),
            segments: Vec::with_capacity(SEGMENTS_CAPACITY),
            storage: Vec::with_capacity(SEGMENTS_CAPACITY),
            indexes: Vec::with_capacity(SEGMENTS_CAPACITY),
        }
    }
}

impl<J> SegmentedLog<J>
where
    J: Journal + Debug,
{
    pub fn has_segments(&self) -> bool {
        !self.segments.is_empty()
    }

    pub fn segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    pub fn segments_mut(&mut self) -> &mut Vec<Segment> {
        &mut self.segments
    }

    pub fn storages_mut(&mut self) -> &mut Vec<Storage> {
        &mut self.storage
    }

    pub fn storages(&self) -> &Vec<Storage> {
        &self.storage
    }

    pub fn active_segment(&self) -> &Segment {
        self.segments
            .last()
            .expect("active segment called on empty log")
    }

    pub fn active_segment_mut(&mut self) -> &mut Segment {
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

    pub fn indexes(&self) -> &Vec<Option<IggyIndexesMut>> {
        &self.indexes
    }

    pub fn indexes_mut(&mut self) -> &mut Vec<Option<IggyIndexesMut>> {
        &mut self.indexes
    }

    pub fn active_indexes(&self) -> Option<&IggyIndexesMut> {
        self.indexes
            .last()
            .expect("active indexes called on empty log")
            .as_ref()
    }

    pub fn active_indexes_mut(&mut self) -> Option<&mut IggyIndexesMut> {
        self.indexes
            .last_mut()
            .expect("active indexes called on empty log")
            .as_mut()
    }

    pub fn clear_active_indexes(&mut self) {
        let indexes = self
            .indexes
            .last_mut()
            .expect("active indexes called on empty log");
        *indexes = None;
    }

    pub fn ensure_indexes(&mut self) {
        let indexes = self
            .indexes
            .last_mut()
            .expect("active indexes called on empty log");
        if indexes.is_none() {
            let capacity = SIZE_16MB / INDEX_SIZE;
            *indexes = Some(IggyIndexesMut::with_capacity(capacity, 0));
        }
    }

    pub fn add_persisted_segment(&mut self, segment: Segment, storage: Storage) {
        self.segments.push(segment);
        self.storage.push(storage);
        self.indexes.push(None);
    }

    pub fn set_segment_indexes(&mut self, segment_index: usize, indexes: IggyIndexesMut) {
        if let Some(segment_indexes) = self.indexes.get_mut(segment_index) {
            *segment_indexes = Some(indexes);
        }
    }
}

impl<J> SegmentedLog<J>
where
    J: Journal + Debug,
{
    pub fn journal_mut(&mut self) -> &mut J {
        &mut self.journal
    }

    pub fn journal(&self) -> &J {
        &self.journal
    }
}

impl<J> Log for SegmentedLog<J> where J: Journal + Debug {}
pub trait Log {}
