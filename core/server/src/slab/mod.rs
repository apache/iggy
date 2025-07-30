pub mod consumer_groups;
pub mod partitions;
pub mod streams;
pub mod topics;

use ahash::AHashMap;
use slab::Slab;
use std::ops::{Index, IndexMut};

pub trait Keyed {
    type Key: Eq + std::hash::Hash + Clone;
    fn key(&self) -> &Self::Key;
}

pub struct IndexedSlab<T: Keyed> {
    slab: Slab<T>,
    index: AHashMap<T::Key, usize>,
}

impl<T> Default for IndexedSlab<T>
where
    T: Keyed + Default,
{
    fn default() -> Self {
        Self {
            slab: Slab::new(),
            index: AHashMap::new(),
        }
    }
}

impl<T: Keyed + Default> IndexedSlab<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slab = Slab::with_capacity(capacity);
        // Advance the slab once, by inserting a default value.
        slab.insert(T::default());
        Self {
            slab,
            index: AHashMap::with_capacity(capacity),
        }
    }

    pub fn exists(&self, key: &T::Key) -> bool {
        self.index.contains_key(key)
    }

    pub fn insert(&mut self, value: T) -> usize {
        let key = value.key().clone();
        let index = self.slab.insert(value);
        self.index.insert(key, index);
        index
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.slab.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.slab.get_mut(index)
    }

    fn get_by_key(&self, key: &T::Key) -> Option<&T> {
        let index = self.index.get(key)?;
        self.slab.get(*index)
    }

    unsafe fn get_by_key_unchecked(&self, key: &T::Key) -> &T {
        let index = self.index.get(key).unwrap();
        unsafe { self.slab.get_unchecked(*index) }
    }

    unsafe fn get_by_key_mut_unchecked(&mut self, key: &T::Key) -> &mut T {
        let index = self.index.get(key).unwrap();
        unsafe { self.slab.get_unchecked_mut(*index) }
    }
}

impl<T: Keyed> Index<usize> for IndexedSlab<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        // Safety: The access pattern for resources that we store in the `IndexedSlab` is similar to one of a `Vec`,
        // we expect the index to be valid, and we ensure that the index is always valid
        unsafe { self.slab.get_unchecked(index) }
    }
}

impl<T: Keyed> IndexMut<usize> for IndexedSlab<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        // Safety: The access pattern for resources that we store in the `IndexedSlab` is similar to one of a `Vec`,
        // we expect the index to be valid, and we ensure that the index is always valid
        unsafe { self.slab.get_unchecked_mut(index) }
    }
}
