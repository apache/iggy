pub mod consumer_groups;
pub mod partitions;
pub mod streams;
pub mod topics;

use ahash::AHashMap;
use slab::Slab;
use std::{
    fmt::{Debug, Display},
    ops::{Index, IndexMut},
};

pub trait Keyed {
    type Key: Eq + std::hash::Hash + Clone + Debug + Display;
    fn key(&self) -> &Self::Key;
}

#[derive(Debug)]
pub struct IndexedSlab<T: Keyed> {
    slab: Slab<T>,
    index: AHashMap<T::Key, usize>,
}

impl<T: Keyed + Debug> IndexedSlab<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slab: Slab::with_capacity(capacity),
            index: AHashMap::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.slab.len()
    }

    pub fn insert(&mut self, value: T) -> usize {
        let key = value.key().clone();
        let index = self.slab.insert(value);
        self.index.insert(key, index);
        index
    }

    pub fn try_remove(&mut self, id: usize) -> Option<T> {
        self.slab.try_remove(id).and_then(|value| {
            self.index.remove(value.key());
            Some(value)
        })
    }

    pub fn try_remove_by_key(&mut self, key: &T::Key) -> Option<T> {
        self.index
            .remove(key)
            .and_then(|index| Some(self.slab.remove(index)))
    }

    pub fn update_key_unchecked(&mut self, old: &T::Key, new_key: T::Key) {
        let idx = self.index.remove(old).expect("Rename key: key not found");
        self.index.insert(new_key, idx);
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.slab.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.slab.get_mut(index)
    }

    pub fn get_by_key(&self, key: &T::Key) -> Option<&T> {
        let index = self.index.get(key)?;
        self.slab.get(*index)
    }

    pub fn get_by_key_mut(&mut self, key: &T::Key) -> Option<&mut T> {
        let index = self.index.get(key)?;
        self.slab.get_mut(*index)
    }

    unsafe fn get_by_key_unchecked(&self, key: &T::Key) -> &T {
        let index = self.index.get(key).unwrap();
        unsafe { self.slab.get_unchecked(*index) }
    }

    unsafe fn get_by_key_mut_unchecked(&mut self, key: &T::Key) -> &mut T {
        let index = self.index.get(key).unwrap();
        unsafe { self.slab.get_unchecked_mut(*index) }
    }

    pub fn contains_key(&self, key: &T::Key) -> bool {
        self.index.contains_key(key)
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
