pub mod consumer_groups;
pub mod partitions;
pub mod streams;
pub mod topics;
pub mod traits_ext;

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

//index: AHashMap<T::Key, usize>,