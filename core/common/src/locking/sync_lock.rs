use maybe_async::{maybe_async, sync_impl};
use std::{rc::Rc, sync::{RwLock, RwLockReadGuard, RwLockWriteGuard}};

use crate::locking::IggySharedMutFn;

pub struct IggySyncLock<T>(Rc<RwLock<T>>);

#[sync_impl]
impl<T> IggySharedMutFn<T> for IggySyncLock<T> {
    type ReadGuard<'a> = RwLockReadGuard<'a, T>
    where
        T: 'a,
        Self: 'a;
    type WriteGuard<'a> = RwLockWriteGuard<'a, T>
    where
        T: 'a,
        Self: 'a;

    fn new(data: T) -> Self {
        IggySyncLock(Rc::new(RwLock::new(data)))
    }
    
}
