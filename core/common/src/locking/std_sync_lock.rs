use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::locking::IggySharedMutFn;

#[derive(Debug)]
pub struct IggyStdSyncRwLock<T>(Arc<RwLock<T>>);

impl<T> IggySharedMutFn<T> for IggyStdSyncRwLock<T>
where
    T: Send + Sync,
{
    type ReadGuard<'a> = RwLockReadGuard<'a, T>
    where
        T: 'a;
    type WriteGuard<'a> = RwLockWriteGuard<'a, T>
    where
        T: 'a;

    fn new(data: T) -> Self {
        IggyStdSyncRwLock(Arc::new(RwLock::new(data)))
    }

    fn read<'a>(&'a self) -> Self::ReadGuard<'a>
    where
        T: 'a,
    {
        self.0.read().unwrap()
    }

    fn write<'a>(&'a self) -> Self::WriteGuard<'a>
    where
        T: 'a,
    {
        self.0.write().unwrap()
    }
}

impl<T> Clone for IggyStdSyncRwLock<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}
