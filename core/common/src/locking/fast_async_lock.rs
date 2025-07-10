/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::locking::IggyRwLockFn;
use fast_async_mutex::rwlock::{RwLock as FastAsyncRwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

#[derive(Debug)]
pub struct IggyFastAsyncRwLock<T>(Arc<FastAsyncRwLock<T>>);

impl<T> IggyRwLockFn<T> for IggyFastAsyncRwLock<T>
where
    T: Send + Sync,
{
    type ReadGuard<'a>
        = RwLockReadGuard<'a, T>
    where
        T: 'a;
    type WriteGuard<'a>
        = RwLockWriteGuard<'a, T>
    where
        T: 'a;
    fn new(data: T) -> Self {
        IggyFastAsyncRwLock(Arc::new(FastAsyncRwLock::new(data)))
    }

    async fn read<'a>(&'a self) -> Self::ReadGuard<'a>
    where
        T: 'a,
    {
        self.0.read().await
    }

    async fn write<'a>(&'a self) -> Self::WriteGuard<'a>
    where
        T: 'a,
    {
        self.0.write().await
    }
}

impl<T> Clone for IggyFastAsyncRwLock<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}
