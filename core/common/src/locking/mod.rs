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

use std::ops::{Deref, DerefMut};

/// This module provides a trait and implementations for a shared mutable reference configurable via feature flags.
#[cfg(feature = "tokio_lock")]
#[cfg(not(any(feature = "fast_async_lock")))]
mod tokio_lock;

// this can be used in the future to provide different locking mechanisms
#[cfg(feature = "fast_async_lock")]
mod fast_async_lock;

#[cfg(feature = "tokio_lock")]
#[cfg(not(any(feature = "fast_async_lock")))]
pub type IggyRwLock<T> = tokio_lock::IggyTokioRwLock<T>;

//this can be used in the future to provide different locking mechanisms
#[cfg(feature = "fast_async_lock")]
pub type IggyRwLock<T> = fast_async_lock::IggyFastAsyncRwLock<T>;

#[allow(async_fn_in_trait)]
pub trait IggyRwLockFn<T> {
    type ReadGuard<'a>: Deref<Target = T>
    where
        T: 'a,
        Self: 'a;
    type WriteGuard<'a>: DerefMut<Target = T>
    where
        T: 'a,
        Self: 'a;

    fn new(data: T) -> Self
    where
        Self: Sized;

    async fn read<'a>(&'a self) -> Self::ReadGuard<'a>
    where
        T: 'a;

    async fn write<'a>(&'a self) -> Self::WriteGuard<'a>
    where
        T: 'a;
}
