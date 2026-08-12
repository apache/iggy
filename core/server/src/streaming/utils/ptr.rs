// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{ops::Deref, ptr::NonNull};

// Wrapper around an imutable pointer to a 'static value, that can be cloned and sent across threads.
pub struct EternalPtr<T: ?Sized + 'static> {
    ptr: NonNull<T>,
    _marker: std::marker::PhantomData<&'static T>,
}

impl<T> From<NonNull<T>> for EternalPtr<T> {
    fn from(value: NonNull<T>) -> Self {
        Self {
            ptr: value,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> From<&'static T> for EternalPtr<T> {
    fn from(value: &'static T) -> Self {
        Self {
            ptr: value.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> From<&'static mut T> for EternalPtr<T> {
    fn from(value: &'static mut T) -> Self {
        Self {
            ptr: value.into(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: ?Sized + 'static> Clone for EternalPtr<T> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: ?Sized + 'static> Deref for EternalPtr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

unsafe impl<T: Send> Send for EternalPtr<T> {}
