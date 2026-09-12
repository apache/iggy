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

use iggy::prelude::IggyConsumer as RustIggyConsumer;

#[allow(dead_code)]
pub struct Consumer {
    pub inner: RustIggyConsumer,
}

/// Releases a consumer previously returned by `create_consumer` or
/// `create_consumer_group`.
///
/// # Safety
///
/// - Passing the pointer to this function more than once is undefined
///   behaviour (double-free).
/// - Using the pointer after this function has been called is undefined
///   behaviour (use-after-free).
pub unsafe fn delete_consumer(consumer: *mut Consumer) {
    if !consumer.is_null() {
        unsafe {
            drop(Box::from_raw(consumer));
        }
    }
}
