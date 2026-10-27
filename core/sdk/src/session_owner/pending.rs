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

use bytes::Bytes;
use iggy_common::{IggyError, PendingOperationsConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::Notify;
use tokio::time::{Instant, timeout_at};

#[derive(Debug)]
pub(super) struct PendingOperations {
    limits: PendingOperationsConfig,
    usage: Mutex<Usage>,
    available: Notify,
}

#[derive(Debug, Default)]
struct Usage {
    operations: usize,
    bytes: usize,
    fence: u64,
}

/// One slot ends with the logical operation. Its bytes stay charged until
/// the last frozen frame, including any queued writer, releases ownership.
#[derive(Debug)]
pub(super) struct Reservation {
    owner: Arc<PendingOperations>,
    bytes: usize,
    fence: u64,
    released: AtomicBool,
}

struct FrozenBody {
    body: Bytes,
    _reservation: Arc<Reservation>,
}

impl PendingOperations {
    pub(super) fn new(limits: PendingOperationsConfig) -> Self {
        Self {
            limits,
            usage: Mutex::default(),
            available: Notify::new(),
        }
    }

    pub(super) async fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        deadline: Instant,
    ) -> Result<Arc<Reservation>, IggyError> {
        if bytes > self.limits.max_bytes.get() {
            return Err(IggyError::InvalidSizeBytes);
        }
        loop {
            if Instant::now() >= deadline {
                return Err(IggyError::TransientNotAccepted);
            }
            // notify_waiters also reaches an unpolled future created here,
            // closing the gap between checking capacity and starting to wait.
            let available = self.available.notified();
            {
                let mut usage = self.usage();
                if usage.operations < self.limits.max_operations.get()
                    && bytes <= self.limits.max_bytes.get() - usage.bytes
                {
                    usage.operations += 1;
                    usage.bytes += bytes;
                    return Ok(Arc::new(Reservation {
                        owner: Arc::clone(self),
                        bytes,
                        fence: usage.fence,
                        released: AtomicBool::new(false),
                    }));
                }
            }
            timeout_at(deadline, available)
                .await
                .map_err(|_| IggyError::TransientNotAccepted)?;
        }
    }

    pub(super) fn reset(&self, fence: u64) {
        let mut usage = self.usage();
        usage.operations = 0;
        usage.fence = fence;
        drop(usage);
        self.available.notify_waiters();
    }

    fn usage(&self) -> MutexGuard<'_, Usage> {
        self.usage.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Reservation {
    pub(super) fn is_current(&self, bytes: usize) -> bool {
        bytes == self.bytes
            && !self.released.load(Ordering::Acquire)
            && self.fence == self.owner.usage().fence
    }

    pub(super) fn release_operation(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut usage = self.owner.usage();
        if self.fence == usage.fence {
            usage.operations -= 1;
        }
        drop(usage);
        self.owner.available.notify_waiters();
    }

    pub(super) fn freeze_body(self: &Arc<Self>, body: Bytes) -> Bytes {
        Bytes::from_owner(FrozenBody {
            body,
            _reservation: Arc::clone(self),
        })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.release_operation();
        self.owner.usage().bytes -= self.bytes;
        self.owner.available.notify_waiters();
    }
}

impl AsRef<[u8]> for FrozenBody {
    fn as_ref(&self) -> &[u8] {
        self.body.as_ref()
    }
}
