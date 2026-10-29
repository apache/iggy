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

use iggy::prelude::{
    Checkpoint as RustCheckpoint, PartitionContext, PollOptions,
    PolledMessages as RustPolledMessages, PollingStrategy,
};
use iggy_common::ReaderStamp;

use crate::ffi;

/// The bridge has no constructor for it, so C++ gets one only from a batch.
pub struct Checkpoint(pub(crate) RustCheckpoint);

/// The bridge has no constructor for it, so C++ gets one only from a batch or a read error.
pub struct Continuation(pub(crate) PollingStrategy);

/// The batch fields that Rust keeps private, out of reach of C++.
pub struct BatchPositions {
    context: PartitionContext,
    stamp: Option<ReaderStamp>,
    // An empty batch continues from the start of its request, which the Rust batch does not
    // expose, so the continuation is taken before its messages cross the bridge.
    continuation: PollingStrategy,
}

pub fn make_message(
    payload: Vec<u8>,
    user_headers: Vec<ffi::HeaderEntry>,
) -> ffi::IggyMessageToSend {
    ffi::IggyMessageToSend {
        id_lo: 0,
        id_hi: 0,
        payload,
        user_headers,
    }
}

pub fn default_poll_options() -> ffi::PollOptions {
    PollOptions::default().into()
}

impl ffi::PolledMessages {
    pub fn checkpoint(&self) -> Result<Box<Checkpoint>, String> {
        match self.messages.len().checked_sub(1) {
            Some(last) => self.checkpoint_at(last),
            None => Err("Could not take a checkpoint: the batch is empty".to_owned()),
        }
    }

    pub fn checkpoint_at(&self, index: usize) -> Result<Box<Checkpoint>, String> {
        let message = self.messages.get(index).ok_or_else(|| {
            format!(
                "Could not take a checkpoint: index {index} is out of range for {} messages",
                self.messages.len()
            )
        })?;
        Ok(Box::new(Checkpoint(RustCheckpoint::stamped(
            self.partition_id,
            message.offset,
            self.positions.context,
            self.positions.stamp,
        ))))
    }

    pub fn continuation(&self) -> Box<Continuation> {
        Box::new(Continuation(self.positions.continuation))
    }
}

impl From<&RustPolledMessages> for BatchPositions {
    fn from(batch: &RustPolledMessages) -> Self {
        Self {
            context: batch.context(),
            stamp: batch.stamp(),
            continuation: batch.continuation(),
        }
    }
}

impl Checkpoint {
    pub fn partition_id(&self) -> u32 {
        self.0.partition_id()
    }

    pub fn offset(&self) -> u64 {
        self.0.offset()
    }
}

impl Continuation {
    pub fn kind(&self) -> String {
        self.0.kind.to_string()
    }

    pub fn value(&self) -> u64 {
        self.0.value
    }
}
