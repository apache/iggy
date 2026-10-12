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

//! The channel a client publishes its [`DiagnosticEvent`]s on.

use async_broadcast::{Receiver, Sender, broadcast};
use iggy_common::DiagnosticEvent;

pub(crate) const DIAGNOSTIC_EVENT_CAPACITY: usize = 1000;

/// Creates the channel a client publishes its diagnostic events on.
///
/// The client keeps the receiver so that `subscribe_events` can clone it, and
/// that receiver never reads. A bounded channel frees a slot only once every
/// receiver has read it, so without overflow mode the channel fills for good
/// and the next publish waits forever, inside the connect or disconnect that
/// sent it. In overflow mode a full channel drops its oldest event instead: a
/// subscriber that falls behind loses old events, never the newest ones.
pub(crate) fn diagnostic_event_channel() -> (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>) {
    let (mut sender, receiver) = broadcast(DIAGNOSTIC_EVENT_CAPACITY);
    sender.set_overflow(true);

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishing_past_the_capacity_with_an_idle_receiver_succeeds() {
        let (sender, _idle) = diagnostic_event_channel();

        for event in 1..=DIAGNOSTIC_EVENT_CAPACITY + 1 {
            sender
                .try_broadcast(DiagnosticEvent::Disconnected)
                .unwrap_or_else(|error| panic!("event {event}: {error}"));
        }
    }
}
