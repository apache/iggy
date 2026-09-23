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

//! `InitProducerId` (API key 22).
//!
//! A Java producer sets `enable.idempotence=true` without being asked (KIP-679, default since
//! Kafka 3.0) and sends this before its first record, so answering it is what lets a stock
//! producer start against this gateway at all. The id is handed out and then ignored: delivery
//! stays at-least-once, and no retry is deduplicated. See `docs/IDEMPOTENCE.md`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_INIT_PRODUCER_ID, ApiVersionRange, ERROR_NONE, ERROR_UNKNOWN_SERVER_ERROR,
    ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_init_producer_id_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, handle_versioned_request, is_transactional,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_INIT_PRODUCER_ID,
    min_version: 0,
    max_version: 5,
};

/// Width of the per-instance counter. The remaining 16 bits of the non-negative range carry the
/// instance number, and bit 63 stays clear because `producer_id` is an `i64` whose `-1` means
/// "no producer id".
const COUNTER_BITS: u32 = 47;
const MAX_COUNTER: u64 = (1 << COUNTER_BITS) - 1;

/// The epoch every allocated id carries. Epochs only advance when a producer is fenced, which
/// needs the transactional state this gateway does not keep.
const PRODUCER_EPOCH: i16 = 0;

/// Hands out producer ids that are unique across gateway instances sharing one Iggy cluster.
///
/// `instance_id` is configured (`IGGY_KAFKA_INSTANCE_ID`), not drawn at startup: a random 16-bit
/// value collides at even odds around 300 instances.
///
/// The counter starts at the wall clock in milliseconds, not at 0. Kafka keys a producer on
/// `(producer_id, producer_epoch)` and the epoch is always 0 here, so a counter restarting at 0
/// would hand a restarted gateway's producers the pairs its previous run gave out. Seeding from
/// the clock keeps every new id above the old ones unless the previous run averaged more than one
/// allocation per millisecond of its uptime, or the clock stepped back across the restart, with
/// nothing persisted.
#[derive(Debug)]
pub struct ProducerIdAllocator {
    instance_id: u16,
    next_counter: AtomicU64,
}

impl ProducerIdAllocator {
    #[must_use]
    pub fn new(instance_id: u16) -> Self {
        Self {
            instance_id,
            next_counter: AtomicU64::new(clock_counter()),
        }
    }

    /// The next id for this instance, or `None` once its counter space is spent.
    fn allocate(&self) -> Option<i64> {
        let counter = self.next_counter.fetch_add(1, Ordering::Relaxed);
        if counter > MAX_COUNTER {
            return None;
        }
        i64::try_from((u64::from(self.instance_id) << COUNTER_BITS) | counter).ok()
    }
}

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    handle_versioned_request(
        API_KEY_INIT_PRODUCER_ID,
        api_version,
        body,
        |v, b| decode_guarded::<InitProducerIdRequest>(v, b, validate_init_producer_id_shape),
        |v, req| encode_response(v, req, &state.producer_ids),
        encode_error_response,
        "InitProducerId",
    )
}

/// `InitProducerId` response carrying `error_code` and no usable producer id.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_inner(version, error_code, -1)
}

/// Allocate a producer id, or refuse a transactional request with `UNSUPPORTED_VERSION` (35).
///
/// 35 is the code the Java producer's `InitProducerIdHandler.handleResponse` cannot recover
/// from; a retriable code would leave `initTransactions()` looping forever instead of failing.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(
    version: i16,
    req: &InitProducerIdRequest,
    allocator: &ProducerIdAllocator,
) -> Result<Bytes> {
    if is_transactional(req.transactional_id.as_ref()) {
        return encode_error_response(version, ERROR_UNSUPPORTED_VERSION);
    }
    let Some(producer_id) = allocator.allocate() else {
        tracing::error!(
            "producer id space exhausted; restart the gateway with a free IGGY_KAFKA_INSTANCE_ID"
        );
        return encode_error_response(version, ERROR_UNKNOWN_SERVER_ERROR);
    };
    encode_inner(version, ERROR_NONE, producer_id)
}

/// Milliseconds since the Unix epoch, capped at [`MAX_COUNTER`] so a far-future clock spends the
/// counter space instead of bleeding into the instance bits. A clock before the epoch reads as 0.
fn clock_counter() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    u64::try_from(millis).map_or(MAX_COUNTER, |millis| millis.min(MAX_COUNTER))
}

fn encode_inner(version: i16, error_code: i16, producer_id: i64) -> Result<Bytes> {
    let resp = InitProducerIdResponse::default()
        .with_error_code(error_code)
        .with_producer_id(ProducerId(producer_id))
        .with_producer_epoch(PRODUCER_EPOCH);
    encode_message(&resp, version, 32)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    use super::{COUNTER_BITS, MAX_COUNTER, ProducerIdAllocator};

    #[test]
    fn given_a_fresh_allocator_when_allocating_twice_should_return_distinct_ids() {
        let allocator = ProducerIdAllocator::new(0);
        let first = allocator.allocate().expect("first id");
        let second = allocator.allocate().expect("second id");
        assert!(first >= 0 && second >= 0);
        assert_ne!(first, second);
    }

    #[test]
    fn given_an_instance_id_when_allocating_should_place_it_above_the_counter() {
        let allocator = ProducerIdAllocator::new(0xBEEF);
        let id = allocator.allocate().expect("id");
        assert_eq!(id >> COUNTER_BITS, 0xBEEF);
    }

    #[test]
    fn given_a_restarted_allocator_when_allocating_should_start_above_the_previous_run() {
        let previous_run = ProducerIdAllocator::new(0);
        let last_of_previous_run = previous_run.allocate().expect("previous run id");
        thread::sleep(Duration::from_millis(2));
        let restarted = ProducerIdAllocator::new(0);
        let first_after_restart = restarted.allocate().expect("restarted id");
        assert!(
            first_after_restart > last_of_previous_run,
            "a restart must not replay a (producer_id, epoch 0) pair a live producer still holds"
        );
    }

    #[test]
    fn given_a_spent_counter_when_allocating_should_refuse_instead_of_bleeding_into_the_instance() {
        let allocator = ProducerIdAllocator::new(1);
        allocator.next_counter.store(MAX_COUNTER, Ordering::Relaxed);
        let last = allocator.allocate().expect("last id of the counter space");
        assert_eq!(last >> COUNTER_BITS, 1);
        assert_eq!(allocator.allocate(), None);
    }
}
