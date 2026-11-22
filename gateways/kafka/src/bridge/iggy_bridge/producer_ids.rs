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

//! Producer ids for `InitProducerId`.
//!
//! Iggy builds a producer id from the session number of the client that asks, so no two clients
//! of the cluster get the same id, also across gateways and restarts. The ids come from a client
//! of their own: one session hands out 2^20 ids, and a new sign-in starts a new range without
//! touching the other clients.
//!
//! A Kafka producer gets the Iggy id with a mark: bit 46 set, and the bits from 46 up one place
//! higher. Older gateways gave ids with a clock in milliseconds in the low 47 bits, which keeps bit
//! 46 clear until the year 4199. Produce passes only a marked id to Iggy, so producers that older
//! gateways gave the same id never share one producer in Iggy.

use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex, PoisonError};

use iggy::prelude::{IggyError, MessageClient};

use super::IggyBridge;
use crate::bridge::error::BridgeError;

/// Ids that [`IssuedProducerIds`] remembers. An older id still writes once, but Produce keys its
/// session on the connection instead.
const ISSUED_IDS_KEPT: usize = 65_536;

/// The bit that marks a Kafka producer id built from an Iggy id.
const IGGY_ID_MARK: u32 = 46;
const BELOW_MARK: u64 = (1 << IGGY_ID_MARK) - 1;

/// The Kafka producer id for `iggy_id`. `None` from 2^62 on, where the mark leaves no room.
#[must_use]
pub fn kafka_producer_id(iggy_id: u64) -> Option<i64> {
    if iggy_id >> (u64::BITS - 2) != 0 {
        return None;
    }
    i64::try_from(((iggy_id & !BELOW_MARK) << 1) | (1 << IGGY_ID_MARK) | (iggy_id & BELOW_MARK))
        .ok()
}

/// The Iggy producer id in a marked Kafka producer id. `None` for an id without the mark, such as
/// one from an older gateway.
#[must_use]
pub fn iggy_producer_id(kafka_id: u64) -> Option<u64> {
    ((kafka_id >> IGGY_ID_MARK) & 1 == 1)
        .then_some(((kafka_id >> (IGGY_ID_MARK + 1)) << IGGY_ID_MARK) | (kafka_id & BELOW_MARK))
}

impl IggyBridge {
    /// A producer id that no other Iggy client gets, with the mark.
    ///
    /// # Errors
    ///
    /// [`BridgeError::Iggy`] if the id client cannot connect or sign in, or Iggy refuses the id.
    pub async fn next_producer_id(&self) -> Result<i64, BridgeError> {
        let mut renewed = false;
        loop {
            let client = self.id_client.connected(&self.config).await?;
            match client.next_producer_id().await {
                Ok(id) => {
                    let id = kafka_producer_id(id)
                        .ok_or(BridgeError::Iggy(IggyError::ProducerIdsExhausted))?;
                    self.issued_producer_ids.remember(id);
                    return Ok(id);
                }
                // The session handed out its whole range. A new session starts a new one.
                Err(IggyError::ProducerIdsExhausted) if !renewed => {
                    renewed = true;
                    self.id_client.close().await?;
                }
                Err(error) => return Err(BridgeError::Iggy(error)),
            }
        }
    }

    /// Whether this gateway gave out `producer_id` lately. An id that a client made up, or one
    /// from another gateway, is not.
    pub fn issued_producer_id(&self, producer_id: i64) -> bool {
        self.issued_producer_ids.contains(producer_id)
    }
}

/// The last [`ISSUED_IDS_KEPT`] producer ids this gateway gave out.
#[derive(Debug, Default)]
pub(super) struct IssuedProducerIds(Mutex<Issued>);

#[derive(Debug, Default)]
struct Issued {
    ids: HashSet<i64>,
    order: VecDeque<i64>,
}

impl IssuedProducerIds {
    fn remember(&self, producer_id: i64) {
        let mut issued = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !issued.ids.insert(producer_id) {
            return;
        }
        issued.order.push_back(producer_id);
        if issued.order.len() > ISSUED_IDS_KEPT
            && let Some(oldest) = issued.order.pop_front()
        {
            issued.ids.remove(&oldest);
        }
    }

    fn contains(&self, producer_id: i64) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ids
            .contains(&producer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::handlers::init_producer_id::ProducerIdAllocator;

    #[test]
    fn given_an_iggy_id_when_marked_should_read_back_unchanged() {
        for iggy_id in [0, 4711 << 20 | 3, BELOW_MARK, BELOW_MARK + 1, (1 << 62) - 1] {
            let kafka_id = kafka_producer_id(iggy_id).unwrap();

            assert_eq!(
                iggy_producer_id(u64::try_from(kafka_id).unwrap()),
                Some(iggy_id)
            );
        }
        assert_eq!(kafka_producer_id(1 << 62), None);
    }

    #[test]
    fn given_an_id_from_an_older_gateway_when_read_should_have_no_mark() {
        for instance_id in [0, 1, 0xBEEF, u16::MAX] {
            let id = ProducerIdAllocator::new(instance_id).allocate().unwrap();

            assert_eq!(iggy_producer_id(u64::try_from(id).unwrap()), None);
        }
    }

    #[test]
    fn given_more_ids_than_kept_when_remembering_should_forget_the_oldest() {
        let issued = IssuedProducerIds::default();
        for id in 0..=i64::try_from(ISSUED_IDS_KEPT).unwrap() {
            issued.remember(id);
        }

        assert!(!issued.contains(0));
        assert!(issued.contains(1));
        assert!(issued.contains(i64::try_from(ISSUED_IDS_KEPT).unwrap()));
        assert!(!issued.contains(-1));
    }
}
