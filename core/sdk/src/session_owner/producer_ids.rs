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

//! Producer ids built from the session number.
//!
//! The session number is the op of the client's `Register` in the metadata
//! log, so no other session of the cluster ever gets it. An id puts it above
//! a local index:
//!
//! ```text
//!  63  62                                    20 19             0
//! +---+----------------------------------------+---------------+
//! | 0 | session number, 43 bits                | index, 20 bits |
//! +---+----------------------------------------+---------------+
//! ```

use iggy_common::IggyError;

const INDEX_BITS: u32 = 20;

/// Ids one session hands out.
pub(crate) const IDS_PER_SESSION: u64 = 1 << INDEX_BITS;

/// The first session number that no longer fits beside the index in a
/// non-negative 64-bit id.
pub(crate) const SESSION_LIMIT: u64 = 1 << (i64::BITS - 1 - INDEX_BITS);

/// The id range of the current session and the next index in it.
#[derive(Debug, Default)]
pub(crate) struct ProducerIds {
    session: u64,
    next_index: u64,
}

impl ProducerIds {
    /// The next id of `session`, the current session number of the client.
    ///
    /// A new session starts its own range, and the ids of an earlier one stay
    /// valid because no other client gets its number.
    pub(crate) fn next(&mut self, session: u64) -> Result<u64, IggyError> {
        if session == 0 {
            return Err(IggyError::Unauthenticated);
        }
        if session >= SESSION_LIMIT {
            return Err(IggyError::ProducerIdsExhausted);
        }
        if session != self.session {
            self.session = session;
            self.next_index = 0;
        }
        if self.next_index == IDS_PER_SESSION {
            return Err(IggyError::ProducerIdsExhausted);
        }
        let id = session << INDEX_BITS | self.next_index;
        self.next_index += 1;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::batch::BatchProducer;

    #[test]
    fn given_a_session_when_handing_out_ids_should_count_from_its_range() {
        let mut ids = ProducerIds::default();

        assert_eq!(ids.next(4711), Ok(4711 << 20));
        assert_eq!(ids.next(4711), Ok(4711 << 20 | 1));
        assert_eq!(ids.next(4711), Ok(4_939_841_538));
    }

    #[test]
    fn given_two_clients_when_handing_out_ids_should_never_share_one() {
        let mut first = ProducerIds::default();
        let mut second = ProducerIds::default();
        let first_ids: Vec<u64> = (0..1_000).map(|_| first.next(10).unwrap()).collect();
        let second_ids: Vec<u64> = (0..1_000).map(|_| second.next(11).unwrap()).collect();

        assert!(first_ids.iter().all(|id| !second_ids.contains(id)));
        assert!(first_ids.iter().all(|id| id >> 20 == 10));
        assert!(second_ids.iter().all(|id| id >> 20 == 11));
    }

    #[test]
    fn given_a_new_session_when_handing_out_ids_should_start_its_own_range() {
        let mut ids = ProducerIds::default();
        let before = ids.next(10).unwrap();

        let after = ids.next(25).unwrap();

        assert_eq!(after, 25 << 20);
        assert!(after > before, "an earlier session's ids stay below");
    }

    #[test]
    fn given_a_spent_session_when_handing_out_another_id_should_fail_until_a_new_session() {
        let mut ids = ProducerIds {
            session: 10,
            next_index: IDS_PER_SESSION - 1,
        };

        assert_eq!(ids.next(10), Ok(10 << 20 | (IDS_PER_SESSION - 1)));
        assert_eq!(ids.next(10), Err(IggyError::ProducerIdsExhausted));
        assert_eq!(ids.next(11), Ok(11 << 20));
    }

    #[test]
    fn given_a_session_number_at_the_limit_when_handing_out_an_id_should_fail() {
        let mut ids = ProducerIds::default();

        assert_eq!(
            ids.next(SESSION_LIMIT),
            Err(IggyError::ProducerIdsExhausted)
        );
        let last = ids.next(SESSION_LIMIT - 1).unwrap();
        assert!(BatchProducer::new(last, 0, 0).is_some());
        assert_eq!(ids.next(0), Err(IggyError::Unauthenticated));
    }
}
