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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ActiveObjectState {
    pub(crate) key: String,
    pub(crate) etag: String,
    pub(crate) size: u64,
    pub(crate) next_byte_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct SourceState {
    pub(crate) active_object: Option<ActiveObjectState>,
}

#[derive(Debug)]
pub(crate) struct StateTracker {
    committed: SourceState,
    pending: Option<PendingState>,
}

#[derive(Debug)]
pub(crate) struct PendingState {
    checkpoint: SourceState,
    completed_key: Option<String>,
}

#[derive(Default, Debug)]
pub(crate) struct ScanState {
    start_after: Option<String>,
    exhausted: bool,
}

impl ScanState {
    pub(crate) fn start_after(&self) -> Option<&str> {
        self.start_after.as_deref()
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn advance_after(&mut self, key: String) {
        self.start_after = Some(key);
    }

    pub(crate) fn mark_exhausted(&mut self) {
        self.exhausted = true;
    }
}

impl StateTracker {
    pub(crate) fn new(committed: SourceState) -> StateTracker {
        Self {
            committed,
            pending: None,
        }
    }

    pub(crate) fn active_object(&self) -> Option<&ActiveObjectState> {
        self.committed.active_object.as_ref()
    }

    pub(crate) fn stage_offset(
        &mut self,
        mut active_object: ActiveObjectState,
        next_byte_offset: u64,
    ) -> Result<(), StateError> {
        active_object.next_byte_offset = next_byte_offset;
        active_object.validate()?;

        self.pending = Some(PendingState {
            checkpoint: SourceState {
                active_object: Some(active_object),
            },
            completed_key: None,
        });

        Ok(())
    }

    pub(crate) fn commit_pending(&mut self) -> Result<Option<String>, StateError> {
        let pending_state = self.pending.take().ok_or(StateError::NoPendingState)?;

        self.committed = pending_state.checkpoint;
        Ok(pending_state.completed_key)
    }

    pub(crate) fn discard_pending(&mut self) -> Result<(), StateError> {
        self.pending.take().ok_or(StateError::NoPendingState)?;
        Ok(())
    }

    pub(crate) fn stage_completion(&mut self, completed_key: String) {
        self.pending = Some(PendingState {
            checkpoint: SourceState {
                active_object: None,
            },
            completed_key: Some(completed_key),
        });
    }

    pub(crate) fn stage_current(&mut self) {
        self.pending = Some(PendingState {
            checkpoint: self.committed.clone(),
            completed_key: None,
        });
    }

    pub(crate) fn pending_checkpoint(&self) -> Option<&SourceState> {
        self.pending.as_ref().map(|pending| &pending.checkpoint)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum StateError {
    EmptyKey,
    EmptyEtag,
    OffsetBeyondObjectSize { next_byte_offset: u64, size: u64 },
    NoPendingState,
}

impl ActiveObjectState {
    pub(crate) fn validate(&self) -> Result<(), StateError> {
        if self.key.is_empty() {
            return Err(StateError::EmptyKey);
        }
        if self.etag.is_empty() {
            return Err(StateError::EmptyEtag);
        }
        if self.next_byte_offset > self.size {
            return Err(StateError::OffsetBeyondObjectSize {
                next_byte_offset: self.next_byte_offset,
                size: self.size,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iggy_connector_sdk::ConnectorState;

    use super::*;

    fn active_object(size: u64, next_byte_offset: u64) -> ActiveObjectState {
        ActiveObjectState {
            key: "logs/events.jsonl".to_string(),
            etag: "abc123".to_string(),
            size,
            next_byte_offset,
        }
    }

    #[test]
    fn given_initial_offset_when_active_object_state_validated_should_succeed() {
        let state = active_object(100, 0);

        assert_eq!(state.validate(), Ok(()));
    }

    #[test]
    fn given_offset_equal_to_object_size_when_active_object_state_validated_should_succeed() {
        let state = active_object(100, 100);

        assert_eq!(state.validate(), Ok(()));
    }

    #[test]
    fn given_offset_beyond_object_size_when_active_object_state_validated_should_return_error() {
        let state = active_object(100, 101);

        assert_eq!(
            state.validate(),
            Err(StateError::OffsetBeyondObjectSize {
                next_byte_offset: 101,
                size: 100,
            })
        );
    }

    #[test]
    fn given_active_object_when_offset_staged_should_preserve_committed_and_set_pending_offset() {
        let mut tracker = StateTracker {
            committed: SourceState {
                active_object: Some(active_object(100, 10)),
            },
            pending: None,
        };

        assert_eq!(tracker.stage_offset(active_object(100, 10), 40), Ok(()));
        assert_eq!(
            tracker
                .committed
                .active_object
                .as_ref()
                .map(|object| object.next_byte_offset),
            Some(10)
        );

        let pending = tracker
            .pending
            .as_ref()
            .expect("staging an offset should create pending state");
        assert_eq!(
            pending
                .checkpoint
                .active_object
                .as_ref()
                .map(|object| object.next_byte_offset),
            Some(40)
        );
        assert_eq!(pending.completed_key, None);
    }

    #[test]
    fn given_pending_offset_when_committed_should_promote_it_and_clear_pending() {
        let mut tracker = StateTracker {
            committed: SourceState {
                active_object: Some(active_object(100, 10)),
            },
            pending: None,
        };
        tracker
            .stage_offset(active_object(100, 10), 40)
            .expect("offset should be staged");

        assert_eq!(tracker.commit_pending(), Ok(None));
        assert_eq!(
            tracker
                .committed
                .active_object
                .as_ref()
                .map(|object| object.next_byte_offset),
            Some(40)
        );
        assert!(tracker.pending.is_none());
    }

    #[test]
    fn given_pending_offset_when_discarded_should_preserve_committed_and_clear_pending() {
        let mut tracker = StateTracker {
            committed: SourceState {
                active_object: Some(active_object(100, 10)),
            },
            pending: None,
        };
        tracker
            .stage_offset(active_object(100, 10), 40)
            .expect("offset should be staged");

        assert_eq!(tracker.discard_pending(), Ok(()));
        assert_eq!(
            tracker
                .committed
                .active_object
                .as_ref()
                .map(|object| object.next_byte_offset),
            Some(10)
        );
        assert!(tracker.pending.is_none());
    }

    #[test]
    fn given_new_object_when_offset_staged_should_create_pending_without_advancing_committed() {
        let mut tracker = StateTracker {
            committed: SourceState {
                active_object: None,
            },
            pending: None,
        };

        assert_eq!(tracker.stage_offset(active_object(100, 0), 40), Ok(()));
        assert!(tracker.committed.active_object.is_none());

        let pending_object = tracker
            .pending
            .as_ref()
            .and_then(|pending| pending.checkpoint.active_object.as_ref())
            .expect("staging should include the newly listed object");
        assert_eq!(pending_object.key, "logs/events.jsonl");
        assert_eq!(pending_object.next_byte_offset, 40);
    }

    #[test]
    fn given_no_pending_state_when_discarded_should_return_error() {
        let mut tracker = StateTracker {
            committed: SourceState {
                active_object: Some(active_object(100, 10)),
            },
            pending: None,
        };

        assert_eq!(tracker.discard_pending(), Err(StateError::NoPendingState));
    }

    #[test]
    fn given_active_object_when_completion_staged_should_preserve_committed_and_set_pending_completion()
     {
        let mut tracker = StateTracker {
            committed: SourceState {
                active_object: Some(active_object(100, 100)),
            },
            pending: None,
        };

        tracker.stage_completion("logs/events.jsonl".to_string());
        assert_eq!(
            tracker
                .committed
                .active_object
                .as_ref()
                .map(|object| object.key.as_str()),
            Some("logs/events.jsonl")
        );

        let pending = tracker
            .pending
            .as_ref()
            .expect("staging completion should create pending state");
        assert!(pending.checkpoint.active_object.is_none());
        assert_eq!(pending.completed_key.as_deref(), Some("logs/events.jsonl"));
    }

    #[test]
    fn given_staged_completion_when_committed_should_clear_active_object_and_return_completed_key()
    {
        let mut tracker = StateTracker {
            committed: SourceState {
                active_object: Some(active_object(100, 100)),
            },
            pending: None,
        };
        tracker.stage_completion("logs/events.jsonl".to_string());

        assert_eq!(
            tracker.commit_pending(),
            Ok(Some("logs/events.jsonl".to_string()))
        );
        assert!(tracker.committed.active_object.is_none());
        assert!(tracker.pending.is_none());
    }

    #[test]
    fn given_new_object_when_completion_staged_should_create_pending_without_advancing_committed() {
        let mut tracker = StateTracker {
            committed: SourceState {
                active_object: None,
            },
            pending: None,
        };

        tracker.stage_completion("logs/events.jsonl".to_string());

        assert!(tracker.committed.active_object.is_none());
        let pending = tracker
            .pending
            .as_ref()
            .expect("staging completion should create pending state");
        assert!(pending.checkpoint.active_object.is_none());
        assert_eq!(pending.completed_key.as_deref(), Some("logs/events.jsonl"));
    }

    #[test]
    fn given_completed_key_when_scan_cursor_advanced_should_set_start_after() {
        let mut scan_state = ScanState::default();

        scan_state.advance_after("logs/events.jsonl".to_string());

        assert_eq!(scan_state.start_after.as_deref(), Some("logs/events.jsonl"));
        assert!(!scan_state.exhausted);
    }

    #[test]
    fn given_active_scan_when_marked_exhausted_should_be_exhausted() {
        let mut scan_state = ScanState::default();

        scan_state.mark_exhausted();

        assert!(scan_state.exhausted);
    }

    #[test]
    fn given_source_state_when_serialized_should_roundtrip_through_connector_state() {
        let original = SourceState {
            active_object: Some(active_object(100, 25)),
        };

        let serialized =
            ConnectorState::serialize(&original, "s3_source", 1).expect("state should serialize");
        let restored = serialized
            .deserialize::<SourceState>("s3_source", 1)
            .expect("state should deserialize");

        assert_eq!(restored, original);
    }
}
