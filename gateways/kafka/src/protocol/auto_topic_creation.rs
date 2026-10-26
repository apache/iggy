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

//! Kafka's `auto.create.topics.enable`: a `Metadata` lookup of a topic that does not exist starts
//! creating it.
//!
//! As in Kafka (`DefaultAutoTopicCreationManager`), the lookup that starts a creation is answered
//! `UNKNOWN_TOPIC_OR_PARTITION` at once, and the client finds the topic on a later refresh. A
//! topic already being created is not started again.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use iggy::prelude::IggyError;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::bridge::{
    BridgeError, DEFAULT_TOPIC_PARTITIONS, IggyBridge, MAX_TOPIC_PARTITIONS, TopicCreationOutcome,
};
use crate::error::KafkaProtocolError;

/// Creations one gateway runs at once.
///
/// They share one Iggy client, so a client naming thousands of missing topics in one `Metadata`
/// must not queue them all. A name not started now is started by a later refresh.
pub(crate) const MAX_IN_FLIGHT_CREATIONS: usize = 100;

/// How long a topic whose creation failed or timed out is not tried again, unless
/// [`REFUSED_CREATION_BACKOFF`] applies.
///
/// A producer refreshes `Metadata` every `retry.backoff.ms` (100 ms by default) while it waits, so
/// without this a creation Iggy refuses, for example at `partitions_max`, would run ten times a
/// second.
pub(crate) const FAILED_CREATION_BACKOFF: Duration = Duration::from_secs(1);

/// How long a topic whose creation Iggy refused for the bridge user's permissions is not tried
/// again.
///
/// Iggy decides that refusal only when it applies the create, after journaling and replicating
/// it, so each attempt costs a metadata write. Only an operator's grant changes the answer, so a
/// minute between attempts delays little next to a write a second per name.
const REFUSED_CREATION_BACKOFF: Duration = Duration::from_secs(60);

/// How long shutdown waits for cancelled creations to end. A cancelled creation ends at its next
/// poll, so this only covers a runtime too busy to poll it.
pub const SHUTDOWN_WAIT: Duration = Duration::from_secs(1);

/// How long the failures of one name log at debug after one logged at warn. A name Iggy refuses
/// is retried every [`FAILED_CREATION_BACKOFF`] for as long as clients name it.
const REPEAT_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// Names [`FailureWarnings`] remembers. A failure past it warns without being remembered.
const MAX_WARNED_NAMES: usize = 1024;

/// `IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED` (Kafka's `auto.create.topics.enable`) and
/// `IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS`.
///
/// Off by default, unlike Kafka: a gateway that started creating topics on its own after an
/// upgrade would surprise operators who rely on `CreateTopics` being the only way in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoTopicCreationConfig {
    pub enabled: bool,
    /// From 1 to [`MAX_TOPIC_PARTITIONS`].
    pub partitions: u32,
}

impl Default for AutoTopicCreationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            partitions: DEFAULT_TOPIC_PARTITIONS,
        }
    }
}

/// The topics Kafka creates for its own coordinators (`Topic.GROUP_METADATA_TOPIC_NAME`,
/// `TRANSACTION_STATE_TOPIC_NAME`, `SHARE_GROUP_STATE_TOPIC_NAME`).
const KAFKA_INTERNAL_TOPICS: [&str; 3] = [
    "__consumer_offsets",
    "__transaction_state",
    "__share_group_state",
];

/// Whether `kafka_topic` is one of Kafka's internal topics. This gateway keeps none of them in
/// Iggy, so none is auto-created. Other `__` names, such as Debezium's heartbeat topics, are
/// ordinary topics.
#[must_use]
pub(crate) fn is_internal_topic_name(kafka_topic: &str) -> bool {
    KAFKA_INTERNAL_TOPICS.contains(&kafka_topic)
}

/// Topics this gateway is creating on behalf of `Metadata` lookups.
#[derive(Debug)]
pub(crate) struct AutoTopicCreator {
    partitions: u32,
    in_flight: Arc<Mutex<HashSet<Arc<str>>>>,
    warnings: Arc<FailureWarnings>,
    tasks: TaskTracker,
    cancel: CancellationToken,
}

impl AutoTopicCreator {
    /// # Errors
    ///
    /// [`KafkaProtocolError::InvalidConfig`] unless `partitions` is from 1 to
    /// [`MAX_TOPIC_PARTITIONS`].
    pub(crate) fn new(partitions: u32) -> Result<Self, KafkaProtocolError> {
        if partitions == 0 || partitions > MAX_TOPIC_PARTITIONS {
            return Err(KafkaProtocolError::InvalidConfig(format!(
                "auto topic creation partitions must be from 1 to {MAX_TOPIC_PARTITIONS}, got \
                 {partitions}"
            )));
        }
        Ok(Self {
            partitions,
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            warnings: Arc::default(),
            tasks: TaskTracker::new(),
            cancel: CancellationToken::new(),
        })
    }

    /// Starts creating `kafka_topic` in the background, unless it is already being created, it
    /// failed less than [`FAILED_CREATION_BACKOFF`] (or [`REFUSED_CREATION_BACKOFF`]) ago,
    /// [`MAX_IN_FLIGHT_CREATIONS`] are running, or shutdown began. Returns whether it started one.
    /// The caller answers `UNKNOWN_TOPIC_OR_PARTITION` either way.
    pub(crate) fn start(&self, bridge: &Arc<IggyBridge>, kafka_topic: &str) -> bool {
        if self.cancel.is_cancelled() {
            return false;
        }
        let Some(claim) = InFlight::claim(&self.in_flight, kafka_topic) else {
            return false;
        };
        let bridge = Arc::clone(bridge);
        let partitions = self.partitions;
        let cancel = self.cancel.clone();
        let warnings = Arc::clone(&self.warnings);
        self.tasks.spawn(async move {
            tokio::select! {
                () = cancel.cancelled() => {}
                () = create(&bridge, &claim.name, partitions, &warnings) => {}
            }
        });
        true
    }

    /// Cancels the creations still running and waits up to `deadline` for them to end. Returns
    /// whether any was running. A cancelled creation may still land in Iggy, and the next lookup
    /// then finds the topic.
    pub(crate) async fn shutdown(&self, deadline: Duration) -> bool {
        let cancelled = !self.tasks.is_empty();
        self.cancel.cancel();
        self.tasks.close();
        if tokio::time::timeout(deadline, self.tasks.wait())
            .await
            .is_err()
        {
            warn!(
                ?deadline,
                "auto topic creations did not end before shutdown went on"
            );
        }
        cancelled
    }

    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

/// Creates `kafka_topic`, then, if that failed, holds it for as long as [`backoff_after`] says.
async fn create(
    bridge: &IggyBridge,
    kafka_topic: &str,
    partitions: u32,
    warnings: &FailureWarnings,
) {
    let hold = match bridge
        .auto_create_kafka_topic(kafka_topic, partitions)
        .await
    {
        Ok(TopicCreationOutcome::Created) => {
            warnings.forget(kafka_topic);
            info!(
                kafka_topic,
                partitions, "auto-created a topic a Metadata request named"
            );
            None
        }
        Ok(TopicCreationOutcome::AlreadyExists)
        | Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => {
            warnings.forget(kafka_topic);
            None
        }
        // The create may still land. Held like a failure only to space out retries of this name:
        // a retry that races it is harmless, since Iggy refuses a name already taken.
        Err(
            error @ (BridgeError::Timeout | BridgeError::Iggy(IggyError::OperationUncertain(_))),
        ) => {
            debug!(kafka_topic, %error, "auto topic creation outcome unknown, it may still land");
            Some(backoff_after(&error))
        }
        Err(error) if warnings.should_warn(kafka_topic, Instant::now()) => {
            warn!(kafka_topic, %error, "auto topic creation failed");
            Some(backoff_after(&error))
        }
        Err(error) => {
            debug!(kafka_topic, %error, "auto topic creation failed again");
            Some(backoff_after(&error))
        }
    };
    if let Some(hold) = hold {
        tokio::time::sleep(hold).await;
    }
}

/// How long a name is held after its creation failed with `error`.
const fn backoff_after(error: &BridgeError) -> Duration {
    if matches!(error, BridgeError::Iggy(IggyError::Unauthorized)) {
        REFUSED_CREATION_BACKOFF
    } else {
        FAILED_CREATION_BACKOFF
    }
}

/// When each name whose creation failed last warned, so a name Iggy keeps refusing warns once per
/// [`REPEAT_WARNING_INTERVAL`], not once per retry.
#[derive(Debug, Default)]
struct FailureWarnings(Mutex<HashMap<Arc<str>, Instant>>);

impl FailureWarnings {
    /// Whether a failure of `kafka_topic` at `now` logs at warn, and if so remembers that it did.
    fn should_warn(&self, kafka_topic: &str, now: Instant) -> bool {
        let recent = |warned_at: &Instant| now.duration_since(*warned_at) < REPEAT_WARNING_INTERVAL;
        let mut warned = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if warned.get(kafka_topic).is_some_and(recent) {
            return false;
        }
        if warned.len() >= MAX_WARNED_NAMES {
            warned.retain(|_, warned_at| recent(warned_at));
        }
        if warned.len() < MAX_WARNED_NAMES {
            warned.insert(Arc::from(kafka_topic), now);
        }
        true
    }

    fn forget(&self, kafka_topic: &str) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(kafka_topic);
    }
}

/// One name in the in-flight set, released on drop so a panicking or cancelled creation frees
/// its name and slot too.
struct InFlight {
    names: Arc<Mutex<HashSet<Arc<str>>>>,
    name: Arc<str>,
}

impl InFlight {
    fn claim(names: &Arc<Mutex<HashSet<Arc<str>>>>, kafka_topic: &str) -> Option<Self> {
        let mut held = names.lock().unwrap_or_else(PoisonError::into_inner);
        if held.len() >= MAX_IN_FLIGHT_CREATIONS || held.contains(kafka_topic) {
            return None;
        }
        let name: Arc<str> = Arc::from(kafka_topic);
        held.insert(Arc::clone(&name));
        drop(held);
        Some(Self {
            names: Arc::clone(names),
            name,
        })
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.names
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::{GroupCoordinator, GroupCoordinatorConfig};
    use crate::protocol::api::GatewayState;

    /// The offline bridge fails every call at once, so each creation lands in its failure backoff
    /// and stays in flight for exactly that long.
    #[tokio::test(start_paused = true)]
    async fn given_a_topic_being_created_when_it_is_named_again_should_not_start_it_twice() {
        let creator = AutoTopicCreator::new(1).expect("a valid partition count");
        let bridge = Arc::new(IggyBridge::offline());

        assert!(creator.start(&bridge, "orders"));
        assert!(!creator.start(&bridge, "orders"));
        tokio::task::yield_now().await;

        assert_eq!(creator.in_flight(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_in_flight_cap_when_more_topics_are_named_should_start_only_up_to_it() {
        let creator = AutoTopicCreator::new(1).expect("a valid partition count");
        let bridge = Arc::new(IggyBridge::offline());

        for index in 0..MAX_IN_FLIGHT_CREATIONS {
            assert!(creator.start(&bridge, &format!("topic-{index}")));
        }

        assert!(!creator.start(&bridge, "one-too-many"));

        assert_eq!(creator.in_flight(), MAX_IN_FLIGHT_CREATIONS);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_failed_creation_when_the_backoff_passes_should_allow_another_attempt() {
        let creator = AutoTopicCreator::new(1).expect("a valid partition count");
        let bridge = Arc::new(IggyBridge::offline());

        assert!(creator.start(&bridge, "orders"));
        tokio::task::yield_now().await;
        tokio::time::sleep(FAILED_CREATION_BACKOFF / 2).await;
        assert_eq!(creator.in_flight(), 1, "still backing off");

        tokio::time::sleep(FAILED_CREATION_BACKOFF).await;
        tokio::task::yield_now().await;
        assert_eq!(creator.in_flight(), 0);
        assert!(creator.start(&bridge, "orders"));
    }

    /// The creation is in its backoff when shutdown begins, so only cancelling it ends it sooner.
    #[tokio::test(start_paused = true)]
    async fn given_a_running_creation_when_shutting_down_should_cancel_it_and_wait_for_it() {
        let creator = AutoTopicCreator::new(1).expect("a valid partition count");
        let bridge = Arc::new(IggyBridge::offline());
        assert!(creator.start(&bridge, "orders"));
        tokio::task::yield_now().await;

        let began = Instant::now();
        creator.shutdown(FAILED_CREATION_BACKOFF * 10).await;

        assert!(
            began.elapsed() < FAILED_CREATION_BACKOFF,
            "cancelled, not waited out"
        );
        assert_eq!(
            creator.in_flight(),
            0,
            "the task ended and released its name"
        );
        assert!(
            !creator.start(&bridge, "orders"),
            "nothing starts after shutdown"
        );
    }

    #[test]
    fn given_a_name_that_keeps_failing_should_warn_once_per_interval() {
        let warnings = FailureWarnings::default();
        let start = Instant::now();

        assert!(warnings.should_warn("orders", start));
        assert!(!warnings.should_warn("orders", start + FAILED_CREATION_BACKOFF));
        assert!(
            warnings.should_warn("payments", start + FAILED_CREATION_BACKOFF),
            "each name warns on its own"
        );
        assert!(warnings.should_warn("orders", start + REPEAT_WARNING_INTERVAL));
    }

    #[test]
    fn given_a_name_that_was_created_when_it_fails_again_should_warn() {
        let warnings = FailureWarnings::default();
        let start = Instant::now();
        assert!(warnings.should_warn("orders", start));

        warnings.forget("orders");

        assert!(warnings.should_warn("orders", start + FAILED_CREATION_BACKOFF));
    }

    #[test]
    fn given_every_remembered_name_when_one_more_fails_should_still_warn_and_stay_bounded() {
        let warnings = FailureWarnings::default();
        let start = Instant::now();
        for index in 0..MAX_WARNED_NAMES {
            assert!(warnings.should_warn(&format!("topic-{index}"), start));
        }

        assert!(warnings.should_warn("one-too-many", start));
        assert!(
            warnings.should_warn("one-too-many", start),
            "not remembered while every remembered name is recent"
        );
        let later = start + REPEAT_WARNING_INTERVAL;
        assert!(warnings.should_warn("one-too-many", later));
        assert!(
            !warnings.should_warn("one-too-many", later),
            "the stale names were pruned to make room"
        );
        assert!(
            warnings
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len()
                <= MAX_WARNED_NAMES
        );
    }

    fn gateway_with(bridge: &Arc<IggyBridge>) -> GatewayState {
        GatewayState::new(
            Some(Arc::clone(bridge)),
            1024,
            0,
            GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
        )
        .with_auto_topic_creation(AutoTopicCreationConfig {
            enabled: true,
            partitions: 1,
        })
        .expect("a valid partition count")
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_running_creation_when_the_gateway_shuts_it_down_should_discard_its_client() {
        let bridge = Arc::new(IggyBridge::offline());
        let state = gateway_with(&bridge);
        let creator = state.auto_topic_creation.as_ref().expect("creation is on");
        assert!(creator.start(&bridge, "orders"));
        tokio::task::yield_now().await;

        state.shutdown_auto_topic_creation(SHUTDOWN_WAIT).await;

        assert!(!bridge.holds_auto_create_client().await);
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_running_creation_when_the_gateway_shuts_it_down_should_keep_the_client_to_sign_out()
     {
        let bridge = Arc::new(IggyBridge::offline());
        let state = gateway_with(&bridge);

        state.shutdown_auto_topic_creation(SHUTDOWN_WAIT).await;

        assert!(bridge.holds_auto_create_client().await);
    }

    #[test]
    fn given_a_partition_count_outside_iggys_range_should_refuse_to_build() {
        assert!(AutoTopicCreator::new(0).is_err());
        assert!(AutoTopicCreator::new(MAX_TOPIC_PARTITIONS + 1).is_err());
        assert!(AutoTopicCreator::new(MAX_TOPIC_PARTITIONS).is_ok());
    }

    #[test]
    fn given_a_name_kafka_reserves_should_report_it_as_internal() {
        for name in [
            "__consumer_offsets",
            "__transaction_state",
            "__share_group_state",
        ] {
            assert!(is_internal_topic_name(name), "{name}");
        }
        assert!(!is_internal_topic_name("_orders"));
        assert!(!is_internal_topic_name("orders"));
    }

    #[test]
    fn given_a_double_underscore_name_kafka_does_not_reserve_should_allow_creating_it() {
        assert!(!is_internal_topic_name("__debezium-heartbeat.x"));
        assert!(!is_internal_topic_name("__consumer_offsets_copy"));
    }
}
