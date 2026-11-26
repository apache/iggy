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

//! The wait of group polls for the geo peers' positions after a start,
//! which leaves out the peers whose origin retired.

use super::segment_uploads::namespace;
use super::*;
use crate::geo_positions::GeoPositionSettings;
use crate::peers::{PeerStatus, PeerStore, PeerStores, SharedPeerStatus};
use crate::tier_service::TierServiceConfig;
use iggy_common::{GeoTopicDurations, IggyDuration};
use iggy_object_store::{KeyLayout, MemoryStore, ReadOnlyStore};

/// The peer `origin` of a node, retired or not.
fn peer(origin: &str, bus: &Rc<IggyMessageBus>, retired: bool) -> PeerStore<Rc<IggyMessageBus>> {
    let status = SharedPeerStatus::default();
    status.set(PeerStatus {
        retired,
        ..PeerStatus::default()
    });
    PeerStore::new(
        origin.to_owned(),
        KeyLayout::new(origin).unwrap(),
        ReadOnlyStore::new(MemoryStore::new()),
        Rc::clone(bus),
        &TierServiceConfig::default(),
        status,
    )
}

/// A node of `dc-c` that reads the peers `dc-a` and `dc-b`, of which only
/// `dc-a` retired when `dc_b_retired` is false.
fn waiting_owner(dc_b_retired: bool) -> IoTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner(&bus, None);
    wait_for_peers(owner, &bus, dc_b_retired, Duration::from_secs(3600))
}

/// [`waiting_owner`] of a geo topic whose own resume timeout is
/// `topic_timeout`, on a node whose timeout is `node_timeout`. The peer
/// `dc-b` never answers.
fn geo_topic_owner(topic_timeout: Duration, node_timeout: Duration) -> IoTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (owner, _sender) = test_owner_of_geo_topic(
        &bus,
        GeoTopicDurations {
            positions_resume_timeout: Some(IggyDuration::new(topic_timeout)),
            ..GeoTopicDurations::default()
        },
    );
    wait_for_peers(owner, &bus, false, node_timeout)
}

fn wait_for_peers(
    owner: IoTestShard,
    bus: &Rc<IggyMessageBus>,
    dc_b_retired: bool,
    resume_timeout: Duration,
) -> IoTestShard {
    owner.set_peer_stores(PeerStores::new(vec![
        peer("dc-a", bus, true),
        peer("dc-b", bus, dc_b_retired),
    ]));
    owner.set_geo_positions(Some(GeoPositionSettings {
        origin: "dc-c".to_owned(),
        peers: vec!["dc-a".to_owned(), "dc-b".to_owned()],
        publish_interval: Duration::from_secs(1),
        offset_upload_interval: None,
        resume_timeout,
    }));
    owner
}

#[compio::test]
async fn given_only_retired_peers_when_a_group_poll_asks_should_wait_for_none() {
    let owner = waiting_owner(true);

    assert!(!owner.geo_positions_resume_waiting(namespace()));
}

#[compio::test]
async fn given_a_peer_that_did_not_retire_when_a_group_poll_asks_should_wait_for_it() {
    let owner = waiting_owner(false);

    assert!(owner.geo_positions_resume_waiting(namespace()));
}

#[compio::test]
async fn given_a_topic_with_a_shorter_resume_timeout_when_it_passed_should_wait_no_longer() {
    let owner = geo_topic_owner(Duration::ZERO, Duration::from_secs(3600));

    assert!(!owner.geo_positions_resume_waiting(namespace()));
}

#[compio::test]
async fn given_a_topic_with_a_longer_resume_timeout_when_the_node_timeout_passed_should_keep_waiting()
 {
    let owner = geo_topic_owner(Duration::from_secs(3600), Duration::ZERO);

    assert!(owner.geo_positions_resume_waiting(namespace()));
    assert!(
        owner.geo_positions_resume_waiting(namespace()),
        "it asks again"
    );
}
