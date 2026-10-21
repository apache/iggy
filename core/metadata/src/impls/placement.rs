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

use crate::impls::metadata::{PartitionPlacementView, PartitionPrimaryTransfer};
use crate::stm::stream::{Partition, Streams, find_partition};
use ahash::AHashMap;
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::partitions::CreatePartitionsWithAssignmentsRequest;
use iggy_binary_protocol::requests::topics::CreateTopicWithAssignmentsRequest;
use iggy_binary_protocol::{Operation, PrepareHeader, WireDecode};
use iggy_common::IggyError;
use server_common::Message;
use server_common::sharding::IggyNamespace;

/// Summaries are only a cache. The live canonical pipeline decides which
/// reservations count, so commit, rejection and log replacement need no undo.
#[derive(Default)]
pub(super) struct PlacementState {
    observations: AHashMap<IggyNamespace, ObservedView>,
    reservations: AHashMap<(u64, u128), Vec<u64>>,
    eligible: Option<(u32, Vec<bool>)>,
}

struct ObservedView {
    report: PartitionPlacementView,
    present: bool,
}

impl PlacementState {
    pub(super) fn observe_replicas(&mut self, view: u32, eligible: &[bool]) {
        self.eligible = (!eligible.is_empty()).then(|| (view, eligible.to_vec()));
    }

    pub(super) fn observe(&mut self, streams: &Streams, reports: &[PartitionPlacementView]) {
        let () = streams.read(|inner| {
            for report in reports {
                let namespace = report.namespace;
                let Some(partition) = inner
                    .items
                    .get(namespace.stream_id())
                    .and_then(|stream| stream.topics.get(namespace.topic_id()))
                    .and_then(|topic| find_partition(&topic.partitions, namespace.partition_id()))
                else {
                    continue;
                };
                if partition.created_revision != report.created_revision {
                    continue;
                }
                if report.view < partition.created_view {
                    continue;
                }
                self.observations
                    .entry(namespace)
                    .and_modify(|previous| {
                        if previous.report.created_revision == report.created_revision {
                            previous.report.view = previous.report.view.max(report.view);
                        } else {
                            previous.report = *report;
                        }
                    })
                    .or_insert(ObservedView {
                        report: *report,
                        present: true,
                    });
            }
        });
    }

    pub(super) fn committed_counts(&mut self, streams: &Streams, replicas: u8) -> Vec<u64> {
        let counts = streams.read(|inner| {
            let mut counts = vec![0; usize::from(replicas)];
            for observation in self.observations.values_mut() {
                observation.present = false;
            }
            for (stream_id, stream) in &inner.items {
                for (topic_id, topic) in &stream.topics {
                    for partition in &topic.partitions {
                        let namespace = IggyNamespace::new(stream_id, topic_id, partition.id);
                        let view = self
                            .observations
                            .get_mut(&namespace)
                            .filter(|observation| {
                                observation.report.created_revision == partition.created_revision
                            })
                            .map_or(partition.created_view, |observation| {
                                observation.present = true;
                                observation.report.view.max(partition.created_view)
                            });
                        counts[(view % u32::from(replicas)) as usize] += 1;
                    }
                }
            }
            counts
        });
        self.observations
            .retain(|_, observation| observation.present);
        counts
    }

    pub(super) fn primary_rebalance_candidate(
        &self,
        streams: &Streams,
        replicas: u8,
        eligible: &[bool],
        after: Option<IggyNamespace>,
    ) -> Option<PartitionPrimaryTransfer> {
        let replicas = usize::from(replicas);
        let available = eligible.iter().filter(|&&available| available).count();
        if eligible.len() != replicas || available < 2 || available <= replicas / 2 {
            return None;
        }
        streams.read(|inner| {
            let mut counts = vec![0_u64; replicas];
            let mut first: Vec<Option<PartitionPlacementView>> = vec![None; replicas];
            let mut next: Vec<Option<PartitionPlacementView>> = vec![None; replicas];
            for (stream_id, stream) in &inner.items {
                for (topic_id, topic) in &stream.topics {
                    if inner.lifecycle_blocks(stream_id, Some(topic_id)) {
                        continue;
                    }
                    for partition in &topic.partitions {
                        let namespace = IggyNamespace::new(stream_id, topic_id, partition.id);
                        let report = self.matching_view(namespace, partition);
                        let primary = report.map_or(partition.created_view, |report| report.view)
                            as usize
                            % replicas;
                        counts[primary] += 1;
                        let Some(report) = report.filter(|_| eligible[primary]) else {
                            continue;
                        };
                        if first[primary].is_none_or(|first| namespace < first.namespace) {
                            first[primary] = Some(report);
                        }
                        if after.is_none_or(|cursor| namespace > cursor)
                            && next[primary].is_none_or(|next| namespace < next.namespace)
                        {
                            next[primary] = Some(report);
                        }
                    }
                }
            }
            let mut available = eligible
                .iter()
                .enumerate()
                .filter_map(|(replica, &eligible)| eligible.then_some(replica));
            let mut source = available.next()?;
            let mut target = source;
            for replica in available {
                if counts[replica] > counts[source] {
                    source = replica;
                }
                if counts[replica] < counts[target] {
                    target = replica;
                }
            }
            if counts[source].saturating_sub(counts[target]) <= 1 {
                return None;
            }
            let report = next[source].or(first[source])?;
            Some(PartitionPrimaryTransfer {
                namespace: report.namespace,
                created_revision: report.created_revision,
                view: report.view,
                target: u8::try_from(target).ok()?,
            })
        })
    }

    pub(super) fn partition_placement_view(
        &self,
        streams: &Streams,
        namespace: IggyNamespace,
        created_revision: u64,
    ) -> Option<PartitionPlacementView> {
        streams.read(|inner| {
            if inner.lifecycle_blocks(namespace.stream_id(), Some(namespace.topic_id())) {
                return None;
            }
            let topic = inner
                .items
                .get(namespace.stream_id())?
                .topics
                .get(namespace.topic_id())?;
            let partition = find_partition(&topic.partitions, namespace.partition_id())?;
            if partition.created_revision != created_revision {
                return None;
            }
            self.matching_view(namespace, partition)
        })
    }

    pub(super) fn select(&self, counts: &mut [u64], view: u32) -> Result<u32, IggyError> {
        let eligible = self.eligible.as_ref().filter(|(observed_view, eligible)| {
            *observed_view == view && eligible.len() == counts.len()
        });
        let (replica, count) = counts
            .iter_mut()
            .enumerate()
            .filter(|(replica, _)| eligible.is_none_or(|(_, eligible)| eligible[*replica]))
            .min_by_key(|(replica, count)| (**count, *replica))
            .ok_or(IggyError::TransientNotAccepted)?;
        *count += 1;
        u32::try_from(replica).map_err(|_| IggyError::TransientNotAccepted)
    }

    pub(super) fn prune(&mut self, retained: impl Fn(u64, u128) -> bool) {
        self.reservations
            .retain(|&(op, checksum), _| retained(op, checksum));
    }

    pub(super) fn contains(&self, header: &PrepareHeader) -> bool {
        self.reservations
            .contains_key(&(header.op, header.checksum))
    }

    /// Partitions the create behind `header` adds when it commits.
    pub(super) fn reserved_partitions(&self, header: &PrepareHeader) -> Result<u64, IggyError> {
        self.reservations
            .get(&(header.op, header.checksum))
            .map(|reserved| reserved.iter().sum())
            .ok_or(IggyError::TransientNotAccepted)
    }

    pub(super) fn add_reservation(
        &self,
        header: &PrepareHeader,
        counts: &mut [u64],
    ) -> Result<(), IggyError> {
        let reserved = self
            .reservations
            .get(&(header.op, header.checksum))
            .ok_or(IggyError::TransientNotAccepted)?;
        for (count, reserved) in counts.iter_mut().zip(reserved) {
            *count += reserved;
        }
        Ok(())
    }

    pub(super) fn remember(
        &mut self,
        prepare: &Message<PrepareHeader>,
        replicas: u8,
    ) -> Result<Option<u64>, IggyError> {
        let partitions = match prepare.header().operation {
            Operation::CreateTopicWithAssignments => {
                CreateTopicWithAssignmentsRequest::decode_from(prepare.body())
                    .map_err(|_| IggyError::InvalidCommand)?
                    .partitions
            }
            Operation::CreatePartitionsWithAssignments => {
                CreatePartitionsWithAssignmentsRequest::decode_from(prepare.body())
                    .map_err(|_| IggyError::InvalidCommand)?
                    .partitions
            }
            _ => return Ok(None),
        };
        let mut counts = vec![0; usize::from(replicas)];
        let mut highest_group_id = 0;
        for CreatedPartitionAssignment {
            consensus_group_id,
            initial_view,
            ..
        } in partitions
        {
            counts[(initial_view % u32::from(replicas)) as usize] += 1;
            highest_group_id = highest_group_id.max(consensus_group_id);
        }
        self.reservations
            .insert((prepare.header().op, prepare.header().checksum), counts);
        Ok(Some(highest_group_id))
    }

    fn matching_view(
        &self,
        namespace: IggyNamespace,
        partition: &Partition,
    ) -> Option<PartitionPlacementView> {
        self.observations
            .get(&namespace)
            .map(|observation| observation.report)
            .filter(|report| report.created_revision == partition.created_revision)
            .filter(|report| report.view >= partition.created_view)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::StateHandler;
    use crate::stm::lifecycle::apply_with_lifecycle_completion;
    use iggy_binary_protocol::WireIdentifier;
    use iggy_binary_protocol::requests::streams::DeleteStreamRequest;
    use iggy_binary_protocol::requests::topics::DeleteTopicRequest;
    use iggy_common::IggyTimestamp;

    fn observed_topics(views: &[u32]) -> (Streams, PlacementState) {
        let streams = Streams::default();
        let mut placement = PlacementState::default();
        for (topic, &view) in views.iter().enumerate() {
            let namespace = IggyNamespace::new(0, topic, 0);
            streams.seed_namespace(namespace, u64::try_from(topic).unwrap() + 1, view);
            placement.observe(
                &streams,
                &[PartitionPlacementView {
                    namespace,
                    created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
                    view,
                }],
            );
        }
        (streams, placement)
    }

    #[test]
    fn given_failed_and_returned_replica_when_rebalancing_should_restore_even_ownership() {
        let views = (0..100).map(|topic| topic % 5).collect::<Vec<_>>();
        let (streams, mut placement) = observed_topics(&views);
        for topic in (0..100).step_by(5) {
            let namespace = IggyNamespace::new(0, topic, 0);
            placement.observe(
                &streams,
                &[PartitionPlacementView {
                    namespace,
                    created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
                    view: 1,
                }],
            );
        }
        assert_eq!(placement.committed_counts(&streams, 5), [0, 40, 20, 20, 20]);
        for (eligible, expected_counts, expected_moves) in [
            ([false, true, true, true, true], [0, 25, 25, 25, 25], 15),
            ([true; 5], [20; 5], 20),
        ] {
            let mut moves = 0;
            let mut after = None;
            while let Some(transfer) =
                placement.primary_rebalance_candidate(&streams, 5, &eligible, after)
            {
                assert!(moves < 100, "each completed transfer must reduce imbalance");
                assert!(eligible[usize::from(transfer.target)]);
                let next_view =
                    transfer.view + (u32::from(transfer.target) + 5 - transfer.view % 5) % 5;
                assert!(next_view > transfer.view);
                placement.observe(
                    &streams,
                    &[PartitionPlacementView {
                        namespace: transfer.namespace,
                        created_revision: transfer.created_revision,
                        view: next_view,
                    }],
                );
                after = Some(transfer.namespace);
                moves += 1;
            }
            assert_eq!(moves, expected_moves);
            assert_eq!(placement.committed_counts(&streams, 5), expected_counts);
        }
        assert_eq!(
            PlacementState::default().committed_counts(&streams, 5),
            [20; 5],
            "transfers must not rewrite committed initial views"
        );
    }

    #[test]
    fn given_balanced_or_almost_balanced_primaries_when_selecting_should_not_transfer() {
        for views in [&[][..], &[0, 1, 2], &[0, 1, 2, 0]] {
            let (streams, placement) = observed_topics(views);
            assert!(
                placement
                    .primary_rebalance_candidate(&streams, 3, &[true; 3], None)
                    .is_none()
            );
        }
    }

    #[test]
    fn given_unsettled_failover_or_no_quorum_when_selecting_should_wait() {
        let (streams, placement) = observed_topics(&[0, 0, 0, 1, 2]);
        for eligible in [
            &[false, true, true, true, true][..],
            &[true, true, false, false, false],
            &[true, false, false, false, false],
            &[true; 4],
        ] {
            assert!(
                placement
                    .primary_rebalance_candidate(&streams, 5, eligible, None)
                    .is_none()
            );
        }
        assert!(
            placement
                .primary_rebalance_candidate(&streams, 0, &[], None)
                .is_none()
        );
        assert!(
            placement
                .primary_rebalance_candidate(&streams, 1, &[true], None)
                .is_none()
        );
    }

    #[test]
    fn given_tied_load_and_recovering_target_when_selecting_should_rotate_candidates() {
        let (streams, placement) = observed_topics(&[0, 0, 0, 1, 1, 1]);
        for (after, expected_topic) in [(None, 0), (Some(0), 1), (Some(1), 2), (Some(2), 0)] {
            let transfer = placement
                .primary_rebalance_candidate(
                    &streams,
                    5,
                    &[true; 5],
                    after.map(|topic| IggyNamespace::new(0, topic, 0)),
                )
                .unwrap();
            assert_eq!(transfer.namespace, IggyNamespace::new(0, expected_topic, 0));
            assert_eq!(transfer.view, 0, "lowest replica id wins the source tie");
            assert_eq!(transfer.target, 2, "lowest replica id wins the target tie");
        }
    }

    #[test]
    fn given_missing_observation_when_selecting_should_require_a_matching_normal_view() {
        let (streams, mut placement) = observed_topics(&[2, 2, 2]);
        let namespace = IggyNamespace::new(0, 0, 0);
        let report = placement.observations.remove(&namespace).unwrap().report;
        for stale in [
            PartitionPlacementView { view: 1, ..report },
            PartitionPlacementView {
                created_revision: report.created_revision + 1,
                ..report
            },
        ] {
            placement.observe(&streams, &[stale]);
            let transfer = placement
                .primary_rebalance_candidate(&streams, 3, &[true; 3], None)
                .unwrap();
            assert_eq!(transfer.namespace, IggyNamespace::new(0, 1, 0));
            assert_eq!(transfer.target, 0);
            assert!(
                placement
                    .partition_placement_view(&streams, namespace, report.created_revision)
                    .is_none()
            );
        }
        placement.observe(&streams, &[report]);
        assert!(
            placement
                .primary_rebalance_candidate(&streams, 3, &[true; 3], None)
                .is_some()
        );
    }

    #[test]
    fn given_pending_parent_deletion_when_selecting_should_exclude_its_partitions() {
        let (streams, placement) = observed_topics(&[0, 0, 0]);
        let namespace = IggyNamespace::new(0, 0, 0);
        let revision = streams.created_revision_for_namespace(namespace).unwrap();
        let mut inner = streams.read(Clone::clone);
        inner.apply_context.metadata_op = 100;
        assert_eq!(
            DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
            }
            .apply(&mut inner, IggyTimestamp::from(1))
            .code,
            0
        );
        let deleting_topic = Streams::from(inner);
        let transfer = placement
            .primary_rebalance_candidate(&deleting_topic, 3, &[true; 3], None)
            .unwrap();
        assert_eq!(transfer.namespace, IggyNamespace::new(0, 1, 0));
        assert!(
            placement
                .partition_placement_view(&deleting_topic, namespace, revision)
                .is_none()
        );

        let mut inner = streams.read(Clone::clone);
        inner.apply_context.metadata_op = 101;
        assert_eq!(
            DeleteStreamRequest {
                stream_id: WireIdentifier::numeric(0),
            }
            .apply(&mut inner, IggyTimestamp::from(1))
            .code,
            0
        );
        let deleting_stream = Streams::from(inner);
        assert!(
            placement
                .primary_rebalance_candidate(&deleting_stream, 3, &[true; 3], None)
                .is_none()
        );
        assert!(
            placement
                .partition_placement_view(&deleting_stream, namespace, revision)
                .is_none()
        );
    }

    #[test]
    fn given_recreated_namespace_when_selecting_should_wait_for_its_own_observation() {
        let (streams, mut placement) = observed_topics(&[0, 0, 0]);
        let namespace = IggyNamespace::new(0, 0, 0);
        let old_report = placement.observations[&namespace].report;
        let mut inner = streams.read(Clone::clone);
        assert_eq!(
            apply_with_lifecycle_completion(
                &DeleteTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                },
                &mut inner,
                IggyTimestamp::from(1),
            )
            .code,
            0
        );
        let recreated = Streams::from(inner);
        assert!(
            placement
                .partition_placement_view(&recreated, namespace, old_report.created_revision)
                .is_none()
        );
        recreated.seed_namespace(namespace, 100, 0);
        let new_revision = recreated.created_revision_for_namespace(namespace).unwrap();
        assert_ne!(new_revision, old_report.created_revision);
        placement.observe(
            &recreated,
            &[PartitionPlacementView {
                view: 100,
                ..old_report
            }],
        );
        let transfer = placement
            .primary_rebalance_candidate(&recreated, 3, &[true; 3], None)
            .unwrap();
        assert_eq!(transfer.namespace, IggyNamespace::new(0, 1, 0));
        assert!(
            placement
                .partition_placement_view(&recreated, namespace, new_revision)
                .is_none()
        );
        placement.observe(
            &recreated,
            &[PartitionPlacementView {
                namespace,
                created_revision: new_revision,
                view: 0,
            }],
        );
        let transfer = placement
            .primary_rebalance_candidate(&recreated, 3, &[true; 3], None)
            .unwrap();
        assert_eq!(transfer.namespace, namespace);
        assert_eq!(transfer.created_revision, new_revision);
        assert!(
            placement
                .partition_placement_view(&recreated, namespace, old_report.created_revision)
                .is_none()
        );
    }

    #[test]
    fn given_an_unavailable_primary_when_other_partitions_are_imbalanced_should_move_a_ready_one() {
        let (streams, placement) = observed_topics(&[0, 1, 1, 1]);
        let transfer = placement
            .primary_rebalance_candidate(&streams, 3, &[false, true, true], None)
            .unwrap();
        assert_eq!(transfer.namespace, IggyNamespace::new(0, 1, 0));
        assert_eq!(transfer.target, 2);
    }

    #[test]
    fn given_readiness_from_another_view_when_placing_should_ignore_stale_evidence() {
        let mut placement = PlacementState::default();
        placement.observe_replicas(1, &[false, true, true]);
        assert_eq!(placement.select(&mut [0, 5, 5], 1), Ok(1));
        assert_eq!(placement.select(&mut [0, 5, 5], 2), Ok(0));
        placement.observe_replicas(2, &[false; 3]);
        assert_eq!(
            placement.select(&mut [0, 5, 5], 2),
            Err(IggyError::TransientNotAccepted)
        );
        placement.observe_replicas(2, &[]);
        assert_eq!(placement.select(&mut [0, 5, 5], 2), Ok(0));
    }
}
