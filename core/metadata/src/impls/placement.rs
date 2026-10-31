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
use crate::stm::stream::{Partition, Streams, StreamsInner, find_partition, primary_of};
use ahash::AHashMap;
use consensus::{ReplicaSet, quorum_view_change};
use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
use iggy_binary_protocol::requests::partitions::CreatePartitionsWithAssignmentsRequest;
use iggy_binary_protocol::requests::topics::CreateTopicWithAssignmentsRequest;
use iggy_binary_protocol::{Operation, PrepareHeader, WireDecode};
use iggy_common::IggyError;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::cmp::Reverse;

/// Summaries are only a cache. The live canonical pipeline decides which
/// reservations count, so commit, rejection and log replacement need no undo.
#[derive(Default)]
pub(super) struct PlacementState {
    observations: AHashMap<IggyNamespace, ObservedView>,
    /// Per replica, observed primaries minus created ones, for one replica count.
    moved: Option<(u8, Vec<i64>)>,
    /// The `namespace_revision` whose removals `observations` reflects.
    synced_revision: Option<u64>,
    /// Changes with every observation, so a scan can tell its input moved.
    observed_generation: u64,
    last_scan: Option<(ScanInputs, Option<PartitionPrimaryTransfer>)>,
    reservations: AHashMap<(u64, u128), PlacementCounts>,
    eligible: Option<(u32, Vec<bool>)>,
    /// A readiness coordinator has reported. A metadata plane without one has
    /// no evidence to wait for and counts every node as ready.
    readiness_reported: bool,
}

/// Per node: partitions it is primary of, and partitions it stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlacementCounts {
    pub(super) primaries: Vec<u64>,
    pub(super) replicas: Vec<u64>,
}

impl PlacementCounts {
    fn new(nodes: u8) -> Self {
        Self {
            primaries: vec![0; usize::from(nodes)],
            replicas: vec![0; usize::from(nodes)],
        }
    }

    fn nodes(&self) -> u8 {
        u8::try_from(self.primaries.len()).unwrap_or(u8::MAX)
    }

    /// Count one partition whose `created_view` names its primary among
    /// `members`, or among every node when it has no member list.
    fn record(&mut self, members: Option<ReplicaSet>, created_view: u32) {
        let nodes = self.nodes();
        if let Some(primary) = primary_of(members, created_view, nodes)
            && let Some(count) = self.primaries.get_mut(usize::from(primary))
        {
            *count += 1;
        }
        match members {
            Some(members) => {
                for node in members.nodes() {
                    if let Some(count) = self.replicas.get_mut(usize::from(*node)) {
                        *count += 1;
                    }
                }
            }
            None => {
                for count in &mut self.replicas {
                    *count += 1;
                }
            }
        }
    }

    fn add(&mut self, other: &Self) {
        for (count, added) in self.primaries.iter_mut().zip(&other.primaries) {
            *count += added;
        }
        for (count, added) in self.replicas.iter_mut().zip(&other.replicas) {
            *count += added;
        }
    }
}

/// A candidate move's order: widest gap, lowest source node, after the cursor,
/// lowest namespace.
type MoveRank = (u64, Reverse<u8>, bool, Reverse<IggyNamespace>);

#[derive(Clone, Copy, PartialEq, Eq)]
struct ObservedView {
    report: PartitionPlacementView,
    created_view: u32,
    members: Option<ReplicaSet>,
}

/// Everything a rebalance scan reads. `revision` moves with the partition set
/// and with every lifecycle intent, which hides partitions from the scan.
#[derive(PartialEq, Eq)]
struct ScanInputs {
    revision: u64,
    observed_generation: u64,
    replicas: u8,
    eligible: Vec<bool>,
    after: Option<IggyNamespace>,
}

impl PlacementState {
    pub(super) fn observe_replicas(&mut self, view: u32, eligible: &[bool]) {
        self.readiness_reported = true;
        self.eligible = (!eligible.is_empty()).then(|| (view, eligible.to_vec()));
    }

    pub(super) fn observe(&mut self, streams: &Streams, reports: &[PartitionPlacementView]) {
        let () = streams.read(|inner| {
            for report in reports {
                let namespace = report.namespace;
                let Some(partition) = committed_partition(inner, namespace) else {
                    continue;
                };
                if partition.created_revision != report.created_revision {
                    continue;
                }
                if report.view < partition.created_view {
                    continue;
                }
                let mut observed = ObservedView {
                    report: *report,
                    created_view: partition.created_view,
                    members: partition.members,
                };
                if let Some(previous) = self.observations.get(&namespace).copied() {
                    if previous.report.created_revision == report.created_revision {
                        observed.report.view = previous.report.view.max(report.view);
                    }
                    if observed == previous {
                        continue;
                    }
                    self.track(previous, -1);
                }
                self.track(observed, 1);
                self.observations.insert(namespace, observed);
                self.observed_generation += 1;
            }
        });
    }

    pub(super) fn committed_counts(&mut self, streams: &Streams, nodes: u8) -> PlacementCounts {
        streams.read(|inner| {
            self.retire_removed(inner);
            let index = &inner.placement_index;
            let mut counts = PlacementCounts::new(nodes);
            let without_members: u64 = index.created_views.values().sum();
            for (&view, &partitions) in &index.created_views {
                counts.primaries[(view % u32::from(nodes)) as usize] += partitions;
            }
            for (&node, &partitions) in &index.created_primaries {
                if let Some(count) = counts.primaries.get_mut(usize::from(node)) {
                    *count += partitions;
                }
            }
            for (node, count) in (0..nodes).zip(counts.replicas.iter_mut()) {
                *count = without_members + index.stored_replicas.get(&node).copied().unwrap_or(0);
            }
            for (count, &moved) in counts.primaries.iter_mut().zip(self.moved(nodes)) {
                *count = count.saturating_add_signed(moved);
            }
            counts
        })
    }

    pub(super) fn primary_rebalance_candidate(
        &mut self,
        streams: &Streams,
        replicas: u8,
        eligible: &[bool],
        after: Option<IggyNamespace>,
    ) -> Option<PartitionPrimaryTransfer> {
        let replica_count = usize::from(replicas);
        let available = eligible.iter().filter(|&&available| available).count();
        if eligible.len() != replica_count || available < 2 || available <= replica_count / 2 {
            return None;
        }
        streams.read(|inner| {
            self.retire_removed(inner);
            let inputs = ScanInputs {
                revision: inner.revision,
                observed_generation: self.observed_generation,
                replicas,
                eligible: eligible.to_vec(),
                after,
            };
            if let Some((scanned, plan)) = &self.last_scan
                && *scanned == inputs
            {
                return *plan;
            }
            let plan = self.scan(inner, replicas, eligible, after);
            self.last_scan = Some((inputs, plan));
            plan
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

    /// Members and created view for one new partition, counted into `counts`.
    ///
    /// The primary is the ready node leading the fewest partitions. The other
    /// members are the nodes storing the fewest, ready nodes first, so a node
    /// that is down is chosen only when too few are up. `None` keeps the
    /// layout from before replication factors: no member list, every node
    /// stores it.
    ///
    /// # Errors
    /// `InvalidReplicationFactor` for a factor of zero or above the node count,
    /// `TransientNotAccepted` when no node is ready to lead or, once a
    /// coordinator reports, while it has no readiness for this view.
    pub(super) fn select(
        &self,
        counts: &mut PlacementCounts,
        view: u32,
        replication_factor: Option<u8>,
    ) -> Result<(Vec<u8>, u32), IggyError> {
        let nodes = counts.nodes();
        if replication_factor.is_some_and(|factor| factor == 0 || factor > nodes) {
            return Err(IggyError::InvalidReplicationFactor);
        }
        let eligible = self.eligible.as_ref().filter(|(observed_view, eligible)| {
            *observed_view == view && eligible.len() == usize::from(nodes)
        });
        // The assignment never moves, so guessing would leave a failed node
        // holding the partition until it returns.
        if self.readiness_reported && eligible.is_none() {
            return Err(IggyError::TransientNotAccepted);
        }
        let ready = |node: u8| eligible.is_none_or(|(_, eligible)| eligible[usize::from(node)]);
        let load = |node: u8| {
            (
                counts.primaries[usize::from(node)],
                counts.replicas[usize::from(node)],
            )
        };
        let primary = (0..nodes)
            .filter(|node| ready(*node))
            .min_by_key(|node| (load(*node), *node))
            .ok_or(IggyError::TransientNotAccepted)?;
        let Some(factor) = replication_factor else {
            counts.record(None, u32::from(primary));
            return Ok((Vec::new(), u32::from(primary)));
        };
        let mut followers: Vec<u8> = (0..nodes).filter(|node| *node != primary).collect();
        followers.sort_by_key(|&node| {
            let (primaries, replicas) = load(node);
            (!ready(node), replicas, primaries, node)
        });
        let mut members = followers[..usize::from(factor) - 1].to_vec();
        members.push(primary);
        members.sort_unstable();
        let created_view = members
            .iter()
            .position(|node| *node == primary)
            .and_then(|index| u32::try_from(index).ok())
            .ok_or(IggyError::InvalidReplicationFactor)?;
        let set = ReplicaSet::new(&members).map_err(|_| IggyError::InvalidReplicationFactor)?;
        counts.record(Some(set), created_view);
        Ok((members, created_view))
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
            .map(|reserved| reserved.primaries.iter().sum())
            .ok_or(IggyError::TransientNotAccepted)
    }

    pub(super) fn add_reservation(
        &self,
        header: &PrepareHeader,
        counts: &mut PlacementCounts,
    ) -> Result<(), IggyError> {
        let reserved = self
            .reservations
            .get(&(header.op, header.checksum))
            .ok_or(IggyError::TransientNotAccepted)?;
        counts.add(reserved);
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
        let mut counts = PlacementCounts::new(replicas);
        let mut highest_group_id = 0;
        for CreatedPartitionAssignment {
            consensus_group_id,
            initial_view,
            members,
            ..
        } in partitions
        {
            let members = (!members.is_empty())
                .then(|| ReplicaSet::new(&members))
                .transpose()
                .map_err(|_| IggyError::InvalidCommand)?;
            counts.record(members, initial_view);
            highest_group_id = highest_group_id.max(consensus_group_id);
        }
        self.reservations
            .insert((prepare.header().op, prepare.header().checksum), counts);
        Ok(Some(highest_group_id))
    }

    /// One primary move to a ready member of the same partition leading at
    /// least two fewer partitions, the widest such gap first. A partition
    /// qualifies only while enough of its members are ready to elect. Equal
    /// gaps go to the lowest source node, then rotate with `after`.
    fn scan(
        &self,
        inner: &StreamsInner,
        nodes: u8,
        eligible: &[bool],
        after: Option<IggyNamespace>,
    ) -> Option<PartitionPrimaryTransfer> {
        let ready = |node: u8| eligible.get(usize::from(node)).copied().unwrap_or(false);
        let mut counts = vec![0_u64; usize::from(nodes)];
        self.for_each_partition(inner, |_, partition, report| {
            let view = report.map_or(partition.created_view, |report| report.view);
            if let Some(count) = partition
                .primary_node(view, nodes)
                .and_then(|primary| counts.get_mut(usize::from(primary)))
            {
                *count += 1;
            }
        });
        let count = |node: u8| counts.get(usize::from(node)).copied().unwrap_or(u64::MAX);
        let mut best: Option<(MoveRank, PartitionPrimaryTransfer)> = None;
        self.for_each_partition(inner, |namespace, partition, report| {
            let Some(report) = report else {
                return;
            };
            let Some(source) = partition
                .primary_node(report.view, nodes)
                .filter(|source| ready(*source))
            else {
                return;
            };
            let members = partition.replica_set(nodes);
            let ready_members = members.nodes().iter().filter(|node| ready(**node)).count();
            if ready_members < quorum_view_change(members.len()) {
                return;
            }
            let Some(target) = members
                .nodes()
                .iter()
                .copied()
                .filter(|node| *node != source && ready(*node))
                .min_by_key(|node| (count(*node), *node))
            else {
                return;
            };
            let gap = count(source).saturating_sub(count(target));
            if gap <= 1 {
                return;
            }
            let key = (
                gap,
                Reverse(source),
                after.is_none_or(|cursor| namespace > cursor),
                Reverse(namespace),
            );
            if best.as_ref().is_none_or(|(best_key, _)| key > *best_key) {
                best = Some((
                    key,
                    PartitionPrimaryTransfer {
                        namespace,
                        created_revision: report.created_revision,
                        view: report.view,
                        target,
                    },
                ));
            }
        });
        best.map(|(_, plan)| plan)
    }

    fn for_each_partition(
        &self,
        inner: &StreamsInner,
        mut visit: impl FnMut(IggyNamespace, &Partition, Option<PartitionPlacementView>),
    ) {
        for (stream_id, stream) in &inner.items {
            for (topic_id, topic) in &stream.topics {
                if inner.lifecycle_blocks(stream_id, Some(topic_id)) {
                    continue;
                }
                for partition in &topic.partitions {
                    let namespace = IggyNamespace::new(stream_id, topic_id, partition.id);
                    visit(
                        namespace,
                        partition,
                        self.matching_view(namespace, partition),
                    );
                }
            }
        }
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

    /// Drops what was observed of partitions that commits removed since the
    /// last call. When the tree no longer records all of those removals, every
    /// observation is checked against the tree instead.
    fn retire_removed(&mut self, inner: &StreamsInner) {
        if self.synced_revision == Some(inner.namespace_revision) {
            return;
        }
        let removals = self
            .synced_revision
            .and_then(|synced| inner.removed_partitions.since(synced));
        if let Some(removals) = removals {
            for removed in removals {
                if self
                    .observations
                    .get(&removed.namespace)
                    .is_some_and(|observed| {
                        observed.report.created_revision == removed.created_revision
                    })
                    && let Some(observed) = self.observations.remove(&removed.namespace)
                {
                    self.track(observed, -1);
                    self.observed_generation += 1;
                }
            }
        } else {
            self.observations.retain(|&namespace, observed| {
                committed_partition(inner, namespace).is_some_and(|partition| {
                    partition.created_revision == observed.report.created_revision
                })
            });
            self.moved = None;
            self.observed_generation += 1;
        }
        self.synced_revision = Some(inner.namespace_revision);
    }

    fn moved(&mut self, replicas: u8) -> &[i64] {
        if self
            .moved
            .as_ref()
            .is_none_or(|(built_for, _)| *built_for != replicas)
        {
            let mut moved = vec![0; usize::from(replicas)];
            for observed in self.observations.values() {
                observed.shift(replicas, &mut moved, 1);
            }
            self.moved = Some((replicas, moved));
        }
        self.moved.as_ref().map_or(&[], |(_, moved)| moved)
    }

    fn track(&mut self, observed: ObservedView, by: i64) {
        if let Some((replicas, moved)) = self.moved.as_mut() {
            observed.shift(*replicas, moved, by);
        }
    }
}

impl ObservedView {
    /// Counts this partition at its observed primary instead of its created
    /// one, or back for a negative `by`.
    fn shift(self, nodes: u8, moved: &mut [i64], by: i64) {
        let observed = self.report.view.max(self.created_view);
        if let Some(count) = primary_of(self.members, observed, nodes)
            .and_then(|node| moved.get_mut(usize::from(node)))
        {
            *count += by;
        }
        if let Some(count) = primary_of(self.members, self.created_view, nodes)
            .and_then(|node| moved.get_mut(usize::from(node)))
        {
            *count -= by;
        }
    }
}

fn committed_partition(inner: &StreamsInner, namespace: IggyNamespace) -> Option<&Partition> {
    inner
        .items
        .get(namespace.stream_id())
        .and_then(|stream| stream.topics.get(namespace.topic_id()))
        .and_then(|topic| find_partition(&topic.partitions, namespace.partition_id()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::StateHandler;
    use crate::stm::lifecycle::apply_with_lifecycle_completion;
    use crate::stm::snapshot::Snapshotable;
    use crate::stm::stream::{REMOVED_PARTITIONS_RETAINED, StreamsInner};
    use iggy_binary_protocol::WireIdentifier;
    use iggy_binary_protocol::requests::partitions::DeletePartitionsRequest;
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
    fn given_creates_moves_deletes_and_recreates_when_counting_should_equal_the_full_walk() {
        const REPLICAS: u8 = 3;
        let (streams, mut placement) = observed_topics(&[0, 1, 2, 0, 1, 2]);
        let wide = IggyNamespace::new(0, 6, 4);
        streams.seed_namespace(wide, 100, 1);
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);

        let moves = [
            ((0, 0), 4),
            ((1, 0), 5),
            ((6, 1), 3),
            ((6, 3), 2),
            ((6, 4), 6),
        ];
        observe_views(&mut placement, &streams, &moves);
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);

        let recreated = IggyNamespace::new(0, 1, 0);
        let old_revision = streams.created_revision_for_namespace(recreated).unwrap();
        let streams = apply_to(
            &streams,
            &DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(1),
            },
        );
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);

        let streams = apply_to(
            &streams,
            &DeletePartitionsRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(6),
                partitions_count: 2,
            },
        );
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);

        streams.seed_namespace(recreated, 200, 2);
        placement.observe(
            &streams,
            &[PartitionPlacementView {
                namespace: recreated,
                created_revision: old_revision,
                view: 9,
            }],
        );
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);
        observe_views(
            &mut placement,
            &streams,
            &[((1, 0), 3), ((6, 2), 7), ((0, 0), 5)],
        );
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);
        assert_counts_match_walk(&mut placement, &streams, 5);
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);
    }

    #[test]
    fn given_a_restored_tree_without_the_removals_when_counting_should_equal_the_full_walk() {
        const REPLICAS: u8 = 3;
        let (streams, mut placement) = observed_topics(&[0, 1, 2, 0]);
        observe_views(&mut placement, &streams, &[((0, 0), 4), ((2, 0), 3)]);
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);

        let deleted = apply_to(
            &streams,
            &DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
            },
        );
        let restored = Streams::from_snapshot(deleted.to_snapshot()).unwrap();

        assert_counts_match_walk(&mut placement, &restored, REPLICAS);
    }

    #[test]
    fn given_more_removals_than_the_tree_keeps_when_counting_should_equal_the_full_walk() {
        const REPLICAS: u8 = 3;
        let (streams, mut placement) = observed_topics(&[0, 1]);
        let wide = IggyNamespace::new(0, 2, REMOVED_PARTITIONS_RETAINED);
        streams.seed_namespace(wide, 100, 1);
        let moves: Vec<_> = (0..=REMOVED_PARTITIONS_RETAINED)
            .map(|partition| ((2, partition), 2))
            .chain([((0, 0), 4)])
            .collect();
        observe_views(&mut placement, &streams, &moves);
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);

        let streams = apply_to(
            &streams,
            &DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(2),
            },
        );
        assert!(
            streams.read(|inner| inner.removed_partitions.since(0).is_none()),
            "the delete must outgrow what the tree keeps"
        );
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);

        let streams = apply_to(
            &streams,
            &DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
            },
        );
        assert_counts_match_walk(&mut placement, &streams, REPLICAS);
        assert_eq!(placement.observations.len(), 1);
    }

    #[test]
    fn given_unchanged_inputs_when_selecting_again_should_reuse_the_last_scan() {
        let (streams, mut placement) = observed_topics(&[0, 0, 0]);
        let first = placement.primary_rebalance_candidate(&streams, 3, &[true; 3], None);
        assert!(first.is_some());
        // A tree at the same revision cannot differ from the scanned one, so a
        // changed tree that kept its revision exposes a skipped scan.
        let mut inner = streams.read(Clone::clone);
        for topic in [1, 2] {
            inner
                .items
                .get_mut(0)
                .unwrap()
                .topics
                .get_mut(topic)
                .unwrap()
                .partitions
                .clear();
        }
        let same_revision = Streams::from(inner);

        assert_eq!(
            placement.primary_rebalance_candidate(&same_revision, 3, &[true; 3], None),
            first
        );
        assert!(
            PlacementState::default()
                .primary_rebalance_candidate(&same_revision, 3, &[true; 3], None)
                .is_none()
        );
    }

    #[test]
    fn given_a_new_lifecycle_intent_when_selecting_again_should_scan_again() {
        let (streams, mut placement) = observed_topics(&[0, 0, 1, 2]);
        assert!(
            placement
                .primary_rebalance_candidate(&streams, 3, &[true; 3], None)
                .is_none()
        );
        let mut inner = streams.read(Clone::clone);
        inner.apply_context.metadata_op = 100;
        assert_eq!(
            DeleteTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(2),
            }
            .apply(&mut inner, IggyTimestamp::from(1))
            .code,
            0
        );
        assert_eq!(
            inner.namespace_revision,
            streams.read(|inner| inner.namespace_revision),
            "a pending delete leaves the partition set alone"
        );
        let deleting = Streams::from(inner);

        let transfer = placement
            .primary_rebalance_candidate(&deleting, 3, &[true; 3], None)
            .unwrap();

        assert_eq!(transfer.target, 1);
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
        assert_eq!(
            placement.committed_counts(&streams, 5).primaries,
            [0, 40, 20, 20, 20]
        );
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
            assert_eq!(
                placement.committed_counts(&streams, 5).primaries,
                expected_counts
            );
        }
        assert_eq!(
            PlacementState::default()
                .committed_counts(&streams, 5)
                .primaries,
            [20; 5],
            "transfers must not rewrite committed initial views"
        );
    }

    #[test]
    fn given_balanced_or_almost_balanced_primaries_when_selecting_should_not_transfer() {
        for views in [&[][..], &[0, 1, 2], &[0, 1, 2, 0]] {
            let (streams, mut placement) = observed_topics(views);
            assert!(
                placement
                    .primary_rebalance_candidate(&streams, 3, &[true; 3], None)
                    .is_none()
            );
        }
    }

    #[test]
    fn given_unsettled_failover_or_no_quorum_when_selecting_should_wait() {
        let (streams, mut placement) = observed_topics(&[0, 0, 0, 1, 2]);
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
        let (streams, mut placement) = observed_topics(&[0, 0, 0, 1, 1, 1]);
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
        let (streams, mut placement) = observed_topics(&[0, 0, 0]);
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

        // A sibling history with the same revision needs its own placement.
        let (streams, mut placement) = observed_topics(&[0, 0, 0]);
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
        let (streams, mut placement) = observed_topics(&[0, 1, 1, 1]);
        let transfer = placement
            .primary_rebalance_candidate(&streams, 3, &[false, true, true], None)
            .unwrap();
        assert_eq!(transfer.namespace, IggyNamespace::new(0, 1, 0));
        assert_eq!(transfer.target, 2);
    }

    #[test]
    fn given_replication_factor_when_placing_many_partitions_should_spread_primaries_and_copies() {
        const NODES: u8 = 5;
        let placement = PlacementState::default();
        let mut counts = PlacementCounts::new(NODES);
        for _ in 0..10 {
            let (members, created_view) = placement.select(&mut counts, 0, Some(3)).unwrap();
            assert_eq!(members.len(), 3);
            assert!(members.is_sorted());
            assert!(usize::try_from(created_view).unwrap() < members.len());
        }
        assert_eq!(counts.primaries, [2; 5]);
        assert_eq!(counts.replicas.iter().sum::<u64>(), 30);
        assert!(
            counts.replicas.iter().all(|copies| copies.abs_diff(6) <= 1),
            "copies stay within one of the mean: {:?}",
            counts.replicas
        );
    }

    #[test]
    fn given_factor_outside_the_cluster_when_placing_should_refuse() {
        let placement = PlacementState::default();
        for factor in [0, 4] {
            assert_eq!(
                placement.select(&mut PlacementCounts::new(3), 0, Some(factor)),
                Err(IggyError::InvalidReplicationFactor)
            );
        }
    }

    #[test]
    fn given_a_down_node_when_placing_should_use_it_only_when_too_few_are_up() {
        let mut placement = PlacementState::default();
        placement.observe_replicas(0, &[true, false, true]);
        for _ in 0..4 {
            let mut counts = PlacementCounts::new(3);
            assert_eq!(
                placement.select(&mut counts, 0, Some(2)).unwrap().0,
                [0, 2],
                "two nodes are up"
            );
            let (members, created_view) = placement.select(&mut counts, 0, Some(3)).unwrap();
            assert_eq!(members, [0, 1, 2]);
            assert_ne!(
                members[usize::try_from(created_view).unwrap()],
                1,
                "a down node never leads"
            );
        }
    }

    #[test]
    fn given_imbalance_outside_a_group_when_rebalancing_should_move_only_to_a_member() {
        const NODES: u8 = 5;
        let streams = Streams::default();
        let mut placement = PlacementState::default();
        for topic in 0..4 {
            let namespace = IggyNamespace::new(0, topic, 0);
            streams.seed_namespace_on(
                namespace,
                u64::try_from(topic).unwrap() + 1,
                0,
                iggy_common::Durability::Persisted,
                &[0, 1, 2],
            );
            placement.observe(
                &streams,
                &[PartitionPlacementView {
                    namespace,
                    created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
                    view: 0,
                }],
            );
        }
        assert_eq!(
            placement.committed_counts(&streams, NODES),
            PlacementCounts {
                primaries: vec![4, 0, 0, 0, 0],
                replicas: vec![4, 4, 4, 0, 0],
            }
        );

        let transfer = placement
            .primary_rebalance_candidate(&streams, NODES, &[true; 5], None)
            .unwrap();
        assert_eq!(
            transfer.target, 1,
            "nodes 3 and 4 lead fewer but store nothing"
        );

        let mut group_down = [true; 5];
        group_down[1] = false;
        group_down[2] = false;
        assert_eq!(
            placement.primary_rebalance_candidate(&streams, NODES, &group_down, None),
            None,
            "one of three members cannot elect"
        );
    }

    #[test]
    fn given_the_busiest_node_has_no_lighter_member_when_rebalancing_should_move_another_group() {
        const NODES: u8 = 5;
        let streams = Streams::default();
        let mut placement = PlacementState::default();
        let layout: [(&[u8], u32); 8] = [
            (&[0, 1, 2], 0),
            (&[0, 1, 2], 0),
            (&[0, 1, 2], 0),
            (&[1, 3, 4], 0),
            (&[1, 3, 4], 0),
            (&[1, 3, 4], 0),
            (&[0, 1, 2], 2),
            (&[0, 1, 2], 2),
        ];
        for (topic, (members, view)) in layout.into_iter().enumerate() {
            let namespace = IggyNamespace::new(0, topic, 0);
            streams.seed_namespace_on(
                namespace,
                u64::try_from(topic).unwrap() + 1,
                view,
                iggy_common::Durability::Persisted,
                members,
            );
            placement.observe(
                &streams,
                &[PartitionPlacementView {
                    namespace,
                    created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
                    view,
                }],
            );
        }
        assert_eq!(
            placement.committed_counts(&streams, NODES).primaries,
            [3, 3, 2, 0, 0]
        );

        let transfer = placement
            .primary_rebalance_candidate(&streams, NODES, &[true; 5], None)
            .expect("node 1 can hand a partition to node 3");
        assert_eq!(transfer.target, 3);
        assert_eq!(
            streams
                .members_for_namespace(transfer.namespace)
                .flatten()
                .unwrap()
                .nodes(),
            &[1, 3, 4]
        );
    }

    #[test]
    fn given_readiness_absent_or_from_another_view_when_placing_should_refuse_until_this_view_reports()
     {
        let counts = || PlacementCounts {
            primaries: vec![0, 5, 5],
            replicas: vec![0; 3],
        };
        let mut placement = PlacementState::default();
        assert_eq!(
            placement.select(&mut counts(), 1, Some(2)),
            Ok((vec![0, 1], 0)),
            "without a coordinator every node counts as ready"
        );
        placement.observe_replicas(1, &[false, true, true]);
        assert_eq!(
            placement.select(&mut counts(), 1, None),
            Ok((Vec::new(), 1))
        );
        for factor in [None, Some(1), Some(2)] {
            assert_eq!(
                placement.select(&mut counts(), 2, factor),
                Err(IggyError::TransientNotAccepted),
                "evidence from an older view must not place factor {factor:?}"
            );
        }
        placement.observe_replicas(2, &[false; 3]);
        assert_eq!(
            placement.select(&mut counts(), 2, None),
            Err(IggyError::TransientNotAccepted)
        );
        placement.observe_replicas(2, &[]);
        assert_eq!(
            placement.select(&mut counts(), 2, Some(1)),
            Err(IggyError::TransientNotAccepted),
            "cleared evidence must not presume every node ready"
        );
        placement.observe_replicas(2, &[true, false, true]);
        assert_eq!(
            placement.select(&mut counts(), 2, Some(2)),
            Ok((vec![0, 2], 0))
        );
    }

    fn observe_views(
        placement: &mut PlacementState,
        streams: &Streams,
        views: &[((usize, usize), u32)],
    ) {
        for &((topic, partition), view) in views {
            let namespace = IggyNamespace::new(0, topic, partition);
            placement.observe(
                streams,
                &[PartitionPlacementView {
                    namespace,
                    created_revision: streams.created_revision_for_namespace(namespace).unwrap(),
                    view,
                }],
            );
        }
    }

    fn apply_to<R: StateHandler<State = StreamsInner>>(streams: &Streams, request: &R) -> Streams {
        let mut inner = streams.read(Clone::clone);
        assert_eq!(
            apply_with_lifecycle_completion(request, &mut inner, IggyTimestamp::from(1)).code,
            0
        );
        Streams::from(inner)
    }

    fn assert_counts_match_walk(placement: &mut PlacementState, streams: &Streams, replicas: u8) {
        let counted = placement.committed_counts(streams, replicas);
        assert_eq!(counted, walked_counts(placement, streams, replicas));
    }

    /// Every committed partition at its observed view, or its created view
    /// before an observation of its incarnation.
    fn walked_counts(
        placement: &PlacementState,
        streams: &Streams,
        replicas: u8,
    ) -> PlacementCounts {
        streams.read(|inner| {
            let mut counts = PlacementCounts::new(replicas);
            for (stream_id, stream) in &inner.items {
                for (topic_id, topic) in &stream.topics {
                    for partition in &topic.partitions {
                        let namespace = IggyNamespace::new(stream_id, topic_id, partition.id);
                        let view = placement
                            .observations
                            .get(&namespace)
                            .filter(|observed| {
                                observed.report.created_revision == partition.created_revision
                            })
                            .map_or(partition.created_view, |observed| {
                                observed.report.view.max(partition.created_view)
                            });
                        counts.primaries
                            [usize::from(partition.primary_node(view, replicas).unwrap())] += 1;
                        for node in partition.replica_set(replicas).nodes() {
                            counts.replicas[usize::from(*node)] += 1;
                        }
                    }
                }
            }
            counts
        })
    }
}
