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

//! Shadow state: the workload's prediction of server-side entity state.
//!
//! Name-keyed throughout. Server does not yet ship reply bodies, so
//! the workload cannot observe server-assigned numeric ids. Lookups are
//! by name; requests route via `WireIdentifier::named(...)`. When typed
//! response bodies land, id-keyed maps return as a parallel index;
//! name-keyed `IndexSet`s stay the primary sampling source.
//!
//! `IndexSet` for state we sample randomly (O(1) `get_index`).
//! `HashMap` for pure-lookup state.

use iggy_binary_protocol::primitives::partition_history::{ConsumerGroupOwner, PartitionContext};
use indexmap::IndexSet;
use metadata::stm::stream::Streams;
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use std::collections::{BTreeMap, HashMap};

use server_common::sharding::IggyNamespace;

use crate::workload::effect::{ApplyResult, Effect};
use crate::workload::ids::IdPermutation;

/// Name of the consumer group a shell run keeps on every fixture topic. Outside
/// the `wl-` prefix, so the entity oracle leaves it out.
pub const FIXTURE_GROUP_NAME: &str = "sim-fixture-group";

/// Id of the fixture group: the first group of its topic, and every topic
/// incarnation numbers its groups from 0.
pub const FIXTURE_GROUP_ID: u32 = 0;

#[derive(Debug, Clone, Copy)]
pub struct CapturedOwner {
    pub namespace: IggyNamespace,
    pub owner: ConsumerGroupOwner,
    pub context: PartitionContext,
}

struct ObservedOwner {
    captured: CapturedOwner,
    current: ConsumerGroupOwner,
}

/// The half of a fixture replacement one request performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Toggle {
    Delete,
    Create { partitions_count: u32 },
}

/// A sampled join of a fixture group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupJoin {
    /// Index of the fixture topic in the shadow.
    pub fixture: usize,
    /// Create the group that a replacement dropped with its topic, instead of
    /// joining it.
    pub create: bool,
    /// Replacements of the topic started at sample time. A delete drops the
    /// group with its topic, so a reply proves the current group only if no
    /// replacement started since.
    pub started: u64,
}

/// What the reply to a fixture toggle or a fixture group join proves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToggleFate {
    /// The request reached its goal at metadata op `op`: the topic is gone or
    /// present, the client is a member, or the group exists.
    Reached { op: u64 },
    /// The request did not take effect.
    Refused,
    /// The request may have taken effect.
    Unknown,
}

/// Replacement counts of one fixture topic, shared by its namespaces.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Replacements {
    /// Deletes sampled, less those refused before they reached the server.
    pub started: u64,
    /// Recreated topics whose successful create has been observed.
    pub created: u64,
    /// Replacements over: every live replica hosts the recreated incarnation.
    pub settled: u64,
}

impl Replacements {
    /// Whether a request that recorded `sampled` may find its history replaced.
    #[must_use]
    pub const fn may_replace(self, sampled: Self) -> bool {
        self.started > sampled.settled
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixtureState {
    Live,
    /// A delete is in flight. A probe asks again after a delete of unknown fate, so
    /// its refusal proves nothing.
    Deleting {
        probe: bool,
    },
    DeleteUnknown,
    Deleted,
    Creating {
        probe: bool,
    },
    CreateUnknown,
    /// The recreated topic exists since metadata op `op`. A replica may still
    /// host the fenced incarnation, so a request can still meet it.
    Settling {
        op: u64,
    },
}

impl FixtureState {
    /// The state a reply moves an in-flight toggle to, or `None` with no toggle in
    /// flight. A probe resolves only on `Reached`: its delete refused under the
    /// pending one, or its create refused, says nothing about the first toggle.
    const fn after(self, fate: ToggleFate) -> Option<Self> {
        Some(match (self, fate) {
            (Self::Deleting { .. }, ToggleFate::Reached { .. })
            | (Self::Creating { probe: false }, ToggleFate::Refused) => Self::Deleted,
            (Self::Deleting { probe: false }, ToggleFate::Refused) => Self::Live,
            (Self::Deleting { .. }, _) => Self::DeleteUnknown,
            (Self::Creating { .. }, ToggleFate::Reached { op }) => Self::Settling { op },
            (Self::Creating { .. }, _) => Self::CreateUnknown,
            _ => return None,
        })
    }
}

/// The fixture group of a topic, which a shell run joins and leaves.
#[derive(Debug, Default)]
struct FixtureGroup {
    /// Whether the group exists in the current incarnation of its topic. A topic
    /// delete drops it, and the next join creates it again.
    exists: bool,
    /// Whether a join committed on the group of the current incarnation, which
    /// gives the group the assignments that the owner oracle compares.
    joined: bool,
    /// Each client whose join may have committed, with the metadata op of its
    /// newest such join. `None` when the fate of a join is unknown, which can
    /// still commit on any later incarnation.
    ///
    /// Membership history only: until its successor installs, an owner that left
    /// can still commit. The partition state checker validates the ordered install
    /// barrier separately, so this history does not authorize a stale owner.
    joined_at: HashMap<u128, Option<u64>>,
    /// The metadata ops at which the deletes of the topic were reached, oldest
    /// first. A delete drops the group with its members, so a join counts only on
    /// the incarnation after the deletes reached before its op.
    deletes_reached: Vec<u64>,
}

impl FixtureGroup {
    /// Whether a join committed at metadata op `op` belongs to an incarnation
    /// that a request which recorded `sampled` can reach: one no older than the
    /// last replacement the request saw settle.
    fn joined_since(&self, op: u64, sampled: Replacements) -> bool {
        let incarnation = self
            .deletes_reached
            .partition_point(|&deleted| deleted < op);
        u64::try_from(incarnation).is_ok_and(|incarnation| incarnation >= sampled.settled)
    }
}

/// A seeded topic holding fixture namespaces, which the workload can delete and
/// recreate under its seeded names. Ids are reused, so its namespaces keep their
/// values under a new incarnation.
#[derive(Debug)]
struct FixtureTopic {
    stream: String,
    topic: String,
    namespaces: Vec<IggyNamespace>,
    partitions_count: u32,
    state: FixtureState,
    replacements: Replacements,
    /// `None` unless the run created fixture groups.
    group: Option<FixtureGroup>,
}

impl FixtureTopic {
    /// The toggle this topic takes next and the state it enters. `None` while one
    /// is in flight or settling, and for a live topic when `deletes_open` is false.
    const fn next_toggle(&self, deletes_open: bool) -> Option<(FixtureState, Toggle)> {
        let create = Toggle::Create {
            partitions_count: self.partitions_count,
        };
        match self.state {
            FixtureState::Live if deletes_open => {
                Some((FixtureState::Deleting { probe: false }, Toggle::Delete))
            }
            FixtureState::DeleteUnknown => {
                Some((FixtureState::Deleting { probe: true }, Toggle::Delete))
            }
            FixtureState::Deleted => Some((FixtureState::Creating { probe: false }, create)),
            FixtureState::CreateUnknown => Some((FixtureState::Creating { probe: true }, create)),
            _ => None,
        }
    }
}

pub struct Shadow {
    pub namespaces_live: IndexSet<IggyNamespace>,

    /// Live streams by name. `CreateStream` inserts; `DeleteStream` removes.
    ///
    /// TODO: name sets to `IndexSet<Arc<str>>` for long-running runs;
    /// bounded today by `tick_budget`.
    pub stream_names: IndexSet<String>,
    /// Live topics by `(stream, topic)`. Only added if parent stream lives.
    pub topic_names: IndexSet<(String, String)>,
    /// Partition count per live topic, keyed like `topic_names`. Lets
    /// `create/delete_partitions` sample in-bounds vs over-count deliberately
    /// (the server rejects an over-count delete with a committed
    /// `InvalidPartitionsCount`, so the count must be known at sample time).
    pub topic_partitions: HashMap<(String, String), u32>,
    pub user_names: IndexSet<String>,
    pub pat_names: IndexSet<String>,
    pub consumer_group_names: IndexSet<(String, String, String)>,

    /// Per-user current password. `AddUser` seeds `pw-{user}`,
    /// `PasswordChanged` rotates, `RenameUser` re-keys, `RemoveUser`
    /// drops. Lets `change_password::sample` track `current_password`
    /// across rotations.
    pub passwords: HashMap<String, String>,

    pub sends_committed: HashMap<IggyNamespace, u64>,
    pub consumer_offsets: HashMap<(IggyNamespace, u8, u32), u64>,

    /// Id permutation for fabricated-id paths (`Outcome::ResourceNotFound`).
    /// Currently unused.
    pub id_permutation: IdPermutation,
    next_index: u64,

    fixtures: Vec<FixtureTopic>,
    /// Cleared before quiesce, so every fixture topic ends up hosted again.
    fixture_deletes_open: bool,
    /// Namespaces of the fixture topics whose delete was reached since the last
    /// [`Self::take_deleted_namespaces`].
    deleted_namespaces: Vec<IggyNamespace>,
    /// One original token per fixture/client; only input selection reads these
    /// observations. The independent committed-log oracle validates effects.
    captured_owners: BTreeMap<(IggyNamespace, u128), ObservedOwner>,
}

impl Shadow {
    /// A shadow whose live namespaces are the fixture `namespaces`.
    ///
    /// # Panics
    /// If a fixture partition id does not fit `u32`.
    #[must_use]
    pub fn new(namespaces: Vec<IggyNamespace>, id_permutation: IdPermutation) -> Self {
        let input_len = namespaces.len();
        let namespaces_live: IndexSet<IggyNamespace> = namespaces.into_iter().collect();
        // IndexSet::collect silently dedups; catch typo'd fixtures.
        debug_assert_eq!(
            input_len,
            namespaces_live.len(),
            "Shadow::new: duplicate namespaces in input",
        );
        let mut fixtures: Vec<FixtureTopic> = Vec::new();
        for &namespace in &namespaces_live {
            let partitions_count =
                u32::try_from(namespace.partition_id() + 1).expect("fixture partition id fits u32");
            let same_topic = |fixture: &&mut FixtureTopic| {
                fixture.namespaces.first().is_some_and(|first| {
                    first.stream_id() == namespace.stream_id()
                        && first.topic_id() == namespace.topic_id()
                })
            };
            // The seed creates every partition below the addressed one.
            if let Some(fixture) = fixtures.iter_mut().find(same_topic) {
                fixture.namespaces.push(namespace);
                fixture.partitions_count = fixture.partitions_count.max(partitions_count);
                continue;
            }
            fixtures.push(FixtureTopic {
                stream: Streams::seeded_stream_name(namespace.stream_id()),
                topic: Streams::seeded_topic_name(namespace.stream_id(), namespace.topic_id()),
                namespaces: vec![namespace],
                partitions_count,
                state: FixtureState::Live,
                replacements: Replacements::default(),
                group: None,
            });
        }
        Self {
            namespaces_live,
            stream_names: IndexSet::new(),
            topic_names: IndexSet::new(),
            topic_partitions: HashMap::new(),
            user_names: IndexSet::new(),
            pat_names: IndexSet::new(),
            consumer_group_names: IndexSet::new(),
            passwords: HashMap::new(),
            sends_committed: HashMap::new(),
            consumer_offsets: HashMap::new(),
            id_permutation,
            next_index: 1,
            fixtures,
            fixture_deletes_open: true,
            deleted_namespaces: Vec::new(),
            captured_owners: BTreeMap::new(),
        }
    }

    pub(crate) fn observe_owner(
        &mut self,
        namespace: IggyNamespace,
        owner: ConsumerGroupOwner,
        context: PartitionContext,
    ) {
        self.captured_owners.retain(|&(ns, _), observed| {
            ns != namespace || observed.captured.context.incarnation == context.incarnation
        });
        for (&(ns, _), observed) in &mut self.captured_owners {
            if ns == namespace {
                observed.current = owner;
            }
        }
        if !owner.is_unassigned() {
            self.captured_owners
                .entry((namespace, owner.client_id))
                .or_insert(ObservedOwner {
                    captured: CapturedOwner {
                        namespace,
                        owner,
                        context,
                    },
                    current: owner,
                });
        }
    }

    pub(crate) fn pick_stale_owner(&self, prng: &mut Xoshiro256PlusPlus) -> Option<CapturedOwner> {
        let stale = || {
            self.captured_owners.values().filter(|observed| {
                observed.current.generation > observed.captured.owner.generation
                    && self.namespaces_live.contains(&observed.captured.namespace)
            })
        };
        let count = stale().count();
        if count == 0 {
            return None;
        }
        stale()
            .nth(prng.random_range(0..count))
            .map(|observed| observed.captured)
    }

    /// Start a toggle of a fixture topic picked uniformly among those that can
    /// take one. A delete starts a replacement.
    ///
    /// After a toggle of unknown fate, the next toggle of its topic is a probe: the
    /// same half again, whose reply tells what the first one did.
    ///
    /// Topic slabs are reused last-freed-first, so a delete starts only while every
    /// fixture topic is live: with one topic away at a time, each recreated topic
    /// lands on its own ids.
    pub fn begin_fixture_toggle(
        &mut self,
        prng: &mut Xoshiro256PlusPlus,
    ) -> Option<(usize, Toggle)> {
        let deletes_open = self.fixture_deletes_open
            && self
                .fixtures
                .iter()
                .all(|fixture| fixture.state == FixtureState::Live);
        let candidates: Vec<usize> = self
            .fixtures
            .iter()
            .enumerate()
            .filter(|(_, fixture)| fixture.next_toggle(deletes_open).is_some())
            .map(|(index, _)| index)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let index = candidates[prng.random_range(0..candidates.len())];
        let fixture = &mut self.fixtures[index];
        let (state, toggle) = fixture.next_toggle(deletes_open)?;
        if fixture.state == FixtureState::Live {
            fixture.replacements.started += 1;
        }
        fixture.state = state;
        Some((index, toggle))
    }

    /// Whether the toggle in flight on fixture topic `index` is a probe.
    #[must_use]
    pub fn fixture_probing(&self, index: usize) -> bool {
        matches!(
            self.fixtures[index].state,
            FixtureState::Deleting { probe: true } | FixtureState::Creating { probe: true }
        )
    }

    /// The seeded stream and topic names of fixture topic `index`.
    #[must_use]
    pub fn fixture_names(&self, index: usize) -> (String, String) {
        let fixture = &self.fixtures[index];
        (fixture.stream.clone(), fixture.topic.clone())
    }

    /// Move fixture topic `index` by what its toggle's reply proved.
    ///
    /// # Panics
    /// If the topic has no toggle in flight: every toggle finishes exactly once.
    pub fn finish_fixture_toggle(&mut self, index: usize, fate: ToggleFate) {
        let previous = self.fixtures[index].state;
        let Some(next) = previous.after(fate) else {
            panic!(
                "fixture topic {} finished a toggle with {fate:?} while {previous:?}",
                self.fixtures[index].topic,
            );
        };
        self.fixtures[index].state = next;
        let namespaces = &self.fixtures[index].namespaces;
        match next {
            // A refused delete replaced nothing. Counted as a settle instead, it
            // would leave a request sampled during it free to see a replacement.
            FixtureState::Live => self.fixtures[index].replacements.started -= 1,
            FixtureState::Deleted if matches!(previous, FixtureState::Deleting { .. }) => {
                self.namespaces_live
                    .retain(|namespace| !namespaces.contains(namespace));
                self.sends_committed
                    .retain(|namespace, _| !namespaces.contains(namespace));
                self.consumer_offsets
                    .retain(|(namespace, _, _), _| !namespaces.contains(namespace));
                self.captured_owners
                    .retain(|(namespace, _), _| !namespaces.contains(namespace));
                self.deleted_namespaces.extend(namespaces);
                if let Some(group) = &mut self.fixtures[index].group {
                    group.exists = false;
                    group.joined = false;
                    if let ToggleFate::Reached { op } = fate {
                        group.deletes_reached.push(op);
                    }
                }
            }
            FixtureState::Settling { .. } => {
                self.namespaces_live.extend(namespaces);
                self.fixtures[index].replacements.created += 1;
            }
            _ => {}
        }
    }

    /// Settle every fixture topic that `hosted(op, namespaces)` reports as applied
    /// at its create op and hosted at the recreated incarnation.
    pub fn settle_fixtures(&mut self, mut hosted: impl FnMut(u64, &[IggyNamespace]) -> bool) {
        for fixture in &mut self.fixtures {
            if let FixtureState::Settling { op } = fixture.state
                && hosted(op, &fixture.namespaces)
            {
                fixture.state = FixtureState::Live;
                fixture.replacements.settled += 1;
            }
        }
    }

    /// Take the namespaces of the fixture topics whose delete was reached since
    /// the last call. Every replica hosts the deleted incarnation until the topic
    /// is recreated.
    pub fn take_deleted_namespaces(&mut self) -> Vec<IggyNamespace> {
        std::mem::take(&mut self.deleted_namespaces)
    }

    /// Start no more fixture deletes, so the toggles left recreate every topic.
    pub const fn close_fixture_deletes(&mut self) {
        self.fixture_deletes_open = false;
    }

    /// Whether every fixture topic is live with no toggle in flight, and its
    /// fixture group, if the run has one, was joined since the topic was last
    /// replaced.
    #[must_use]
    pub fn fixtures_restored(&self) -> bool {
        self.fixtures.iter().all(|fixture| {
            fixture.state == FixtureState::Live
                && fixture.group.as_ref().is_none_or(|group| group.joined)
        })
    }

    /// Replacement counts of the fixture topic holding `namespace`, zero for any
    /// other namespace.
    #[must_use]
    pub fn replacements(&self, namespace: IggyNamespace) -> Replacements {
        self.fixtures
            .iter()
            .find(|fixture| fixture.namespaces.contains(&namespace))
            .map_or_else(Replacements::default, |fixture| fixture.replacements)
    }

    /// A pending delete can still refuse, so retain committed effects until its
    /// success clears them. After a successful delete, late replies belong to the
    /// old history once the replacement has been created, including while it settles.
    #[must_use]
    pub fn effect_is_current(&self, namespace: IggyNamespace, sampled: Replacements) -> bool {
        self.fixtures
            .iter()
            .find(|fixture| fixture.namespaces.contains(&namespace))
            .is_none_or(|fixture| {
                sampled.created == fixture.replacements.created
                    && matches!(
                        fixture.state,
                        FixtureState::Live
                            | FixtureState::Deleting { .. }
                            | FixtureState::DeleteUnknown
                            | FixtureState::Settling { .. }
                    )
            })
    }

    /// Replacement counts summed over every fixture topic.
    #[must_use]
    pub fn replacement_totals(&self) -> Replacements {
        self.fixtures
            .iter()
            .fold(Replacements::default(), |total, fixture| Replacements {
                started: total.started + fixture.replacements.started,
                created: total.created + fixture.replacements.created,
                settled: total.settled + fixture.replacements.settled,
            })
    }

    /// Give every fixture topic its fixture group, and return the stream and topic
    /// each group is created under.
    pub fn enable_fixture_groups(&mut self) -> Vec<(String, String)> {
        self.fixtures
            .iter_mut()
            .map(|fixture| {
                fixture.group = Some(FixtureGroup {
                    exists: true,
                    ..FixtureGroup::default()
                });
                (fixture.stream.clone(), fixture.topic.clone())
            })
            .collect()
    }

    /// A fixture topic with a fixture group and no toggle in flight, picked
    /// uniformly among those whose group exists when `existing` is set. A settling
    /// topic is already in metadata, where its group lives.
    pub fn pick_fixture_group(
        &self,
        prng: &mut Xoshiro256PlusPlus,
        existing: bool,
    ) -> Option<usize> {
        let candidates: Vec<usize> = self
            .fixtures
            .iter()
            .enumerate()
            .filter(|(_, fixture)| {
                matches!(
                    fixture.state,
                    FixtureState::Live | FixtureState::Settling { .. }
                ) && fixture
                    .group
                    .as_ref()
                    .is_some_and(|group| group.exists || !existing)
            })
            .map(|(index, _)| index)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        Some(candidates[prng.random_range(0..candidates.len())])
    }

    /// Sample a join of a fixture group picked by [`Self::pick_fixture_group`], or
    /// a create of one that a replacement dropped with its topic.
    pub fn begin_group_join(&self, prng: &mut Xoshiro256PlusPlus) -> Option<GroupJoin> {
        let fixture = self.pick_fixture_group(prng, false)?;
        let topic = &self.fixtures[fixture];
        Some(GroupJoin {
            fixture,
            create: !topic.group.as_ref()?.exists,
            started: topic.replacements.started,
        })
    }

    /// Record what the reply to `join` from `client` proved.
    pub fn finish_group_join(&mut self, join: GroupJoin, client: u128, fate: ToggleFate) {
        let fixture = &mut self.fixtures[join.fixture];
        let started = fixture.replacements.started;
        let Some(group) = fixture.group.as_mut() else {
            return;
        };
        let current = matches!(fate, ToggleFate::Reached { .. }) && join.started == started;
        if join.create {
            group.exists |= current;
            return;
        }
        let joined_at = match fate {
            ToggleFate::Reached { op } => Some(op),
            ToggleFate::Unknown => None,
            ToggleFate::Refused => return,
        };
        let known = group.joined_at.entry(client).or_insert(joined_at);
        // `None` has no end, so it absorbs every op.
        *known = known.zip(joined_at).map(|(known, op)| known.max(op));
        group.joined |= current;
    }

    /// Whether `client` may have joined consumer group `group_id` of the fixture
    /// topic holding `namespace`, on an incarnation that a request which recorded
    /// `sampled` can reach by its reply. A fixture topic holds no other group.
    #[must_use]
    pub fn may_have_joined(
        &self,
        namespace: IggyNamespace,
        group_id: u32,
        client: u128,
        sampled: Replacements,
    ) -> bool {
        group_id == FIXTURE_GROUP_ID
            && self.fixtures.iter().any(|fixture| {
                fixture.namespaces.contains(&namespace)
                    && fixture.group.as_ref().is_some_and(|group| {
                        group.joined_at.get(&client).is_some_and(|joined_at| {
                            joined_at.is_none_or(|op| group.joined_since(op, sampled))
                        })
                    })
            })
    }

    /// Pick a live namespace uniformly. `IndexSet` preserves insertion
    /// order, so for deduplicated input this matches `Vec::get(i)`.
    pub fn pick_namespace(&self, prng: &mut Xoshiro256PlusPlus) -> Option<IggyNamespace> {
        let n = self.namespaces_live.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.namespaces_live.get_index(i).copied()
    }

    pub fn pick_stream_name(&self, prng: &mut Xoshiro256PlusPlus) -> Option<String> {
        let n = self.stream_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.stream_names.get_index(i).cloned()
    }

    pub fn pick_topic_pair(&self, prng: &mut Xoshiro256PlusPlus) -> Option<(String, String)> {
        let n = self.topic_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.topic_names.get_index(i).cloned()
    }

    pub fn pick_user_name(&self, prng: &mut Xoshiro256PlusPlus) -> Option<String> {
        let n = self.user_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.user_names.get_index(i).cloned()
    }

    pub fn pick_pat_name(&self, prng: &mut Xoshiro256PlusPlus) -> Option<String> {
        let n = self.pat_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.pat_names.get_index(i).cloned()
    }

    pub fn pick_consumer_group_triple(
        &self,
        prng: &mut Xoshiro256PlusPlus,
    ) -> Option<(String, String, String)> {
        let n = self.consumer_group_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.consumer_group_names.get_index(i).cloned()
    }

    /// Fresh monotonically increasing stream name. Uses the raw counter;
    /// will route through `id_permutation` once `ResourceNotFound` is wired.
    pub fn fresh_stream_name(&mut self) -> String {
        self.fresh_name("stream")
    }

    /// Fresh prefixed entity name. `next_index` is shared across entity
    /// kinds so names stay distinct regardless of which op claims them.
    pub fn fresh_name(&mut self, prefix: &str) -> String {
        let index = self.next_index;
        self.next_index += 1;
        format!("wl-{prefix}-{index:08x}")
    }

    /// A unique name guaranteed absent from the shadow, hence from the server: it
    /// draws a brand-new index no op has been issued, so nothing created it. Used
    /// to target `*NotFound` outcomes. Race-stable even across concurrent clients
    /// (fresh indices are never reused), so the targeted rejection is deterministic.
    pub fn fabricate_absent_name(&mut self, prefix: &str) -> String {
        self.fresh_name(prefix)
    }

    /// Apply a predicted effect. Returns [`SimCommand`](crate::workload::effect::SimCommand)s for the driver
    /// plus an `applied` flag gating `auditor.note_committed`.
    ///
    /// `applied = false` when a precondition no longer holds (parent
    /// gone, `Rename*` `old` gone). Reachable under multi-client
    /// interleave: another commit can defeat sample-time preconditions.
    ///
    /// Insertion order is part of the determinism contract
    /// (`pick_*_name` samples by index). Cascade deletes use
    /// `IndexSet::retain` (O(n), order preserved). Cascade renames
    /// rebuild via `into_iter().map().collect()` (O(n), order
    /// preserved). The outer rename target does `shift_remove(old)` +
    /// `insert(new)`, moving the entry to the tail; same op sequence
    /// replays to the same tail.
    pub fn apply(&mut self, e: Effect) -> ApplyResult {
        let sim_commands = Vec::new();
        let applied = match e {
            Effect::None => true,
            Effect::AddStream { name } => self.stream_names.insert(name),
            Effect::RemoveStream { name } => self.remove_stream(&name),
            Effect::AddTopic {
                stream,
                name,
                partitions,
            } => self.add_topic(stream, name, partitions),
            Effect::RemoveTopic { stream, name } => self.remove_topic(&stream, &name),
            Effect::AddPartitions {
                stream,
                topic,
                count,
            } => self.add_partitions(stream, topic, count),
            Effect::RemovePartitions {
                stream,
                topic,
                count,
            } => self.remove_partitions(stream, topic, count),
            Effect::AddUser { name } => {
                // Matches `create_user::sample`'s pw-{name} baseline.
                let password = format!("pw-{name}");
                let inserted = self.user_names.insert(name.clone());
                self.passwords.insert(name, password);
                inserted
            }
            Effect::RemoveUser { name } => {
                self.passwords.remove(&name);
                self.user_names.shift_remove(&name)
            }
            Effect::AddPat { name } => self.pat_names.insert(name),
            Effect::RemovePat { name } => self.pat_names.shift_remove(&name),
            Effect::AddConsumerGroup {
                stream,
                topic,
                name,
            } => {
                if self.topic_names.contains(&(stream.clone(), topic.clone())) {
                    self.consumer_group_names.insert((stream, topic, name))
                } else {
                    false
                }
            }
            Effect::RemoveConsumerGroup {
                stream,
                topic,
                name,
            } => self
                .consumer_group_names
                .shift_remove(&(stream, topic, name)),
            Effect::SendCommitted { ns, count } => {
                *self.sends_committed.entry(ns).or_insert(0) += count;
                true
            }
            Effect::OffsetStored { key, value } => {
                self.consumer_offsets.insert(key, value);
                true
            }
            Effect::OffsetDeleted { key } => {
                self.consumer_offsets.remove(&key);
                true
            }
            Effect::RenameStream { old, new } => self.rename_stream(&old, &new),
            Effect::RenameTopic { stream, old, new } => self.rename_topic(&stream, &old, &new),
            Effect::RenameUser { old, new, password } => {
                if self.user_names.shift_remove(&old) {
                    self.user_names.insert(new.clone());
                    self.passwords.remove(&old);
                    self.passwords.insert(new, password);
                    true
                } else {
                    false
                }
            }
            Effect::PasswordChanged { user, new_password } => {
                if self.user_names.contains(&user) {
                    self.passwords.insert(user, new_password);
                    true
                } else {
                    false
                }
            }
        };
        ApplyResult {
            sim_commands,
            applied,
        }
    }

    fn remove_stream(&mut self, name: &str) -> bool {
        let removed = self.stream_names.shift_remove(name);
        self.topic_names.retain(|(s, _)| s != name);
        self.topic_partitions.retain(|(s, _), _| s != name);
        self.consumer_group_names.retain(|(s, _, _)| s != name);
        removed
    }

    fn add_topic(&mut self, stream: String, name: String, partitions: u32) -> bool {
        if self.stream_names.contains(&stream) {
            self.topic_partitions
                .insert((stream.clone(), name.clone()), partitions);
            self.topic_names.insert((stream, name))
        } else {
            false
        }
    }

    fn remove_topic(&mut self, stream: &str, name: &str) -> bool {
        let removed = self
            .topic_names
            .shift_remove(&(stream.to_string(), name.to_string()));
        self.topic_partitions
            .remove(&(stream.to_string(), name.to_string()));
        self.consumer_group_names
            .retain(|(s, t, _)| !(s == stream && t == name));
        removed
    }

    fn add_partitions(&mut self, stream: String, topic: String, count: u32) -> bool {
        self.topic_partitions
            .get_mut(&(stream, topic))
            .is_some_and(|partitions| {
                *partitions = partitions.saturating_add(count);
                true
            })
    }

    /// The committed delete was in bounds on the server; a shadow count below
    /// it means a concurrent commit defeated the sample-time precondition,
    /// same as the other `applied = false` paths.
    fn remove_partitions(&mut self, stream: String, topic: String, count: u32) -> bool {
        self.topic_partitions
            .get_mut(&(stream, topic))
            .is_some_and(|partitions| {
                if *partitions >= count {
                    *partitions -= count;
                    true
                } else {
                    false
                }
            })
    }

    fn rename_stream(&mut self, old: &str, new: &str) -> bool {
        if !self.stream_names.shift_remove(old) {
            return false;
        }
        self.stream_names.insert(new.to_string());
        // O(N) rebuild per cascade (was O(N*M) via shift_remove loop).
        // IndexSet::into_iter -> collect preserves insertion order.
        let new_owned = new.to_string();
        self.topic_names = std::mem::take(&mut self.topic_names)
            .into_iter()
            .map(|(s, t)| {
                if s == old {
                    (new_owned.clone(), t)
                } else {
                    (s, t)
                }
            })
            .collect();
        self.topic_partitions = std::mem::take(&mut self.topic_partitions)
            .into_iter()
            .map(|((s, t), partitions)| {
                if s == old {
                    ((new_owned.clone(), t), partitions)
                } else {
                    ((s, t), partitions)
                }
            })
            .collect();
        self.consumer_group_names = std::mem::take(&mut self.consumer_group_names)
            .into_iter()
            .map(|(s, t, g)| {
                if s == old {
                    (new_owned.clone(), t, g)
                } else {
                    (s, t, g)
                }
            })
            .collect();
        true
    }

    fn rename_topic(&mut self, stream: &str, old: &str, new: &str) -> bool {
        if !self
            .topic_names
            .shift_remove(&(stream.to_string(), old.to_string()))
        {
            return false;
        }
        self.topic_names
            .insert((stream.to_string(), new.to_string()));
        if let Some(partitions) = self
            .topic_partitions
            .remove(&(stream.to_string(), old.to_string()))
        {
            self.topic_partitions
                .insert((stream.to_string(), new.to_string()), partitions);
        }
        let new_owned = new.to_string();
        self.consumer_group_names = std::mem::take(&mut self.consumer_group_names)
            .into_iter()
            .map(|(s, t, g)| {
                if s == stream && t == old {
                    (s, new_owned.clone(), g)
                } else {
                    (s, t, g)
                }
            })
            .collect();
        true
    }

    /// Current password for `user`; `None` if unknown. Use instead of
    /// reconstructing `pw-{user}` (only correct pre-rotation).
    #[must_use]
    pub fn password_for(&self, user: &str) -> Option<&str> {
        self.passwords.get(user).map(String::as_str)
    }

    #[must_use]
    pub fn sends_committed(&self, ns: IggyNamespace) -> u64 {
        self.sends_committed.get(&ns).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn consumer_offset(
        &self,
        ns: IggyNamespace,
        consumer_kind: u8,
        consumer_id: u32,
    ) -> Option<u64> {
        self.consumer_offsets
            .get(&(ns, consumer_kind, consumer_id))
            .copied()
    }
}
