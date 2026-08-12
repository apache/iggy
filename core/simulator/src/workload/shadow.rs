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
//! Name-keyed throughout. Server-ng does not yet ship reply bodies, so
//! the workload cannot observe server-assigned numeric ids. Lookups are
//! by name; requests route via `WireIdentifier::named(...)`. When typed
//! response bodies land, id-keyed maps return as a parallel index;
//! name-keyed `IndexSet`s stay the primary sampling source.
//!
//! `IndexSet` for state we sample randomly (O(1) `get_index`).
//! `HashMap` for pure-lookup state.

use indexmap::IndexSet;
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use std::collections::HashMap;

use server_common::sharding::IggyNamespace;

use crate::workload::effect::{ApplyResult, Effect};
use crate::workload::ids::IdPermutation;

pub struct Shadow {
    pub namespaces_live: IndexSet<IggyNamespace>,

    /// Live streams by name. `CreateStream` inserts; `DeleteStream` removes.
    ///
    /// TODO: name sets to `IndexSet<Arc<str>>` for long-running runs;
    /// bounded today by `tick_budget`.
    pub stream_names: IndexSet<String>,
    /// Live topics by `(stream, topic)`. Only added if parent stream lives.
    pub topic_names: IndexSet<(String, String)>,
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
}

impl Shadow {
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
        Self {
            namespaces_live,
            stream_names: IndexSet::new(),
            topic_names: IndexSet::new(),
            user_names: IndexSet::new(),
            pat_names: IndexSet::new(),
            consumer_group_names: IndexSet::new(),
            passwords: HashMap::new(),
            sends_committed: HashMap::new(),
            consumer_offsets: HashMap::new(),
            id_permutation,
            next_index: 1,
        }
    }

    /// Pick a live namespace uniformly. `IndexSet` preserves insertion
    /// order, so for deduplicated input this matches `Vec::get(i)`.
    pub fn pick_namespace(&self, prng: &mut Xoshiro256Plus) -> Option<IggyNamespace> {
        let n = self.namespaces_live.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.namespaces_live.get_index(i).copied()
    }

    pub fn pick_stream_name(&self, prng: &mut Xoshiro256Plus) -> Option<String> {
        let n = self.stream_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.stream_names.get_index(i).cloned()
    }

    pub fn pick_topic_pair(&self, prng: &mut Xoshiro256Plus) -> Option<(String, String)> {
        let n = self.topic_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.topic_names.get_index(i).cloned()
    }

    pub fn pick_user_name(&self, prng: &mut Xoshiro256Plus) -> Option<String> {
        let n = self.user_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.user_names.get_index(i).cloned()
    }

    pub fn pick_pat_name(&self, prng: &mut Xoshiro256Plus) -> Option<String> {
        let n = self.pat_names.len();
        if n == 0 {
            return None;
        }
        let i = prng.random_range(0..n);
        self.pat_names.get_index(i).cloned()
    }

    pub fn pick_consumer_group_triple(
        &self,
        prng: &mut Xoshiro256Plus,
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

    /// Apply a predicted effect. Returns [`SimCommand`]s for the driver
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
            Effect::RemoveStream { name } => {
                let removed = self.stream_names.shift_remove(&name);
                self.topic_names.retain(|(s, _)| s != &name);
                self.consumer_group_names.retain(|(s, _, _)| s != &name);
                removed
            }
            Effect::AddTopic {
                stream,
                name,
                partitions: _,
            } => {
                if self.stream_names.contains(&stream) {
                    self.topic_names.insert((stream, name))
                } else {
                    false
                }
            }
            Effect::RemoveTopic { stream, name } => {
                let removed = self
                    .topic_names
                    .shift_remove(&(stream.clone(), name.clone()));
                self.consumer_group_names
                    .retain(|(s, t, _)| !(s == &stream && t == &name));
                removed
            }
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
