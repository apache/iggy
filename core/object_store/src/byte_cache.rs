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

//! A cache of bytes read from the store, evicted by S3-FIFO (Yang, Zhang,
//! Qiu, Yue and Rashmi, SOSP 2023).
//!
//! Bytes enter a small queue. An entry that leaves the small queue without
//! readers having come back to it is evicted, and the cache remembers its
//! key for a while. An entry that readers came back to moves to the main
//! queue, and so does one that returns while its key is remembered. The main
//! queue takes a tenth less than the whole budget and gives an entry one
//! more round for every time readers came back to it.
//!
//! So one pass over a long history only ever turns the small queue over. It
//! puts nothing into the main queue, and the main queue evicts only to take
//! an entry in. What many consumers share stays there however much is read
//! once beside it.
//!
//! Sizes are in bytes, not entries, because the buffers differ in size.
//!
//! What readahead fetched waits for its reader in a queue of its own, which
//! takes up to an eighth of the budget. In the small queue it would have to
//! outlast everything that is read once meanwhile, and several readers'
//! readahead would push each other out before any of them arrived. Its first
//! read moves it to the small queue as the read that fetched it, or every
//! sequential reader would look like one that came back. Readahead makes room
//! only in its own queue and the small queue, so it never evicts what readers
//! came back to, nor what this node holds.
//!
//! Bytes this node stored itself can be kept apart from both queues until
//! their owner releases them, see [`Admission::Held`]. They take at most all
//! but an eighth of the budget, see [`UNHELD_SHARE`].
//!
//! An entry can name its owner, and an owner can have a limit: the most its
//! entries hold of the budget, see [`ByteCache::set_limit`]. A limit reserves
//! nothing. An owner over its limit evicts its own entries, by the rules
//! above applied to its entries alone: what it fetched ahead takes an eighth
//! of its limit, its small queue a tenth, and it remembers the keys of as
//! many bytes as its main queue may hold. A full budget evicts as without
//! limits, whoever owns what leaves, and an insert never evicts another
//! owner's entry while the budget has room for it, but for what readahead
//! fetched past its share of the budget, the oldest first. Entries without
//! an owner, and every held entry, count against the budget alone.

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

/// Of the budget, one part in this many is the small queue's.
const SMALL_SHARE: usize = 10;

/// Of the budget of a [`ByteCache`], one part in this many is for what
/// readahead fetched and nobody has read. Past it, the oldest of that goes
/// first.
pub const AHEAD_SHARE: usize = 8;

/// Of the budget, one part in this many is never held.
///
/// Past the rest, the oldest held entry is released into the small queue
/// where its age puts it, so a node that stores more than its owner releases
/// still keeps what readers fetched since.
pub const UNHELD_SHARE: usize = 8;

/// The most times the cache counts readers coming back to one entry. More
/// would keep an entry that was popular once for too many rounds.
const RETURNS_MAX: u8 = 3;

/// Returns above which an entry that leaves the small queue moves to the
/// main queue. One return can be a second consumer on the same pass.
const RETURNS_TO_STAY: u8 = 1;

/// Why an entry enters the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// A reader asked for the bytes.
    Read,
    /// Readahead fetched the bytes before any reader asked for them.
    Ahead,
    /// This node stored the bytes and the store is still the only other
    /// place that has them, so every reader of them is yet to come. They
    /// give way only when nothing else is left to evict, until
    /// [`ByteCache::release`] puts them in the small queue.
    Held,
}

/// What a read of the cache found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    Hit(Bytes),
    /// Like [`Self::Hit`], and the first read of bytes that readahead put
    /// there: the reader goes on where readahead expected it.
    AheadHit(Bytes),
    Miss,
}

/// Bytes by key, with the most memory they may hold together, and the most
/// that the entries of each owner `O` with a limit may hold.
#[derive(Debug)]
pub struct ByteCache<K, O = ()> {
    entries: BTreeMap<K, Entry<O>>,
    lines: [Line<K>; 5],
    /// The entries of each owner with a limit, by where they are. An owner
    /// without one has no row: its entries count against the budget alone.
    limited: BTreeMap<O, Limited<K>>,
    /// The keys of each owner that has entries, the ones it only remembers
    /// too, so that giving an owner a limit or dropping it looks at its own
    /// entries alone.
    owned: BTreeMap<O, BTreeSet<K>>,
    budget: usize,
    entered: u64,
    /// Entries whose bytes left because their owner was over its limit,
    /// since the last [`ByteCache::take_limit_evictions`].
    limit_evictions: u64,
    /// Keys of held entries that the cache let go of by itself since the
    /// last [`ByteCache::take_let_go`]: released past their share, or
    /// evicted when nothing else was left.
    let_go: Vec<K>,
}

/// The entries of one owner with a limit.
#[derive(Debug)]
struct Limited<K> {
    limit: usize,
    lines: [Line<K>; 5],
}

impl<K: Ord + Clone, O: Copy + Ord> ByteCache<K, O> {
    #[must_use]
    pub const fn new(budget: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            lines: [
                Line::new(),
                Line::new(),
                Line::new(),
                Line::new(),
                Line::new(),
            ],
            limited: BTreeMap::new(),
            owned: BTreeMap::new(),
            budget,
            entered: 0,
            limit_evictions: 0,
            let_go: Vec::new(),
        }
    }

    /// The bytes under `key`, counted as a read of them.
    pub fn read(&mut self, key: &K) -> Lookup {
        let Some(entry) = self.entries.get_mut(key) else {
            return Lookup::Miss;
        };
        let Some(bytes) = entry.bytes.clone() else {
            return Lookup::Miss;
        };
        if entry.ahead {
            // The first use of bytes nobody asked for. It is the read that
            // an entry a reader fetched has behind it when it enters.
            entry.ahead = false;
            match entry.place {
                Place::Kept => return Lookup::Hit(bytes),
                Place::Ahead => self.relink(key, Place::Small),
                _ => {}
            }
            return Lookup::AheadHit(bytes);
        }
        entry.returns = (entry.returns + 1).min(RETURNS_MAX);
        Lookup::Hit(bytes)
    }

    /// Whether the bytes under `key` are still kept apart as
    /// [`Admission::Held`], and not released.
    #[must_use]
    pub fn is_held(&self, key: &K) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.place == Place::Kept)
    }

    /// Whether the cache holds the bytes under `key`. Not a read of them.
    #[must_use]
    pub fn holds(&self, key: &K) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.bytes.is_some())
    }

    /// The bytes under `key`, for a reader that read them already. Not a
    /// read of them.
    #[must_use]
    pub fn peek(&self, key: &K) -> Option<Bytes> {
        self.entries.get(key).and_then(|entry| entry.bytes.clone())
    }

    /// Keep `bytes` under `key`. `held` is the memory the buffer takes, which
    /// can be more than its length. Bytes that do not fit the whole budget
    /// are not kept, and neither are held bytes past [`Self::held_limit`]
    /// nor bytes of readahead that find no room in its share and the small
    /// queue.
    pub fn insert(&mut self, key: K, bytes: Bytes, held: usize, admission: Admission) {
        self.admit(key, bytes, held, admission, None);
    }

    /// [`Self::insert`] for `owner`. Bytes that do not fit the owner's limit
    /// are not kept, and an owner over its limit makes room among its own
    /// entries first. Held bytes belong to no owner.
    pub fn insert_owned(
        &mut self,
        key: K,
        bytes: Bytes,
        held: usize,
        admission: Admission,
        owner: O,
    ) {
        self.admit(key, bytes, held, admission, Some(owner));
    }

    fn admit(&mut self, key: K, bytes: Bytes, held: usize, admission: Admission, owner: Option<O>) {
        // The store's objects never change, so bytes the cache holds already
        // are these bytes.
        if self.holds(&key) || held > self.budget {
            return;
        }
        // Released as they entered, they would first release every older
        // held entry for nothing.
        if admission == Admission::Held && held > self.held_limit() {
            return;
        }
        let owner = owner.filter(|_| admission != Admission::Held);
        if owner.is_some_and(|owner| self.limit(&owner).is_some_and(|limit| held > limit)) {
            return;
        }
        match admission {
            Admission::Read => {
                if let Some(owner) = owner {
                    self.make_owner_room(owner, held);
                }
                self.make_room(held);
            }
            Admission::Ahead => {
                let room = owner.is_none_or(|owner| self.make_owner_room_ahead(owner, held))
                    && self.make_room_ahead(held);
                if !room {
                    // The keys of what it evicted on the way are remembered.
                    self.forget_old_keys();
                    if let Some(owner) = owner {
                        self.forget_old_keys_of(owner);
                    }
                    return;
                }
            }
            Admission::Held => {
                // Released before room is made, so that what it releases can
                // be what leaves.
                self.release_held_past(self.held_limit().saturating_sub(held));
                self.make_room(held);
            }
        }
        let remembered = self.entries.contains_key(&key);
        self.unlink(&key);
        let place = match admission {
            Admission::Held => Place::Kept,
            // A reader came back for bytes that had no room to wait in.
            Admission::Read if remembered => Place::Main,
            Admission::Read => Place::Small,
            // Readahead is no reader, so it comes back for nothing.
            Admission::Ahead => Place::Ahead,
        };
        self.link(
            key,
            Entry {
                bytes: Some(bytes),
                held,
                place,
                entered: 0,
                returns: 0,
                ahead: admission != Admission::Read,
                owner,
            },
        );
        // Bytes held alone past the share are released as they enter.
        self.release_held_past(self.held_limit());
        self.forget_old_keys();
        if let Some(owner) = owner {
            self.forget_old_keys_of(owner);
        }
    }

    /// The limit of `owner`, `None` for one without.
    #[must_use]
    pub fn limit(&self, owner: &O) -> Option<usize> {
        self.limited.get(owner).map(|limited| limited.limit)
    }

    /// Give `owner` the limit `limit`, evicting its entries down to it, or
    /// take its limit away with `None`.
    pub fn set_limit(&mut self, owner: O, limit: Option<usize>) {
        let Some(limit) = limit else {
            self.limited.remove(&owner);
            return;
        };
        if let Some(limited) = self.limited.get_mut(&owner) {
            limited.limit = limit;
        } else {
            let mut lines = [
                Line::new(),
                Line::new(),
                Line::new(),
                Line::new(),
                Line::new(),
            ];
            let keys = self.owned.get(&owner).into_iter().flatten();
            for (key, entry) in keys.filter_map(|key| Some((key, self.entries.get(key)?))) {
                lines[entry.place as usize].put(entry.entered, key.clone(), entry.held);
            }
            self.limited.insert(owner, Limited { limit, lines });
        }
        self.make_owner_room(owner, 0);
        self.forget_old_keys();
        self.forget_old_keys_of(owner);
    }

    /// Drop every entry of `owner`, the keys it remembers too, and its limit.
    pub fn remove_owner(&mut self, owner: &O) {
        self.limited.remove(owner);
        for key in self.owned.remove(owner).unwrap_or_default() {
            self.unlink(&key);
        }
    }

    /// Bytes of memory the buffers of `owner` hold, when it has a limit.
    #[must_use]
    pub fn held_by(&self, owner: &O) -> Option<usize> {
        self.limited.get(owner).map(Limited::held)
    }

    /// Entries whose bytes left because their owner was over its limit,
    /// since the last call.
    pub const fn take_limit_evictions(&mut self) -> u64 {
        std::mem::replace(&mut self.limit_evictions, 0)
    }

    /// Put bytes kept under [`Admission::Held`] in the small queue, as its
    /// newest entry. What readers did with them while they were held stays
    /// with them.
    pub fn release(&mut self, key: &K) {
        if self
            .entries
            .get(key)
            .is_some_and(|entry| entry.place == Place::Kept)
        {
            self.relink(key, Place::Small);
        }
    }

    /// Drop the bytes under `key` and what the cache remembers of it.
    pub fn remove(&mut self, key: &K) {
        self.unlink(key);
    }

    /// Drop every key that `keep` refuses.
    pub fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        let dropped: Vec<K> = self
            .entries
            .keys()
            .filter(|key| !keep(key))
            .cloned()
            .collect();
        for key in dropped {
            self.unlink(&key);
        }
    }

    /// Change the budget, evicting down to it.
    pub fn set_budget(&mut self, budget: usize) {
        self.budget = budget;
        self.release_held_past(self.held_limit());
        self.make_room(0);
        self.forget_old_keys();
    }

    #[must_use]
    pub const fn budget(&self) -> usize {
        self.budget
    }

    /// Bytes of memory the buffers kept as [`Admission::Held`] hold.
    #[must_use]
    pub const fn held_apart(&self) -> usize {
        self.held_in(Place::Kept)
    }

    /// Bytes of memory the kept buffers hold.
    #[must_use]
    pub const fn held(&self) -> usize {
        self.held_in(Place::Small)
            + self.held_in(Place::Main)
            + self.held_in(Place::Ahead)
            + self.held_in(Place::Kept)
    }

    const fn held_in(&self, place: Place) -> usize {
        self.lines[place as usize].held
    }

    /// Evict until `incoming` more bytes that a reader asked for, or that
    /// this node holds, fit the budget.
    fn make_room(&mut self, incoming: usize) {
        while self.held() + incoming > self.budget {
            let small = self.held_in(Place::Small);
            let waiting = self.held_in(Place::Ahead);
            let passed = if waiting > self.budget / AHEAD_SHARE {
                self.drop_oldest(Place::Ahead)
            } else if small > 0 && (small >= self.small_target() || self.main_is_empty()) {
                self.pass_oldest_small()
            } else if !self.main_is_empty() {
                self.pass_oldest_main()
            } else {
                // Only bytes nobody has read are left, and the budget bounds
                // them too.
                self.drop_oldest(Place::Ahead) || self.drop_oldest(Place::Kept)
            };
            if !passed {
                return;
            }
        }
    }

    /// Make room for `incoming` bytes that readahead fetched: in its share,
    /// by dropping the oldest of what it fetched before, and in the budget,
    /// by passing on the oldest entries of the small queue. `false` when that
    /// is not enough. Bytes larger than the share wait alone.
    fn make_room_ahead(&mut self, incoming: usize) -> bool {
        // They would not fit with only the main queue and the held bytes
        // left, so evicting anything else first would gain nothing.
        if self.held_in(Place::Main) + self.held_in(Place::Kept) + incoming > self.budget {
            return false;
        }
        while self.held_in(Place::Ahead) > 0
            && self.held_in(Place::Ahead) + incoming > self.budget / AHEAD_SHARE
        {
            self.drop_oldest(Place::Ahead);
        }
        while self.held() + incoming > self.budget {
            if !self.pass_oldest_small() {
                return false;
            }
        }
        true
    }

    /// [`Self::make_room_ahead`] among the entries of `owner`, by its
    /// limit. Its entries are never held, so only its main queue stays.
    fn make_owner_room_ahead(&mut self, owner: O, incoming: usize) -> bool {
        loop {
            let Some(limited) = self.limited.get(&owner) else {
                return true;
            };
            let limit = limited.limit;
            let held = limited.held();
            let waiting = limited.held_in(Place::Ahead);
            let oldest = |place: Place| limited.lines[place as usize].oldest();
            let passed = if limited.held_in(Place::Main) + incoming > limit {
                return false;
            } else if waiting > 0 && waiting + incoming > limit / AHEAD_SHARE {
                oldest(Place::Ahead).inspect(|key| self.unlink(key))
            } else if held + incoming > limit {
                oldest(Place::Small).inspect(|key| self.pass_small(key))
            } else {
                return true;
            };
            if passed.is_none() {
                return false;
            }
            if self
                .limited
                .get(&owner)
                .is_none_or(|limited| limited.held() < held)
            {
                self.limit_evictions += 1;
            }
        }
    }

    /// Evict among the entries of `owner` until `incoming` more bytes of it
    /// that a reader asked for fit its limit, by the rules of
    /// [`Self::make_room`] applied to its entries alone.
    fn make_owner_room(&mut self, owner: O, incoming: usize) {
        loop {
            let Some(limited) = self.limited.get(&owner) else {
                return;
            };
            let limit = limited.limit;
            let held = limited.held();
            if held + incoming <= limit {
                return;
            }
            let small = limited.held_in(Place::Small);
            let waiting = limited.held_in(Place::Ahead);
            let main_is_empty = limited.held_in(Place::Main) == 0;
            let oldest = |place: Place| limited.lines[place as usize].oldest();
            let passed = if waiting > limit / AHEAD_SHARE {
                oldest(Place::Ahead).inspect(|key| self.unlink(key))
            } else if small > 0 && (small >= limit / SMALL_SHARE || main_is_empty) {
                oldest(Place::Small).inspect(|key| self.pass_small(key))
            } else if !main_is_empty {
                oldest(Place::Main).inspect(|key| self.pass_main(key))
            } else {
                oldest(Place::Ahead).inspect(|key| self.unlink(key))
            };
            if passed.is_none() {
                return;
            }
            if self
                .limited
                .get(&owner)
                .is_none_or(|limited| limited.held() < held)
            {
                self.limit_evictions += 1;
            }
        }
    }

    /// The keys of the held entries that the cache let go of by itself
    /// since the last call, in the order it did.
    pub fn take_let_go(&mut self) -> Vec<K> {
        std::mem::take(&mut self.let_go)
    }

    /// The most bytes that held entries take.
    #[must_use]
    pub const fn held_limit(&self) -> usize {
        self.budget - self.budget / UNHELD_SHARE
    }

    /// Drop the oldest entry of `place` with nothing remembered. `false`
    /// when it has none.
    fn drop_oldest(&mut self, place: Place) -> bool {
        let Some(key) = self.lines[place as usize].oldest() else {
            return false;
        };
        if place == Place::Kept {
            self.let_go.push(key.clone());
        }
        self.unlink(&key);
        true
    }

    /// Release the oldest held entries until they take at most `limit`.
    fn release_held_past(&mut self, limit: usize) {
        while self.held_in(Place::Kept) > limit {
            let Some(key) = self.lines[Place::Kept as usize].oldest() else {
                return;
            };
            self.move_keeping_age(&key, Place::Small);
            self.let_go.push(key);
        }
    }

    const fn small_target(&self) -> usize {
        self.budget / SMALL_SHARE
    }

    const fn main_is_empty(&self) -> bool {
        self.held_in(Place::Main) == 0
    }

    /// Take the oldest entry out of the small queue, see
    /// [`Self::pass_small`]. `false` when the queue is empty.
    fn pass_oldest_small(&mut self) -> bool {
        self.lines[Place::Small as usize]
            .oldest()
            .inspect(|key| self.pass_small(key))
            .is_some()
    }

    /// Take `key` out of the small queue: into the main queue if readers
    /// came back to it, and out of the cache otherwise, with its key
    /// remembered.
    fn pass_small(&mut self, key: &K) {
        let (returns, ahead) = self
            .entries
            .get(key)
            .map_or((0, false), |entry| (entry.returns, entry.ahead));
        if returns > RETURNS_TO_STAY {
            self.relink(key, Place::Main);
            if let Some(entry) = self.entries.get_mut(key) {
                entry.returns = 0;
            }
        } else if ahead {
            // Nobody read the bytes, so a reader that arrives late is a
            // first reader. Remembering the key would count it as one that
            // came back.
            self.unlink(key);
        } else {
            self.relink(key, Place::Gone);
            if let Some(entry) = self.entries.get_mut(key) {
                entry.bytes = None;
            }
        }
    }

    /// Take the oldest entry of the main queue, see [`Self::pass_main`].
    /// `false` when the queue is empty.
    fn pass_oldest_main(&mut self) -> bool {
        self.lines[Place::Main as usize]
            .oldest()
            .inspect(|key| self.pass_main(key))
            .is_some()
    }

    /// Take `key` out of the main queue: back in as its newest, with one
    /// return less, if readers came back to it, and out of the cache
    /// otherwise.
    fn pass_main(&mut self, key: &K) {
        let returns = self.entries.get(key).map_or(0, |entry| entry.returns);
        if returns == 0 {
            self.unlink(key);
        } else {
            self.relink(key, Place::Main);
            if let Some(entry) = self.entries.get_mut(key) {
                entry.returns = returns - 1;
            }
        }
    }

    /// Remember keys for as many bytes as the main queue may hold, and no
    /// more.
    fn forget_old_keys(&mut self) {
        let most = self.budget - self.small_target();
        while self.held_in(Place::Gone) > most {
            let Some(key) = self.lines[Place::Gone as usize].oldest() else {
                break;
            };
            self.unlink(&key);
        }
    }

    /// [`Self::forget_old_keys`] among the keys of `owner`, by its limit,
    /// so that an owner that reads much once leaves room for the keys that
    /// others remember.
    fn forget_old_keys_of(&mut self, owner: O) {
        loop {
            let Some(limited) = self.limited.get(&owner) else {
                return;
            };
            let most = limited.limit - limited.limit / SMALL_SHARE;
            if limited.held_in(Place::Gone) <= most {
                return;
            }
            let Some(key) = limited.lines[Place::Gone as usize].oldest() else {
                return;
            };
            self.unlink(&key);
        }
    }

    /// Move the entry under `key` to `place`, where the time it entered
    /// puts it.
    fn move_keeping_age(&mut self, key: &K, place: Place) {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        let (from, entered, held) = (entry.place, entry.entered, entry.held);
        entry.place = place;
        let owner = entry.owner;
        for lines in self.lines_of(owner) {
            lines[from as usize].take(entered, held);
            lines[place as usize].put(entered, key.clone(), held);
        }
    }

    /// Move the entry under `key` to `place`, as its newest.
    fn relink(&mut self, key: &K, place: Place) {
        let Some(mut entry) = self.entries.remove(key) else {
            return;
        };
        for lines in self.lines_of(entry.owner) {
            lines[entry.place as usize].take(entry.entered, entry.held);
        }
        entry.place = place;
        self.link(key.clone(), entry);
    }

    fn link(&mut self, key: K, mut entry: Entry<O>) {
        self.entered += 1;
        entry.entered = self.entered;
        for lines in self.lines_of(entry.owner) {
            lines[entry.place as usize].put(entry.entered, key.clone(), entry.held);
        }
        if let Some(owner) = entry.owner {
            self.owned.entry(owner).or_default().insert(key.clone());
        }
        self.entries.insert(key, entry);
    }

    fn unlink(&mut self, key: &K) {
        let Some(entry) = self.entries.remove(key) else {
            return;
        };
        for lines in self.lines_of(entry.owner) {
            lines[entry.place as usize].take(entry.entered, entry.held);
        }
        if let Some(owner) = entry.owner
            && let Some(keys) = self.owned.get_mut(&owner)
        {
            keys.remove(key);
            if keys.is_empty() {
                self.owned.remove(&owner);
            }
        }
    }

    /// The lines that an entry of `owner` is in: the cache's, and its
    /// owner's when that has a limit.
    fn lines_of(&mut self, owner: Option<O>) -> impl Iterator<Item = &mut [Line<K>; 5]> {
        let limited = owner.and_then(|owner| self.limited.get_mut(&owner));
        std::iter::once(&mut self.lines).chain(limited.map(|limited| &mut limited.lines))
    }
}

impl<K> Limited<K> {
    const fn held_in(&self, place: Place) -> usize {
        self.lines[place as usize].held
    }

    /// Bytes of memory its buffers hold.
    const fn held(&self) -> usize {
        self.held_in(Place::Small)
            + self.held_in(Place::Main)
            + self.held_in(Place::Ahead)
            + self.held_in(Place::Kept)
    }
}

/// Where an entry is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    /// Where bytes enter, and where what is read once stays.
    Small = 0,
    /// Where bytes go that readers came back to.
    Main = 1,
    /// Keys the small queue evicted. No bytes.
    Gone = 2,
    /// Apart from both queues until released.
    Kept = 3,
    /// What readahead fetched and nobody has read.
    Ahead = 4,
}

#[derive(Debug)]
struct Entry<O> {
    /// `None` for a key the cache only remembers.
    bytes: Option<Bytes>,
    held: usize,
    place: Place,
    /// The position in its line.
    entered: u64,
    /// Times readers came back since the entry entered its queue, up to
    /// [`RETURNS_MAX`].
    returns: u8,
    /// Nobody asked for the bytes when they entered, and no reader has read
    /// them.
    ahead: bool,
    owner: Option<O>,
}

/// Keys in the order they entered, with the memory their buffers hold.
#[derive(Debug)]
struct Line<K> {
    order: BTreeMap<u64, K>,
    held: usize,
}

impl<K: Clone> Line<K> {
    const fn new() -> Self {
        Self {
            order: BTreeMap::new(),
            held: 0,
        }
    }

    fn put(&mut self, entered: u64, key: K, held: usize) {
        self.order.insert(entered, key);
        self.held += held;
    }

    fn take(&mut self, entered: u64, held: usize) {
        if self.order.remove(&entered).is_some() {
            self.held -= held;
        }
    }

    fn oldest(&self) -> Option<K> {
        self.order.first_key_value().map(|(_, key)| key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    fn buffer(len: usize) -> Bytes {
        Bytes::from(vec![0; len])
    }

    fn read_into(cache: &mut ByteCache<u32>, key: u32, len: usize) -> bool {
        match cache.read(&key) {
            Lookup::Miss => {
                cache.insert(key, buffer(len), len, Admission::Read);
                false
            }
            Lookup::Hit(_) | Lookup::AheadHit(_) => true,
        }
    }

    fn keys_in(cache: &ByteCache<u32>, place: Place) -> Vec<u32> {
        owned_keys_in(cache, place)
    }

    fn assert_bounds(cache: &ByteCache<u32>) {
        assert_owned_bounds(cache);
    }

    /// The keys whose bytes the cache holds in `place`, the oldest first.
    fn owned_keys_in<O>(cache: &ByteCache<u32, O>, place: Place) -> Vec<u32> {
        cache.lines[place as usize]
            .order
            .values()
            .copied()
            .collect()
    }

    fn assert_owned_bounds<O: Copy + Ord + std::fmt::Debug>(cache: &ByteCache<u32, O>) {
        assert!(cache.held() <= cache.budget());
        assert!(cache.held_apart() <= cache.held_limit());
        assert!(cache.held_in(Place::Gone) <= cache.budget());
        let mut summed = [0; 5];
        let mut by_owner: BTreeMap<O, [usize; 5]> = BTreeMap::new();
        for (key, entry) in &cache.entries {
            summed[entry.place as usize] += entry.held;
            assert_eq!(entry.bytes.is_none(), entry.place == Place::Gone);
            assert_eq!(
                cache.lines[entry.place as usize].order.get(&entry.entered),
                Some(key)
            );
            if entry.place == Place::Kept {
                assert_eq!(entry.owner, None, "a held entry belongs to no owner");
            }
            if let Some(limited) = entry.owner.and_then(|owner| cache.limited.get(&owner)) {
                assert_eq!(
                    limited.lines[entry.place as usize]
                        .order
                        .get(&entry.entered),
                    Some(key)
                );
                by_owner.entry(entry.owner.unwrap()).or_default()[entry.place as usize] +=
                    entry.held;
            }
        }
        for (line, held) in cache.lines.iter().zip(summed) {
            assert_eq!(line.held, held);
        }
        let linked: usize = cache.lines.iter().map(|line| line.order.len()).sum();
        assert_eq!(linked, cache.entries.len());
        for (owner, limited) in &cache.limited {
            let summed = by_owner.get(owner).copied().unwrap_or_default();
            for (line, held) in limited.lines.iter().zip(summed) {
                assert_eq!(line.held, held, "{owner:?}");
            }
            assert!(limited.held() <= limited.limit, "{owner:?}");
        }
        let mut recounted: BTreeMap<O, BTreeSet<u32>> = BTreeMap::new();
        for (key, entry) in &cache.entries {
            if let Some(owner) = entry.owner {
                recounted.entry(owner).or_default().insert(*key);
            }
        }
        assert_eq!(cache.owned, recounted, "the keys tracked by owner drifted");
    }

    /// A cache whose main queue holds `shared`, each read by four readers.
    fn cache_with_shared(budget: usize, shared: &[u32]) -> ByteCache<u32> {
        let mut cache = ByteCache::new(budget);
        for key in shared {
            for _ in 0..4 {
                read_into(&mut cache, *key, 10);
            }
        }
        // Enough once-read entries to turn the small queue over.
        for key in 10_000..10_000 + u32::try_from(budget / 10).unwrap() {
            read_into(&mut cache, key, 10);
        }
        assert_eq!(keys_in(&cache, Place::Main), shared);
        cache
    }

    #[test]
    fn given_kept_bytes_when_read_should_hit_every_time() {
        let mut cache = ByteCache::new(100);

        assert!(!read_into(&mut cache, 1, 10));
        assert!(read_into(&mut cache, 1, 10));

        assert_eq!(cache.read(&1), Lookup::Hit(buffer(10)));
        assert_eq!(cache.read(&2), Lookup::Miss);
        assert_eq!(cache.held(), 10);
    }

    #[test]
    fn given_bytes_peeked_at_when_they_leave_the_small_queue_should_not_count_as_coming_back() {
        let mut cache = ByteCache::new(30);
        cache.insert(1, buffer(10), 10, Admission::Ahead);
        read_into(&mut cache, 2, 10);
        for _ in 0..3 {
            assert_eq!(cache.peek(&1), Some(buffer(10)));
            assert_eq!(cache.peek(&2), Some(buffer(10)));
        }
        assert_eq!(cache.peek(&3), None);
        assert_eq!(cache.read(&1), Lookup::AheadHit(buffer(10)));

        for key in [3, 4, 5] {
            read_into(&mut cache, key, 10);
        }

        assert_eq!(keys_in(&cache, Place::Main), [0_u32; 0]);
        assert_bounds(&cache);
    }

    #[test]
    fn given_a_full_cache_when_more_bytes_enter_should_evict_the_oldest_read_once() {
        let mut cache = ByteCache::new(30);
        for key in [1, 2, 3] {
            read_into(&mut cache, key, 10);
        }

        read_into(&mut cache, 4, 10);

        assert_eq!(keys_in(&cache, Place::Small), [2, 3, 4]);
        assert_eq!(keys_in(&cache, Place::Gone), [1]);
        assert_bounds(&cache);
    }

    #[test]
    fn given_an_entry_readers_came_back_to_when_it_leaves_the_small_queue_should_move_to_the_main()
    {
        let mut cache = ByteCache::new(30);
        for key in [1, 2, 3] {
            read_into(&mut cache, key, 10);
        }
        assert!(read_into(&mut cache, 1, 10));
        assert!(read_into(&mut cache, 1, 10));

        read_into(&mut cache, 4, 10);

        assert_eq!(keys_in(&cache, Place::Main), [1]);
        assert_eq!(keys_in(&cache, Place::Small), [3, 4]);
        assert_bounds(&cache);
    }

    /// One return can be a second consumer reading the same history once.
    #[test]
    fn given_an_entry_read_by_two_readers_when_it_leaves_the_small_queue_should_be_evicted() {
        let mut cache = ByteCache::new(30);
        for key in [1, 2, 3] {
            read_into(&mut cache, key, 10);
        }
        assert!(read_into(&mut cache, 1, 10));

        read_into(&mut cache, 4, 10);

        assert_eq!(keys_in(&cache, Place::Main), [0_u32; 0]);
        assert_eq!(keys_in(&cache, Place::Gone), [1]);
    }

    #[test]
    fn given_an_evicted_key_the_cache_remembers_when_read_again_should_enter_the_main_queue() {
        let mut cache = ByteCache::new(30);
        for key in [1, 2, 3, 4] {
            read_into(&mut cache, key, 10);
        }
        assert_eq!(keys_in(&cache, Place::Gone), [1]);

        assert!(!read_into(&mut cache, 1, 10));

        assert_eq!(keys_in(&cache, Place::Main), [1]);
        assert!(cache.holds(&1));
        assert_bounds(&cache);
    }

    /// The reason for the policy. However long the history that is read
    /// once, and with nobody reading the shared entries meanwhile, the main
    /// queue is not touched.
    #[test]
    fn given_entries_many_readers_share_when_a_long_history_is_read_once_should_keep_them() {
        let shared = [1, 2, 3, 4, 5];
        let mut cache = cache_with_shared(100, &shared);

        for key in 100..5_000 {
            read_into(&mut cache, key, 10);
            assert_bounds(&cache);
        }

        assert_eq!(keys_in(&cache, Place::Main), shared);
        for key in shared {
            assert!(cache.holds(&key));
        }
    }

    /// The same with readahead in front of the reader: every entry enters
    /// before its reader and is read once.
    #[test]
    fn given_entries_many_readers_share_when_a_long_history_is_read_through_readahead_should_keep_them()
     {
        let shared = [1, 2, 3, 4, 5];
        let mut cache = cache_with_shared(100, &shared);

        for key in 100..5_000 {
            cache.insert(key + 1, buffer(10), 10, Admission::Ahead);
            read_into(&mut cache, key, 10);
            assert_bounds(&cache);
        }

        assert_eq!(keys_in(&cache, Place::Main), shared);
    }

    /// Two consumers on the same history, the second a few entries behind
    /// the first, still read every entry once each.
    #[test]
    fn given_entries_many_readers_share_when_two_readers_follow_one_history_should_keep_them() {
        let shared = [1, 2, 3, 4, 5];
        let mut cache = cache_with_shared(100, &shared);

        for key in 100..5_000 {
            read_into(&mut cache, key, 10);
            read_into(&mut cache, key - 2, 10);
            assert_bounds(&cache);
        }

        assert_eq!(keys_in(&cache, Place::Main), shared);
    }

    #[test]
    fn given_a_full_main_queue_when_another_entry_moves_in_should_evict_the_one_nobody_came_back_to()
     {
        let mut cache = cache_with_shared(100, &[1, 2, 3, 4, 5, 6, 7, 8, 9]);
        for key in [2, 3, 4, 5, 6, 7, 8, 9] {
            assert!(read_into(&mut cache, key, 10));
        }

        // A tenth entry that readers share: in through the small queue.
        for _ in 0..4 {
            read_into(&mut cache, 50, 10);
        }
        for key in 20_000..20_020 {
            read_into(&mut cache, key, 10);
        }

        let main = keys_in(&cache, Place::Main);
        assert!(main.contains(&50), "{main:?}");
        assert!(!main.contains(&1), "{main:?}");
        assert_eq!(main.len(), 9);
        assert_bounds(&cache);
    }

    /// A sequential reader meets every entry after readahead fetched it. Had
    /// that meeting counted as coming back, two readers behind one readahead
    /// would move a whole history into the main queue.
    #[test]
    fn given_bytes_fetched_ahead_when_first_read_should_not_count_as_coming_back() {
        let mut cache = ByteCache::new(30);
        cache.insert(1, buffer(10), 10, Admission::Ahead);
        assert!(cache.holds(&1));

        assert_eq!(cache.read(&1), Lookup::AheadHit(buffer(10)));
        assert_eq!(cache.read(&1), Lookup::Hit(buffer(10)));

        for key in [2, 3, 4] {
            read_into(&mut cache, key, 10);
        }
        assert_eq!(keys_in(&cache, Place::Main), [0_u32; 0]);
        assert_eq!(keys_in(&cache, Place::Gone), [1]);
    }

    #[test]
    fn given_bytes_fetched_ahead_and_never_read_when_evicted_should_not_be_remembered() {
        let mut cache = ByteCache::new(30);
        cache.insert(1, buffer(10), 10, Admission::Ahead);
        for key in [2, 3, 4] {
            read_into(&mut cache, key, 10);
        }
        assert!(!cache.entries.contains_key(&1));

        // A reader that arrives late is a first reader.
        read_into(&mut cache, 1, 10);

        assert_eq!(keys_in(&cache, Place::Main), [0_u32; 0]);
        assert_bounds(&cache);
    }

    #[test]
    fn given_bytes_fetched_ahead_for_a_remembered_key_when_inserted_should_not_enter_the_main_queue()
     {
        let mut cache = ByteCache::new(30);
        for key in [1, 2, 3, 4] {
            read_into(&mut cache, key, 10);
        }
        assert_eq!(keys_in(&cache, Place::Gone), [1]);

        cache.insert(1, buffer(10), 10, Admission::Ahead);

        assert_eq!(keys_in(&cache, Place::Main), [0_u32; 0]);
        assert_eq!(keys_in(&cache, Place::Ahead), [1]);
        assert_bounds(&cache);
    }

    /// Readahead runs in front of its reader. What it fetched has to be
    /// there when the reader arrives, whatever is read once in between.
    #[test]
    fn given_bytes_fetched_ahead_when_much_is_read_once_before_their_reader_should_still_be_kept() {
        let mut cache = ByteCache::new(80);
        cache.insert(1, buffer(10), 10, Admission::Ahead);

        for key in 100..200 {
            read_into(&mut cache, key, 10);
            assert_bounds(&cache);
        }

        assert_eq!(cache.read(&1), Lookup::AheadHit(buffer(10)));
        assert_eq!(keys_in(&cache, Place::Ahead), [0_u32; 0]);
        assert_eq!(keys_in(&cache, Place::Small).last(), Some(&1));
    }

    /// Readahead that no reader follows may not take the cache over.
    #[test]
    fn given_more_fetched_ahead_than_its_share_when_the_cache_is_full_should_drop_the_oldest_of_it()
    {
        let mut cache = cache_with_shared(80, &[1, 2, 3]);
        for key in 100..120 {
            cache.insert(key, buffer(10), 10, Admission::Ahead);
            assert_bounds(&cache);
        }

        assert_eq!(keys_in(&cache, Place::Ahead), [119]);
        assert_eq!(keys_in(&cache, Place::Main), [1, 2, 3]);
        assert!(!cache.entries.contains_key(&100), "nothing is remembered");
    }

    /// Free space is no reason to keep more readahead than its share.
    #[test]
    fn given_free_space_when_more_than_the_share_of_readahead_enters_should_drop_the_oldest_of_it()
    {
        let mut cache = ByteCache::new(80);
        for key in [1, 2, 3] {
            cache.insert(key, buffer(10), 10, Admission::Ahead);
            assert_bounds(&cache);
        }

        assert_eq!(keys_in(&cache, Place::Ahead), [3]);
        assert_eq!(cache.held(), 10);
    }

    #[test]
    fn given_a_cache_full_of_what_readers_came_back_to_when_readahead_needs_room_should_not_keep_it()
     {
        let shared = [1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut cache = cache_with_shared(100, &shared);
        for key in keys_in(&cache, Place::Small) {
            cache.remove(&key);
        }

        cache.insert(50, buffer(12), 12, Admission::Ahead);

        assert!(!cache.holds(&50));
        assert_eq!(keys_in(&cache, Place::Main), shared);
        assert_bounds(&cache);
    }

    #[test]
    fn given_a_cache_full_of_held_bytes_when_readahead_needs_room_should_not_keep_it() {
        let mut cache = ByteCache::new(80);
        for key in 1..=7 {
            cache.insert(key, buffer(10), 10, Admission::Held);
        }
        cache.insert(50, buffer(10), 10, Admission::Ahead);
        assert_eq!(cache.held(), cache.budget());

        cache.insert(51, buffer(11), 11, Admission::Ahead);

        assert_eq!(keys_in(&cache, Place::Ahead), [50]);
        assert!(!cache.holds(&51));
        assert_eq!(keys_in(&cache, Place::Kept), [1, 2, 3, 4, 5, 6, 7]);
        assert_bounds(&cache);
    }

    /// The small queue gives way to readahead, which is what a sequential
    /// reader reads once next.
    #[test]
    fn given_a_cache_full_of_bytes_read_once_when_readahead_enters_should_take_the_place_of_the_oldest()
     {
        let mut cache = ByteCache::new(80);
        for key in 1..=8 {
            read_into(&mut cache, key, 10);
        }

        cache.insert(50, buffer(10), 10, Admission::Ahead);

        assert_eq!(keys_in(&cache, Place::Ahead), [50]);
        assert_eq!(keys_in(&cache, Place::Small), [2, 3, 4, 5, 6, 7, 8]);
        assert_bounds(&cache);
    }

    /// An owner at its limit fetches ahead into its own small queue, never
    /// into its main queue.
    #[test]
    fn given_an_owner_whose_limit_holds_what_readers_came_back_to_when_it_fetches_ahead_should_not_keep_it()
     {
        let shared = [1, 2, 3, 4, 5];
        let mut cache = owned_cache_with_shared(1_000, &shared);
        for key in owned_keys_in(&cache, Place::Small) {
            cache.remove(&key);
        }
        cache.set_limit('B', Some(55));
        assert_eq!(cache.held_by(&'B'), Some(50));

        cache.insert_owned(60, buffer(6), 6, Admission::Ahead, 'B');

        assert!(!cache.holds(&60));
        assert_eq!(owned_keys_in(&cache, Place::Main), shared);
        assert_eq!(cache.take_limit_evictions(), 0);
        assert_owned_bounds(&cache);
    }

    #[test]
    fn given_bytes_larger_than_the_budget_when_inserted_should_not_be_kept() {
        let mut cache = ByteCache::new(10);
        read_into(&mut cache, 1, 10);

        cache.insert(2, buffer(11), 11, Admission::Read);

        assert!(cache.holds(&1));
        assert!(!cache.holds(&2));
    }

    #[test]
    fn given_one_large_buffer_when_inserted_should_evict_as_many_small_ones_as_it_needs() {
        let mut cache = ByteCache::new(40);
        for key in [1, 2, 3, 4] {
            read_into(&mut cache, key, 10);
        }

        read_into(&mut cache, 5, 25);

        assert_eq!(keys_in(&cache, Place::Small), [4, 5]);
        assert_eq!(cache.held(), 35);
        assert_bounds(&cache);
    }

    #[test]
    fn given_a_smaller_budget_when_set_should_evict_down_to_it() {
        let mut cache = ByteCache::new(40);
        for key in [1, 2, 3, 4] {
            read_into(&mut cache, key, 10);
        }

        cache.set_budget(20);

        assert_eq!(keys_in(&cache, Place::Small), [3, 4]);
        assert_bounds(&cache);
    }

    #[test]
    fn given_keys_of_two_objects_when_retaining_one_should_drop_the_other_and_its_memory() {
        let mut cache = ByteCache::new(40);
        for key in [1, 2, 3, 4] {
            read_into(&mut cache, key, 10);
        }

        cache.retain(|key| key % 2 == 0);

        assert_eq!(keys_in(&cache, Place::Small), [2, 4]);
        assert_eq!(cache.held(), 20);
        cache.remove(&4);
        assert_eq!(cache.held(), 10);
        assert_bounds(&cache);
    }

    /// What this node stored and nothing else holds yet outlives everything
    /// a reader fetched, however often that was read.
    #[test]
    fn given_held_bytes_when_the_cache_fills_should_evict_everything_else_first() {
        let mut cache = ByteCache::new(40);
        cache.insert(1, buffer(10), 10, Admission::Held);
        for key in 2..40 {
            for _ in 0..4 {
                read_into(&mut cache, key, 10);
            }
            assert_bounds(&cache);
        }

        assert_eq!(keys_in(&cache, Place::Kept), [1]);
        assert_eq!(cache.read(&1), Lookup::Hit(buffer(10)));
        assert_eq!(
            keys_in(&cache, Place::Kept),
            [1],
            "a read does not release it"
        );
    }

    /// Held bytes take all but an eighth of the budget. Past it the oldest
    /// is released, and then leaves as bytes nobody read do.
    #[test]
    fn given_held_bytes_past_their_share_when_more_are_held_should_release_the_oldest() {
        let mut cache = ByteCache::new(20);
        for key in [1, 2, 3] {
            cache.insert(key, buffer(10), 10, Admission::Held);
            assert_bounds(&cache);
        }

        assert_eq!(keys_in(&cache, Place::Kept), [3]);
        assert_eq!(keys_in(&cache, Place::Small), [2]);
        assert!(!cache.entries.contains_key(&1), "nothing is remembered");
        assert_eq!(cache.held_apart(), 10);
    }

    /// Bytes past the held share would release every older held entry, and
    /// then themselves.
    #[test]
    fn given_held_bytes_when_bytes_larger_than_the_held_share_are_held_should_refuse_them_and_keep_the_others()
     {
        let mut cache = ByteCache::new(80);
        for key in [1, 2] {
            cache.insert(key, buffer(10), 10, Admission::Held);
        }

        cache.insert(3, buffer(75), 75, Admission::Held);

        assert!(!cache.holds(&3));
        assert_eq!(keys_in(&cache, Place::Kept), [1, 2]);
        assert_eq!(cache.take_let_go(), [0_u32; 0]);
        assert_bounds(&cache);
    }

    /// Whoever holds bytes learns of each one the cache let go of, whether
    /// past the held share or evicted when nothing else was left.
    #[test]
    fn given_held_bytes_that_the_cache_lets_go_of_when_taking_should_name_each_once() {
        let mut cache = ByteCache::new(80);
        for key in 1..=7 {
            cache.insert(key, buffer(10), 10, Admission::Held);
        }
        // Nothing but held bytes to make room among.
        cache.insert(50, buffer(20), 20, Admission::Read);
        cache.insert(8, buffer(10), 10, Admission::Held);
        cache.insert(9, buffer(10), 10, Admission::Held);

        assert_eq!(cache.take_let_go(), [1, 2]);
        assert_eq!(cache.take_let_go(), [0_u32; 0]);
        assert!(!cache.is_held(&1) && !cache.is_held(&2));
        assert_bounds(&cache);
    }

    /// A node that holds more than its share keeps what readers fetched
    /// after it released an entry, which leaves first.
    #[test]
    fn given_held_bytes_past_their_share_when_a_reader_fetched_since_should_keep_what_it_fetched() {
        let mut cache = ByteCache::new(80);
        for key in 0..8 {
            cache.insert(key, buffer(10), 10, Admission::Held);
        }
        assert_eq!(keys_in(&cache, Place::Small), [0]);
        read_into(&mut cache, 100, 10);

        cache.insert(8, buffer(10), 10, Admission::Held);

        assert!(cache.holds(&100));
        assert!(!cache.holds(&0));
        assert!(cache.is_held(&8) && !cache.is_held(&1));
        assert_eq!(cache.held_apart(), 70);
        assert_bounds(&cache);
    }

    #[test]
    fn given_held_bytes_when_released_should_be_evicted_like_bytes_read_as_often() {
        let mut cache = ByteCache::new(30);
        for key in [1, 2, 3] {
            cache.insert(key, buffer(10), 10, Admission::Held);
        }
        // Three readers of the third while it is held: two came back.
        for _ in 0..3 {
            assert_eq!(cache.read(&3), Lookup::Hit(buffer(10)));
        }
        assert_eq!(cache.read(&2), Lookup::Hit(buffer(10)));

        for key in [1, 2, 3] {
            cache.release(&key);
        }

        assert_eq!(keys_in(&cache, Place::Small), [1, 2, 3]);
        // Nobody read the first, so its first read is the first.
        assert_eq!(cache.read(&1), Lookup::AheadHit(buffer(10)));
        for key in [4, 5, 6] {
            read_into(&mut cache, key, 10);
        }
        assert_eq!(keys_in(&cache, Place::Main), [3]);
        assert!(!cache.holds(&1) && !cache.holds(&2));
        assert_bounds(&cache);
    }

    /// A cache of `budget` whose owner `B` holds `shared` in the main queue,
    /// each read by four readers.
    fn owned_cache_with_shared(budget: usize, shared: &[u32]) -> ByteCache<u32, char> {
        let mut cache = ByteCache::new(budget);
        for key in shared {
            for _ in 0..4 {
                read_as(&mut cache, 'B', *key, 10);
            }
        }
        for key in 10_000..10_000 + u32::try_from(budget / 10).unwrap() {
            read_as(&mut cache, 'B', key, 10);
        }
        assert_eq!(owned_keys_in(&cache, Place::Main), shared);
        cache
    }

    fn read_as(cache: &mut ByteCache<u32, char>, owner: char, key: u32, len: usize) -> bool {
        match cache.read(&key) {
            Lookup::Miss => {
                cache.insert_owned(key, buffer(len), len, Admission::Read, owner);
                false
            }
            Lookup::Hit(_) | Lookup::AheadHit(_) => true,
        }
    }

    /// The keys of `owner` whose bytes the cache holds.
    fn held_keys_of(cache: &ByteCache<u32, char>, owner: char) -> Vec<u32> {
        cache
            .entries
            .iter()
            .filter(|(_, entry)| entry.owner == Some(owner) && entry.bytes.is_some())
            .map(|(key, _)| *key)
            .collect()
    }

    #[test]
    fn given_an_owner_over_its_limit_when_it_inserts_should_evict_only_its_own_entries() {
        let mut cache = ByteCache::new(100);
        for key in [1, 2, 3] {
            read_as(&mut cache, 'B', key, 10);
        }
        cache.set_limit('A', Some(30));

        for key in 10..20 {
            read_as(&mut cache, 'A', key, 10);
            assert_owned_bounds(&cache);
        }

        assert_eq!(held_keys_of(&cache, 'A'), [17, 18, 19]);
        assert_eq!(held_keys_of(&cache, 'B'), [1, 2, 3]);
        assert_eq!(cache.held_by(&'A'), Some(30));
        assert_eq!(cache.take_limit_evictions(), 7);
        assert_eq!(cache.take_limit_evictions(), 0);
    }

    /// An owner over its limit turns over its own small queue, so what
    /// others share stays in the main queue however much it reads once.
    #[test]
    fn given_entries_another_owner_shares_when_an_owner_over_its_limit_reads_a_long_history_should_keep_them()
     {
        let shared = [1, 2, 3, 4, 5];
        let mut cache = owned_cache_with_shared(100, &shared);
        cache.set_limit('A', Some(50));

        for key in 100..5_000 {
            read_as(&mut cache, 'A', key, 10);
            assert_owned_bounds(&cache);
        }

        assert_eq!(owned_keys_in(&cache, Place::Main), shared);
        assert!(cache.held_by(&'A').unwrap() <= 50);
    }

    #[test]
    fn given_limits_that_add_up_to_more_than_the_budget_when_every_owner_inserts_should_keep_within_the_budget()
     {
        let mut cache = ByteCache::new(100);
        for owner in ['A', 'B', 'C'] {
            cache.set_limit(owner, Some(60));
        }

        for key in 0..300 {
            let owner = ['A', 'B', 'C'][key as usize % 3];
            read_as(&mut cache, owner, key, 10);
            assert_owned_bounds(&cache);
        }

        assert_eq!(cache.held(), 100);
        for owner in ['A', 'B', 'C'] {
            assert!(cache.held_by(&owner).unwrap() <= 60);
        }
    }

    #[test]
    fn given_a_limit_below_the_entry_size_when_inserting_should_keep_nothing_for_that_owner() {
        let mut cache = ByteCache::new(100);
        cache.set_limit('A', Some(9));

        read_as(&mut cache, 'A', 1, 10);
        cache.insert_owned(2, buffer(10), 10, Admission::Ahead, 'A');
        read_as(&mut cache, 'B', 3, 10);

        assert!(!cache.holds(&1) && !cache.holds(&2));
        assert!(cache.holds(&3));
        assert_eq!(cache.held_by(&'A'), Some(0));
        assert_owned_bounds(&cache);
    }

    #[test]
    fn given_a_lower_limit_when_set_should_evict_the_owner_down_to_it() {
        let mut cache = ByteCache::new(100);
        for key in [1, 2, 3, 4] {
            read_as(&mut cache, 'A', key, 10);
        }
        read_as(&mut cache, 'B', 5, 10);
        cache.set_limit('A', Some(40));
        assert_eq!(
            cache.held_by(&'A'),
            Some(40),
            "the first limit finds its entries"
        );

        cache.set_limit('A', Some(20));

        assert_eq!(held_keys_of(&cache, 'A'), [3, 4]);
        assert_eq!(held_keys_of(&cache, 'B'), [5]);
        assert_eq!(cache.take_limit_evictions(), 2);
        cache.set_limit('A', None);
        assert_eq!(cache.held_by(&'A'), None);
        assert_eq!(cache.limit(&'A'), None);
        assert_owned_bounds(&cache);
    }

    #[test]
    fn given_an_owner_when_removed_should_drop_its_entries_its_keys_and_its_limit() {
        let mut cache = ByteCache::new(40);
        for key in [1, 2, 3, 4, 5] {
            read_as(&mut cache, 'A', key, 10);
        }
        read_as(&mut cache, 'B', 6, 10);
        assert_eq!(owned_keys_in(&cache, Place::Gone), [1, 2]);
        let mut unlimited = ByteCache::<u32, char>::new(40);
        for key in [1, 2, 3] {
            read_as(&mut unlimited, 'A', key, 10);
        }
        cache.set_limit('A', Some(40));

        cache.remove_owner(&'A');
        unlimited.remove_owner(&'A');

        assert_eq!(cache.held(), 10);
        assert_eq!(owned_keys_in(&cache, Place::Gone), [0_u32; 0]);
        assert_eq!(cache.limit(&'A'), None);
        assert_eq!(unlimited.held(), 0);
        assert_owned_bounds(&cache);
        assert_owned_bounds(&unlimited);
    }

    /// An owner that never cached a block has no keys to look at, so its
    /// first limit and its removal touch nothing of the others.
    #[test]
    fn given_owners_that_never_cached_when_limited_or_removed_should_leave_the_others_alone() {
        let mut cache = ByteCache::<u32, char>::new(100);
        for key in [1, 2, 3] {
            read_as(&mut cache, 'A', key, 10);
        }

        cache.set_limit('B', Some(30));
        cache.remove_owner(&'B');
        cache.remove_owner(&'C');

        assert_eq!(held_keys_of(&cache, 'A'), [1, 2, 3]);
        assert_eq!(cache.owned.keys().copied().collect::<Vec<_>>(), ['A']);
        assert_eq!(cache.limit(&'B'), None);
        assert_owned_bounds(&cache);
    }

    /// What this node stored and holds belongs to no owner, so no limit
    /// touches it.
    #[test]
    fn given_held_entries_when_owners_are_over_their_limits_should_keep_them() {
        let mut cache = ByteCache::new(100);
        cache.insert(1, buffer(10), 10, Admission::Held);
        cache.insert_owned(2, buffer(10), 10, Admission::Held, 'A');
        cache.set_limit('A', Some(20));

        for key in 10..40 {
            read_as(&mut cache, 'A', key, 10);
        }

        assert!(cache.is_held(&1) && cache.is_held(&2));
        assert_eq!(cache.held_by(&'A'), Some(20));
        assert_owned_bounds(&cache);
    }

    /// What readahead fetched for an owner counts towards its limit, and
    /// an owner at its limit keeps at most an eighth of it waiting.
    #[test]
    fn given_bytes_fetched_ahead_for_an_owner_when_inserted_should_count_towards_its_limit_and_its_ahead_share()
     {
        let mut cache = ByteCache::new(1_000);
        cache.set_limit('A', Some(80));
        cache.insert_owned(1, buffer(10), 10, Admission::Ahead, 'A');
        assert_eq!(cache.held_by(&'A'), Some(10));
        for key in 10..17 {
            read_as(&mut cache, 'A', key, 10);
        }
        assert_eq!(cache.held_by(&'A'), Some(80));

        for key in [2, 3, 4] {
            cache.insert_owned(key, buffer(10), 10, Admission::Ahead, 'A');
            assert_owned_bounds(&cache);
        }

        assert_eq!(owned_keys_in(&cache, Place::Ahead), [4]);
        assert_eq!(cache.held_by(&'A'), Some(80));
    }

    #[test]
    fn given_random_reads_of_buffers_of_many_sizes_when_run_should_keep_every_bound() {
        let mut random = StdRng::seed_from_u64(0x1667);
        let mut cache = ByteCache::new(1_000);
        for step in 0..50_000_u32 {
            let key = if random.random_bool(0.6) {
                random.random_range(0..20)
            } else {
                random.random_range(0..400)
            };
            // One size per key, as for a range of an object that never changes.
            let len = 1 + (key as usize * 37) % 300;
            match random.random_range(0..12) {
                0 => cache.insert(key, buffer(len), len, Admission::Ahead),
                1 => cache.remove(&key),
                2 => cache.insert(key, buffer(len), len, Admission::Held),
                3 => cache.release(&key),
                _ => {
                    read_into(&mut cache, key, len);
                }
            }
            if step % 5_000 == 0 {
                cache.set_budget(random.random_range(300..2_000));
            }
            assert_owned_bounds(&cache);
        }
    }

    /// Owners with and without limits that come and go: every bound holds
    /// after each step, and an owner's insert evicts no other owner's bytes
    /// while the budget has room for it.
    #[test]
    fn given_random_reads_of_owners_with_limits_when_run_should_keep_every_bound_and_leave_others_alone()
     {
        let mut random = StdRng::seed_from_u64(0x0768);
        let mut cache = ByteCache::<u32, u8>::new(1_000);
        for step in 0..50_000_u32 {
            let key = if random.random_bool(0.6) {
                random.random_range(0..20)
            } else {
                random.random_range(0..400)
            };
            let len = 1 + (key as usize * 37) % 300;
            // One owner per key, as for the blocks of a partition's objects.
            let owner = u8::try_from(key % 5).unwrap();
            let fits = cache.held() + len <= cache.budget()
                && cache.limit(&owner).is_none_or(|limit| len <= limit);
            let others: Vec<(u32, bool)> = cache
                .entries
                .iter()
                .filter(|(_, entry)| entry.bytes.is_some() && entry.owner != Some(owner))
                .map(|(key, entry)| (*key, entry.place == Place::Ahead))
                .collect();
            let mut ahead_insert = false;
            let owned_insert = match random.random_range(0..14) {
                0 => {
                    cache.insert_owned(key, buffer(len), len, Admission::Ahead, owner);
                    ahead_insert = true;
                    true
                }
                1 => {
                    cache.remove(&key);
                    false
                }
                2 => {
                    cache.insert(key, buffer(len), len, Admission::Held);
                    false
                }
                3 => {
                    cache.release(&key);
                    false
                }
                4 => {
                    let limit = random.random_bool(0.8).then(|| random.random_range(0..600));
                    cache.set_limit(owner, limit);
                    false
                }
                5 if random.random_bool(0.1) => {
                    cache.remove_owner(&owner);
                    false
                }
                6 if random.random_bool(0.05) => {
                    let dropped = key % 7;
                    cache.retain(|key| key % 7 != dropped);
                    false
                }
                _ => match cache.read(&key) {
                    Lookup::Miss => {
                        cache.insert_owned(key, buffer(len), len, Admission::Read, owner);
                        true
                    }
                    Lookup::Hit(_) | Lookup::AheadHit(_) => false,
                },
            };
            if owned_insert && fits {
                // Readahead past its share of the budget goes, the oldest
                // first, whoever fetched it.
                let kept = others
                    .into_iter()
                    .filter(|(_, ahead)| !(ahead_insert && *ahead));
                for (other, _) in kept {
                    assert!(cache.holds(&other), "step {step}: {other} left for {key}");
                }
            }
            if step % 5_000 == 0 {
                cache.set_budget(random.random_range(300..2_000));
            }
            assert_owned_bounds(&cache);
        }
    }
}
