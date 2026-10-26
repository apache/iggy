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

//! The k links to one peer form a bundle, which shard 0 tracks.
//!
//! Frames flow to a peer only while its bundle is Up, so a peer is reachable
//! over all k links or over none. When a link of the bundle closes, every
//! link closes and redials. The epoch tells the links that a teardown closes
//! from the links of the next bundle, so a late close never takes down a
//! newer bundle.
//!
//! A failed handshake changes nothing here: its link stays missing and the
//! next sweep dials it again. An inbound handshake is not authenticated
//! before it completes, so letting its failure tear a bundle down would let
//! anyone who reaches the replica port close a forming bundle. A bundle that
//! stays incomplete tears down after the forming limit.

use crate::replica::links::LinkClaim;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BundlePhase {
    /// No link of the current epoch is installed.
    Down,
    /// Some links of the current epoch are installed, the first at `since`.
    Forming { since: Instant },
    /// All links of the current epoch are installed. Frames flow.
    Up,
    /// Links of older epochs are closing. Nothing is dialed until they are gone.
    TearingDown,
}

#[derive(Clone, Copy, Debug)]
pub struct Bundle {
    epoch: u32,
    phase: BundlePhase,
}

/// What shard 0 does after the bundle of one peer moved.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BundleChange {
    /// The bundle left Forming or Up: frames to the peer stop.
    pub tore_down: bool,
    /// The bundle became Up: frames to the peer flow.
    pub came_up: bool,
    /// Installed links of other epochs, by link index. Each one must close.
    pub stale: Vec<(u16, LinkClaim)>,
}

impl Bundle {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            epoch: 0,
            phase: BundlePhase::Down,
        }
    }

    #[must_use]
    pub const fn epoch(&self) -> u32 {
        self.epoch
    }

    #[must_use]
    pub const fn phase(&self) -> BundlePhase {
        self.phase
    }

    /// Whether missing links of the current epoch may be dialed.
    #[must_use]
    pub const fn dials(&self) -> bool {
        matches!(self.phase, BundlePhase::Down | BundlePhase::Forming { .. })
    }

    /// A link placed in `epoch` closed. `claims` are the owner-table slots of
    /// the k links of this peer after the close.
    pub fn link_closed(
        &mut self,
        epoch: u32,
        claims: &[Option<LinkClaim>],
        now: Instant,
        forming_limit: Duration,
    ) -> BundleChange {
        let tore_down = epoch == self.epoch
            && matches!(self.phase, BundlePhase::Forming { .. } | BundlePhase::Up);
        if tore_down {
            self.tear_down();
        }
        let mut change = self.refresh(claims, now, forming_limit);
        change.tore_down |= tore_down;
        change
    }

    /// Move the phase to match `claims`, the owner-table slots of the k links
    /// of this peer.
    ///
    /// Runs on every handshake outcome and on every sweep, so an Up bundle
    /// that lost a link without a close report still tears down within one
    /// sweep.
    pub fn refresh(
        &mut self,
        claims: &[Option<LinkClaim>],
        now: Instant,
        forming_limit: Duration,
    ) -> BundleChange {
        let mut change = BundleChange::default();
        let complete = self.current_links(claims) == claims.len();
        let forming_expired = matches!(
            self.phase,
            BundlePhase::Forming { since } if now.saturating_duration_since(since) >= forming_limit
        );
        if !complete && (self.phase == BundlePhase::Up || forming_expired) {
            self.tear_down();
            change.tore_down = true;
        }
        change.stale = claims
            .iter()
            .zip(0u16..)
            .filter_map(|(claim, link)| {
                claim
                    .filter(|claim| claim.epoch != self.epoch)
                    .map(|claim| (link, claim))
            })
            .collect();
        if self.phase == BundlePhase::TearingDown && change.stale.is_empty() {
            self.phase = BundlePhase::Down;
        }
        let current = self.current_links(claims);
        if self.phase == BundlePhase::Down && current > 0 {
            self.phase = BundlePhase::Forming { since: now };
        }
        if matches!(self.phase, BundlePhase::Forming { .. }) && current == claims.len() {
            self.phase = BundlePhase::Up;
            change.came_up = true;
        }
        change
    }

    fn current_links(&self, claims: &[Option<LinkClaim>]) -> usize {
        claims
            .iter()
            .flatten()
            .filter(|claim| claim.epoch == self.epoch)
            .count()
    }

    const fn tear_down(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        self.phase = BundlePhase::TearingDown;
    }
}

impl Default for Bundle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: Duration = Duration::from_secs(10);
    const OWNER: u16 = 3;

    const fn claim(epoch: u32) -> LinkClaim {
        LinkClaim {
            owner: OWNER,
            epoch,
        }
    }

    fn up_bundle(now: Instant) -> Bundle {
        let mut bundle = Bundle::new();
        assert!(
            bundle
                .refresh(&[Some(claim(0)), Some(claim(0))], now, LIMIT)
                .came_up
        );
        bundle
    }

    #[test]
    fn bundle_is_up_only_with_every_link_of_its_epoch() {
        let now = Instant::now();
        let mut bundle = Bundle::new();
        let change = bundle.refresh(&[Some(claim(0)), None], now, LIMIT);
        assert!(!change.came_up);
        assert_eq!(bundle.phase(), BundlePhase::Forming { since: now });
        assert!(bundle.dials());
        let change = bundle.refresh(&[Some(claim(0)), Some(claim(0))], now, LIMIT);
        assert!(change.came_up);
        assert_eq!(bundle.phase(), BundlePhase::Up);
        assert!(!bundle.dials());
    }

    #[test]
    fn close_of_an_older_epoch_changes_nothing() {
        let now = Instant::now();
        let mut bundle = up_bundle(now);
        let change = bundle.link_closed(7, &[Some(claim(0)), Some(claim(0))], now, LIMIT);
        assert_eq!(change, BundleChange::default());
        assert_eq!(bundle.phase(), BundlePhase::Up);
    }

    #[test]
    fn close_in_the_current_epoch_tears_down_and_closes_the_rest() {
        let now = Instant::now();
        let mut bundle = up_bundle(now);
        let change = bundle.link_closed(0, &[Some(claim(0)), None], now, LIMIT);
        assert!(change.tore_down);
        assert_eq!(change.stale, vec![(0, claim(0))]);
        assert_eq!(bundle.epoch(), 1);
        assert_eq!(bundle.phase(), BundlePhase::TearingDown);
        assert!(!bundle.dials(), "no dial while old links close");

        let change = bundle.refresh(&[None, None], now, LIMIT);
        assert_eq!(change, BundleChange::default());
        assert_eq!(bundle.phase(), BundlePhase::Down);
    }

    #[test]
    fn up_bundle_that_lost_a_link_without_a_report_tears_down() {
        let now = Instant::now();
        let mut bundle = up_bundle(now);
        let change = bundle.refresh(&[None, Some(claim(0))], now, LIMIT);
        assert!(change.tore_down);
        assert_eq!(change.stale, vec![(1, claim(0))]);
    }

    #[test]
    fn forming_bundle_tears_down_past_the_limit() {
        let start = Instant::now();
        let mut bundle = Bundle::new();
        bundle.refresh(&[Some(claim(0)), None], start, LIMIT);
        let change = bundle.refresh(&[Some(claim(0)), None], start + LIMIT / 2, LIMIT);
        assert!(!change.tore_down);
        let change = bundle.refresh(&[Some(claim(0)), None], start + LIMIT, LIMIT);
        assert!(change.tore_down);
        assert_eq!(bundle.phase(), BundlePhase::TearingDown);
    }

    #[test]
    fn late_link_of_an_older_epoch_is_closed_and_blocks_up() {
        let now = Instant::now();
        let mut bundle = up_bundle(now);
        bundle.link_closed(0, &[None, None], now, LIMIT);
        assert_eq!(bundle.phase(), BundlePhase::Down);
        let change = bundle.refresh(&[Some(claim(1)), Some(claim(0))], now, LIMIT);
        assert!(!change.came_up);
        assert_eq!(change.stale, vec![(1, claim(0))]);
        assert_eq!(bundle.phase(), BundlePhase::Forming { since: now });
    }

    #[test]
    fn teardown_with_nothing_installed_goes_straight_down() {
        let now = Instant::now();
        let mut bundle = Bundle::new();
        bundle.refresh(&[Some(claim(0)), None], now, LIMIT);
        let change = bundle.link_closed(0, &[None, None], now, LIMIT);
        assert!(change.tore_down);
        assert_eq!(bundle.phase(), BundlePhase::Down);
        assert_eq!(bundle.epoch(), 1);
    }
}
