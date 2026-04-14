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

use bevy::prelude::*;

use crate::types::{Role, Status};

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub(crate) enum VocabMode {
    #[default]
    Dog,
    Technical,
}

#[derive(Resource)]
pub(crate) struct Vocab {
    pub(crate) mode: VocabMode,
}

impl Default for Vocab {
    fn default() -> Self {
        Self {
            mode: VocabMode::Dog,
        }
    }
}

#[allow(dead_code)]
impl VocabMode {
    pub(crate) fn node_name(self, id: u8) -> &'static str {
        match self {
            Self::Dog => match id {
                0 => "Iggy",
                1 => "Zippy",
                2 => "Dash",
                3 => "Bolt",
                4 => "Flash",
                5 => "Storm",
                6 => "Blaze",
                _ => "Pup",
            },
            Self::Technical => match id {
                0 => "R0",
                1 => "R1",
                2 => "R2",
                3 => "R3",
                4 => "R4",
                5 => "R5",
                6 => "R6",
                _ => "RN",
            },
        }
    }

    pub(crate) fn primary(self) -> &'static str {
        match self {
            Self::Dog => "LEAD DOG",
            Self::Technical => "PRIMARY",
        }
    }

    pub(crate) fn backup(self) -> &'static str {
        match self {
            Self::Dog => "PACK DOG",
            Self::Technical => "BACKUP",
        }
    }

    pub(crate) fn role_label(self, role: Role, status: Status) -> &'static str {
        match self {
            Self::Dog => match (role, status) {
                (Role::Primary, Status::Normal) => "LEAD DOG / running",
                (Role::Primary, Status::ViewChange) => "LEAD DOG / howling",
                (Role::Primary, Status::Recovering) => "LEAD DOG / limping",
                (Role::Backup, Status::Normal) => "PACK DOG / running",
                (Role::Backup, Status::ViewChange) => "PACK DOG / howling",
                (Role::Backup, Status::Recovering) => "PACK DOG / limping",
            },
            Self::Technical => match (role, status) {
                (Role::Primary, Status::Normal) => "PRIMARY / normal",
                (Role::Primary, Status::ViewChange) => "PRIMARY / view-change",
                (Role::Primary, Status::Recovering) => "PRIMARY / recovering",
                (Role::Backup, Status::Normal) => "BACKUP / normal",
                (Role::Backup, Status::ViewChange) => "BACKUP / view-change",
                (Role::Backup, Status::Recovering) => "BACKUP / recovering",
            },
        }
    }

    pub(crate) fn callout_normal(self) -> &'static str {
        match self {
            Self::Dog => "RUNNING!",
            Self::Technical => "NORMAL",
        }
    }

    pub(crate) fn callout_view_change(self) -> &'static str {
        match self {
            Self::Dog => "HOWLING!",
            Self::Technical => "VIEW-CHANGE",
        }
    }

    pub(crate) fn callout_recovering(self) -> &'static str {
        match self {
            Self::Dog => "LIMPING...",
            Self::Technical => "RECOVERING",
        }
    }

    pub(crate) fn callout_crashed(self) -> &'static str {
        match self {
            Self::Dog => "NAPPING",
            Self::Technical => "CRASHED",
        }
    }

    pub(crate) fn callout_committed(self) -> &'static str {
        match self {
            Self::Dog => "GOOD BOY!",
            Self::Technical => "COMMITTED",
        }
    }

    pub(crate) fn callout_node_down(self) -> &'static str {
        match self {
            Self::Dog => "GREYHOUND DOWN",
            Self::Technical => "NODE CRASHED",
        }
    }

    pub(crate) fn callout_view_change_started(self) -> &'static str {
        match self {
            Self::Dog => "PACK SHUFFLE!",
            Self::Technical => "VIEW CHANGE",
        }
    }

    pub(crate) fn callout_primary_elected(self) -> &'static str {
        match self {
            Self::Dog => "NEW LEAD DOG!",
            Self::Technical => "PRIMARY ELECTED",
        }
    }

    pub(crate) fn callout_recovered(self) -> &'static str {
        match self {
            Self::Dog => "BACK ON TRACK!",
            Self::Technical => "NODE RECOVERED",
        }
    }

    pub(crate) fn callout_healed(self) -> &'static str {
        match self {
            Self::Dog => "HEALED!",
            Self::Technical => "RECOVERED",
        }
    }

    pub(crate) fn callout_fenced(self) -> &'static str {
        match self {
            Self::Dog => "FENCED!",
            Self::Technical => "PARTITIONED",
        }
    }

    pub(crate) fn hud_title(self) -> &'static str {
        match self {
            Self::Dog => "iggy::pack",
            Self::Technical => "vsr::cluster",
        }
    }

    pub(crate) fn hud_commits_label(self) -> &'static str {
        match self {
            Self::Dog => "TREATS",
            Self::Technical => "COMMITS",
        }
    }

    pub(crate) fn hud_ops_label(self) -> &'static str {
        match self {
            Self::Dog => "LAPS/S",
            Self::Technical => "OPS/S",
        }
    }

    pub(crate) fn hud_playing(self) -> &'static str {
        match self {
            Self::Dog => "RACING",
            Self::Technical => "RUNNING",
        }
    }

    pub(crate) fn hud_paused(self) -> &'static str {
        match self {
            Self::Dog => "RESTING",
            Self::Technical => "PAUSED",
        }
    }

    pub(crate) fn node_unit(self) -> &'static str {
        match self {
            Self::Dog => "GREYHOUNDS",
            Self::Technical => "REPLICAS",
        }
    }

    pub(crate) fn action_toggle(self) -> &'static str {
        match self {
            Self::Dog => "race/rest",
            Self::Technical => "start/pause",
        }
    }

    pub(crate) fn action_inject(self) -> &'static str {
        match self {
            Self::Dog => "throw ball",
            Self::Technical => "inject request",
        }
    }

    pub(crate) fn action_kill(self) -> &'static str {
        match self {
            Self::Dog => "trip dog",
            Self::Technical => "crash node",
        }
    }

    pub(crate) fn app_name(self, id: usize) -> &'static str {
        match self {
            Self::Dog => match id {
                0 => "BALL THROWER",
                1 => "BALL FETCHER",
                2 => "TREAT DISPENSER",
                _ => "TREAT GOBBLER",
            },
            Self::Technical => match id {
                0 => "PRODUCER A",
                1 => "CONSUMER A",
                2 => "PRODUCER B",
                _ => "CONSUMER B",
            },
        }
    }

    pub(crate) fn event_log_title(self) -> &'static str {
        match self {
            Self::Dog => "PACK ACTIVITY LOG",
            Self::Technical => "EVENT LOG",
        }
    }

    pub(crate) fn pause_title(self) -> &'static str {
        match self {
            Self::Dog => "RESTING",
            Self::Technical => "PAUSED",
        }
    }

    pub(crate) fn pause_hint(self) -> &'static str {
        match self {
            Self::Dog => "press SPACE to unleash",
            Self::Technical => "press SPACE to start",
        }
    }

    pub(crate) fn subtitle(self) -> &'static str {
        match self {
            Self::Dog => "Italian greyhounds racing to consensus",
            Self::Technical => "Viewstamped Replication consensus protocol",
        }
    }

    pub(crate) fn choose_label(self) -> &'static str {
        match self {
            Self::Dog => "CHOOSE YOUR PACK",
            Self::Technical => "SELECT REPLICA COUNT",
        }
    }

    pub(crate) fn pack_option_name(self, idx: usize) -> &'static str {
        match self {
            Self::Dog => match idx {
                0 => "TIGHT PACK",
                1 => "BALANCED",
                _ => "FULL PACK",
            },
            Self::Technical => match idx {
                0 => "MINIMAL",
                1 => "BALANCED",
                _ => "MAXIMUM",
            },
        }
    }

    pub(crate) fn confirm_action(self) -> &'static str {
        match self {
            Self::Dog => "unleash the pack!",
            Self::Technical => "start simulation",
        }
    }

    pub(crate) fn kill_headline(self, name: &str, id: u8) -> String {
        match self {
            Self::Dog => format!("{name} (R{id}) was tripped!"),
            Self::Technical => format!("Node R{id} crashed"),
        }
    }

    pub(crate) fn kill_detail(self, name: &str) -> String {
        match self {
            Self::Dog => format!(
                "{name} crashed! Network links disabled. The pack loses a member -- \
                 if quorum is lost, no new operations can commit."
            ),
            Self::Technical => format!(
                "Node {name} crashed. Network links disabled. \
                 If quorum is lost, commits will stall."
            ),
        }
    }

    pub(crate) fn heal_headline(self) -> &'static str {
        match self {
            Self::Dog => "Pack healed! All dogs back, all fences down.",
            Self::Technical => "All nodes recovered. Partitions cleared.",
        }
    }

    pub(crate) fn heal_detail(self) -> &'static str {
        match self {
            Self::Dog => {
                "All crashed replicas re-enabled, all network partitions cleared. \
                 The pack is whole again."
            }
            Self::Technical => {
                "All crashed replicas re-enabled. All network partitions cleared. \
                 Cluster is fully connected."
            }
        }
    }

    pub(crate) fn partition_headline(self, a: &str, b: &str) -> String {
        match self {
            Self::Dog => format!("Fence between {a} and {b}!"),
            Self::Technical => format!("Network partition: {a} <-> {b}"),
        }
    }

    pub(crate) fn partition_detail(self, a: &str, a_id: u8, b: &str, b_id: u8) -> String {
        match self {
            Self::Dog => format!(
                "Network partition between {a} (R{a_id}) and {b} (R{b_id}). \
                 Messages between them will be dropped. If this splits quorum, \
                 expect a view change."
            ),
            Self::Technical => format!(
                "Bidirectional partition between {a} (R{a_id}) and {b} (R{b_id}). \
                 Messages dropped. May trigger view change if quorum is split."
            ),
        }
    }

    pub(crate) fn mode_label(self) -> &'static str {
        match self {
            Self::Dog => "FUN",
            Self::Technical => "TECHNICAL",
        }
    }

    pub(crate) fn toggle(self) -> Self {
        match self {
            Self::Dog => Self::Technical,
            Self::Technical => Self::Dog,
        }
    }
}
