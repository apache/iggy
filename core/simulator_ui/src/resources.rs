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

use std::collections::VecDeque;
use std::f32::consts::PI;

use bevy::prelude::*;

use crate::bridge::UiSimulator;
use crate::theme::*;
use crate::tracing_layer::RawLineBuffer;
use crate::types::Status;

pub(crate) struct SimulationState {
    pub(crate) simulator: UiSimulator,
    pub(crate) playing: bool,
    pub(crate) speed: f32,
    pub(crate) tick_accumulator: f32,
    pub(crate) total_commits: u64,
    pub(crate) ops_per_second: f64,
    pub(crate) ops_window_start: f64,
    pub(crate) ops_window_count: u64,
    pub(crate) frame_count: u64,
}

#[derive(Resource)]
pub(crate) struct ReplicaPositions(pub(crate) Vec<Vec2>);

impl Default for ReplicaPositions {
    fn default() -> Self {
        let mut positions = Vec::with_capacity(REPLICA_COUNT as usize);
        for idx in 0..REPLICA_COUNT {
            let angle = 2.0 * PI * (idx as f32) / (REPLICA_COUNT as f32) - PI / 2.0;
            positions.push(Vec2::new(
                CIRCLE_RADIUS * angle.cos(),
                CIRCLE_RADIUS * angle.sin() + WORLD_CENTER_Y,
            ));
        }
        Self(positions)
    }
}

#[derive(Resource)]
pub(crate) struct ReplicaFxState {
    pub(crate) kill: [f32; REPLICA_COUNT as usize],
    pub(crate) revive: [f32; REPLICA_COUNT as usize],
    pub(crate) healthy: [f32; REPLICA_COUNT as usize],
    pub(crate) last_alive: [bool; REPLICA_COUNT as usize],
    pub(crate) last_status: [Status; REPLICA_COUNT as usize],
    pub(crate) callout_timer: [f32; REPLICA_COUNT as usize],
    pub(crate) callout_text: [String; REPLICA_COUNT as usize],
    pub(crate) callout_color: [Color; REPLICA_COUNT as usize],
}

impl Default for ReplicaFxState {
    fn default() -> Self {
        Self {
            kill: [0.0; REPLICA_COUNT as usize],
            revive: [0.0; REPLICA_COUNT as usize],
            healthy: [0.0; REPLICA_COUNT as usize],
            last_alive: [true; REPLICA_COUNT as usize],
            last_status: [Status::Normal; REPLICA_COUNT as usize],
            callout_timer: [0.0; REPLICA_COUNT as usize],
            callout_text: std::array::from_fn(|_| String::new()),
            callout_color: [COOL_WHITE; REPLICA_COUNT as usize],
        }
    }
}

#[derive(Resource, Default)]
pub(crate) struct AppFxState {
    pub(crate) pulse: [f32; 4],
}

#[derive(Clone)]
pub(crate) struct EventLogEntry {
    pub(crate) tick: u64,
    pub(crate) icon: &'static str,
    pub(crate) headline: String,
    pub(crate) detail: String,
    pub(crate) color: Color,
}

#[derive(Resource)]
pub(crate) struct EventLog {
    pub(crate) entries: VecDeque<EventLogEntry>,
    pub(crate) visible: bool,
    pub(crate) show_details: bool,
    pub(crate) generation: u64,
    pub(crate) last_rendered: u64,
    pub(crate) slide: f32,
}

impl Default for EventLog {
    fn default() -> Self {
        Self {
            entries: VecDeque::with_capacity(EVENT_LOG_MAX),
            visible: false,
            show_details: false,
            generation: 0,
            last_rendered: 0,
            slide: 0.0,
        }
    }
}

impl EventLog {
    pub(crate) fn push(&mut self, entry: EventLogEntry) {
        if self.entries.len() >= EVENT_LOG_MAX {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
        self.generation += 1;
    }
}

#[derive(Resource)]
pub(crate) struct GameConsole {
    pub(crate) raw_lines: RawLineBuffer,
    pub(crate) open: bool,
    pub(crate) slide: f32,
    pub(crate) last_line_count: usize,
}

#[derive(Resource, Default)]
pub(crate) struct ScreenFlash {
    pub(crate) timer: f32,
    pub(crate) color: Color,
    pub(crate) intensity: f32,
}

#[derive(Resource, Default)]
pub(crate) struct TrackPulse {
    pub(crate) energy: f32,
}
