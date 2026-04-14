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
use std::collections::VecDeque;
use std::f32::consts::PI;

use crate::bridge::UiSimulator;
use crate::theme::*;
use crate::tracing_layer::{EventBuffer, RawLineBuffer, RawLineGeneration};
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
pub(crate) struct ReplicaConfig {
    pub(crate) count: u8,
}

#[derive(Resource, Clone)]
pub(crate) struct SharedBuffers {
    pub(crate) event_buffer: EventBuffer,
}

#[derive(Resource)]
pub(crate) struct ReplicaPositions(pub(crate) Vec<Vec2>);

impl ReplicaPositions {
    pub(crate) fn new(count: u8) -> Self {
        let mut positions = Vec::with_capacity(count as usize);
        for idx in 0..count {
            let angle = 2.0 * PI * (idx as f32) / (count as f32) - PI / 2.0;
            positions.push(Vec2::new(
                CIRCLE_RADIUS * angle.cos(),
                CIRCLE_RADIUS * angle.sin() + WORLD_CENTER_Y,
            ));
        }
        Self(positions)
    }
}

impl Default for ReplicaPositions {
    fn default() -> Self {
        Self::new(DEFAULT_REPLICA_COUNT)
    }
}

#[derive(Resource)]
pub(crate) struct ReplicaFxState {
    pub(crate) kill: Vec<f32>,
    pub(crate) revive: Vec<f32>,
    pub(crate) healthy: Vec<f32>,
    pub(crate) last_alive: Vec<bool>,
    pub(crate) last_status: Vec<Status>,
    pub(crate) callout_timer: Vec<f32>,
    pub(crate) callout_text: Vec<String>,
    pub(crate) callout_color: Vec<Color>,
}

impl ReplicaFxState {
    pub(crate) fn new(count: u8) -> Self {
        let n = count as usize;
        Self {
            kill: vec![0.0; n],
            revive: vec![0.0; n],
            healthy: vec![0.0; n],
            last_alive: vec![true; n],
            last_status: vec![Status::Normal; n],
            callout_timer: vec![0.0; n],
            callout_text: (0..n).map(|_| String::new()).collect(),
            callout_color: vec![COOL_WHITE; n],
        }
    }
}

impl Default for ReplicaFxState {
    fn default() -> Self {
        Self::new(DEFAULT_REPLICA_COUNT)
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
    pub(crate) raw_generation: RawLineGeneration,
    pub(crate) open: bool,
    pub(crate) slide: f32,
    pub(crate) last_seen_generation: u64,
}

pub(crate) const PACK_OPTIONS: [u8; 3] = [3, 5, 7];

#[derive(Resource)]
pub(crate) struct SelectionState {
    pub(crate) index: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for SelectionState {
    fn default() -> Self {
        Self { index: 0 }
    }
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
