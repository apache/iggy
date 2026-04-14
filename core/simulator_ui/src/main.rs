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

mod bridge;
mod components;
mod helpers;
mod input;
mod panels;
mod queries;
mod resources;
mod setup;
mod simulation;
mod theme;
mod tracing_layer;
mod types;
mod util;
mod visuals;

use bevy::asset::AssetPlugin;
use bevy::prelude::*;
use bevy_prototype_lyon::prelude::*;

use bridge::UiSimulator;
use resources::*;
use theme::*;
use tracing_layer::EventBuffer;

#[derive(States, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(crate) enum AppPhase {
    #[default]
    Selecting,
    Simulating,
}

fn main() {
    use tracing_layer::SimEventLayer;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    let event_buffer = EventBuffer::default();
    let raw_lines = tracing_layer::RawLineBuffer::default();
    let raw_generation = tracing_layer::RawLineGeneration::default();

    let fmt_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,iggy.sim=debug"));
    let sim_filter = EnvFilter::new("off,iggy.sim=debug");

    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_ansi(true)
        .compact()
        .with_filter(fmt_filter);

    tracing_subscriber::registry()
        .with(
            SimEventLayer::new(
                event_buffer.clone(),
                raw_lines.clone(),
                raw_generation.clone(),
            )
            .with_filter(sim_filter),
        )
        .with(fmt_layer)
        .init();

    iggy_common::MemoryPool::init_pool(&iggy_common::MemoryPoolConfigOther {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });

    // Placeholder simulation — overwritten in OnEnter(Simulating) by reinit_simulation
    let placeholder_sim = SimulationState {
        simulator: UiSimulator::new(DEFAULT_REPLICA_COUNT, 42, event_buffer.clone()),
        playing: false,
        speed: 1.0,
        tick_accumulator: 0.0,
        total_commits: 0,
        ops_per_second: 0.0,
        ops_window_start: 0.0,
        ops_window_count: 0,
        frame_count: 0,
    };

    App::new()
        .add_plugins(
            DefaultPlugins
                .build()
                .disable::<bevy::log::LogPlugin>()
                .set(WindowPlugin {
                    primary_window: Some(Window {
                        title: "VSR Simulator".to_string(),
                        resolution: (1400, 900).into(),
                        canvas: Some("#bevy-canvas".to_string()),
                        ..default()
                    }),
                    ..default()
                })
                .set(AssetPlugin {
                    file_path: "../../assets".to_string(),
                    ..default()
                }),
        )
        .add_plugins(ShapePlugin)
        .init_state::<AppPhase>()
        .insert_resource(ClearColor(BG_DARK))
        .insert_non_send_resource(placeholder_sim)
        .insert_resource(ReplicaConfig {
            count: DEFAULT_REPLICA_COUNT,
        })
        .insert_resource(SelectionState::default())
        .insert_resource(SharedBuffers { event_buffer })
        .insert_resource(ReplicaPositions::default())
        .insert_resource(ReplicaFxState::default())
        .insert_resource(AppFxState::default())
        .insert_resource(ScreenFlash::default())
        .insert_resource(TrackPulse::default())
        .insert_resource(EventLog::default())
        .insert_resource(GameConsole {
            raw_lines,
            raw_generation,
            open: false,
            slide: 0.0,
            last_seen_generation: 0,
        })
        .add_systems(Startup, setup::setup_camera)
        .add_systems(Startup, setup::setup_background.after(setup::setup_camera))
        .add_systems(OnEnter(AppPhase::Selecting), setup::setup_selection_screen)
        .add_systems(
            Update,
            (
                input::handle_selection_input,
                setup::update_selection_screen,
            )
                .run_if(in_state(AppPhase::Selecting)),
        )
        .add_systems(OnExit(AppPhase::Selecting), setup::despawn_selection_screen)
        .add_systems(OnEnter(AppPhase::Simulating), setup::reinit_simulation)
        .add_systems(
            OnEnter(AppPhase::Simulating),
            (setup::setup_simulation_world, setup::setup_hud).after(setup::reinit_simulation),
        )
        .add_systems(
            Update,
            input::handle_keyboard_input.run_if(in_state(AppPhase::Simulating)),
        )
        .add_systems(Update, simulation::tick_simulation)
        .add_systems(
            Update,
            visuals::update_replica_visuals.run_if(in_state(AppPhase::Simulating)),
        )
        .add_systems(
            Update,
            visuals::update_hud_text.run_if(in_state(AppPhase::Simulating)),
        )
        .add_systems(
            Update,
            visuals::update_pause_overlay.run_if(in_state(AppPhase::Simulating)),
        )
        .add_systems(
            Update,
            visuals::update_app_visuals.run_if(in_state(AppPhase::Simulating)),
        )
        .add_systems(
            Update,
            visuals::update_app_flow_particles.run_if(in_state(AppPhase::Simulating)),
        )
        .add_systems(Update, visuals::update_message_particles)
        .add_systems(Update, visuals::update_trail_dots)
        .add_systems(
            Update,
            visuals::update_link_lines.run_if(in_state(AppPhase::Simulating)),
        )
        .add_systems(Update, visuals::update_scan_lines)
        .add_systems(Update, visuals::update_commit_rings)
        .add_systems(Update, visuals::update_ambient_particles)
        .add_systems(Update, visuals::update_screen_flash)
        .add_systems(Update, visuals::update_speed_lines)
        .add_systems(
            Update,
            visuals::update_track_ring.run_if(in_state(AppPhase::Simulating)),
        )
        .add_systems(Update, visuals::update_paw_bursts)
        .add_systems(Update, visuals::update_lightning)
        .add_systems(Update, panels::update_event_log_panel)
        .add_systems(Update, panels::update_game_console)
        .run();
}
