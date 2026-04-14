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

use crate::AppPhase;
use crate::components::*;
use crate::helpers::*;
use crate::queries::FxParams;
use crate::resources::*;
use crate::theme::*;
use crate::vocabulary::Vocab;

pub(crate) fn handle_selection_input(
    keys: Res<ButtonInput<KeyCode>>,
    mut state: ResMut<SelectionState>,
    mut config: ResMut<ReplicaConfig>,
    mut vocab: ResMut<Vocab>,
    mut next_state: ResMut<NextState<AppPhase>>,
) {
    if keys.just_pressed(KeyCode::ArrowLeft) && state.index > 0 {
        state.index -= 1;
    }
    if keys.just_pressed(KeyCode::ArrowRight) && state.index < PACK_OPTIONS.len() - 1 {
        state.index += 1;
    }

    if keys.just_pressed(KeyCode::Digit3) || keys.just_pressed(KeyCode::Numpad3) {
        state.index = 0;
    }
    if keys.just_pressed(KeyCode::Digit5) || keys.just_pressed(KeyCode::Numpad5) {
        state.index = 1;
    }
    if keys.just_pressed(KeyCode::Digit7) || keys.just_pressed(KeyCode::Numpad7) {
        state.index = 2;
    }

    if keys.just_pressed(KeyCode::Tab) {
        vocab.mode = vocab.mode.toggle();
    }

    if keys.just_pressed(KeyCode::Space) || keys.just_pressed(KeyCode::Enter) {
        config.count = PACK_OPTIONS[state.index];
        next_state.set(AppPhase::Simulating);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_keyboard_input(
    mut commands: Commands,
    keys: Res<ButtonInput<KeyCode>>,
    mut sim: NonSendMut<SimulationState>,
    mut event_log: ResMut<EventLog>,
    mut console: ResMut<GameConsole>,
    config: Res<ReplicaConfig>,
    positions: Res<ReplicaPositions>,
    vocab: Res<Vocab>,
    mut fx: FxParams,
) {
    let replica_count = config.count;

    if keys.just_pressed(KeyCode::Space) {
        sim.playing = !sim.playing;
    }

    if keys.just_pressed(KeyCode::ArrowRight) && !sim.playing {
        sim.simulator.step();
    }

    if keys.just_pressed(KeyCode::ArrowUp) {
        sim.speed = (sim.speed * 1.5).min(16.0);
    }
    if keys.just_pressed(KeyCode::ArrowDown) {
        sim.speed = (sim.speed / 1.5).max(0.1);
    }

    if keys.just_pressed(KeyCode::KeyR) {
        sim.simulator.inject_client_request();
        if !sim.playing {
            for _ in 0..50 {
                sim.simulator.step();
            }
        }
    }

    if keys.just_pressed(KeyCode::KeyK) {
        let states = sim.simulator.replica_states();
        for state in &states {
            if !state.alive {
                continue;
            }
            let name = vocab.mode.node_name(state.id);
            sim.simulator.kill_replica(state.id);
            fx.screen_flash.timer = SCREEN_FLASH_DURATION;
            fx.screen_flash.color = NEON_MAGENTA;
            fx.screen_flash.intensity = 0.10;
            fx.replica_fx.kill[state.id as usize] = 1.0;
            trigger_replica_callout(
                &mut fx.replica_fx,
                state.id,
                vocab.mode.callout_node_down(),
                NEON_MAGENTA,
                1.4,
            );
            event_log.push(EventLogEntry {
                tick: sim.simulator.tick,
                icon: "[KIL]",
                headline: vocab.mode.kill_headline(name, state.id),
                detail: vocab.mode.kill_detail(name),
                color: NEON_MAGENTA,
            });
            let center = positions.0[state.id as usize];
            commands.spawn((
                CommitRing {
                    center,
                    lifetime: 0.8,
                    max_lifetime: 0.8,
                    color: NEON_MAGENTA,
                    max_radius: 180.0,
                },
                build_stroke(&circle_shape(1.0), NEON_MAGENTA.with_alpha(0.5), 3.0),
                Transform::from_xyz(center.x, center.y, 4.5),
            ));
            break;
        }
    }

    if keys.just_pressed(KeyCode::KeyH) {
        sim.simulator.heal_all();
        fx.screen_flash.timer = SCREEN_FLASH_DURATION * 0.5;
        fx.screen_flash.color = NEON_CYAN;
        fx.screen_flash.intensity = 0.04;
        for replica_id in 0..replica_count {
            fx.replica_fx.revive[replica_id as usize] = 1.0;
            fx.replica_fx.healthy[replica_id as usize] = 0.6;
            trigger_replica_callout(
                &mut fx.replica_fx,
                replica_id,
                vocab.mode.callout_healed(),
                NEON_CYAN,
                1.2,
            );
            let center = positions.0[replica_id as usize];
            commands.spawn((
                CommitRing {
                    center,
                    lifetime: 0.6,
                    max_lifetime: 0.6,
                    color: NEON_CYAN,
                    max_radius: 110.0,
                },
                build_stroke(&circle_shape(1.0), NEON_CYAN.with_alpha(0.4), 2.0),
                Transform::from_xyz(center.x, center.y, 4.5),
            ));
        }
        event_log.push(EventLogEntry {
            tick: sim.simulator.tick,
            icon: "[HEL]",
            headline: vocab.mode.heal_headline().to_string(),
            detail: vocab.mode.heal_detail().to_string(),
            color: NEON_CYAN,
        });
    }

    if keys.just_pressed(KeyCode::KeyP) {
        let alive: Vec<u8> = (0..replica_count)
            .filter(|&replica_id| !sim.simulator.is_crashed(replica_id))
            .collect();

        if alive.len() >= 2 {
            let first = alive[0];
            let second = alive[1];
            sim.simulator.partition_link(first, second);
            let name_first = vocab.mode.node_name(first);
            let name_second = vocab.mode.node_name(second);
            fx.screen_flash.timer = SCREEN_FLASH_DURATION * 0.5;
            fx.screen_flash.color = NEON_MAGENTA;
            fx.screen_flash.intensity = 0.04;
            trigger_replica_callout(
                &mut fx.replica_fx,
                first,
                vocab.mode.callout_fenced(),
                NEON_MAGENTA,
                1.3,
            );
            trigger_replica_callout(
                &mut fx.replica_fx,
                second,
                vocab.mode.callout_fenced(),
                NEON_MAGENTA,
                1.3,
            );
            for &replica_id in &[first, second] {
                let center = positions.0[replica_id as usize];
                commands.spawn((
                    CommitRing {
                        center,
                        lifetime: 0.6,
                        max_lifetime: 0.6,
                        color: NEON_MAGENTA,
                        max_radius: 100.0,
                    },
                    build_stroke(&circle_shape(1.0), NEON_MAGENTA.with_alpha(0.35), 2.0),
                    Transform::from_xyz(center.x, center.y, 4.5),
                ));
            }
            event_log.push(EventLogEntry {
                tick: sim.simulator.tick,
                icon: "[FNC]",
                headline: vocab.mode.partition_headline(name_first, name_second),
                detail: vocab
                    .mode
                    .partition_detail(name_first, first, name_second, second),
                color: NEON_MAGENTA,
            });
        }
    }

    if keys.just_pressed(KeyCode::KeyE) {
        event_log.visible = !event_log.visible;
    }
    if keys.just_pressed(KeyCode::KeyD) {
        event_log.show_details = !event_log.show_details;
        event_log.last_rendered = 0;
    }

    if keys.just_pressed(KeyCode::Backquote) || keys.just_pressed(KeyCode::KeyT) {
        console.open = !console.open;
    }
}
