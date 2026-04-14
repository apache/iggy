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

use std::f32::consts::PI;

use bevy::prelude::*;
use bevy_prototype_lyon::prelude::*;

use crate::components::*;
use crate::helpers::*;
use crate::queries::FxParams;
use crate::resources::*;
use crate::theme::*;
use crate::tracing_layer::CapturedSimEvent;
use crate::types::{MessageType, Status};

pub(crate) fn tick_simulation(
    mut commands: Commands,
    time: Res<Time>,
    mut sim: NonSendMut<SimulationState>,
    config: Res<ReplicaConfig>,
    positions: Res<ReplicaPositions>,
    mut fx: FxParams,
    mut event_log: ResMut<EventLog>,
) {
    let elapsed_secs = time.elapsed_secs_f64();
    if elapsed_secs - sim.ops_window_start >= 1.0 {
        sim.ops_per_second = sim.ops_window_count as f64 / (elapsed_secs - sim.ops_window_start);
        sim.ops_window_start = elapsed_secs;
        sim.ops_window_count = 0;
    }

    if !sim.playing {
        return;
    }

    let replica_count = config.count;
    let delta = time.delta_secs();
    sim.tick_accumulator += delta * sim.speed * 60.0;

    let ticks_to_run = sim.tick_accumulator as u32;
    sim.tick_accumulator -= ticks_to_run as f32;

    for _ in 0..ticks_to_run.min(10) {
        let tick = sim.simulator.tick;
        let events = sim.simulator.step();

        for event in &events {
            if let Some(entry) = narrate_event(event, tick, replica_count) {
                event_log.push(entry);
            }
            match event.sim_event.as_str() {
                "ControlMessageScheduled" => {
                    spawn_control_message_particles(
                        &mut commands,
                        event,
                        &positions,
                        replica_count,
                    );
                }
                "PrepareQueued" => {
                    spawn_prepare_particles(&mut commands, event, &positions, replica_count);
                }
                "OperationCommitted" => {
                    handle_commit_event(
                        &mut commands,
                        event,
                        &mut sim,
                        &positions,
                        &mut fx.replica_fx,
                        &mut fx.app_fx,
                        &mut fx.track_pulse,
                    );
                }
                "ViewChangeStarted" => {
                    handle_view_change_event(
                        &mut commands,
                        event,
                        &mut sim,
                        &positions,
                        &mut fx.replica_fx,
                        &mut fx.app_fx,
                        &mut fx.screen_flash,
                        replica_count,
                    );
                }
                "ClientRequestReceived" => {
                    sim.ops_window_count += 1;
                    fx.app_fx.pulse[0] = 0.7;
                }
                "PrimaryElected" => {
                    handle_primary_elected_event(
                        &mut commands,
                        event,
                        &positions,
                        &mut fx.replica_fx,
                        &mut fx.screen_flash,
                    );
                }
                "ReplicaStateChanged" => {
                    let replica_id = event.replica_id.unwrap_or(0) as u8;
                    let new_status = event
                        .status
                        .as_deref()
                        .map(Status::from_str)
                        .unwrap_or(Status::Normal);
                    let (label, color, duration) = match new_status {
                        Status::Normal => ("RUNNING!", NEON_CYAN, 0.85),
                        Status::ViewChange => ("HOWLING!", NEON_MAGENTA, 1.1),
                        Status::Recovering => ("LIMPING...", NEON_YELLOW, 1.1),
                    };
                    trigger_replica_callout(&mut fx.replica_fx, replica_id, label, color, duration);
                }
                _ => {}
            }
        }
    }
}

pub(crate) fn spawn_control_message_particles(
    commands: &mut Commands,
    event: &CapturedSimEvent,
    positions: &ReplicaPositions,
    replica_count: u8,
) {
    let action = event.action.as_deref().unwrap_or("");
    let msg_type = MessageType::from_action(action).unwrap_or(MessageType::Prepare);
    let from_idx = event.replica_id.unwrap_or(0) as usize;
    let to_idx = event.target_replica.unwrap_or(0) as usize;

    if from_idx >= replica_count as usize || to_idx >= replica_count as usize {
        return;
    }

    let from_pos = positions.0[from_idx];
    let to_pos = positions.0[to_idx];
    let control = perpendicular_control(from_pos, to_pos, 40.0);
    let msg_color = msg_type.color();

    commands.spawn((
        MessageParticle {
            from: from_pos,
            to: to_pos,
            control,
            progress: 0.0,
            duration: MSG_DURATION,
            msg_type,
        },
        build_fill_stroke(
            &circle_shape(4.0),
            msg_color,
            msg_color.with_alpha(0.3),
            1.0,
        ),
        Transform::from_xyz(from_pos.x, from_pos.y, 5.0),
    ));

    commands.spawn((
        MessageGlow,
        MessageParticle {
            from: from_pos,
            to: to_pos,
            control,
            progress: 0.0,
            duration: MSG_DURATION,
            msg_type,
        },
        build_fill(&circle_shape(8.0), msg_color.with_alpha(0.2)),
        Transform::from_xyz(from_pos.x, from_pos.y, 4.9),
    ));
}

pub(crate) fn spawn_prepare_particles(
    commands: &mut Commands,
    event: &CapturedSimEvent,
    positions: &ReplicaPositions,
    replica_count: u8,
) {
    let from_idx = event.replica_id.unwrap_or(0) as usize;
    if from_idx >= replica_count as usize {
        return;
    }

    let from_pos = positions.0[from_idx];
    let msg_color = MessageType::Prepare.color();

    for to_idx in 0..replica_count as usize {
        if to_idx == from_idx {
            continue;
        }
        let to_pos = positions.0[to_idx];
        let control = perpendicular_control(from_pos, to_pos, 40.0);

        commands.spawn((
            MessageParticle {
                from: from_pos,
                to: to_pos,
                control,
                progress: 0.0,
                duration: MSG_DURATION,
                msg_type: MessageType::Prepare,
            },
            build_fill_stroke(
                &circle_shape(4.0),
                msg_color,
                msg_color.with_alpha(0.3),
                1.0,
            ),
            Transform::from_xyz(from_pos.x, from_pos.y, 5.0),
        ));

        commands.spawn((
            MessageGlow,
            MessageParticle {
                from: from_pos,
                to: to_pos,
                control,
                progress: 0.0,
                duration: MSG_DURATION,
                msg_type: MessageType::Prepare,
            },
            build_fill(&circle_shape(8.0), msg_color.with_alpha(0.2)),
            Transform::from_xyz(from_pos.x, from_pos.y, 4.9),
        ));
    }
}

pub(crate) fn handle_commit_event(
    commands: &mut Commands,
    event: &CapturedSimEvent,
    sim: &mut NonSendMut<SimulationState>,
    positions: &ReplicaPositions,
    replica_fx: &mut ResMut<ReplicaFxState>,
    app_fx: &mut ResMut<AppFxState>,
    track_pulse: &mut ResMut<TrackPulse>,
) {
    let replica_id = event.replica_id.unwrap_or(0) as u8;
    sim.total_commits += 1;
    sim.ops_window_count += 1;
    replica_fx.healthy[replica_id as usize] = 0.45;
    trigger_replica_callout(replica_fx, replica_id, "GOOD BOY!", IGGY_ORANGE, 0.9);
    app_fx.pulse[2] = 0.55;
    track_pulse.energy = (track_pulse.energy + 0.4).min(1.0);
    commands.spawn(GlowFlash {
        id: replica_id,
        timer: 0.3,
        flash_color: IGGY_ORANGE,
    });

    let center = positions.0[replica_id as usize];
    for ring_idx in 0..3u8 {
        let delay_factor = ring_idx as f32 * 0.08;
        commands.spawn((
            CommitRing {
                center,
                lifetime: COMMIT_RING_LIFETIME + delay_factor,
                max_lifetime: COMMIT_RING_LIFETIME + delay_factor,
                color: if ring_idx == 0 {
                    IGGY_ORANGE
                } else {
                    NEON_CYAN
                },
                max_radius: 120.0 + ring_idx as f32 * 40.0,
            },
            build_stroke(
                &circle_shape(1.0),
                IGGY_ORANGE.with_alpha(0.6),
                2.5 - ring_idx as f32 * 0.5,
            ),
            Transform::from_xyz(center.x, center.y, 4.5 - ring_idx as f32 * 0.1),
        ));
    }

    let paw_count = 6 + (sim.frame_count % 3) as usize;
    for paw_idx in 0..paw_count {
        let angle =
            (paw_idx as f32 / paw_count as f32) * PI * 2.0 + (sim.frame_count as f32 * 0.37);
        let speed = 120.0 + (paw_idx as f32 * 37.3).fract() * 80.0;
        let velocity = Vec2::new(angle.cos() * speed, angle.sin() * speed);
        commands.spawn((
            PawBurst {
                velocity,
                lifetime: 0.4,
                max_lifetime: 0.4,
            },
            build_fill(&circle_shape(3.0), IGGY_ORANGE.with_alpha(0.8)),
            Transform::from_xyz(center.x, center.y, 5.5),
        ));
    }

    if sim.total_commits == 0 || !sim.total_commits.is_multiple_of(10) {
        return;
    }

    for celebration_idx in 0..5u8 {
        let delay = celebration_idx as f32 * 0.06;
        let max_radius = 200.0 + celebration_idx as f32 * 37.5;
        let color = if celebration_idx % 2 == 0 {
            IGGY_ORANGE
        } else {
            NEON_CYAN
        };
        commands.spawn((
            CommitRing {
                center,
                lifetime: 0.9 + delay,
                max_lifetime: 0.9 + delay,
                color,
                max_radius,
            },
            build_stroke(
                &circle_shape(1.0),
                color.with_alpha(0.7),
                3.0 - celebration_idx as f32 * 0.3,
            ),
            Transform::from_xyz(center.x, center.y, 4.6 - celebration_idx as f32 * 0.05),
        ));
    }

    let celebration_paw_count = 12 + (sim.frame_count % 5) as usize;
    for paw_idx in 0..celebration_paw_count {
        let angle = (paw_idx as f32 / celebration_paw_count as f32) * PI * 2.0
            + (sim.frame_count as f32 * 0.13);
        let speed = 160.0 + (paw_idx as f32 * 23.7).fract() * 140.0;
        let velocity = Vec2::new(angle.cos() * speed, angle.sin() * speed);
        let color = if paw_idx % 2 == 0 {
            IGGY_ORANGE
        } else {
            NEON_CYAN
        };
        commands.spawn((
            PawBurst {
                velocity,
                lifetime: 0.5,
                max_lifetime: 0.5,
            },
            build_fill(&circle_shape(4.0), color.with_alpha(0.9)),
            Transform::from_xyz(center.x, center.y, 5.6),
        ));
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_view_change_event(
    commands: &mut Commands,
    event: &CapturedSimEvent,
    sim: &mut NonSendMut<SimulationState>,
    positions: &ReplicaPositions,
    replica_fx: &mut ResMut<ReplicaFxState>,
    app_fx: &mut ResMut<AppFxState>,
    screen_flash: &mut ResMut<ScreenFlash>,
    replica_count: u8,
) {
    let replica_id = event.replica_id.unwrap_or(0) as u8;
    trigger_replica_callout(replica_fx, replica_id, "PACK SHUFFLE!", NEON_MAGENTA, 1.15);
    app_fx.pulse[3] = 0.8;
    screen_flash.timer = SCREEN_FLASH_DURATION;
    screen_flash.color = NEON_MAGENTA;
    screen_flash.intensity = 0.12;
    for rid in 0..replica_count {
        commands.spawn(GlowFlash {
            id: rid,
            timer: 0.3,
            flash_color: NEON_MAGENTA,
        });

        if rid != replica_id {
            continue;
        }

        let center = positions.0[rid as usize];
        commands.spawn((
            CommitRing {
                center,
                lifetime: 0.9,
                max_lifetime: 0.9,
                color: NEON_MAGENTA,
                max_radius: 200.0,
            },
            build_stroke(&circle_shape(1.0), NEON_MAGENTA.with_alpha(0.5), 3.0),
            Transform::from_xyz(center.x, center.y, 4.5),
        ));
    }

    let bolt_count = 3 + (sim.frame_count % 2) as usize;
    for bolt_idx in 0..bolt_count {
        let from_idx = bolt_idx % replica_count as usize;
        let to_idx = (bolt_idx + 1) % replica_count as usize;
        let from_pos = positions.0[from_idx];
        let to_pos = positions.0[to_idx];
        let segment_count = 3 + (bolt_idx % 2);
        let mut path = ShapePath::new().move_to(from_pos);
        for segment_idx in 1..segment_count {
            let frac = segment_idx as f32 / segment_count as f32;
            let mid = from_pos.lerp(to_pos, frac);
            let dir = to_pos - from_pos;
            let perp = Vec2::new(-dir.y, dir.x).normalize_or_zero();
            let seed = sim
                .frame_count
                .wrapping_mul(17)
                .wrapping_add(bolt_idx as u64 * 31)
                .wrapping_add(segment_idx as u64 * 53);
            let jitter = ((seed as f32 * 0.618).fract() - 0.5) * 80.0;
            path = path.line_to(mid + perp * jitter);
        }
        path = path.line_to(to_pos);
        commands.spawn((
            Lightning {
                lifetime: 0.5,
                max_lifetime: 0.5,
            },
            build_path_stroke(&path, NEON_MAGENTA.with_alpha(0.8), 2.0),
            Transform::from_xyz(0.0, 0.0, 6.0),
        ));
    }
}

pub(crate) fn handle_primary_elected_event(
    commands: &mut Commands,
    event: &CapturedSimEvent,
    positions: &ReplicaPositions,
    replica_fx: &mut ResMut<ReplicaFxState>,
    screen_flash: &mut ResMut<ScreenFlash>,
) {
    let replica_id = event.replica_id.unwrap_or(0) as u8;
    trigger_replica_callout(replica_fx, replica_id, "NEW LEAD DOG!", NEON_YELLOW, 1.2);
    screen_flash.timer = SCREEN_FLASH_DURATION * 0.7;
    screen_flash.color = NEON_YELLOW;
    screen_flash.intensity = 0.08;
    let center = positions.0[replica_id as usize];
    for ring_idx in 0..2u8 {
        commands.spawn((
            CommitRing {
                center,
                lifetime: 0.8,
                max_lifetime: 0.8,
                color: NEON_YELLOW,
                max_radius: 150.0 + ring_idx as f32 * 50.0,
            },
            build_stroke(&circle_shape(1.0), NEON_YELLOW.with_alpha(0.45), 2.5),
            Transform::from_xyz(center.x, center.y, 4.4),
        ));
    }
}
