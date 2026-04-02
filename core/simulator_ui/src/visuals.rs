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
use crate::queries::*;
use crate::resources::*;
use crate::theme::*;
use crate::types::{Role, Status};

pub(crate) fn update_replica_visuals(
    (time, sim, positions, mut replica_fx, mut commands): (
        Res<Time>,
        NonSend<SimulationState>,
        Res<ReplicaPositions>,
        ResMut<ReplicaFxState>,
        Commands,
    ),
    mut flash_query: Query<(Entity, &mut GlowFlash)>,
    mut shape_visuals: ParamSet<(
        ReplicaShadowQuery,
        ReplicaGlowQuery,
        ReplicaOutlineQuery,
        ReplicaBodyQuery,
    )>,
    mut text_visuals: ParamSet<(
        ReplicaSleepQuery,
        ReplicaIdQuery,
        ReplicaRoleQuery,
        ReplicaCalloutQuery,
    )>,
) {
    let states = sim.simulator.replica_states();
    let elapsed = time.elapsed_secs();
    let delta = time.delta_secs();

    for idx in 0..REPLICA_COUNT as usize {
        replica_fx.kill[idx] = (replica_fx.kill[idx] - delta * 1.4).max(0.0);
        replica_fx.revive[idx] = (replica_fx.revive[idx] - delta * 1.1).max(0.0);
        replica_fx.healthy[idx] = (replica_fx.healthy[idx] - delta * 0.9).max(0.0);
        replica_fx.callout_timer[idx] = (replica_fx.callout_timer[idx] - delta).max(0.0);
    }

    let mut flash_overrides: [Option<Color>; REPLICA_COUNT as usize] =
        [None; REPLICA_COUNT as usize];
    for (entity, mut flash) in flash_query.iter_mut() {
        flash.timer -= delta;
        if flash.timer <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }

        let idx = flash.id as usize;
        if idx >= REPLICA_COUNT as usize {
            continue;
        }
        flash_overrides[idx] = Some(flash.flash_color);
    }

    for state in &states {
        let id = state.id;
        let idx = id as usize;
        let accent = status_color(state.status, state.alive);
        let base_pos = positions.0[idx];
        let is_primary = state.role == Role::Primary;
        let facing_right = base_pos.x < 0.0;

        if replica_fx.last_alive[idx] != state.alive {
            replica_fx.last_alive[idx] = state.alive;
            if state.alive {
                replica_fx.revive[idx] = 1.0;
                replica_fx.healthy[idx] = 0.6;
                trigger_replica_callout(&mut replica_fx, id, "BACK ON TRACK!", IGGY_ORANGE, 1.3);
            } else {
                replica_fx.kill[idx] = 1.0;
                trigger_replica_callout(&mut replica_fx, id, "GREYHOUND DOWN", NEON_MAGENTA, 1.4);
            }
        }

        if replica_fx.last_status[idx] != state.status {
            replica_fx.last_status[idx] = state.status;
            match state.status {
                Status::Normal => {
                    replica_fx.healthy[idx] = 0.5;
                    trigger_replica_callout(&mut replica_fx, id, "RUNNING!", NEON_CYAN, 0.8);
                }
                Status::Recovering => {
                    replica_fx.revive[idx] = 0.8;
                    trigger_replica_callout(&mut replica_fx, id, "LIMPING...", NEON_YELLOW, 1.1);
                }
                Status::ViewChange => {
                    trigger_replica_callout(&mut replica_fx, id, "HOWLING!", NEON_MAGENTA, 1.1);
                }
            }
        }

        let direction: f32 = if facing_right { 1.0 } else { -1.0 };

        let busyness = state.pipeline_depth as f32 / 8.0;
        let hover =
            (elapsed * (1.3 + busyness * 0.8) + id as f32 * 1.7).sin() * (7.0 + busyness * 4.0);
        let sway =
            (elapsed * (0.9 + busyness * 1.2) + id as f32 * 0.8).sin() * (0.06 + busyness * 0.04);
        let breathe = 1.0
            + (elapsed * (1.8 + busyness * 1.5) + id as f32 * 0.9).sin()
                * (0.035 + busyness * 0.025);
        let run_lean = busyness * 0.08 * direction;
        let run_stretch_x = 1.0 + busyness * 0.06;
        let run_squash_y = 1.0 - busyness * 0.03;

        let glow_color = flash_overrides[idx].unwrap_or(accent);
        let flash_energy = if flash_overrides[idx].is_some() {
            1.0
        } else {
            0.0
        };
        let glow_alpha = if is_primary && state.alive {
            let heartbeat_speed = 3.5 + busyness * 4.0;
            let heartbeat = (elapsed * heartbeat_speed).sin() * 0.5 + 0.5;
            0.16 + heartbeat * 0.16 + flash_energy * 0.18
        } else {
            0.12 + ((elapsed * 2.0 + id as f32 * 1.5).sin() * 0.5 + 0.5) * 0.08
                + flash_energy * 0.18
        };
        let body_alpha = if state.alive { 1.0 } else { 0.24 };
        let impact_scale = 1.0 + flash_energy * 0.08;
        let kill_energy = replica_fx.kill[idx];
        let revive_energy = replica_fx.revive[idx];
        let healthy_energy = replica_fx.healthy[idx];
        let fx_lift = revive_energy * 18.0 - kill_energy * 12.0;

        for (shadow, mut sprite, mut transform) in shape_visuals.p0().iter_mut() {
            if shadow.id != id {
                continue;
            }
            sprite.color = BG_DARK.with_alpha(0.22 + glow_alpha * 0.45 + kill_energy * 0.12);
            transform.translation.x = base_pos.x;
            transform.translation.y = base_pos.y - 88.0 + hover * 0.15 - fx_lift * 0.2;
            transform.scale = Vec3::new(
                1.0 + flash_energy * 0.12 + kill_energy * 0.18,
                0.9 - hover * 0.003 - revive_energy * 0.08,
                1.0,
            );
        }

        for (glow, mut shape, mut transform) in shape_visuals.p1().iter_mut() {
            if glow.id != id {
                continue;
            }
            let pulse_color = if kill_energy > 0.0 {
                NEON_MAGENTA
            } else if revive_energy > 0.0 || healthy_energy > 0.0 {
                IGGY_ORANGE
            } else {
                glow_color
            };
            shape.fill = Some(Fill::color(pulse_color.with_alpha(
                (glow_alpha + revive_energy * 0.18 + healthy_energy * 0.12) * body_alpha,
            )));
            transform.translation.x = base_pos.x;
            transform.translation.y = base_pos.y - 10.0 + hover * 0.45 + fx_lift * 0.35;
            transform.scale = Vec3::splat(
                1.0 + ((elapsed * 1.4 + id as f32).sin() * 0.03)
                    + flash_energy * 0.14
                    + revive_energy * 0.12
                    + healthy_energy * 0.08,
            );
        }

        for (outline, mut shape, mut transform) in shape_visuals.p2().iter_mut() {
            if outline.id != id {
                continue;
            }
            shape.stroke = Some(Stroke::new(
                (if kill_energy > 0.0 {
                    NEON_MAGENTA
                } else if revive_energy > 0.0 {
                    IGGY_ORANGE
                } else {
                    glow_color
                })
                .with_alpha((0.30 + flash_energy * 0.25 + healthy_energy * 0.18) * body_alpha),
                2.0 + flash_energy + revive_energy * 1.2,
            ));
            transform.translation.x = base_pos.x;
            transform.translation.y = base_pos.y - 4.0 + hover * 0.35 + fx_lift * 0.25;
            transform.rotation = Quat::from_rotation_z(-sway * 0.45 + kill_energy * 0.08);
        }

        for (body, mut sprite, mut transform) in shape_visuals.p3().iter_mut() {
            if body.id != id {
                continue;
            }
            sprite.color = if !state.alive {
                Color::srgba(0.55, 0.52, 0.60, 0.33)
            } else if revive_energy > 0.0 {
                Color::srgba(1.0, 0.96, 0.9, body_alpha)
            } else if is_primary {
                Color::srgba(1.0, 0.97, 0.94, body_alpha)
            } else {
                Color::srgba(0.94, 0.98, 1.0, body_alpha)
            };
            sprite.flip_x = !facing_right;
            transform.translation.x = base_pos.x;
            transform.translation.y = base_pos.y + 12.0 + hover + fx_lift - kill_energy * 14.0;
            transform.rotation = Quat::from_rotation_z(
                sway + run_lean + flash_energy * 0.04 * direction - kill_energy * 0.24 * direction,
            );
            transform.scale = Vec3::new(
                direction
                    * breathe
                    * impact_scale
                    * run_stretch_x
                    * (1.0 + revive_energy * 0.1 - kill_energy * 0.16),
                (breathe / impact_scale)
                    * run_squash_y
                    * (1.0 - kill_energy * 0.34 + healthy_energy * 0.08),
                1.0,
            );
        }

        for (sleep, mut transform, mut color, mut visibility) in text_visuals.p0().iter_mut() {
            if sleep.id != id {
                continue;
            }
            *visibility = if state.alive {
                Visibility::Hidden
            } else {
                Visibility::Inherited
            };
            let phase = (elapsed * 0.85 + sleep.layer as f32 * 0.55 + id as f32 * 0.4).fract();
            let rise = phase * 34.0;
            let drift = (phase * PI * 2.0).sin() * 7.0;
            transform.translation.x =
                base_pos.x + (70.0 + sleep.layer as f32 * 12.0) * direction + drift * direction;
            transform.translation.y = base_pos.y + 68.0 + rise + sleep.layer as f32 * 8.0;
            transform.scale = Vec3::splat(0.7 + phase * 0.55);
            *color = TextColor(COOL_WHITE.with_alpha((1.0 - phase) * 0.8 * kill_energy.max(0.65)));
        }

        for (id_text, mut color) in text_visuals.p1().iter_mut() {
            if id_text.id != id {
                continue;
            }
            *color = TextColor(if state.alive {
                COOL_WHITE
            } else {
                NEON_MAGENTA
            });
        }

        for (role_text, mut text, mut color) in text_visuals.p2().iter_mut() {
            if role_text.id != id {
                continue;
            }
            let label = if !state.alive {
                "NAPPING"
            } else {
                role_label(state.role, state.status)
            };
            **text = label.to_string();
            let status_pulse = ((elapsed * 4.2 + id as f32 * 0.7).sin() * 0.5) + 0.5;
            *color = TextColor(if !state.alive {
                NEON_MAGENTA.with_alpha(0.88 + status_pulse * 0.12)
            } else if state.status == Status::ViewChange {
                NEON_MAGENTA.with_alpha(0.84 + status_pulse * 0.16)
            } else if state.status == Status::Recovering {
                NEON_YELLOW.with_alpha(0.84 + status_pulse * 0.16)
            } else if is_primary {
                IGGY_ORANGE
            } else {
                DIM_GRAY
            });
        }

        for (callout, mut text, mut transform, mut color, mut visibility) in
            text_visuals.p3().iter_mut()
        {
            if callout.id != id {
                continue;
            }

            let timer = replica_fx.callout_timer[idx];
            let persistent = if !state.alive {
                Some(("NAPPING", NEON_MAGENTA))
            } else {
                match state.status {
                    Status::Normal => None,
                    Status::ViewChange => Some(("HOWLING!", NEON_MAGENTA)),
                    Status::Recovering => Some(("LIMPING...", NEON_YELLOW)),
                }
            };

            if timer <= 0.0 && persistent.is_none() {
                *visibility = Visibility::Hidden;
                continue;
            }

            *visibility = Visibility::Inherited;
            let (label, base_color, energy) = if let Some((label, color_hint)) = persistent {
                (label, color_hint, 0.78)
            } else {
                (
                    replica_fx.callout_text[idx].as_str(),
                    replica_fx.callout_color[idx],
                    timer.clamp(0.0, 1.0),
                )
            };
            let pulse = ((elapsed * 6.0 + id as f32 * 1.3).sin() * 0.5) + 0.5;
            let rise = (1.0 - energy) * 18.0;
            **text = label.to_string();
            transform.translation = Vec3::new(base_pos.x, base_pos.y + 118.0 + rise, 3.2);
            transform.scale = Vec3::splat(0.92 + energy * 0.22 + pulse * 0.04);
            *color =
                TextColor(base_color.with_alpha((0.55 + energy * 0.35 + pulse * 0.1).min(1.0)));
        }
    }
}

pub(crate) fn update_message_particles(
    mut commands: Commands,
    time: Res<Time>,
    mut sim: NonSendMut<SimulationState>,
    mut query: Query<(Entity, &mut MessageParticle, &mut Transform)>,
) {
    let delta = time.delta_secs();
    sim.frame_count = sim.frame_count.wrapping_add(1);
    let should_spawn_trail = sim.frame_count.is_multiple_of(3);

    for (entity, mut particle, mut transform) in query.iter_mut() {
        particle.progress += delta / particle.duration;

        if particle.progress >= 1.0 {
            commands.entity(entity).despawn();
            continue;
        }

        let position = bezier_pos(
            particle.from,
            particle.to,
            particle.control,
            particle.progress,
        );
        transform.translation.x = position.x;
        transform.translation.y = position.y;

        if !should_spawn_trail {
            continue;
        }

        let trail_color = particle.msg_type.color();
        spawn_trail_dot(&mut commands, position, trail_color);

        if particle.progress <= 0.1 || particle.progress >= 0.9 {
            continue;
        }

        let travel_dir = (particle.to - particle.from).normalize_or_zero();
        let perp = Vec2::new(-travel_dir.y, travel_dir.x);
        let offset = perp * ((sim.frame_count as f32 * 0.7).sin() * 6.0);
        let line_len = 8.0 + particle.progress * 12.0;
        let line = shapes::Line(
            position + offset - travel_dir * line_len * 0.5,
            position + offset + travel_dir * line_len * 0.5,
        );
        commands.spawn((
            SpeedLine {
                lifetime: SPEED_LINE_LIFETIME,
                max_lifetime: SPEED_LINE_LIFETIME,
                color: trail_color,
            },
            build_stroke(&line, trail_color.with_alpha(0.25), 0.8),
            Transform::from_xyz(0.0, 0.0, 4.2),
        ));
    }
}

pub(crate) fn update_trail_dots(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut TrailDot, &mut Shape)>,
) {
    let delta = time.delta_secs();

    for (entity, mut dot, mut shape) in query.iter_mut() {
        dot.lifetime -= delta;
        if dot.lifetime <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }
        let alpha = (dot.lifetime / dot.max_lifetime) * 0.35;
        shape.fill = Some(Fill::color(dot.color.with_alpha(alpha)));
    }
}

pub(crate) fn update_link_lines(
    time: Res<Time>,
    sim: NonSend<SimulationState>,
    mut query: Query<(&LinkLine, &mut Shape)>,
) {
    let elapsed = time.elapsed_secs();
    for (link, mut shape) in query.iter_mut() {
        let from_alive = !sim.simulator.is_crashed(link.from_id);
        let to_alive = !sim.simulator.is_crashed(link.to_id);
        let is_partitioned = sim.simulator.is_link_partitioned(link.from_id, link.to_id);
        let pulse = ((elapsed * 1.7 + (link.from_id + link.to_id) as f32 * 0.9).sin() * 0.5) + 0.5;

        if is_partitioned || !from_alive || !to_alive {
            shape.stroke = Some(Stroke::new(
                NEON_MAGENTA.with_alpha(0.15 + pulse * 0.10),
                1.0,
            ));
        } else {
            shape.stroke = Some(Stroke::new(GRID_LINE.with_alpha(0.25 + pulse * 0.05), 0.8));
        }
    }
}

pub(crate) fn update_app_visuals(
    time: Res<Time>,
    positions: Res<ReplicaPositions>,
    sim: NonSend<SimulationState>,
    mut app_fx: ResMut<AppFxState>,
    mut visuals: ParamSet<(AppCardQuery, AppLinkQuery, AppCardLabelQuery, AppIconQuery)>,
) {
    let delta = time.delta_secs();
    let elapsed = time.elapsed_secs();

    for pulse in &mut app_fx.pulse {
        *pulse = (*pulse - delta * 1.15).max(0.0);
    }

    for (card, mut shape, mut transform) in visuals.p0().iter_mut() {
        let pulse = app_fx.pulse[card.id];
        let hover = (elapsed * 1.05 + card.id as f32 * 0.9).sin() * 5.0;
        shape.fill = Some(Fill::color(card.accent.with_alpha(0.08 + pulse * 0.12)));
        shape.stroke = Some(Stroke::new(
            card.accent.with_alpha(0.18 + pulse * 0.45),
            1.0 + pulse * 1.5,
        ));
        transform.translation = Vec3::new(card.anchor.x, card.anchor.y + hover + pulse * 10.0, 1.2);
        transform.rotation = Quat::from_rotation_z((elapsed * 0.35 + card.id as f32).sin() * 0.015);
        transform.scale = Vec3::splat(1.0 + pulse * 0.06);
    }

    for (label, mut transform, mut color) in visuals.p2().iter_mut() {
        let pulse = app_fx.pulse[label.id];
        let hover = (elapsed * 1.05 + label.id as f32 * 0.9).sin() * 5.0;
        transform.translation = Vec3::new(
            label.anchor.x,
            label.anchor.y - 64.0 + hover + pulse * 10.0,
            1.3,
        );
        *color = TextColor(COOL_WHITE.with_alpha(0.9 + pulse * 0.1));
    }

    for (icon, mut shape, mut transform) in visuals.p3().iter_mut() {
        let pulse = app_fx.pulse[icon.id];
        let hover = (elapsed * 1.05 + icon.id as f32 * 0.9).sin() * 5.0;
        let tilt = match icon.kind {
            AppKind::Producer => -0.03,
            AppKind::Consumer => 0.03,
        };
        shape.stroke = Some(Stroke::new(
            icon.accent.with_alpha(0.8 + pulse * 0.2),
            2.4 + pulse * 0.8,
        ));
        transform.translation = Vec3::new(
            icon.anchor.x,
            icon.anchor.y + 4.0 + hover + pulse * 9.0,
            1.35,
        );
        transform.rotation =
            Quat::from_rotation_z(tilt + (elapsed * 0.7 + icon.id as f32).sin() * 0.025);
        transform.scale = Vec3::splat(1.0 + pulse * 0.08);
    }

    for (link, mut shape) in visuals.p1().iter_mut() {
        let pulse = app_fx.pulse[link.id];
        let target = positions.0[link.target_replica as usize];
        let target_alive = !sim.simulator.is_crashed(link.target_replica);
        let distance_bias = ((target.y - WORLD_CENTER_Y).abs() / 400.0).min(0.2);
        shape.stroke = Some(Stroke::new(
            if target_alive {
                link.accent.with_alpha(0.10 + pulse * 0.26 + distance_bias)
            } else {
                NEON_MAGENTA.with_alpha(0.05 + pulse * 0.08)
            },
            if target_alive {
                0.9 + pulse * 0.8
            } else {
                0.45 + pulse * 0.2
            },
        ));
    }
}

pub(crate) fn update_app_flow_particles(
    mut commands: Commands,
    time: Res<Time>,
    positions: Res<ReplicaPositions>,
    sim: NonSend<SimulationState>,
    links: Query<&AppLink, Without<AppFlowParticle>>,
    mut particles: Query<
        (Entity, &mut AppFlowParticle, &mut Transform, &mut Shape),
        Without<AppLink>,
    >,
) {
    let delta = time.delta_secs();

    if sim.frame_count.is_multiple_of(36) {
        for link in links.iter() {
            if sim.simulator.is_crashed(link.target_replica) {
                continue;
            }

            let target = positions.0[link.target_replica as usize];
            let (from, to) = if link.inbound {
                (link.anchor, target)
            } else {
                (target, link.anchor)
            };
            let dir = to - from;
            let normal = Vec2::new(-dir.y, dir.x).normalize_or_zero();
            let control = (from + to) * 0.5 + normal * if link.inbound { 70.0 } else { -70.0 };

            commands.spawn((
                AppFlowParticle {
                    replica_id: link.target_replica,
                    from,
                    to,
                    control,
                    progress: 0.0,
                    speed: 0.26 + link.id as f32 * 0.025,
                    color: link.accent,
                },
                build_fill(&circle_shape(3.2), link.accent.with_alpha(0.85)),
                Transform::from_xyz(from.x, from.y, 1.1),
            ));
        }
    }

    for (entity, mut particle, mut transform, mut shape) in particles.iter_mut() {
        if sim.simulator.is_crashed(particle.replica_id) {
            commands.entity(entity).despawn();
            continue;
        }

        particle.progress += delta * particle.speed;
        if particle.progress >= 1.0 {
            commands.entity(entity).despawn();
            continue;
        }

        let position = bezier_pos(
            particle.from,
            particle.to,
            particle.control,
            particle.progress,
        );
        transform.translation.x = position.x;
        transform.translation.y = position.y;
        let alpha = (1.0 - (particle.progress - 0.5).abs() * 1.4).clamp(0.18, 0.92);
        shape.fill = Some(Fill::color(particle.color.with_alpha(alpha)));
    }
}

pub(crate) fn update_hud_text(
    sim: NonSend<SimulationState>,
    mut hud: ParamSet<(
        HudTickQuery,
        HudOpsQuery,
        HudCommitsQuery,
        HudStateQuery,
        HudSpeedQuery,
    )>,
) {
    for mut text in hud.p0().iter_mut() {
        **text = format!("T {}", sim.simulator.tick);
    }

    for mut text in hud.p1().iter_mut() {
        **text = format!("{:.0}", sim.ops_per_second);
    }

    for mut text in hud.p2().iter_mut() {
        **text = format!("{}", sim.total_commits);
    }

    for (mut text, mut color) in hud.p3().iter_mut() {
        if sim.playing {
            **text = "RACING".to_string();
            *color = TextColor(NEON_CYAN);
        } else {
            **text = "RESTING".to_string();
            *color = TextColor(NEON_YELLOW);
        }
    }

    for mut text in hud.p4().iter_mut() {
        **text = format!("x{:.1}", sim.speed);
    }
}

pub(crate) fn update_pause_overlay(
    sim: NonSend<SimulationState>,
    time: Res<Time>,
    mut pause: ParamSet<(PauseOverlayVisQuery, PauseMainColorQuery, PauseSubVisQuery)>,
) {
    let target_vis = if sim.playing {
        Visibility::Hidden
    } else {
        Visibility::Inherited
    };

    for mut vis in pause.p0().iter_mut() {
        *vis = target_vis;
    }

    if !sim.playing {
        let pulse = ((time.elapsed_secs() * 3.0).sin() * 0.5 + 0.5) * 0.6 + 0.4;
        for mut color in pause.p1().iter_mut() {
            *color = TextColor(NEON_YELLOW.with_alpha(pulse));
        }
    }

    for mut vis in pause.p2().iter_mut() {
        *vis = target_vis;
    }
}

pub(crate) fn update_scan_lines(time: Res<Time>, mut query: Query<(&ScanLine, &mut Transform)>) {
    let delta = time.delta_secs();

    for (scan, mut transform) in query.iter_mut() {
        transform.translation.y -= scan.speed * delta;
        if transform.translation.y < -500.0 {
            transform.translation.y = 500.0;
        }
    }
}

pub(crate) fn update_commit_rings(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut CommitRing, &mut Shape, &mut Transform)>,
) {
    let delta = time.delta_secs();

    for (entity, mut ring, mut shape, mut transform) in query.iter_mut() {
        ring.lifetime -= delta;
        if ring.lifetime <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }

        let progress = 1.0 - (ring.lifetime / ring.max_lifetime);
        let eased = 1.0 - (1.0 - progress).powi(3);
        let current_radius = ring.max_radius * eased;
        let alpha = (1.0 - progress) * 0.6;
        let width = (1.0 - progress) * 3.0 + 0.5;

        shape.stroke = Some(Stroke::new(ring.color.with_alpha(alpha), width));
        transform.translation.x = ring.center.x;
        transform.translation.y = ring.center.y;
        transform.scale = Vec3::splat(current_radius.max(1.0));
    }
}

pub(crate) fn update_ambient_particles(
    time: Res<Time>,
    sim: NonSend<SimulationState>,
    mut query: Query<(&mut AmbientParticle, &mut Transform, &mut Shape)>,
) {
    let delta = time.delta_secs();
    let elapsed = time.elapsed_secs();
    let activity_boost = if sim.playing { 1.0 } else { 0.3 };

    for (particle, mut transform, mut shape) in query.iter_mut() {
        let drift_x = (elapsed * particle.drift_speed + particle.drift_phase).sin() * 18.0;
        transform.translation.x += (particle.velocity.x + drift_x * 0.3) * delta * activity_boost;
        transform.translation.y += particle.velocity.y * delta * activity_boost;

        if transform.translation.y > 600.0 {
            transform.translation.y = -600.0;
        }
        if transform.translation.x > 900.0 {
            transform.translation.x = -900.0;
        } else if transform.translation.x < -900.0 {
            transform.translation.x = 900.0;
        }

        let twinkle = ((elapsed * 2.5 + particle.drift_phase * 3.7).sin() * 0.5 + 0.5) * 0.7 + 0.3;
        let alpha = particle.base_alpha * twinkle * activity_boost;
        if let Some(ref mut fill) = shape.fill {
            *fill = Fill::color(COOL_WHITE.with_alpha(alpha));
        }
    }
}

pub(crate) fn update_screen_flash(
    time: Res<Time>,
    mut flash: ResMut<ScreenFlash>,
    mut query: Query<&mut Sprite, With<ScreenFlashOverlay>>,
) {
    let delta = time.delta_secs();

    if flash.timer <= 0.0 {
        for mut sprite in query.iter_mut() {
            sprite.color = NEON_CYAN.with_alpha(0.0);
        }
        return;
    }

    flash.timer -= delta;
    let progress = (flash.timer / SCREEN_FLASH_DURATION).max(0.0);
    let alpha = progress * flash.intensity;

    for mut sprite in query.iter_mut() {
        sprite.color = flash.color.with_alpha(alpha);
    }
}

pub(crate) fn update_speed_lines(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut SpeedLine, &mut Shape)>,
) {
    let delta = time.delta_secs();

    for (entity, mut line, mut shape) in query.iter_mut() {
        line.lifetime -= delta;
        if line.lifetime <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }
        let alpha = (line.lifetime / line.max_lifetime) * 0.25;
        shape.stroke = Some(Stroke::new(line.color.with_alpha(alpha), 0.6));
    }
}

pub(crate) fn update_track_ring(
    time: Res<Time>,
    mut track_pulse: ResMut<TrackPulse>,
    mut query: Query<&mut Shape, With<TrackRing>>,
) {
    let delta = time.delta_secs();
    let elapsed = time.elapsed_secs();

    track_pulse.energy = (track_pulse.energy - delta * 0.6).max(0.0);

    let base_alpha = 0.25 + track_pulse.energy * 0.35;
    let breathe = (elapsed * 0.8).sin() * 0.08;
    let width = 0.9 + track_pulse.energy * 1.5 + breathe.abs() * 0.3;
    let color = if track_pulse.energy > 0.2 {
        IGGY_ORANGE
    } else {
        NEON_CYAN
    };

    for mut shape in query.iter_mut() {
        shape.stroke = Some(Stroke::new(color.with_alpha(base_alpha + breathe), width));
    }
}

pub(crate) fn update_paw_bursts(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut PawBurst, &mut Shape, &mut Transform)>,
) {
    let delta = time.delta_secs();

    for (entity, mut paw, mut shape, mut transform) in query.iter_mut() {
        paw.lifetime -= delta;
        if paw.lifetime <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }
        let progress = 1.0 - (paw.lifetime / paw.max_lifetime);
        let alpha = (1.0 - progress) * 0.8;
        let scale = 1.0 - progress * 0.5;
        transform.translation.x += paw.velocity.x * delta;
        transform.translation.y += paw.velocity.y * delta;
        let damping = (1.0 - delta * 3.0).clamp(0.0, 1.0);
        paw.velocity *= damping;
        shape.fill = Some(Fill::color(IGGY_ORANGE.with_alpha(alpha)));
        transform.scale = Vec3::splat(scale);
    }
}

pub(crate) fn update_lightning(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut Lightning, &mut Shape)>,
) {
    let delta = time.delta_secs();

    for (entity, mut bolt, mut shape) in query.iter_mut() {
        bolt.lifetime -= delta;
        if bolt.lifetime <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }
        let alpha = (bolt.lifetime / bolt.max_lifetime) * 0.8;
        let width = (bolt.lifetime / bolt.max_lifetime) * 2.0 + 0.5;
        shape.stroke = Some(Stroke::new(NEON_MAGENTA.with_alpha(alpha), width));
    }
}
