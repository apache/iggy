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
use bevy_prototype_lyon::prelude::*;

use crate::components::*;
use crate::resources::*;
use crate::theme::*;
use crate::tracing_layer::CapturedSimEvent;
use crate::types::{MessageType, Role, Status};
use crate::vocabulary::VocabMode;

pub(crate) fn circle_shape(radius: f32) -> shapes::Circle {
    shapes::Circle {
        radius,
        center: Vec2::ZERO,
    }
}

pub(crate) fn rectangle_shape(width: f32, height: f32) -> shapes::Rectangle {
    shapes::Rectangle {
        extents: Vec2::new(width, height),
        origin: shapes::RectangleOrigin::Center,
        radii: None,
    }
}

pub(crate) fn iggy_sprite_rect() -> Rect {
    Rect::new(140.0, 0.0, 920.0, 652.0)
}

pub(crate) fn build_fill(
    geom: &impl Geometry<tess::path::path::Builder>,
    fill_color: Color,
) -> Shape {
    ShapeBuilder::with(geom)
        .fill(Fill::color(fill_color))
        .build()
}

pub(crate) fn build_stroke(
    geom: &impl Geometry<tess::path::path::Builder>,
    stroke_color: Color,
    stroke_width: f32,
) -> Shape {
    ShapeBuilder::with(geom)
        .stroke(Stroke::new(stroke_color, stroke_width))
        .build()
}

pub(crate) fn build_fill_stroke(
    geom: &impl Geometry<tess::path::path::Builder>,
    fill_color: Color,
    stroke_color: Color,
    stroke_width: f32,
) -> Shape {
    ShapeBuilder::with(geom)
        .fill(Fill::color(fill_color))
        .stroke(Stroke::new(stroke_color, stroke_width))
        .build()
}

pub(crate) fn build_path_stroke(path: &ShapePath, stroke_color: Color, stroke_width: f32) -> Shape {
    ShapeBuilder::with(path)
        .stroke(Stroke::new(stroke_color, stroke_width))
        .build()
}

pub(crate) fn bezier_pos(from: Vec2, to: Vec2, control: Vec2, progress: f32) -> Vec2 {
    let inv = 1.0 - progress;
    inv * inv * from + 2.0 * inv * progress * control + progress * progress * to
}

pub(crate) fn perpendicular_control(from: Vec2, to: Vec2, offset: f32) -> Vec2 {
    let mid = (from + to) * 0.5;
    let dir = to - from;
    let perp = Vec2::new(-dir.y, dir.x).normalize_or_zero();
    mid + perp * offset
}

pub(crate) fn status_color(status: Status, alive: bool) -> Color {
    if !alive {
        return NEON_MAGENTA;
    }
    match status {
        Status::Normal => NEON_CYAN,
        Status::ViewChange => NEON_YELLOW,
        Status::Recovering => NEON_MAGENTA,
    }
}

pub(crate) fn trigger_replica_callout(
    replica_fx: &mut ReplicaFxState,
    id: u8,
    label: &str,
    color: Color,
    duration: f32,
) {
    let idx = id as usize;
    replica_fx.callout_timer[idx] = duration;
    replica_fx.callout_text[idx] = label.to_string();
    replica_fx.callout_color[idx] = color;
}

pub(crate) fn app_icon_path(kind: AppKind) -> ShapePath {
    let mut path = ShapePath::new();

    match kind {
        AppKind::Producer => {
            path = path
                .move_to(Vec2::new(-24.0, 18.0))
                .line_to(Vec2::new(18.0, 18.0))
                .line_to(Vec2::new(18.0, -14.0))
                .line_to(Vec2::new(-24.0, -14.0))
                .close()
                .move_to(Vec2::new(-18.0, 10.0))
                .line_to(Vec2::new(8.0, 10.0))
                .move_to(Vec2::new(-18.0, 3.0))
                .line_to(Vec2::new(2.0, 3.0))
                .move_to(Vec2::new(-18.0, -4.0))
                .line_to(Vec2::new(6.0, -4.0))
                .move_to(Vec2::new(24.0, 6.0))
                .line_to(Vec2::new(34.0, 6.0))
                .move_to(Vec2::new(24.0, 0.0))
                .line_to(Vec2::new(34.0, 0.0))
                .move_to(Vec2::new(24.0, -6.0))
                .line_to(Vec2::new(34.0, -6.0));
        }
        AppKind::Consumer => {
            path = path
                .move_to(Vec2::new(-18.0, 18.0))
                .line_to(Vec2::new(24.0, 18.0))
                .line_to(Vec2::new(24.0, -14.0))
                .line_to(Vec2::new(-18.0, -14.0))
                .close()
                .move_to(Vec2::new(-12.0, 8.0))
                .line_to(Vec2::new(12.0, 8.0))
                .move_to(Vec2::new(-12.0, 0.0))
                .line_to(Vec2::new(12.0, 0.0))
                .move_to(Vec2::new(-12.0, -8.0))
                .line_to(Vec2::new(12.0, -8.0))
                .move_to(Vec2::new(-28.0, 6.0))
                .line_to(Vec2::new(-38.0, 6.0))
                .move_to(Vec2::new(-28.0, 0.0))
                .line_to(Vec2::new(-38.0, 0.0))
                .move_to(Vec2::new(-28.0, -6.0))
                .line_to(Vec2::new(-38.0, -6.0));
        }
    }

    path
}

pub(crate) fn app_link_path(from: Vec2, to: Vec2, inbound: bool) -> ShapePath {
    let dir = to - from;
    let normal = Vec2::new(-dir.y, dir.x).normalize_or_zero();
    let bend = if inbound { 70.0 } else { -70.0 };
    let unit = dir.normalize_or_zero();
    let start = from + unit * 42.0;
    let end = to - unit * 96.0;
    let control = (start + end) * 0.5 + normal * bend;

    ShapePath::new()
        .move_to(start)
        .quadratic_bezier_to(control, end)
}

pub(crate) fn replica_link_path(from: Vec2, to: Vec2, bend: f32) -> ShapePath {
    let dir = to - from;
    let unit = dir.normalize_or_zero();
    let normal = Vec2::new(-dir.y, dir.x).normalize_or_zero();
    let start = from + unit * 92.0;
    let end = to - unit * 92.0;
    let control = (start + end) * 0.5 + normal * bend;

    ShapePath::new()
        .move_to(start)
        .quadratic_bezier_to(control, end)
}

pub(crate) fn spawn_trail_dot(commands: &mut Commands, position: Vec2, color: Color) {
    commands.spawn((
        TrailDot {
            lifetime: TRAIL_LIFETIME,
            max_lifetime: TRAIL_LIFETIME,
            color,
        },
        build_fill(&circle_shape(1.5), color.with_alpha(0.35)),
        Transform::from_xyz(position.x, position.y, 4.0),
    ));
}

pub(crate) fn narrate_event(
    event: &CapturedSimEvent,
    tick: u64,
    replica_count: u8,
    vocab: VocabMode,
) -> Option<EventLogEntry> {
    match event.sim_event.as_str() {
        "ClientRequestReceived" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let name = vocab.node_name(replica_id);
            let (headline, detail) = match vocab {
                VocabMode::Dog => (
                    format!("Ball thrown to {name}!"),
                    format!(
                        "A client tossed a new request to {name} (R{replica_id}). \
                         As lead dog, {name} will replicate it to the pack before confirming."
                    ),
                ),
                VocabMode::Technical => (
                    format!("Client request -> {name}"),
                    format!(
                        "Primary {name} (R{replica_id}) received client request. \
                         Will broadcast Prepare to backups."
                    ),
                ),
            };
            Some(EventLogEntry {
                tick,
                icon: "[REQ]",
                headline,
                detail,
                color: IGGY_ORANGE,
            })
        }
        "PrepareQueued" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let op = event.op.unwrap_or(0);
            let pipeline_depth = event.pipeline_depth.unwrap_or(0);
            let name = vocab.node_name(replica_id);
            let (headline, detail) = match vocab {
                VocabMode::Dog => (
                    format!("{name} queued op #{op}"),
                    format!(
                        "{name} added operation #{op} to the pipeline ({pipeline_depth}/8 slots used). \
                         Now broadcasting Prepare to the pack -- every dog must log this before it counts."
                    ),
                ),
                VocabMode::Technical => (
                    format!("{name} queued op #{op}"),
                    format!(
                        "Operation #{op} added to pipeline ({pipeline_depth}/8 used). \
                         Broadcasting Prepare to all backups."
                    ),
                ),
            };
            Some(EventLogEntry {
                tick,
                icon: "[QUE]",
                headline,
                detail,
                color: Color::srgb_u8(95, 135, 253),
            })
        }
        "PrepareAcked" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let op = event.op.unwrap_or(0);
            let ack_from = event.ack_from_replica.unwrap_or(0) as u8;
            let ack_count = event.ack_count.unwrap_or(0);
            let quorum = event.quorum.unwrap_or(0);
            let quorum_reached = event.quorum_reached.unwrap_or(false);
            let lead = vocab.node_name(replica_id);
            let acker = vocab.node_name(ack_from);
            let quorum_suffix = if quorum_reached { " -- QUORUM!" } else { "" };
            let (headline, detail) = match vocab {
                VocabMode::Dog => {
                    let status_detail = if quorum_reached {
                        format!(
                            "That's {ack_count}/{quorum} -- quorum reached! {lead} can now commit."
                        )
                    } else {
                        format!(
                            "That's {ack_count}/{quorum} acks. Need {} more for quorum.",
                            quorum.saturating_sub(ack_count)
                        )
                    };
                    (
                        format!("{acker} acked op #{op}{quorum_suffix}"),
                        format!(
                            "{acker} (R{ack_from}) confirmed it logged operation #{op}. {status_detail}"
                        ),
                    )
                }
                VocabMode::Technical => {
                    let status_detail = if quorum_reached {
                        format!("{ack_count}/{quorum} acks -- quorum reached. {lead} will commit.")
                    } else {
                        format!(
                            "{ack_count}/{quorum} acks. Need {} more for quorum.",
                            quorum.saturating_sub(ack_count)
                        )
                    };
                    (
                        format!("{acker} acked op #{op}{quorum_suffix}"),
                        format!(
                            "{acker} (R{ack_from}) acknowledged operation #{op}. {status_detail}"
                        ),
                    )
                }
            };
            Some(EventLogEntry {
                tick,
                icon: if quorum_reached { "[OK!]" } else { "[ACK]" },
                headline,
                detail,
                color: if quorum_reached {
                    IGGY_ORANGE
                } else {
                    Color::srgb_u8(20, 184, 166)
                },
            })
        }
        "OperationCommitted" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let op = event.op.unwrap_or(0);
            let name = vocab.node_name(replica_id);
            let (headline, detail) = match vocab {
                VocabMode::Dog => (
                    format!("{name} committed op #{op}!"),
                    format!(
                        "Operation #{op} is officially committed on {name} (R{replica_id}). \
                         The pack agreed -- this data is now durable and safe. Good boy!"
                    ),
                ),
                VocabMode::Technical => (
                    format!("{name} committed op #{op}"),
                    format!(
                        "Op #{op} committed on {name} (R{replica_id}). \
                         Quorum achieved, data durable."
                    ),
                ),
            };
            Some(EventLogEntry {
                tick,
                icon: "[WIN]",
                headline,
                detail,
                color: IGGY_ORANGE,
            })
        }
        "ClientReplyEmitted" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let op = event.op.unwrap_or(0);
            let name = vocab.node_name(replica_id);
            let (headline, detail) = match vocab {
                VocabMode::Dog => (
                    format!("{name} returned the ball!"),
                    format!(
                        "{name} sent a reply to the client. The request for op #{op} \
                         was committed and acknowledged. Fetch complete!"
                    ),
                ),
                VocabMode::Technical => (
                    format!("{name} replied to client"),
                    format!("Reply sent for op #{op}. Request complete."),
                ),
            };
            Some(EventLogEntry {
                tick,
                icon: "[RPL]",
                headline,
                detail,
                color: MessageType::ClientReply.color(),
            })
        }
        "ControlMessageScheduled" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let target = event.target_replica.unwrap_or(0) as u8;
            let action = event.action.as_deref().unwrap_or("");
            let from_name = vocab.node_name(replica_id);
            let to_name = vocab.node_name(target);
            let msg_type = MessageType::from_action(action);
            let (icon, headline, detail) = match vocab {
                VocabMode::Dog => match msg_type {
                    Some(MessageType::PrepareOk) => (
                        "[POK]",
                        format!("{from_name} -> {to_name}: PrepareOk"),
                        format!(
                            "{from_name} confirmed to {to_name}: \"I logged op #{}!\" \
                             One step closer to quorum.",
                            event.op.unwrap_or(0)
                        ),
                    ),
                    Some(MessageType::StartViewChange) => (
                        "[SVC]",
                        format!("{from_name} howls: StartViewChange!"),
                        format!(
                            "{from_name} suspects the lead dog is down! Broadcasting view change \
                             to view {} -- the pack needs a new leader.",
                            event.view.unwrap_or(0)
                        ),
                    ),
                    Some(MessageType::DoViewChange) => (
                        "[DVC]",
                        format!("{from_name} -> {to_name}: DoViewChange"),
                        format!(
                            "{from_name} is voting for {to_name} to become the new lead dog in view {}. \
                             Sending its log state so the new leader has all committed data.",
                            event.view.unwrap_or(0)
                        ),
                    ),
                    Some(MessageType::StartView) => (
                        "[NEW]",
                        format!("{from_name} announces: I'm lead dog!"),
                        format!(
                            "{from_name} won the election for view {}! Broadcasting StartView to the pack -- \
                             everyone sync up and follow the new leader.",
                            event.view.unwrap_or(0)
                        ),
                    ),
                    _ => (
                        "[MSG]",
                        format!("{from_name} -> {to_name}: {action}"),
                        format!("Control message from {from_name} to {to_name}."),
                    ),
                },
                VocabMode::Technical => match msg_type {
                    Some(MessageType::PrepareOk) => (
                        "[POK]",
                        format!("{from_name} -> {to_name}: PrepareOk"),
                        format!(
                            "{from_name} acknowledged op #{} to {to_name}.",
                            event.op.unwrap_or(0)
                        ),
                    ),
                    Some(MessageType::StartViewChange) => (
                        "[SVC]",
                        format!("{from_name} -> {to_name}: StartViewChange"),
                        format!(
                            "{from_name} initiated view change to view {}.",
                            event.view.unwrap_or(0)
                        ),
                    ),
                    Some(MessageType::DoViewChange) => (
                        "[DVC]",
                        format!("{from_name} -> {to_name}: DoViewChange"),
                        format!(
                            "{from_name} sending DoViewChange to {to_name} for view {}.",
                            event.view.unwrap_or(0)
                        ),
                    ),
                    Some(MessageType::StartView) => (
                        "[NEW]",
                        format!("{from_name} -> {to_name}: StartView"),
                        format!(
                            "{from_name} broadcasting StartView for view {}.",
                            event.view.unwrap_or(0)
                        ),
                    ),
                    _ => (
                        "[MSG]",
                        format!("{from_name} -> {to_name}: {action}"),
                        format!("Control message from {from_name} to {to_name}."),
                    ),
                },
            };
            let color = msg_type.map(|mt| mt.color()).unwrap_or(DIM_GRAY);
            Some(EventLogEntry {
                tick,
                icon,
                headline,
                detail,
                color,
            })
        }
        "ViewChangeStarted" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let old_view = event.old_view.unwrap_or(0);
            let new_view = event.new_view.unwrap_or(0);
            let name = vocab.node_name(replica_id);
            let (headline, detail) = match vocab {
                VocabMode::Dog => {
                    let reason_text = event
                        .reason
                        .as_deref()
                        .map(|reason| match reason {
                            "heartbeat_timeout" => format!(
                                "{name} hasn't heard from the lead dog in too long (heartbeat timeout)."
                            ),
                            "view_change_timeout" => {
                                "The view change itself timed out -- the pack couldn't agree fast enough."
                                    .to_string()
                            }
                            "received_start_view_change" => {
                                format!("{name} heard another dog howling for a view change and joined in.")
                            }
                            "received_do_view_change" => {
                                format!(
                                    "{name} received a DoViewChange vote, triggering its own view change."
                                )
                            }
                            other => format!("Reason: {other}"),
                        })
                        .unwrap_or_default();
                    (
                        format!("{name} started view change! (v{old_view} -> v{new_view})"),
                        format!(
                            "{reason_text} The pack is reshuffling -- \
                             the dog at position (view {new_view} mod {replica_count}) = R{} \
                             will become the new lead dog if quorum agrees.",
                            new_view % replica_count as u64
                        ),
                    )
                }
                VocabMode::Technical => {
                    let reason_text = event
                        .reason
                        .as_deref()
                        .map(|reason| match reason {
                            "heartbeat_timeout" => "Heartbeat timeout.".to_string(),
                            "view_change_timeout" => "View change timeout.".to_string(),
                            "received_start_view_change" => {
                                "Received StartViewChange from peer.".to_string()
                            }
                            "received_do_view_change" => {
                                "Received DoViewChange from peer.".to_string()
                            }
                            other => format!("Reason: {other}."),
                        })
                        .unwrap_or_default();
                    (
                        format!("{name} started view change (v{old_view} -> v{new_view})"),
                        format!(
                            "{reason_text} New primary will be R{} (view {new_view} mod {replica_count}).",
                            new_view % replica_count as u64
                        ),
                    )
                }
            };
            Some(EventLogEntry {
                tick,
                icon: "[VCH]",
                headline,
                detail,
                color: NEON_MAGENTA,
            })
        }
        "PrimaryElected" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let view = event.view.unwrap_or(0);
            let name = vocab.node_name(replica_id);
            let (headline, detail) = match vocab {
                VocabMode::Dog => (
                    format!("{name} is the new lead dog! (view {view})"),
                    format!(
                        "{name} (R{replica_id}) collected enough DoViewChange votes and is now \
                         the lead dog for view {view}. The pack will follow {name}'s lead \
                         for all new operations. Broadcasting StartView to synchronize everyone."
                    ),
                ),
                VocabMode::Technical => (
                    format!("{name} elected primary (view {view})"),
                    format!(
                        "{name} (R{replica_id}) elected primary for view {view}. \
                         Received sufficient DoViewChange votes. Broadcasting StartView."
                    ),
                ),
            };
            Some(EventLogEntry {
                tick,
                icon: "[LDR]",
                headline,
                detail,
                color: NEON_YELLOW,
            })
        }
        "ReplicaStateChanged" => {
            let replica_id = event.replica_id.unwrap_or(0) as u8;
            let name = vocab.node_name(replica_id);
            let new_status = event
                .status
                .as_deref()
                .map(Status::from_str)
                .unwrap_or(Status::Normal);
            let role = event
                .role
                .as_deref()
                .map(Role::from_str)
                .unwrap_or(Role::Backup);
            let (headline, detail) = match vocab {
                VocabMode::Dog => {
                    let role_str = match role {
                        Role::Primary => "lead dog",
                        Role::Backup => "pack dog",
                    };
                    (
                        format!("{name} ({role_str}): -> {new_status:?}"),
                        match new_status {
                            Status::Normal => {
                                format!("{name} is back to running! The pack is stable again.")
                            }
                            Status::ViewChange => {
                                format!("{name} senses trouble -- howling for a new leader!")
                            }
                            Status::Recovering => format!(
                                "{name} stumbled and is now recovering -- needs to catch up with the pack."
                            ),
                        },
                    )
                }
                VocabMode::Technical => {
                    let role_str = match role {
                        Role::Primary => "primary",
                        Role::Backup => "backup",
                    };
                    (
                        format!("{name} ({role_str}): -> {new_status:?}"),
                        match new_status {
                            Status::Normal => {
                                format!(
                                    "{name} (R{replica_id}) transitioned to Normal. Cluster stable."
                                )
                            }
                            Status::ViewChange => {
                                format!("{name} (R{replica_id}) entered ViewChange state.")
                            }
                            Status::Recovering => {
                                format!(
                                    "{name} (R{replica_id}) entered Recovering state. Awaiting state transfer."
                                )
                            }
                        },
                    )
                }
            };
            Some(EventLogEntry {
                tick,
                icon: match new_status {
                    Status::Normal => "[RUN]",
                    Status::ViewChange => "[HWL]",
                    Status::Recovering => "[LMP]",
                },
                headline,
                detail,
                color: match new_status {
                    Status::Normal => NEON_CYAN,
                    Status::ViewChange => NEON_MAGENTA,
                    Status::Recovering => NEON_YELLOW,
                },
            })
        }
        _ => None,
    }
}
