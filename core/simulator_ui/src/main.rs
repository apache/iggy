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

mod vsr;

use bevy::asset::AssetPlugin;
use bevy::prelude::*;
use bevy_prototype_lyon::prelude::*;
use std::f32::consts::PI;

// -- Apache Iggy brand palette (matches iggy.apache.org dark theme) ---------

const BG_DARK: Color = Color::srgb_u8(0x07, 0x0c, 0x17); // website main bg
const BG_PANEL: Color = Color::srgb_u8(0x0c, 0x12, 0x20); // card/panel bg
const NEON_CYAN: Color = Color::srgb_u8(0x67, 0xe8, 0xf9); // entity cyan
const NEON_MAGENTA: Color = Color::srgb_u8(0xfa, 0x5e, 0x8a); // gradient pink
const NEON_YELLOW: Color = Color::srgb_u8(0xfe, 0xbc, 0x2e); // macOS yellow
const IGGY_ORANGE: Color = Color::srgb_u8(0xff, 0x91, 0x03); // brand primary #ff9103
const COOL_WHITE: Color = Color::srgb_u8(0xff, 0xfa, 0xeb); // website cream text
const DIM_GRAY: Color = Color::srgb_u8(0xaa, 0xaf, 0xb6); // website muted text
const GRID_LINE: Color = Color::srgb_u8(0x11, 0x1d, 0x35); // dark overlay blue
const HUD_EDGE: Color = Color::srgb_u8(0x3d, 0x44, 0x50); // border/line color
const HUD_ALERT: Color = Color::srgb_u8(0xff, 0x5f, 0x57); // macOS red

// -- Layout constants -------------------------------------------------------

const REPLICA_COUNT: u8 = 3;
const CIRCLE_RADIUS: f32 = 300.0;
const WORLD_CENTER_Y: f32 = 90.0;
const IGGY_SPRITE_SIZE: Vec2 = Vec2::new(210.0, 176.0);
const MSG_DURATION: f32 = 0.5;
const TRAIL_LIFETIME: f32 = 0.25;
const SCAN_LINE_COUNT: usize = 4;
const AMBIENT_PARTICLE_COUNT: usize = 60;
const ORBIT_DOTS_PER_REPLICA: usize = 5;
const COMMIT_RING_LIFETIME: f32 = 0.7;
const SCREEN_FLASH_DURATION: f32 = 0.25;
const SPEED_LINE_LIFETIME: f32 = 0.15;

// -- Resources --------------------------------------------------------------

#[derive(Resource)]
struct SimState {
    simulator: vsr::VsrSimulator,
    playing: bool,
    speed: f32,
    tick_accumulator: f32,
    total_commits: u64,
    ops_per_second: f64,
    ops_window_start: f64,
    ops_window_count: u64,
    frame_count: u64,
}

impl Default for SimState {
    fn default() -> Self {
        Self {
            simulator: vsr::VsrSimulator::new(REPLICA_COUNT, 42),
            playing: true,
            speed: 1.0,
            tick_accumulator: 0.0,
            total_commits: 0,
            ops_per_second: 0.0,
            ops_window_start: 0.0,
            ops_window_count: 0,
            frame_count: 0,
        }
    }
}

#[derive(Resource)]
struct ReplicaPositions(Vec<Vec2>);

impl Default for ReplicaPositions {
    fn default() -> Self {
        let mut positions = Vec::with_capacity(REPLICA_COUNT as usize);
        for i in 0..REPLICA_COUNT {
            let angle = 2.0 * PI * (i as f32) / (REPLICA_COUNT as f32) - PI / 2.0;
            positions.push(Vec2::new(
                CIRCLE_RADIUS * angle.cos(),
                CIRCLE_RADIUS * angle.sin() + WORLD_CENTER_Y,
            ));
        }
        Self(positions)
    }
}

#[derive(Resource)]
struct ReplicaFxState {
    kill: [f32; REPLICA_COUNT as usize],
    revive: [f32; REPLICA_COUNT as usize],
    healthy: [f32; REPLICA_COUNT as usize],
    last_alive: [bool; REPLICA_COUNT as usize],
    last_status: [vsr::Status; REPLICA_COUNT as usize],
    callout_timer: [f32; REPLICA_COUNT as usize],
    callout_text: [String; REPLICA_COUNT as usize],
    callout_color: [Color; REPLICA_COUNT as usize],
}

impl Default for ReplicaFxState {
    fn default() -> Self {
        Self {
            kill: [0.0; REPLICA_COUNT as usize],
            revive: [0.0; REPLICA_COUNT as usize],
            healthy: [0.0; REPLICA_COUNT as usize],
            last_alive: [true; REPLICA_COUNT as usize],
            last_status: [vsr::Status::Normal; REPLICA_COUNT as usize],
            callout_timer: [0.0; REPLICA_COUNT as usize],
            callout_text: std::array::from_fn(|_| String::new()),
            callout_color: [COOL_WHITE; REPLICA_COUNT as usize],
        }
    }
}

#[derive(Resource)]
struct AppFxState {
    pulse: [f32; 4],
}

impl Default for AppFxState {
    fn default() -> Self {
        Self { pulse: [0.0; 4] }
    }
}

const EVENT_LOG_MAX: usize = 200;

#[derive(Clone)]
struct EventLogEntry {
    tick: u64,
    icon: &'static str,
    headline: String,
    detail: String,
    color: Color,
}

#[derive(Resource)]
struct EventLog {
    entries: Vec<EventLogEntry>,
    visible: bool,
    show_details: bool,
}

impl Default for EventLog {
    fn default() -> Self {
        Self {
            entries: Vec::with_capacity(EVENT_LOG_MAX),
            visible: false,
            show_details: false,
        }
    }
}

impl EventLog {
    fn push(&mut self, entry: EventLogEntry) {
        if self.entries.len() >= EVENT_LOG_MAX {
            self.entries.remove(0);
        }
        self.entries.push(entry);
    }
}

fn dog_name(id: u8) -> &'static str {
    match id {
        0 => "Iggy",
        1 => "Zippy",
        2 => "Dash",
        3 => "Bolt",
        4 => "Flash",
        _ => "Pup",
    }
}

fn narrate_event(event: &vsr::SimEvent) -> Option<EventLogEntry> {
    match event {
        vsr::SimEvent::ClientRequest {
            tick, replica_id, ..
        } => {
            let name = dog_name(*replica_id);
            Some(EventLogEntry {
                tick: *tick,
                icon: "[REQ]",
                headline: format!("Ball thrown to {name}!"),
                detail: format!(
                    "A client tossed a new request to {name} (R{replica_id}). \
                     As lead dog, {name} will replicate it to the pack before confirming."
                ),
                color: IGGY_ORANGE,
            })
        }
        vsr::SimEvent::PrepareQueued {
            tick,
            replica_id,
            op,
            pipeline_depth,
            ..
        } => {
            let name = dog_name(*replica_id);
            Some(EventLogEntry {
                tick: *tick,
                icon: "[QUE]",
                headline: format!("{name} queued op #{op}"),
                detail: format!(
                    "{name} added operation #{op} to the pipeline ({pipeline_depth}/8 slots used). \
                     Now broadcasting Prepare to the pack -- every dog must log this before it counts."
                ),
                color: Color::srgb_u8(95, 135, 253),
            })
        }
        vsr::SimEvent::PrepareAcked {
            tick,
            replica_id,
            op,
            ack_from,
            ack_count,
            quorum,
            quorum_reached,
            ..
        } => {
            let lead = dog_name(*replica_id);
            let acker = dog_name(*ack_from);
            let status = if *quorum_reached {
                format!("That's {ack_count}/{quorum} -- quorum reached! {lead} can now commit.")
            } else {
                format!(
                    "That's {ack_count}/{quorum} acks. Need {} more for quorum.",
                    quorum - ack_count
                )
            };
            Some(EventLogEntry {
                tick: *tick,
                icon: if *quorum_reached { "[OK!]" } else { "[ACK]" },
                headline: format!(
                    "{acker} acked op #{op}{}",
                    if *quorum_reached { " -- QUORUM!" } else { "" }
                ),
                detail: format!(
                    "{acker} (R{ack_from}) confirmed it logged operation #{op}. {status}"
                ),
                color: if *quorum_reached {
                    IGGY_ORANGE
                } else {
                    Color::srgb_u8(20, 184, 166)
                },
            })
        }
        vsr::SimEvent::OperationCommitted {
            tick,
            replica_id,
            op,
            ..
        } => {
            let name = dog_name(*replica_id);
            Some(EventLogEntry {
                tick: *tick,
                icon: "[WIN]",
                headline: format!("{name} committed op #{op}!"),
                detail: format!(
                    "Operation #{op} is officially committed on {name} (R{replica_id}). \
                     The pack agreed -- this data is now durable and safe. Good boy!"
                ),
                color: IGGY_ORANGE,
            })
        }
        vsr::SimEvent::MessageSent { tick, message, .. } => {
            let from = dog_name(message.from);
            let to = dog_name(message.to);
            let (icon, headline, detail) = match message.msg_type {
                vsr::MessageType::Prepare => (
                    "[PRE]",
                    format!("{from} -> {to}: Prepare"),
                    format!(
                        "{from} sent a Prepare for op #{} (view {}) to {to}. \
                         Asking {to} to log this operation and ack back.",
                        message.op, message.view
                    ),
                ),
                vsr::MessageType::PrepareOk => (
                    "[POK]",
                    format!("{from} -> {to}: PrepareOk"),
                    format!(
                        "{from} confirmed to {to}: \"I logged op #{}!\" \
                         One step closer to quorum.",
                        message.op
                    ),
                ),
                vsr::MessageType::StartViewChange => (
                    "[SVC]",
                    format!("{from} howls: StartViewChange!"),
                    format!(
                        "{from} suspects the lead dog is down! Broadcasting view change \
                         to view {} -- the pack needs a new leader.",
                        message.view
                    ),
                ),
                vsr::MessageType::DoViewChange => (
                    "[DVC]",
                    format!("{from} -> {to}: DoViewChange"),
                    format!(
                        "{from} is voting for {to} to become the new lead dog in view {}. \
                         Sending its log state so the new leader has all committed data.",
                        message.view
                    ),
                ),
                vsr::MessageType::StartView => (
                    "[NEW]",
                    format!("{from} announces: I'm lead dog!"),
                    format!(
                        "{from} won the election for view {}! Broadcasting StartView to the pack -- \
                         everyone sync up and follow the new leader.",
                        message.view
                    ),
                ),
                vsr::MessageType::ClientReply => (
                    "[RPL]",
                    format!("{from} returned the ball!"),
                    format!(
                        "{from} sent a reply to the client. The request for op #{} \
                         was committed and acknowledged. Fetch complete!",
                        message.op
                    ),
                ),
                vsr::MessageType::ClientRequest => (
                    "[REQ]",
                    format!("Ball thrown to {to}"),
                    format!("New client request heading to {to}."),
                ),
                vsr::MessageType::Commit => (
                    "[CMT]",
                    format!("{from} -> {to}: Commit"),
                    format!("{from} notified {to} that op #{} is committed.", message.op),
                ),
            };
            Some(EventLogEntry {
                tick: *tick,
                icon,
                headline,
                detail,
                color: message.msg_type.color(),
            })
        }
        vsr::SimEvent::MessageDropped {
            tick, message_id, ..
        } => Some(EventLogEntry {
            tick: *tick,
            icon: "[DRP]",
            headline: format!("Message #{message_id} lost in the wind!"),
            detail: "A message was dropped by the network -- it never arrived. \
                         This can happen due to partitions or packet loss. The sender won't \
                         know until a timeout fires."
                .to_string(),
            color: NEON_MAGENTA,
        }),
        vsr::SimEvent::ViewChangeStarted {
            tick,
            replica_id,
            old_view,
            new_view,
            reason,
            ..
        } => {
            let name = dog_name(*replica_id);
            let reason_text = match reason {
                vsr::ViewChangeReason::HeartbeatTimeout => format!(
                    "{name} hasn't heard from the lead dog in too long (heartbeat timeout)."
                ),
                vsr::ViewChangeReason::ViewChangeTimeout => {
                    "The view change itself timed out -- the pack couldn't agree fast enough."
                        .to_string()
                }
                vsr::ViewChangeReason::ReceivedStartViewChange => {
                    format!("{name} heard another dog howling for a view change and joined in.")
                }
                vsr::ViewChangeReason::ReceivedDoViewChange => {
                    format!("{name} received a DoViewChange vote, triggering its own view change.")
                }
            };
            Some(EventLogEntry {
                tick: *tick,
                icon: "[VCH]",
                headline: format!("{name} started view change! (v{old_view} -> v{new_view})"),
                detail: format!(
                    "{reason_text} The pack is reshuffling -- \
                     the dog at position (view {new_view} mod {REPLICA_COUNT}) = R{} \
                     will become the new lead dog if quorum agrees.",
                    new_view % REPLICA_COUNT as u32
                ),
                color: NEON_MAGENTA,
            })
        }
        vsr::SimEvent::PrimaryElected {
            tick,
            replica_id,
            view,
            ..
        } => {
            let name = dog_name(*replica_id);
            Some(EventLogEntry {
                tick: *tick,
                icon: "[LDR]",
                headline: format!("{name} is the new lead dog! (view {view})"),
                detail: format!(
                    "{name} (R{replica_id}) collected enough DoViewChange votes and is now \
                     the lead dog for view {view}. The pack will follow {name}'s lead \
                     for all new operations. Broadcasting StartView to synchronize everyone."
                ),
                color: NEON_YELLOW,
            })
        }
        vsr::SimEvent::ReplicaStateChanged {
            tick,
            replica_id,
            old_status,
            new_status,
            role,
            ..
        } => {
            let name = dog_name(*replica_id);
            let role_str = match role {
                vsr::Role::Primary => "lead dog",
                vsr::Role::Backup => "pack dog",
            };
            let transition = match (old_status, new_status) {
                (vsr::Status::Normal, vsr::Status::ViewChange) => format!(
                    "{name} was running fine but now senses trouble -- howling for a new leader!"
                ),
                (vsr::Status::ViewChange, vsr::Status::Normal) => format!(
                    "{name} finished the view change and is back to running! The pack is stable again."
                ),
                (vsr::Status::Recovering, vsr::Status::Normal) => format!(
                    "{name} finished recovering and rejoined the race! Back on all four paws."
                ),
                (vsr::Status::Normal, vsr::Status::Recovering) => format!(
                    "{name} stumbled and is now recovering -- needs to catch up with the pack."
                ),
                _ => format!("{name} changed from {old_status:?} to {new_status:?}."),
            };
            Some(EventLogEntry {
                tick: *tick,
                icon: match new_status {
                    vsr::Status::Normal => "[RUN]",
                    vsr::Status::ViewChange => "[HWL]",
                    vsr::Status::Recovering => "[LMP]",
                },
                headline: format!("{name} ({role_str}): {old_status:?} -> {new_status:?}"),
                detail: transition,
                color: match new_status {
                    vsr::Status::Normal => NEON_CYAN,
                    vsr::Status::ViewChange => NEON_MAGENTA,
                    vsr::Status::Recovering => NEON_YELLOW,
                },
            })
        }
        vsr::SimEvent::LinkPartitioned { tick, from, to, .. } => {
            let a = dog_name(*from);
            let b = dog_name(*to);
            Some(EventLogEntry {
                tick: *tick,
                icon: "[FNC]",
                headline: format!("Fence between {a} and {b}!"),
                detail: format!(
                    "A network partition cut the link between {a} (R{from}) and {b} (R{to}). \
                     They can't hear each other anymore. If one side loses quorum, \
                     it won't be able to commit new operations."
                ),
                color: NEON_MAGENTA,
            })
        }
        vsr::SimEvent::LinkHealed { tick, from, to, .. } => {
            let a = dog_name(*from);
            let b = dog_name(*to);
            Some(EventLogEntry {
                tick: *tick,
                icon: "[HEL]",
                headline: format!("Path clear between {a} and {b}!"),
                detail: format!(
                    "The partition between {a} (R{from}) and {b} (R{to}) healed. \
                     Messages can flow again -- the pack can reunite."
                ),
                color: NEON_CYAN,
            })
        }
        vsr::SimEvent::Tick { .. } | vsr::SimEvent::MessageDelivered { .. } => None,
    }
}

#[derive(Component)]
struct ReplicaGlow {
    id: u8,
}

#[derive(Component)]
struct ReplicaNeonOutline {
    id: u8,
}

#[derive(Component)]
struct ReplicaBody {
    id: u8,
}

#[derive(Component)]
struct ReplicaShadow {
    id: u8,
}

#[derive(Component)]
struct ReplicaIdText {
    id: u8,
}

#[derive(Component)]
struct ReplicaRoleText {
    id: u8,
}

#[derive(Component)]
struct ReplicaCalloutText {
    id: u8,
}

#[derive(Component)]
struct ReplicaSleepText {
    id: u8,
    layer: u8,
}

#[derive(Component)]
struct AppCard {
    id: usize,
    anchor: Vec2,
    accent: Color,
}

#[derive(Component)]
struct AppCardLabel {
    id: usize,
    anchor: Vec2,
}

#[derive(Component)]
struct AppLink {
    id: usize,
    anchor: Vec2,
    target_replica: u8,
    accent: Color,
    inbound: bool,
}

#[derive(Component, Clone, Copy)]
struct AppIcon {
    id: usize,
    anchor: Vec2,
    accent: Color,
    kind: AppKind,
}

#[derive(Component)]
struct AppFlowParticle {
    replica_id: u8,
    from: Vec2,
    to: Vec2,
    control: Vec2,
    progress: f32,
    speed: f32,
    color: Color,
}

#[derive(Clone, Copy)]
enum AppKind {
    Producer,
    Consumer,
}

#[derive(Component)]
struct LinkLine {
    from_id: u8,
    to_id: u8,
}

#[derive(Component)]
struct MessageParticle {
    from: Vec2,
    to: Vec2,
    control: Vec2,
    progress: f32,
    duration: f32,
    msg_type: vsr::MessageType,
}

#[derive(Component)]
struct MessageGlow;

#[derive(Component)]
struct TrailDot {
    lifetime: f32,
    max_lifetime: f32,
    color: Color,
}

#[derive(Component)]
struct ScanLine {
    speed: f32,
}

#[derive(Component)]
struct HudTickText;

#[derive(Component)]
struct HudOpsText;

#[derive(Component)]
struct HudCommitsText;

#[derive(Component)]
struct HudStateText;

#[derive(Component)]
struct HudSpeedText;

#[derive(Component)]
struct PauseOverlay;

#[derive(Component)]
struct PauseMainText;

#[derive(Component)]
struct PauseSubtext;

#[derive(Component)]
struct GlowFlash {
    id: u8,
    timer: f32,
    flash_color: Color,
}

#[derive(Component)]
struct CommitRing {
    center: Vec2,
    lifetime: f32,
    max_lifetime: f32,
    color: Color,
    max_radius: f32,
}

#[derive(Component)]
struct AmbientParticle {
    velocity: Vec2,
    drift_phase: f32,
    drift_speed: f32,
    base_alpha: f32,
}

#[derive(Component)]
struct EnergyOrbit {
    replica_id: u8,
    orbit_index: usize,
    angle: f32,
    radius: f32,
    speed: f32,
}

#[derive(Resource)]
struct ScreenFlash {
    timer: f32,
    color: Color,
    intensity: f32,
}

impl Default for ScreenFlash {
    fn default() -> Self {
        Self {
            timer: 0.0,
            color: NEON_CYAN,
            intensity: 0.0,
        }
    }
}

#[derive(Component)]
struct ScreenFlashOverlay;

#[derive(Component)]
struct EventLogPanel;

#[derive(Component)]
struct EventLogContent;

#[derive(Component)]
struct EventLogToggleHint;

#[derive(Component)]
struct SpeedLine {
    lifetime: f32,
    max_lifetime: f32,
    color: Color,
}

#[derive(Component)]
struct TrackRing;

#[derive(Resource)]
struct TrackPulse {
    energy: f32,
}

impl Default for TrackPulse {
    fn default() -> Self {
        Self { energy: 0.0 }
    }
}

// -- Shape builder helpers --------------------------------------------------

fn circle_shape(radius: f32) -> shapes::Circle {
    shapes::Circle {
        radius,
        center: Vec2::ZERO,
    }
}

fn rectangle_shape(width: f32, height: f32) -> shapes::Rectangle {
    shapes::Rectangle {
        extents: Vec2::new(width, height),
        origin: shapes::RectangleOrigin::Center,
        radii: None,
    }
}

fn iggy_sprite_rect() -> Rect {
    Rect::new(140.0, 0.0, 920.0, 652.0)
}

fn app_icon_path(kind: AppKind) -> ShapePath {
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

fn app_link_path(from: Vec2, to: Vec2, inbound: bool) -> ShapePath {
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

fn replica_link_path(from: Vec2, to: Vec2, bend: f32) -> ShapePath {
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

fn build_fill(geom: &impl Geometry<tess::path::path::Builder>, fill_color: Color) -> Shape {
    ShapeBuilder::with(geom)
        .fill(Fill::color(fill_color))
        .build()
}

fn build_stroke(
    geom: &impl Geometry<tess::path::path::Builder>,
    stroke_color: Color,
    stroke_width: f32,
) -> Shape {
    ShapeBuilder::with(geom)
        .stroke(Stroke::new(stroke_color, stroke_width))
        .build()
}

fn build_fill_stroke(
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

fn build_path_stroke(path: &ShapePath, stroke_color: Color, stroke_width: f32) -> Shape {
    ShapeBuilder::with(path)
        .stroke(Stroke::new(stroke_color, stroke_width))
        .build()
}

// -- Helpers ----------------------------------------------------------------

fn bezier_pos(from: Vec2, to: Vec2, control: Vec2, t: f32) -> Vec2 {
    let inv = 1.0 - t;
    inv * inv * from + 2.0 * inv * t * control + t * t * to
}

fn perpendicular_control(from: Vec2, to: Vec2, offset: f32) -> Vec2 {
    let mid = (from + to) * 0.5;
    let dir = to - from;
    let perp = Vec2::new(-dir.y, dir.x).normalize_or_zero();
    mid + perp * offset
}

fn status_color(status: vsr::Status, alive: bool) -> Color {
    if !alive {
        return NEON_MAGENTA;
    }
    match status {
        vsr::Status::Normal => NEON_CYAN,
        vsr::Status::ViewChange => NEON_YELLOW,
        vsr::Status::Recovering => NEON_MAGENTA,
    }
}

fn role_label(role: vsr::Role, status: vsr::Status) -> &'static str {
    match (role, status) {
        (vsr::Role::Primary, vsr::Status::Normal) => "LEAD DOG / running",
        (vsr::Role::Primary, vsr::Status::ViewChange) => "LEAD DOG / howling",
        (vsr::Role::Primary, vsr::Status::Recovering) => "LEAD DOG / limping",
        (vsr::Role::Backup, vsr::Status::Normal) => "PACK DOG / running",
        (vsr::Role::Backup, vsr::Status::ViewChange) => "PACK DOG / howling",
        (vsr::Role::Backup, vsr::Status::Recovering) => "PACK DOG / limping",
    }
}

fn trigger_replica_callout(
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

// -- Entry point ------------------------------------------------------------

fn main() {
    App::new()
        .add_plugins(
            DefaultPlugins
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
        .insert_resource(ClearColor(BG_DARK))
        .insert_resource(SimState::default())
        .insert_resource(ReplicaPositions::default())
        .insert_resource(ReplicaFxState::default())
        .insert_resource(AppFxState::default())
        .insert_resource(ScreenFlash::default())
        .insert_resource(TrackPulse::default())
        .insert_resource(EventLog::default())
        .add_systems(Startup, setup_camera)
        .add_systems(Startup, (setup_world, setup_hud).after(setup_camera))
        .add_systems(
            Update,
            (
                keyboard_input_system,
                simulation_tick_system,
                update_replica_visuals,
                update_app_visuals,
                update_app_flow_particles,
                update_message_particles,
                update_trail_dots,
                update_link_lines,
                update_hud_text,
                update_pause_overlay,
                update_scan_lines,
                update_commit_rings,
                update_ambient_particles,
                update_energy_orbits,
                update_screen_flash,
                update_speed_lines,
                update_track_ring,
                update_event_log_panel,
            ),
        )
        .run();
}

// -- Startup: camera --------------------------------------------------------

fn setup_camera(mut commands: Commands) {
    commands.spawn(Camera2d);
}

// -- Startup: world grid + track ring + greyhounds + links ------------------

fn setup_world(
    mut commands: Commands,
    positions: Res<ReplicaPositions>,
    asset_server: Res<AssetServer>,
) {
    let mascot_handle: Handle<Image> = asset_server.load("iggy.png");

    // Diagonal grid (tactical arena feel)
    for offset in [-420.0, -280.0, -140.0, 0.0, 140.0, 280.0, 420.0] {
        let rising = shapes::Line(
            Vec2::new(-700.0, -520.0 + offset),
            Vec2::new(700.0, 880.0 + offset),
        );
        commands.spawn((
            build_stroke(&rising, GRID_LINE.with_alpha(0.22), 0.75),
            Transform::from_xyz(0.0, -90.0, -0.19),
        ));
        let falling = shapes::Line(
            Vec2::new(-700.0, 880.0 - offset),
            Vec2::new(700.0, -520.0 - offset),
        );
        commands.spawn((
            build_stroke(&falling, GRID_LINE.with_alpha(0.16), 0.75),
            Transform::from_xyz(0.0, -90.0, -0.19),
        ));
    }

    // Framing diamonds
    for (size, alpha, z) in [(640.0, 0.10, -0.14), (760.0, 0.04, -0.13)] {
        commands.spawn((
            build_stroke(
                &rectangle_shape(size, size),
                GRID_LINE.with_alpha(alpha),
                1.0,
            ),
            Transform {
                translation: Vec3::new(0.0, WORLD_CENTER_Y, z),
                rotation: Quat::from_rotation_z(PI / 4.0),
                ..default()
            },
        ));
    }

    // Scan lines (faint horizontal, scrolling down)
    for i in 0..SCAN_LINE_COUNT {
        let y = -500.0 + (i as f32) * (1000.0 / SCAN_LINE_COUNT as f32);
        let line = shapes::Line(Vec2::new(-700.0, 0.0), Vec2::new(700.0, 0.0));
        commands.spawn((
            ScanLine {
                speed: 30.0 + (i as f32) * 15.0,
            },
            build_stroke(&line, NEON_CYAN.with_alpha(0.03), 1.0),
            Transform::from_xyz(0.0, y, -0.05),
        ));
    }

    // Track ring
    commands.spawn((
        TrackRing,
        build_stroke(
            &circle_shape(CIRCLE_RADIUS),
            GRID_LINE.with_alpha(0.35),
            0.9,
        ),
        Transform::from_xyz(0.0, WORLD_CENTER_Y, 0.0),
    ));
    commands.spawn((
        build_stroke(
            &circle_shape(CIRCLE_RADIUS - 18.0),
            GRID_LINE.with_alpha(0.12),
            0.5,
        ),
        Transform::from_xyz(0.0, WORLD_CENTER_Y, -0.01),
    ));
    commands.spawn((
        build_stroke(
            &circle_shape(CIRCLE_RADIUS + 18.0),
            GRID_LINE.with_alpha(0.12),
            0.5,
        ),
        Transform::from_xyz(0.0, WORLD_CENTER_Y, -0.01),
    ));

    // Ambient floating particles
    for i in 0..AMBIENT_PARTICLE_COUNT {
        let phase = i as f32 / AMBIENT_PARTICLE_COUNT as f32;
        let x = (phase * 17.3 + 0.5).fract() * 1600.0 - 800.0;
        let y = (phase * 23.7 + 0.2).fract() * 1100.0 - 550.0;
        let size = 0.8 + (phase * 7.1).fract() * 1.8;
        let alpha = 0.04 + (phase * 13.3).fract() * 0.10;
        let color = match i % 5 {
            0 => NEON_CYAN,
            1 => IGGY_ORANGE,
            _ => COOL_WHITE,
        };

        commands.spawn((
            AmbientParticle {
                velocity: Vec2::new(
                    ((phase * 31.1).fract() - 0.5) * 12.0,
                    8.0 + (phase * 11.7).fract() * 18.0,
                ),
                drift_phase: phase * PI * 2.0,
                drift_speed: 0.4 + (phase * 5.3).fract() * 0.8,
                base_alpha: alpha,
            },
            build_fill(&circle_shape(size), color.with_alpha(alpha)),
            Transform::from_xyz(x, y, -0.08),
        ));
    }

    // Energy orbit dots around each replica
    for i in 0..REPLICA_COUNT {
        let pos = positions.0[i as usize];
        for j in 0..ORBIT_DOTS_PER_REPLICA {
            let angle = (j as f32 / ORBIT_DOTS_PER_REPLICA as f32) * PI * 2.0;
            let radius = 96.0 + (j as f32) * 3.0;
            let speed = 1.2 + (j as f32) * 0.3;

            commands.spawn((
                EnergyOrbit {
                    replica_id: i,
                    orbit_index: j,
                    angle,
                    radius,
                    speed,
                },
                build_fill(&circle_shape(2.2), NEON_CYAN.with_alpha(0.0)),
                Transform::from_xyz(pos.x, pos.y, 0.9),
            ));
        }
    }

    // Screen flash overlay (full-screen quad, initially transparent)
    commands.spawn((
        ScreenFlashOverlay,
        Sprite::from_color(NEON_CYAN.with_alpha(0.0), Vec2::new(1600.0, 1100.0)),
        Transform::from_xyz(0.0, 0.0, 90.0),
    ));

    // Spawn branded iggy replicas
    for i in 0..REPLICA_COUNT {
        let pos = positions.0[i as usize];
        let is_primary = i == 0;
        let sc = if is_primary { IGGY_ORANGE } else { NEON_CYAN };
        let facing_right = pos.x < 0.0;

        commands.spawn((
            ReplicaShadow { id: i },
            Sprite::from_color(BG_DARK.with_alpha(0.45), Vec2::new(118.0, 24.0)),
            Transform::from_xyz(pos.x, pos.y - 84.0, 0.35),
        ));

        commands.spawn((
            ReplicaGlow { id: i },
            build_fill(&circle_shape(74.0), sc.with_alpha(0.12)),
            Transform::from_xyz(pos.x, pos.y - 6.0, 0.5),
        ));

        commands.spawn((
            ReplicaNeonOutline { id: i },
            build_stroke(&circle_shape(82.0), sc.with_alpha(0.35), 2.0),
            Transform::from_xyz(pos.x, pos.y - 2.0, 0.8),
        ));

        commands.spawn((
            ReplicaBody { id: i },
            Sprite {
                image: mascot_handle.clone(),
                rect: Some(iggy_sprite_rect()),
                custom_size: Some(IGGY_SPRITE_SIZE),
                flip_x: !facing_right,
                ..default()
            },
            Transform::from_xyz(pos.x, pos.y + 8.0, 1.6),
        ));

        for (layer, glyph, size) in [(0u8, "z", 34.0), (1u8, "z", 28.0), (2u8, "z", 22.0)] {
            commands.spawn((
                ReplicaSleepText { id: i, layer },
                Text2d::new(glyph),
                TextFont {
                    font_size: size,
                    ..default()
                },
                TextColor(COOL_WHITE.with_alpha(0.9)),
                Transform::from_xyz(pos.x + 60.0, pos.y + 88.0, 1.8),
                Visibility::Hidden,
            ));
        }

        // ID text below mascot (z=3)
        commands.spawn((
            ReplicaIdText { id: i },
            Text2d::new(format!("R{i}")),
            TextFont {
                font_size: 28.0,
                ..default()
            },
            TextColor(COOL_WHITE),
            Transform::from_xyz(pos.x, pos.y - 112.0, 3.0),
        ));

        // Role + status info text below ID (z=3)
        let label = if is_primary {
            "LEAD DOG / running"
        } else {
            "PACK DOG / running"
        };
        let role_color = if is_primary { IGGY_ORANGE } else { DIM_GRAY };
        commands.spawn((
            ReplicaRoleText { id: i },
            Text2d::new(label),
            TextFont {
                font_size: 11.0,
                ..default()
            },
            TextColor(role_color),
            Transform::from_xyz(pos.x, pos.y - 130.0, 3.0),
        ));

        commands.spawn((
            ReplicaCalloutText { id: i },
            Text2d::new(""),
            TextFont {
                font_size: 17.0,
                ..default()
            },
            TextColor(COOL_WHITE.with_alpha(0.0)),
            Transform::from_xyz(pos.x, pos.y + 118.0, 3.2),
            Visibility::Hidden,
        ));
    }

    // Link lines between every pair
    for i in 0..REPLICA_COUNT {
        for j in (i + 1)..REPLICA_COUNT {
            let a = positions.0[i as usize];
            let b = positions.0[j as usize];
            let bend = if (i + j) % 2 == 0 { 56.0 } else { -56.0 };
            let lane = replica_link_path(a, b, bend);

            commands.spawn((
                LinkLine {
                    from_id: i,
                    to_id: j,
                },
                build_path_stroke(&lane, GRID_LINE.with_alpha(0.65), 1.4),
                Transform::from_xyz(0.0, 0.0, 0.05),
            ));
        }
    }

    let apps = [
        (
            0usize,
            "BALL THROWER",
            AppKind::Producer,
            Vec2::new(-560.0, 330.0),
            0u8,
            IGGY_ORANGE,
            true,
        ),
        (
            1usize,
            "BALL FETCHER",
            AppKind::Consumer,
            Vec2::new(560.0, 330.0),
            1u8,
            NEON_CYAN,
            false,
        ),
        (
            2usize,
            "TREAT DISPENSER",
            AppKind::Producer,
            Vec2::new(-560.0, 70.0),
            2u8,
            NEON_YELLOW,
            true,
        ),
        (
            3usize,
            "TREAT GOBBLER",
            AppKind::Consumer,
            Vec2::new(560.0, 70.0),
            0u8,
            COOL_WHITE,
            false,
        ),
    ];

    for (id, title, kind, anchor, target_replica, accent, inbound) in apps {
        let target = positions.0[target_replica as usize];
        let lane = app_link_path(anchor, target, inbound);
        commands.spawn((
            AppLink {
                id,
                anchor,
                target_replica,
                accent,
                inbound,
            },
            build_path_stroke(&lane, accent.with_alpha(0.18), 1.5),
            Transform::from_xyz(0.0, 0.0, 0.03),
        ));

        commands.spawn((
            AppCard { id, anchor, accent },
            build_fill(&circle_shape(50.0), accent.with_alpha(0.10)),
            Transform::from_xyz(anchor.x, anchor.y, 1.2),
        ));

        commands.spawn((
            AppIcon {
                id,
                anchor,
                accent,
                kind,
            },
            build_path_stroke(&app_icon_path(kind), accent.with_alpha(0.95), 2.4),
            Transform::from_xyz(anchor.x, anchor.y + 4.0, 1.35),
        ));

        commands.spawn((
            AppCardLabel { id, anchor },
            Text2d::new(title),
            TextFont {
                font_size: 13.0,
                ..default()
            },
            TextColor(COOL_WHITE),
            Transform::from_xyz(anchor.x, anchor.y - 64.0, 1.3),
        ));
    }
}

// -- Startup: HUD overlay ---------------------------------------------------

fn setup_hud(mut commands: Commands, asset_server: Res<AssetServer>) {
    let sygnet_handle: Handle<Image> =
        asset_server.load("logo/4x/iggy-apache-sygnet-color-darkbg@4x.png");
    let wordmark_handle: Handle<Image> =
        asset_server.load("logo/4x/iggy-apache-logo-wo-sygnet-light@4x.png");

    commands
        .spawn(Node {
            width: Val::Percent(100.0),
            height: Val::Percent(100.0),
            ..default()
        })
        .with_children(|root| {
            // ==================== TOP BAR ====================
            root.spawn((
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(0.0),
                    right: Val::Px(0.0),
                    top: Val::Px(0.0),
                    height: Val::Px(56.0),
                    padding: UiRect::axes(Val::Px(20.0), Val::Px(8.0)),
                    justify_content: JustifyContent::SpaceBetween,
                    align_items: AlignItems::Center,
                    ..default()
                },
                BackgroundColor(Color::srgba_u8(0x07, 0x0c, 0x17, 0xe8)),
            ))
            .with_children(|bar| {
                // Left: title + stats row
                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(24.0),
                    ..default()
                })
                .with_children(|left| {
                    left.spawn((
                        Text::new("iggy::pack"),
                        TextFont {
                            font_size: 16.0,
                            ..default()
                        },
                        TextColor(IGGY_ORANGE),
                    ));
                    // Treats
                    left.spawn(Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Baseline,
                        column_gap: Val::Px(6.0),
                        ..default()
                    })
                    .with_children(|stat| {
                        stat.spawn((
                            Text::new("TREATS"),
                            TextFont {
                                font_size: 9.0,
                                ..default()
                            },
                            TextColor(DIM_GRAY),
                        ));
                        stat.spawn((
                            HudCommitsText,
                            Text::new("0"),
                            TextFont {
                                font_size: 20.0,
                                ..default()
                            },
                            TextColor(NEON_CYAN),
                        ));
                    });
                    // Laps/sec
                    left.spawn(Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Baseline,
                        column_gap: Val::Px(6.0),
                        ..default()
                    })
                    .with_children(|stat| {
                        stat.spawn((
                            Text::new("LAPS/S"),
                            TextFont {
                                font_size: 9.0,
                                ..default()
                            },
                            TextColor(DIM_GRAY),
                        ));
                        stat.spawn((
                            HudOpsText,
                            Text::new("0"),
                            TextFont {
                                font_size: 20.0,
                                ..default()
                            },
                            TextColor(IGGY_ORANGE),
                        ));
                    });
                });

                // Center: sygnet + wordmark
                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(6.0),
                    ..default()
                })
                .with_children(|logo| {
                    logo.spawn((
                        ImageNode::new(sygnet_handle),
                        Node {
                            width: Val::Px(38.0),
                            height: Val::Px(29.0),
                            ..default()
                        },
                    ));
                    logo.spawn((
                        ImageNode::new(wordmark_handle),
                        Node {
                            width: Val::Px(78.0),
                            height: Val::Px(25.0),
                            ..default()
                        },
                    ));
                });

                // Right: state + tick
                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(16.0),
                    ..default()
                })
                .with_children(|right| {
                    right.spawn((
                        HudTickText,
                        Text::new("T 0"),
                        TextFont {
                            font_size: 18.0,
                            ..default()
                        },
                        TextColor(COOL_WHITE),
                    ));
                    right
                        .spawn(Node {
                            flex_direction: FlexDirection::Column,
                            align_items: AlignItems::End,
                            ..default()
                        })
                        .with_children(|col| {
                            col.spawn((
                                HudStateText,
                                Text::new("RACING"),
                                TextFont {
                                    font_size: 13.0,
                                    ..default()
                                },
                                TextColor(NEON_CYAN),
                            ));
                            col.spawn((
                                HudSpeedText,
                                Text::new("x1.0"),
                                TextFont {
                                    font_size: 10.0,
                                    ..default()
                                },
                                TextColor(DIM_GRAY),
                            ));
                        });
                });
            });

            // ==================== BOTTOM BAR ====================
            root.spawn((
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(0.0),
                    right: Val::Px(0.0),
                    bottom: Val::Px(0.0),
                    padding: UiRect::axes(Val::Px(20.0), Val::Px(10.0)),
                    justify_content: JustifyContent::SpaceBetween,
                    align_items: AlignItems::Center,
                    border: UiRect::top(Val::Px(1.0)),
                    ..default()
                },
                BackgroundColor(Color::srgba_u8(0x07, 0x0c, 0x17, 0xe8)),
                BorderColor::all(HUD_EDGE),
            ))
            .with_children(|bar| {
                // Left: title + subtitle
                bar.spawn(Node {
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(2.0),
                    ..default()
                })
                .with_children(|col| {
                    col.spawn((
                        Text::new("APACHE IGGY SIMULATOR"),
                        TextFont {
                            font_size: 14.0,
                            ..default()
                        },
                        TextColor(COOL_WHITE),
                    ));
                    col.spawn((
                        Text::new("Italian greyhounds racing to consensus"),
                        TextFont {
                            font_size: 10.0,
                            ..default()
                        },
                        TextColor(DIM_GRAY),
                    ));
                });

                // Center: keybinds
                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    column_gap: Val::Px(8.0),
                    ..default()
                })
                .with_children(|keys| {
                    for (key, action, color) in [
                        ("SPACE", "race/rest", NEON_CYAN),
                        ("R", "throw ball", IGGY_ORANGE),
                        ("K", "trip dog", HUD_ALERT),
                        ("H", "heal pack", NEON_CYAN),
                        ("E", "event log", NEON_YELLOW),
                        ("UP/DN", "speed", DIM_GRAY),
                    ] {
                        keys.spawn(Node {
                            flex_direction: FlexDirection::Row,
                            align_items: AlignItems::Center,
                            column_gap: Val::Px(4.0),
                            ..default()
                        })
                        .with_children(|pair| {
                            pair.spawn((
                                Node {
                                    padding: UiRect::axes(Val::Px(5.0), Val::Px(2.0)),
                                    border: UiRect::all(Val::Px(1.0)),
                                    ..default()
                                },
                                BackgroundColor(BG_PANEL),
                                BorderColor::all(color.with_alpha(0.4)),
                            ))
                            .with_children(|badge| {
                                badge.spawn((
                                    Text::new(key),
                                    TextFont {
                                        font_size: 10.0,
                                        ..default()
                                    },
                                    TextColor(color),
                                ));
                            });
                            pair.spawn((
                                Text::new(action),
                                TextFont {
                                    font_size: 9.0,
                                    ..default()
                                },
                                TextColor(DIM_GRAY),
                            ));
                        });
                    }
                });

                // Right: chips
                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    column_gap: Val::Px(8.0),
                    ..default()
                })
                .with_children(|chips| {
                    for (label, color) in
                        [("3 GREYHOUNDS", IGGY_ORANGE), ("FAULT READY", HUD_ALERT)]
                    {
                        chips
                            .spawn((
                                Node {
                                    padding: UiRect::axes(Val::Px(8.0), Val::Px(4.0)),
                                    border: UiRect::all(Val::Px(1.0)),
                                    ..default()
                                },
                                BackgroundColor(BG_PANEL.with_alpha(0.9)),
                                BorderColor::all(color.with_alpha(0.4)),
                            ))
                            .with_children(|chip| {
                                chip.spawn((
                                    Text::new(label),
                                    TextFont {
                                        font_size: 10.0,
                                        ..default()
                                    },
                                    TextColor(color),
                                ));
                            });
                    }
                });
            });

            // ==================== PAUSED OVERLAY ====================
            root.spawn((
                PauseOverlay,
                Node {
                    position_type: PositionType::Absolute,
                    width: Val::Percent(100.0),
                    height: Val::Percent(100.0),
                    justify_content: JustifyContent::Center,
                    align_items: AlignItems::Center,
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(8.0),
                    ..default()
                },
                Visibility::Hidden,
            ))
            .with_children(|col| {
                col.spawn((
                    PauseMainText,
                    Text::new("RESTING"),
                    TextFont {
                        font_size: 52.0,
                        ..default()
                    },
                    TextColor(NEON_YELLOW),
                ));
                col.spawn((
                    PauseSubtext,
                    Text::new("press SPACE to unleash"),
                    TextFont {
                        font_size: 16.0,
                        ..default()
                    },
                    TextColor(DIM_GRAY),
                ));
            });

            // ==================== EVENT LOG PANEL ====================
            // Sits between top bar (56px) and bottom bar (~48px)
            root.spawn((
                EventLogPanel,
                Node {
                    position_type: PositionType::Absolute,
                    right: Val::Px(0.0),
                    top: Val::Px(58.0),
                    bottom: Val::Px(50.0),
                    width: Val::Px(360.0),
                    flex_direction: FlexDirection::Column,
                    padding: UiRect::all(Val::Px(12.0)),
                    border: UiRect::left(Val::Px(2.0)),
                    overflow: Overflow::clip_y(),
                    ..default()
                },
                BackgroundColor(Color::srgba_u8(0x07, 0x0c, 0x17, 0xf2)),
                BorderColor::all(IGGY_ORANGE.with_alpha(0.3)),
                Visibility::Hidden,
            ))
            .with_children(|panel| {
                panel.spawn((
                    Text::new("PACK ACTIVITY LOG"),
                    TextFont {
                        font_size: 13.0,
                        ..default()
                    },
                    TextColor(IGGY_ORANGE),
                    Node {
                        margin: UiRect::bottom(Val::Px(4.0)),
                        ..default()
                    },
                ));
                panel.spawn((
                    Text::new("[D] toggle details  [E] close"),
                    TextFont {
                        font_size: 9.0,
                        ..default()
                    },
                    TextColor(DIM_GRAY),
                    Node {
                        margin: UiRect::bottom(Val::Px(8.0)),
                        ..default()
                    },
                ));
                panel.spawn((
                    Node {
                        width: Val::Percent(100.0),
                        height: Val::Px(1.0),
                        margin: UiRect::bottom(Val::Px(6.0)),
                        ..default()
                    },
                    BackgroundColor(IGGY_ORANGE.with_alpha(0.25)),
                ));
                panel.spawn((
                    EventLogContent,
                    Node {
                        flex_direction: FlexDirection::ColumnReverse,
                        flex_grow: 1.0,
                        overflow: Overflow::clip_y(),
                        row_gap: Val::Px(2.0),
                        ..default()
                    },
                ));
            });

            // Event log toggle hint (hidden when log is open)
            root.spawn((
                EventLogToggleHint,
                Node {
                    position_type: PositionType::Absolute,
                    right: Val::Px(16.0),
                    bottom: Val::Px(54.0),
                    ..default()
                },
            ))
            .with_children(|hint| {
                hint.spawn((
                    Text::new("[E] event log"),
                    TextFont {
                        font_size: 10.0,
                        ..default()
                    },
                    TextColor(DIM_GRAY),
                ));
            });
        });
}

// -- Update: keyboard input -------------------------------------------------

fn keyboard_input_system(
    keys: Res<ButtonInput<KeyCode>>,
    mut sim: ResMut<SimState>,
    mut event_log: ResMut<EventLog>,
) {
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
    }

    if keys.just_pressed(KeyCode::KeyK) {
        let states = sim.simulator.replica_states();
        for s in &states {
            if s.alive {
                sim.simulator.kill_replica(s.id);
                break;
            }
        }
    }

    if keys.just_pressed(KeyCode::KeyH) {
        for i in 0..REPLICA_COUNT {
            sim.simulator.restart_replica(i);
        }
        for i in 0..REPLICA_COUNT {
            for j in (i + 1)..REPLICA_COUNT {
                sim.simulator.heal_link(i, j);
            }
        }
    }

    if keys.just_pressed(KeyCode::KeyE) {
        event_log.visible = !event_log.visible;
    }
    if keys.just_pressed(KeyCode::KeyD) {
        event_log.show_details = !event_log.show_details;
    }
}

// -- Update: simulation tick -- spawn message particles + glow flashes ------

#[allow(clippy::too_many_arguments)]
fn simulation_tick_system(
    mut commands: Commands,
    time: Res<Time>,
    mut sim: ResMut<SimState>,
    positions: Res<ReplicaPositions>,
    mut replica_fx: ResMut<ReplicaFxState>,
    mut app_fx: ResMut<AppFxState>,
    mut screen_flash: ResMut<ScreenFlash>,
    mut track_pulse: ResMut<TrackPulse>,
    mut event_log: ResMut<EventLog>,
) {
    let elapsed = time.elapsed_secs_f64();
    if elapsed - sim.ops_window_start >= 1.0 {
        sim.ops_per_second = sim.ops_window_count as f64 / (elapsed - sim.ops_window_start);
        sim.ops_window_start = elapsed;
        sim.ops_window_count = 0;
    }

    if !sim.playing {
        return;
    }

    let dt = time.delta_secs();
    sim.tick_accumulator += dt * sim.speed * 60.0;

    let ticks_to_run = sim.tick_accumulator as u32;
    sim.tick_accumulator -= ticks_to_run as f32;

    for _ in 0..ticks_to_run.min(10) {
        let events = sim.simulator.step();

        for event in &events {
            if let Some(entry) = narrate_event(event) {
                event_log.push(entry);
            }
            match event {
                vsr::SimEvent::MessageSent { message, .. } => {
                    if message.msg_type == vsr::MessageType::ClientReply {
                        app_fx.pulse[1] = 0.7;
                    }

                    let from_idx = message.from as usize;
                    let to_idx = message.to as usize;

                    if from_idx >= REPLICA_COUNT as usize || to_idx >= REPLICA_COUNT as usize {
                        continue;
                    }

                    let from_pos = positions.0[from_idx];
                    let to_pos = positions.0[to_idx];
                    let control = perpendicular_control(from_pos, to_pos, 40.0);
                    let msg_color = message.msg_type.color();

                    // Inner particle: neon-colored filled circle
                    let shape = build_fill_stroke(
                        &circle_shape(4.0),
                        msg_color,
                        msg_color.with_alpha(0.3),
                        1.0,
                    );
                    commands.spawn((
                        MessageParticle {
                            from: from_pos,
                            to: to_pos,
                            control,
                            progress: 0.0,
                            duration: MSG_DURATION,
                            msg_type: message.msg_type,
                        },
                        shape,
                        Transform::from_xyz(from_pos.x, from_pos.y, 5.0),
                    ));

                    // Outer glow circle: larger, faint
                    let glow_shape = build_fill(&circle_shape(8.0), msg_color.with_alpha(0.2));
                    commands.spawn((
                        MessageGlow,
                        MessageParticle {
                            from: from_pos,
                            to: to_pos,
                            control,
                            progress: 0.0,
                            duration: MSG_DURATION,
                            msg_type: message.msg_type,
                        },
                        glow_shape,
                        Transform::from_xyz(from_pos.x, from_pos.y, 4.9),
                    ));
                }
                vsr::SimEvent::OperationCommitted { replica_id, .. } => {
                    sim.total_commits += 1;
                    sim.ops_window_count += 1;
                    replica_fx.healthy[*replica_id as usize] = 0.45;
                    trigger_replica_callout(
                        &mut replica_fx,
                        *replica_id,
                        "GOOD BOY!",
                        IGGY_ORANGE,
                        0.9,
                    );
                    app_fx.pulse[2] = 0.55;
                    track_pulse.energy = (track_pulse.energy + 0.4).min(1.0);
                    commands.spawn(GlowFlash {
                        id: *replica_id,
                        timer: 0.3,
                        flash_color: IGGY_ORANGE,
                    });

                    // Commit ring explosion!
                    let center = positions.0[*replica_id as usize];
                    for ring_i in 0..3u8 {
                        let delay_factor = ring_i as f32 * 0.08;
                        commands.spawn((
                            CommitRing {
                                center,
                                lifetime: COMMIT_RING_LIFETIME + delay_factor,
                                max_lifetime: COMMIT_RING_LIFETIME + delay_factor,
                                color: if ring_i == 0 { IGGY_ORANGE } else { NEON_CYAN },
                                max_radius: 120.0 + ring_i as f32 * 40.0,
                            },
                            build_stroke(
                                &circle_shape(1.0),
                                IGGY_ORANGE.with_alpha(0.6),
                                2.5 - ring_i as f32 * 0.5,
                            ),
                            Transform::from_xyz(center.x, center.y, 4.5 - ring_i as f32 * 0.1),
                        ));
                    }
                }
                vsr::SimEvent::ViewChangeStarted { replica_id, .. } => {
                    trigger_replica_callout(
                        &mut replica_fx,
                        *replica_id,
                        "PACK SHUFFLE!",
                        NEON_MAGENTA,
                        1.15,
                    );
                    app_fx.pulse[3] = 0.8;
                    // Screen-wide magenta flash for dramatic effect
                    screen_flash.timer = SCREEN_FLASH_DURATION;
                    screen_flash.color = NEON_MAGENTA;
                    screen_flash.intensity = 0.12;
                    for rid in 0..REPLICA_COUNT {
                        commands.spawn(GlowFlash {
                            id: rid,
                            timer: 0.3,
                            flash_color: NEON_MAGENTA,
                        });
                        // Shockwave ring from the triggering replica
                        if rid == *replica_id {
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
                    }
                }
                vsr::SimEvent::ClientRequest { .. } => {
                    sim.ops_window_count += 1;
                    app_fx.pulse[0] = 0.7;
                }
                vsr::SimEvent::PrimaryElected { replica_id, .. } => {
                    trigger_replica_callout(
                        &mut replica_fx,
                        *replica_id,
                        "NEW LEAD DOG!",
                        NEON_YELLOW,
                        1.2,
                    );
                    // Golden flash for new leader
                    screen_flash.timer = SCREEN_FLASH_DURATION * 0.7;
                    screen_flash.color = NEON_YELLOW;
                    screen_flash.intensity = 0.08;
                    // Victory ring
                    let center = positions.0[*replica_id as usize];
                    for ring_i in 0..2u8 {
                        commands.spawn((
                            CommitRing {
                                center,
                                lifetime: 0.8,
                                max_lifetime: 0.8,
                                color: NEON_YELLOW,
                                max_radius: 150.0 + ring_i as f32 * 50.0,
                            },
                            build_stroke(&circle_shape(1.0), NEON_YELLOW.with_alpha(0.45), 2.5),
                            Transform::from_xyz(center.x, center.y, 4.4),
                        ));
                    }
                }
                vsr::SimEvent::ReplicaStateChanged {
                    replica_id,
                    new_status,
                    ..
                } => {
                    let (label, color, duration) = match new_status {
                        vsr::Status::Normal => ("RUNNING!", NEON_CYAN, 0.85),
                        vsr::Status::ViewChange => ("HOWLING!", NEON_MAGENTA, 1.1),
                        vsr::Status::Recovering => ("LIMPING...", NEON_YELLOW, 1.1),
                    };
                    trigger_replica_callout(&mut replica_fx, *replica_id, label, color, duration);
                }
                _ => {}
            }
        }
    }
}

// -- Update: replica visuals (neon glow breathing, bobbing, status text) -----

#[allow(clippy::type_complexity, clippy::too_many_arguments)]
fn update_replica_visuals(
    time: Res<Time>,
    sim: Res<SimState>,
    positions: Res<ReplicaPositions>,
    mut replica_fx: ResMut<ReplicaFxState>,
    mut commands: Commands,
    mut flash_q: Query<(Entity, &mut GlowFlash)>,
    mut visuals: ParamSet<(
        Query<(&ReplicaShadow, &mut Sprite, &mut Transform)>,
        Query<(&ReplicaGlow, &mut Shape, &mut Transform), Without<ReplicaNeonOutline>>,
        Query<(&ReplicaNeonOutline, &mut Shape, &mut Transform), Without<ReplicaGlow>>,
        Query<(&ReplicaBody, &mut Sprite, &mut Transform)>,
        Query<(
            &ReplicaSleepText,
            &mut Transform,
            &mut TextColor,
            &mut Visibility,
        )>,
        Query<(&ReplicaIdText, &mut TextColor), Without<ReplicaRoleText>>,
        Query<(&ReplicaRoleText, &mut Text2d, &mut TextColor), Without<ReplicaIdText>>,
        Query<
            (
                &ReplicaCalloutText,
                &mut Text2d,
                &mut Transform,
                &mut TextColor,
                &mut Visibility,
            ),
            (Without<ReplicaIdText>, Without<ReplicaRoleText>),
        >,
    )>,
) {
    let states = sim.simulator.replica_states();
    let t = time.elapsed_secs();
    let dt = time.delta_secs();

    for i in 0..REPLICA_COUNT as usize {
        replica_fx.kill[i] = (replica_fx.kill[i] - dt * 1.4).max(0.0);
        replica_fx.revive[i] = (replica_fx.revive[i] - dt * 1.1).max(0.0);
        replica_fx.healthy[i] = (replica_fx.healthy[i] - dt * 0.9).max(0.0);
        replica_fx.callout_timer[i] = (replica_fx.callout_timer[i] - dt).max(0.0);
    }

    // Process glow flash timers
    let mut flash_overrides: [Option<Color>; REPLICA_COUNT as usize] =
        [None; REPLICA_COUNT as usize];
    for (entity, mut flash) in flash_q.iter_mut() {
        flash.timer -= dt;
        if flash.timer <= 0.0 {
            commands.entity(entity).despawn();
        } else {
            let idx = flash.id as usize;
            if idx < REPLICA_COUNT as usize {
                flash_overrides[idx] = Some(flash.flash_color);
            }
        }
    }

    for state in &states {
        let id = state.id;
        let idx = id as usize;
        let sc = status_color(state.status, state.alive);
        let base_pos = positions.0[idx];
        let is_primary = state.role == vsr::Role::Primary;
        let facing_right = base_pos.x < 0.0;

        if replica_fx.last_alive[idx] != state.alive {
            if state.alive {
                replica_fx.revive[idx] = 1.0;
                replica_fx.healthy[idx] = 0.6;
                trigger_replica_callout(&mut replica_fx, id, "BACK ON TRACK!", IGGY_ORANGE, 1.3);
            } else {
                replica_fx.kill[idx] = 1.0;
                trigger_replica_callout(&mut replica_fx, id, "GREYHOUND DOWN", NEON_MAGENTA, 1.4);
            }
            replica_fx.last_alive[idx] = state.alive;
        }
        if replica_fx.last_status[idx] != state.status {
            if state.status == vsr::Status::Normal {
                replica_fx.healthy[idx] = 0.5;
                trigger_replica_callout(&mut replica_fx, id, "RUNNING!", NEON_CYAN, 0.8);
            } else if state.status == vsr::Status::Recovering {
                replica_fx.revive[idx] = 0.8;
                trigger_replica_callout(&mut replica_fx, id, "LIMPING...", NEON_YELLOW, 1.1);
            } else if state.status == vsr::Status::ViewChange {
                trigger_replica_callout(&mut replica_fx, id, "HOWLING!", NEON_MAGENTA, 1.1);
            }
            replica_fx.last_status[idx] = state.status;
        }

        let dir: f32 = if facing_right { 1.0 } else { -1.0 };

        // Game-like idle motion - greyhound vibes
        let busy = state.pipeline_depth as f32 / 8.0; // 0..1 how loaded
        let hover = (t * (1.3 + busy * 0.8) + id as f32 * 1.7).sin() * (7.0 + busy * 4.0);
        let sway = (t * (0.9 + busy * 1.2) + id as f32 * 0.8).sin() * (0.06 + busy * 0.04);
        let breathe =
            1.0 + (t * (1.8 + busy * 1.5) + id as f32 * 0.9).sin() * (0.035 + busy * 0.025);
        // Running lean: greyhound tilts forward when busy
        let run_lean = busy * 0.08 * dir;
        // Stretch horizontally when running (greyhound elongation)
        let run_stretch_x = 1.0 + busy * 0.06;
        let run_squash_y = 1.0 - busy * 0.03;

        // Determine effective glow color (flash overrides normal)
        let glow_color = flash_overrides[idx].unwrap_or(sc);
        let flash_energy = if flash_overrides[idx].is_some() {
            1.0
        } else {
            0.0
        };
        let glow_alpha =
            0.12 + ((t * 2.0 + id as f32 * 1.5).sin() * 0.5 + 0.5) * 0.08 + flash_energy * 0.18;
        let body_alpha = if state.alive { 1.0 } else { 0.24 };
        let impact_scale = 1.0 + flash_energy * 0.08;
        let kill_energy = replica_fx.kill[idx];
        let revive_energy = replica_fx.revive[idx];
        let healthy_energy = replica_fx.healthy[idx];
        let fx_lift = revive_energy * 18.0 - kill_energy * 12.0;
        for (shadow, mut sprite, mut transform) in visuals.p0().iter_mut() {
            if shadow.id == id {
                sprite.color = BG_DARK.with_alpha(0.22 + glow_alpha * 0.45 + kill_energy * 0.12);
                transform.translation.x = base_pos.x;
                transform.translation.y = base_pos.y - 88.0 + hover * 0.15 - fx_lift * 0.2;
                transform.scale = Vec3::new(
                    1.0 + flash_energy * 0.12 + kill_energy * 0.18,
                    0.9 - hover * 0.003 - revive_energy * 0.08,
                    1.0,
                );
            }
        }

        // Outer glow (z=0.5)
        for (glow, mut shape, mut transform) in visuals.p1().iter_mut() {
            if glow.id == id {
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
                    1.0 + ((t * 1.4 + id as f32).sin() * 0.03)
                        + flash_energy * 0.14
                        + revive_energy * 0.12
                        + healthy_energy * 0.08,
                );
            }
        }

        // Neon outline (z=0.8)
        for (outline, mut shape, mut transform) in visuals.p2().iter_mut() {
            if outline.id == id {
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
        }

        // Mascot body sprite
        for (body, mut shape, mut transform) in visuals.p3().iter_mut() {
            if body.id == id {
                shape.color = if !state.alive {
                    Color::srgba(0.55, 0.52, 0.60, 0.33)
                } else if revive_energy > 0.0 {
                    Color::srgba(1.0, 0.96, 0.9, body_alpha)
                } else if is_primary {
                    Color::srgba(1.0, 0.97, 0.94, body_alpha)
                } else {
                    Color::srgba(0.94, 0.98, 1.0, body_alpha)
                };
                shape.flip_x = !facing_right;
                transform.translation.x = base_pos.x;
                transform.translation.y = base_pos.y + 12.0 + hover + fx_lift - kill_energy * 14.0;
                transform.rotation = Quat::from_rotation_z(
                    sway + run_lean + flash_energy * 0.04 * dir - kill_energy * 0.24 * dir,
                );
                transform.scale = Vec3::new(
                    dir * breathe
                        * impact_scale
                        * run_stretch_x
                        * (1.0 + revive_energy * 0.1 - kill_energy * 0.16),
                    (breathe / impact_scale)
                        * run_squash_y
                        * (1.0 - kill_energy * 0.34 + healthy_energy * 0.08),
                    1.0,
                );
            }
        }

        for (sleep, mut transform, mut color, mut visibility) in visuals.p4().iter_mut() {
            if sleep.id == id {
                *visibility = if state.alive {
                    Visibility::Hidden
                } else {
                    Visibility::Inherited
                };
                let phase = (t * 0.85 + sleep.layer as f32 * 0.55 + id as f32 * 0.4).fract();
                let rise = phase * 34.0;
                let drift = (phase * PI * 2.0).sin() * 7.0;
                transform.translation.x =
                    base_pos.x + (70.0 + sleep.layer as f32 * 12.0) * dir + drift * dir;
                transform.translation.y = base_pos.y + 68.0 + rise + sleep.layer as f32 * 8.0;
                transform.scale = Vec3::splat(0.7 + phase * 0.55);
                *color =
                    TextColor(COOL_WHITE.with_alpha((1.0 - phase) * 0.8 * kill_energy.max(0.65)));
            }
        }

        // ID text color: magenta if dead
        for (id_txt, mut color) in visuals.p5().iter_mut() {
            if id_txt.id == id {
                *color = TextColor(if state.alive {
                    COOL_WHITE
                } else {
                    NEON_MAGENTA
                });
            }
        }

        // Role text
        for (role_txt, mut text, mut color) in visuals.p6().iter_mut() {
            if role_txt.id == id {
                let label = if !state.alive {
                    "NAPPING"
                } else {
                    role_label(state.role, state.status)
                };
                **text = label.to_string();
                let status_pulse = ((t * 4.2 + id as f32 * 0.7).sin() * 0.5) + 0.5;
                *color = TextColor(if !state.alive {
                    NEON_MAGENTA.with_alpha(0.88 + status_pulse * 0.12)
                } else if state.status == vsr::Status::ViewChange {
                    NEON_MAGENTA.with_alpha(0.84 + status_pulse * 0.16)
                } else if state.status == vsr::Status::Recovering {
                    NEON_YELLOW.with_alpha(0.84 + status_pulse * 0.16)
                } else if is_primary {
                    IGGY_ORANGE
                } else {
                    DIM_GRAY
                });
            }
        }

        for (callout, mut text, mut transform, mut color, mut visibility) in visuals.p7().iter_mut()
        {
            if callout.id != id {
                continue;
            }

            let timer = replica_fx.callout_timer[idx];
            let persistent = if !state.alive {
                Some(("NAPPING", NEON_MAGENTA))
            } else {
                match state.status {
                    vsr::Status::Normal => None,
                    vsr::Status::ViewChange => Some(("HOWLING!", NEON_MAGENTA)),
                    vsr::Status::Recovering => Some(("LIMPING...", NEON_YELLOW)),
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
            let pulse = ((t * 6.0 + id as f32 * 1.3).sin() * 0.5) + 0.5;
            let rise = (1.0 - energy) * 18.0;
            **text = label.to_string();
            transform.translation = Vec3::new(base_pos.x, base_pos.y + 118.0 + rise, 3.2);
            transform.scale = Vec3::splat(0.92 + energy * 0.22 + pulse * 0.04);
            *color =
                TextColor(base_color.with_alpha((0.55 + energy * 0.35 + pulse * 0.1).min(1.0)));
        }
    }
}

// -- Update: move message particles along bezier, spawn trail dots ----------

fn update_message_particles(
    mut commands: Commands,
    time: Res<Time>,
    mut sim: ResMut<SimState>,
    mut query: Query<(Entity, &mut MessageParticle, &mut Transform)>,
) {
    let dt = time.delta_secs();
    sim.frame_count = sim.frame_count.wrapping_add(1);
    let spawn_trail = sim.frame_count.is_multiple_of(3);

    for (entity, mut particle, mut transform) in query.iter_mut() {
        particle.progress += dt / particle.duration;

        if particle.progress >= 1.0 {
            commands.entity(entity).despawn();
            continue;
        }

        let pos = bezier_pos(
            particle.from,
            particle.to,
            particle.control,
            particle.progress,
        );
        transform.translation.x = pos.x;
        transform.translation.y = pos.y;

        // Spawn trail dot (tiny fading circle)
        if spawn_trail {
            let trail_color = particle.msg_type.color();
            spawn_trail_dot(&mut commands, pos, trail_color);

            // Speed lines perpendicular to travel direction
            if particle.progress > 0.1 && particle.progress < 0.9 {
                let dir = (particle.to - particle.from).normalize_or_zero();
                let perp = Vec2::new(-dir.y, dir.x);
                let offset = perp * ((sim.frame_count as f32 * 0.7).sin() * 6.0);
                let line_len = 8.0 + particle.progress * 12.0;
                let line = shapes::Line(
                    pos + offset - dir * line_len * 0.5,
                    pos + offset + dir * line_len * 0.5,
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
    }
}

/// Spawn a tiny trail dot at `pos`.
fn spawn_trail_dot(commands: &mut Commands, pos: Vec2, color: Color) {
    let alpha = 0.35;
    let c = color.with_alpha(alpha);

    commands.spawn((
        TrailDot {
            lifetime: TRAIL_LIFETIME,
            max_lifetime: TRAIL_LIFETIME,
            color,
        },
        build_fill(&circle_shape(1.5), c),
        Transform::from_xyz(pos.x, pos.y, 4.0),
    ));
}

// -- Update: fade and despawn trail dots ------------------------------------

fn update_trail_dots(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut TrailDot, &mut Shape)>,
) {
    let dt = time.delta_secs();

    for (entity, mut dot, mut shape) in query.iter_mut() {
        dot.lifetime -= dt;
        if dot.lifetime <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }
        let alpha = (dot.lifetime / dot.max_lifetime) * 0.35;
        shape.fill = Some(Fill::color(dot.color.with_alpha(alpha)));
    }
}

// -- Update: link lines -----------------------------------------------------

fn update_link_lines(
    time: Res<Time>,
    sim: Res<SimState>,
    mut query: Query<(&LinkLine, &mut Shape)>,
) {
    let t = time.elapsed_secs();
    for (link, mut shape) in query.iter_mut() {
        let is_partitioned = sim
            .simulator
            .network
            .is_partitioned(link.from_id, link.to_id);
        let from_alive = sim
            .simulator
            .replicas
            .get(link.from_id as usize)
            .is_some_and(|r| r.alive);
        let to_alive = sim
            .simulator
            .replicas
            .get(link.to_id as usize)
            .is_some_and(|r| r.alive);
        let pulse = ((t * 1.7 + (link.from_id + link.to_id) as f32 * 0.9).sin() * 0.5) + 0.5;

        if is_partitioned || !from_alive || !to_alive {
            shape.stroke = Some(Stroke::new(
                NEON_MAGENTA.with_alpha(0.28 + pulse * 0.22),
                1.8 + pulse * 0.5,
            ));
        } else {
            shape.stroke = Some(Stroke::new(
                NEON_CYAN.with_alpha(0.12 + pulse * 0.14),
                1.3 + pulse * 0.35,
            ));
        }
    }
}

#[allow(clippy::type_complexity)]
fn update_app_visuals(
    time: Res<Time>,
    positions: Res<ReplicaPositions>,
    sim: Res<SimState>,
    mut app_fx: ResMut<AppFxState>,
    mut visuals: ParamSet<(
        Query<(&AppCard, &mut Shape, &mut Transform)>,
        Query<(&AppLink, &mut Shape)>,
        Query<(&AppCardLabel, &mut Transform, &mut TextColor)>,
        Query<(&AppIcon, &mut Shape, &mut Transform)>,
    )>,
) {
    let dt = time.delta_secs();
    let t = time.elapsed_secs();

    for pulse in &mut app_fx.pulse {
        *pulse = (*pulse - dt * 1.15).max(0.0);
    }

    for (card, mut shape, mut transform) in visuals.p0().iter_mut() {
        let pulse = app_fx.pulse[card.id];
        let hover = (t * 1.05 + card.id as f32 * 0.9).sin() * 5.0;
        shape.fill = Some(Fill::color(card.accent.with_alpha(0.08 + pulse * 0.12)));
        shape.stroke = Some(Stroke::new(
            card.accent.with_alpha(0.18 + pulse * 0.45),
            1.0 + pulse * 1.5,
        ));
        transform.translation = Vec3::new(card.anchor.x, card.anchor.y + hover + pulse * 10.0, 1.2);
        transform.rotation = Quat::from_rotation_z((t * 0.35 + card.id as f32).sin() * 0.015);
        transform.scale = Vec3::splat(1.0 + pulse * 0.06);
    }

    for (label, mut transform, mut color) in visuals.p2().iter_mut() {
        let pulse = app_fx.pulse[label.id];
        let hover = (t * 1.05 + label.id as f32 * 0.9).sin() * 5.0;
        transform.translation = Vec3::new(
            label.anchor.x,
            label.anchor.y - 64.0 + hover + pulse * 10.0,
            1.3,
        );
        *color = TextColor(COOL_WHITE.with_alpha(0.9 + pulse * 0.1));
    }

    for (icon, mut shape, mut transform) in visuals.p3().iter_mut() {
        let pulse = app_fx.pulse[icon.id];
        let hover = (t * 1.05 + icon.id as f32 * 0.9).sin() * 5.0;
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
        transform.rotation = Quat::from_rotation_z(tilt + (t * 0.7 + icon.id as f32).sin() * 0.025);
        transform.scale = Vec3::splat(1.0 + pulse * 0.08);
    }

    for (link, mut shape) in visuals.p1().iter_mut() {
        let pulse = app_fx.pulse[link.id];
        let target = positions.0[link.target_replica as usize];
        let target_alive = sim
            .simulator
            .replicas
            .get(link.target_replica as usize)
            .is_some_and(|r| r.alive);
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

fn update_app_flow_particles(
    mut commands: Commands,
    time: Res<Time>,
    positions: Res<ReplicaPositions>,
    sim: Res<SimState>,
    links: Query<&AppLink>,
    mut particles: Query<(Entity, &mut AppFlowParticle, &mut Transform, &mut Shape)>,
) {
    let dt = time.delta_secs();

    if sim.frame_count.is_multiple_of(36) {
        for link in links.iter() {
            let target_alive = sim
                .simulator
                .replicas
                .get(link.target_replica as usize)
                .is_some_and(|r| r.alive);
            if !target_alive {
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
        let target_alive = sim
            .simulator
            .replicas
            .get(particle.replica_id as usize)
            .is_some_and(|r| r.alive);
        if !target_alive {
            commands.entity(entity).despawn();
            continue;
        }

        particle.progress += dt * particle.speed;
        if particle.progress >= 1.0 {
            commands.entity(entity).despawn();
            continue;
        }

        let pos = bezier_pos(
            particle.from,
            particle.to,
            particle.control,
            particle.progress,
        );
        transform.translation.x = pos.x;
        transform.translation.y = pos.y;
        let alpha = (1.0 - (particle.progress - 0.5).abs() * 1.4).clamp(0.18, 0.92);
        shape.fill = Some(Fill::color(particle.color.with_alpha(alpha)));
    }
}

// -- Update: HUD text -------------------------------------------------------

#[allow(clippy::type_complexity)]
fn update_hud_text(
    sim: Res<SimState>,
    mut hud: ParamSet<(
        Query<&mut Text, With<HudTickText>>,
        Query<&mut Text, (With<HudOpsText>, Without<HudTickText>)>,
        Query<
            &mut Text,
            (
                With<HudCommitsText>,
                Without<HudTickText>,
                Without<HudOpsText>,
            ),
        >,
        Query<
            (&mut Text, &mut TextColor),
            (
                With<HudStateText>,
                Without<HudTickText>,
                Without<HudOpsText>,
                Without<HudCommitsText>,
            ),
        >,
        Query<
            &mut Text,
            (
                With<HudSpeedText>,
                Without<HudTickText>,
                Without<HudOpsText>,
                Without<HudCommitsText>,
                Without<HudStateText>,
            ),
        >,
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

// -- Update: pause overlay with pulsing alpha -------------------------------

#[allow(clippy::type_complexity)]
fn update_pause_overlay(
    sim: Res<SimState>,
    time: Res<Time>,
    mut pause: ParamSet<(
        Query<&mut Visibility, (With<PauseOverlay>, Without<PauseMainText>)>,
        Query<
            &mut TextColor,
            (
                With<PauseMainText>,
                Without<PauseOverlay>,
                Without<PauseSubtext>,
            ),
        >,
        Query<&mut Visibility, (With<PauseSubtext>, Without<PauseOverlay>)>,
    )>,
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

// -- Update: scan lines scroll down -----------------------------------------

fn update_scan_lines(time: Res<Time>, mut query: Query<(&ScanLine, &mut Transform)>) {
    let dt = time.delta_secs();

    for (scan, mut transform) in query.iter_mut() {
        transform.translation.y -= scan.speed * dt;
        if transform.translation.y < -500.0 {
            transform.translation.y = 500.0;
        }
    }
}

// -- Update: expanding commit rings -----------------------------------------

fn update_commit_rings(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut CommitRing, &mut Shape, &mut Transform)>,
) {
    let dt = time.delta_secs();

    for (entity, mut ring, mut shape, mut transform) in query.iter_mut() {
        ring.lifetime -= dt;
        if ring.lifetime <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }

        let progress = 1.0 - (ring.lifetime / ring.max_lifetime);
        let eased = 1.0 - (1.0 - progress).powi(3); // ease-out cubic
        let current_radius = ring.max_radius * eased;
        let alpha = (1.0 - progress) * 0.6;
        let width = (1.0 - progress) * 3.0 + 0.5;

        shape.stroke = Some(Stroke::new(ring.color.with_alpha(alpha), width));
        transform.translation.x = ring.center.x;
        transform.translation.y = ring.center.y;
        transform.scale = Vec3::splat(current_radius.max(1.0));
    }
}

// -- Update: ambient floating particles -------------------------------------

fn update_ambient_particles(
    time: Res<Time>,
    sim: Res<SimState>,
    mut query: Query<(&mut AmbientParticle, &mut Transform, &mut Shape)>,
) {
    let dt = time.delta_secs();
    let t = time.elapsed_secs();
    let activity_boost = if sim.playing { 1.0 } else { 0.3 };

    for (particle, mut transform, mut shape) in query.iter_mut() {
        // Gentle upward drift with sinusoidal wandering
        let drift_x = (t * particle.drift_speed + particle.drift_phase).sin() * 18.0;
        transform.translation.x += (particle.velocity.x + drift_x * 0.3) * dt * activity_boost;
        transform.translation.y += particle.velocity.y * dt * activity_boost;

        // Wrap around screen edges
        if transform.translation.y > 600.0 {
            transform.translation.y = -600.0;
        }
        if transform.translation.x > 900.0 {
            transform.translation.x = -900.0;
        } else if transform.translation.x < -900.0 {
            transform.translation.x = 900.0;
        }

        // Twinkle effect
        let twinkle = ((t * 2.5 + particle.drift_phase * 3.7).sin() * 0.5 + 0.5) * 0.7 + 0.3;
        let alpha = particle.base_alpha * twinkle * activity_boost;
        if let Some(ref mut fill) = shape.fill {
            *fill = Fill::color(COOL_WHITE.with_alpha(alpha));
        }
    }
}

// -- Update: energy orbit dots around active replicas -----------------------

fn update_energy_orbits(
    time: Res<Time>,
    sim: Res<SimState>,
    positions: Res<ReplicaPositions>,
    mut query: Query<(&mut EnergyOrbit, &mut Transform, &mut Shape)>,
) {
    let dt = time.delta_secs();
    let states = sim.simulator.replica_states();

    for (mut orbit, mut transform, mut shape) in query.iter_mut() {
        let state = states.iter().find(|s| s.id == orbit.replica_id);
        let (alive, is_primary, status) = state
            .map(|s| (s.alive, s.role == vsr::Role::Primary, s.status))
            .unwrap_or((false, false, vsr::Status::Normal));

        // Only orbit active replicas
        if !alive {
            shape.fill = Some(Fill::color(NEON_MAGENTA.with_alpha(0.0)));
            continue;
        }

        let speed_mult = if is_primary { 1.4 } else { 1.0 };
        orbit.angle += dt * orbit.speed * speed_mult;

        let center = positions.0[orbit.replica_id as usize];
        let x = center.x + orbit.angle.cos() * orbit.radius;
        let y = center.y + orbit.angle.sin() * orbit.radius;
        transform.translation.x = x;
        transform.translation.y = y;

        let color = match status {
            vsr::Status::Normal => {
                if is_primary {
                    IGGY_ORANGE
                } else {
                    NEON_CYAN
                }
            }
            vsr::Status::ViewChange => NEON_MAGENTA,
            vsr::Status::Recovering => NEON_YELLOW,
        };

        // Pulsing alpha based on orbit position
        let pulse =
            ((orbit.angle * 2.0 + orbit.orbit_index as f32 * 1.2).sin() * 0.5 + 0.5) * 0.4 + 0.2;
        shape.fill = Some(Fill::color(color.with_alpha(pulse)));
    }
}

// -- Update: screen flash overlay -------------------------------------------

fn update_screen_flash(
    time: Res<Time>,
    mut flash: ResMut<ScreenFlash>,
    mut query: Query<&mut Sprite, With<ScreenFlashOverlay>>,
) {
    let dt = time.delta_secs();

    if flash.timer > 0.0 {
        flash.timer -= dt;
        let progress = (flash.timer / SCREEN_FLASH_DURATION).max(0.0);
        let alpha = progress * flash.intensity;

        for mut sprite in query.iter_mut() {
            sprite.color = flash.color.with_alpha(alpha);
        }
    } else {
        for mut sprite in query.iter_mut() {
            sprite.color = NEON_CYAN.with_alpha(0.0);
        }
    }
}

// -- Update: speed lines fade out ------------------------------------------

fn update_speed_lines(
    mut commands: Commands,
    time: Res<Time>,
    mut query: Query<(Entity, &mut SpeedLine, &mut Shape)>,
) {
    let dt = time.delta_secs();

    for (entity, mut line, mut shape) in query.iter_mut() {
        line.lifetime -= dt;
        if line.lifetime <= 0.0 {
            commands.entity(entity).despawn();
            continue;
        }
        let alpha = (line.lifetime / line.max_lifetime) * 0.25;
        shape.stroke = Some(Stroke::new(line.color.with_alpha(alpha), 0.6));
    }
}

// -- Update: track ring pulses with activity --------------------------------

fn update_track_ring(
    time: Res<Time>,
    mut track_pulse: ResMut<TrackPulse>,
    mut query: Query<&mut Shape, With<TrackRing>>,
) {
    let dt = time.delta_secs();
    let t = time.elapsed_secs();

    track_pulse.energy = (track_pulse.energy - dt * 0.6).max(0.0);

    let base_alpha = 0.25 + track_pulse.energy * 0.35;
    let breathe = (t * 0.8).sin() * 0.08;
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

// -- Update: event log panel ------------------------------------------------

#[allow(clippy::type_complexity)]
fn update_event_log_panel(
    mut commands: Commands,
    event_log: Res<EventLog>,
    mut panel_q: Query<&mut Visibility, (With<EventLogPanel>, Without<EventLogToggleHint>)>,
    mut hint_q: Query<&mut Visibility, (With<EventLogToggleHint>, Without<EventLogPanel>)>,
    content_q: Query<(Entity, Option<&Children>), With<EventLogContent>>,
) {
    let target_vis = if event_log.visible {
        Visibility::Inherited
    } else {
        Visibility::Hidden
    };
    for mut vis in panel_q.iter_mut() {
        *vis = target_vis;
    }
    // Hide the hint when log is open
    for mut vis in hint_q.iter_mut() {
        *vis = if event_log.visible {
            Visibility::Hidden
        } else {
            Visibility::Inherited
        };
    }

    if !event_log.visible {
        return;
    }

    // Rebuild log content (show last ~40 entries)
    for (content_entity, children) in content_q.iter() {
        // Despawn old children
        if let Some(children) = children {
            for child in children.iter() {
                commands.entity(child).despawn();
            }
        }

        let max_visible = if event_log.show_details { 30 } else { 60 };
        let start = event_log.entries.len().saturating_sub(max_visible);
        let visible_entries = &event_log.entries[start..];

        commands.entity(content_entity).with_children(|parent| {
            for entry in visible_entries.iter().rev() {
                parent
                    .spawn((
                        Node {
                            flex_direction: FlexDirection::Column,
                            padding: UiRect::axes(Val::Px(6.0), Val::Px(5.0)),
                            border: UiRect::left(Val::Px(3.0)),
                            margin: UiRect::bottom(Val::Px(3.0)),
                            ..default()
                        },
                        BackgroundColor(Color::srgba_u8(0x0c, 0x12, 0x20, 0xcc)),
                        BorderColor::all(entry.color.with_alpha(0.5)),
                    ))
                    .with_children(|card| {
                        card.spawn((
                            Text::new(format!("T{} {} {}", entry.tick, entry.icon, entry.headline)),
                            TextFont {
                                font_size: 12.0,
                                ..default()
                            },
                            TextColor(entry.color),
                        ));
                        if event_log.show_details {
                            card.spawn((
                                Text::new(&entry.detail),
                                TextFont {
                                    font_size: 10.0,
                                    ..default()
                                },
                                TextColor(DIM_GRAY),
                                Node {
                                    margin: UiRect::top(Val::Px(2.0)),
                                    ..default()
                                },
                            ));
                        }
                    });
            }
        });
    }
}
