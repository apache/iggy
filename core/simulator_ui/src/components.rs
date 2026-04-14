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

use crate::types::MessageType;

#[derive(Component)]
pub(crate) struct ReplicaGlow {
    pub(crate) id: u8,
}

#[derive(Component)]
pub(crate) struct ReplicaNeonOutline {
    pub(crate) id: u8,
}

#[derive(Component)]
pub(crate) struct ReplicaBody {
    pub(crate) id: u8,
}

#[derive(Component)]
pub(crate) struct ReplicaShadow {
    pub(crate) id: u8,
}

#[derive(Component)]
pub(crate) struct ReplicaIdText {
    pub(crate) id: u8,
}

#[derive(Component)]
pub(crate) struct ReplicaRoleText {
    pub(crate) id: u8,
}

#[derive(Component)]
pub(crate) struct ReplicaCalloutText {
    pub(crate) id: u8,
}

#[derive(Component)]
pub(crate) struct ReplicaSleepText {
    pub(crate) id: u8,
    pub(crate) layer: u8,
}

#[derive(Component)]
pub(crate) struct AppCard {
    pub(crate) id: usize,
    pub(crate) anchor: Vec2,
    pub(crate) accent: Color,
}

#[derive(Component)]
pub(crate) struct AppCardLabel {
    pub(crate) id: usize,
    pub(crate) anchor: Vec2,
}

#[derive(Component)]
pub(crate) struct AppLink {
    pub(crate) id: usize,
    pub(crate) anchor: Vec2,
    pub(crate) target_replica: u8,
    pub(crate) accent: Color,
    pub(crate) inbound: bool,
}

#[derive(Component, Clone, Copy)]
pub(crate) struct AppIcon {
    pub(crate) id: usize,
    pub(crate) anchor: Vec2,
    pub(crate) accent: Color,
    pub(crate) kind: AppKind,
}

#[derive(Component)]
pub(crate) struct AppFlowParticle {
    pub(crate) replica_id: u8,
    pub(crate) from: Vec2,
    pub(crate) to: Vec2,
    pub(crate) control: Vec2,
    pub(crate) progress: f32,
    pub(crate) speed: f32,
    pub(crate) color: Color,
}

#[derive(Clone, Copy)]
pub(crate) enum AppKind {
    Producer,
    Consumer,
}

#[derive(Component)]
pub(crate) struct LinkLine {
    pub(crate) from_id: u8,
    pub(crate) to_id: u8,
}

#[derive(Component)]
pub(crate) struct MessageParticle {
    pub(crate) from: Vec2,
    pub(crate) to: Vec2,
    pub(crate) control: Vec2,
    pub(crate) progress: f32,
    pub(crate) duration: f32,
    pub(crate) msg_type: MessageType,
}

#[derive(Component)]
pub(crate) struct MessageGlow;

#[derive(Component)]
pub(crate) struct TrailDot {
    pub(crate) lifetime: f32,
    pub(crate) max_lifetime: f32,
    pub(crate) color: Color,
}

#[derive(Component)]
pub(crate) struct ScanLine {
    pub(crate) speed: f32,
}

#[derive(Component)]
pub(crate) struct HudTickText;

#[derive(Component)]
pub(crate) struct HudOpsText;

#[derive(Component)]
pub(crate) struct HudCommitsText;

#[derive(Component)]
pub(crate) struct HudStateText;

#[derive(Component)]
pub(crate) struct HudSpeedText;

#[derive(Component)]
pub(crate) struct PauseOverlay;

#[derive(Component)]
pub(crate) struct PauseMainText;

#[derive(Component)]
pub(crate) struct PauseSubtext;

#[derive(Component)]
pub(crate) struct GlowFlash {
    pub(crate) id: u8,
    pub(crate) timer: f32,
    pub(crate) flash_color: Color,
}

#[derive(Component)]
pub(crate) struct CommitRing {
    pub(crate) center: Vec2,
    pub(crate) lifetime: f32,
    pub(crate) max_lifetime: f32,
    pub(crate) color: Color,
    pub(crate) max_radius: f32,
}

#[derive(Component)]
pub(crate) struct AmbientParticle {
    pub(crate) velocity: Vec2,
    pub(crate) drift_phase: f32,
    pub(crate) drift_speed: f32,
    pub(crate) base_alpha: f32,
}

#[derive(Component)]
pub(crate) struct ScreenFlashOverlay;

#[derive(Component)]
pub(crate) struct EventLogPanel;

#[derive(Component)]
pub(crate) struct EventLogContent;

#[derive(Component)]
pub(crate) struct EventLogToggleHint;

#[derive(Component)]
pub(crate) struct ConsolePanel;

#[derive(Component)]
pub(crate) struct ConsoleContent;

#[derive(Component)]
pub(crate) struct SpeedLine {
    pub(crate) lifetime: f32,
    pub(crate) max_lifetime: f32,
    pub(crate) color: Color,
}

#[derive(Component)]
pub(crate) struct TrackRing;

#[derive(Component)]
pub(crate) struct PawBurst {
    pub(crate) velocity: Vec2,
    pub(crate) lifetime: f32,
    pub(crate) max_lifetime: f32,
}

#[derive(Component)]
pub(crate) struct Lightning {
    pub(crate) lifetime: f32,
    pub(crate) max_lifetime: f32,
}

#[derive(Component)]
pub(crate) struct SelectionScreen;

#[derive(Component)]
pub(crate) struct SelectionCard {
    pub(crate) index: usize,
}

#[derive(Component)]
pub(crate) struct SelectionNumber {
    pub(crate) index: usize,
}

#[derive(Component)]
pub(crate) struct SelectionLabel {
    pub(crate) index: usize,
}

#[derive(Component)]
pub(crate) struct SelectionTitle;

#[derive(Component)]
pub(crate) struct VocabCard {
    pub(crate) is_dog: bool,
}

#[derive(Component)]
pub(crate) struct VocabLabel {
    pub(crate) is_dog: bool,
}
