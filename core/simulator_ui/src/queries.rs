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

use bevy::ecs::system::SystemParam;
use bevy::prelude::*;
use bevy_prototype_lyon::prelude::*;

use crate::components::*;
use crate::resources::*;

pub(crate) type ReplicaShadowQuery<'w, 's> = Query<
    'w,
    's,
    (
        &'static ReplicaShadow,
        &'static mut Sprite,
        &'static mut Transform,
    ),
    Without<ReplicaBody>,
>;
pub(crate) type ReplicaGlowQuery<'w, 's> = Query<
    'w,
    's,
    (
        &'static ReplicaGlow,
        &'static mut Shape,
        &'static mut Transform,
    ),
    Without<ReplicaNeonOutline>,
>;
pub(crate) type ReplicaOutlineQuery<'w, 's> = Query<
    'w,
    's,
    (
        &'static ReplicaNeonOutline,
        &'static mut Shape,
        &'static mut Transform,
    ),
    Without<ReplicaGlow>,
>;
pub(crate) type ReplicaBodyQuery<'w, 's> = Query<
    'w,
    's,
    (
        &'static ReplicaBody,
        &'static mut Sprite,
        &'static mut Transform,
    ),
    Without<ReplicaShadow>,
>;
pub(crate) type ReplicaSleepQuery<'w, 's> = Query<
    'w,
    's,
    (
        &'static ReplicaSleepText,
        &'static mut Transform,
        &'static mut TextColor,
        &'static mut Visibility,
    ),
    (
        Without<ReplicaCalloutText>,
        Without<ReplicaShadow>,
        Without<ReplicaBody>,
        Without<ReplicaGlow>,
        Without<ReplicaNeonOutline>,
    ),
>;
pub(crate) type ReplicaIdQuery<'w, 's> = Query<
    'w,
    's,
    (&'static ReplicaIdText, &'static mut TextColor),
    (
        Without<ReplicaRoleText>,
        Without<ReplicaSleepText>,
        Without<ReplicaCalloutText>,
    ),
>;
pub(crate) type ReplicaRoleQuery<'w, 's> = Query<
    'w,
    's,
    (
        &'static ReplicaRoleText,
        &'static mut Text2d,
        &'static mut TextColor,
    ),
    (
        Without<ReplicaIdText>,
        Without<ReplicaSleepText>,
        Without<ReplicaCalloutText>,
    ),
>;
pub(crate) type ReplicaCalloutQuery<'w, 's> = Query<
    'w,
    's,
    (
        &'static ReplicaCalloutText,
        &'static mut Text2d,
        &'static mut Transform,
        &'static mut TextColor,
        &'static mut Visibility,
    ),
    (
        Without<ReplicaIdText>,
        Without<ReplicaRoleText>,
        Without<ReplicaSleepText>,
        Without<ReplicaShadow>,
        Without<ReplicaBody>,
        Without<ReplicaGlow>,
        Without<ReplicaNeonOutline>,
    ),
>;

pub(crate) type HudTickQuery<'w, 's> = Query<'w, 's, &'static mut Text, With<HudTickText>>;
pub(crate) type HudOpsQuery<'w, 's> =
    Query<'w, 's, &'static mut Text, (With<HudOpsText>, Without<HudTickText>)>;
pub(crate) type HudCommitsQuery<'w, 's> = Query<
    'w,
    's,
    &'static mut Text,
    (
        With<HudCommitsText>,
        Without<HudTickText>,
        Without<HudOpsText>,
    ),
>;
pub(crate) type HudStateQuery<'w, 's> = Query<
    'w,
    's,
    (&'static mut Text, &'static mut TextColor),
    (
        With<HudStateText>,
        Without<HudTickText>,
        Without<HudOpsText>,
        Without<HudCommitsText>,
    ),
>;
pub(crate) type HudSpeedQuery<'w, 's> = Query<
    'w,
    's,
    &'static mut Text,
    (
        With<HudSpeedText>,
        Without<HudTickText>,
        Without<HudOpsText>,
        Without<HudCommitsText>,
        Without<HudStateText>,
    ),
>;

pub(crate) type PauseOverlayVisQuery<'w, 's> = Query<
    'w,
    's,
    &'static mut Visibility,
    (
        With<PauseOverlay>,
        Without<PauseMainText>,
        Without<PauseSubtext>,
    ),
>;
pub(crate) type PauseMainColorQuery<'w, 's> = Query<
    'w,
    's,
    &'static mut TextColor,
    (
        With<PauseMainText>,
        Without<PauseOverlay>,
        Without<PauseSubtext>,
    ),
>;
pub(crate) type PauseSubVisQuery<'w, 's> = Query<
    'w,
    's,
    &'static mut Visibility,
    (
        With<PauseSubtext>,
        Without<PauseOverlay>,
        Without<PauseMainText>,
    ),
>;

pub(crate) type AppCardQuery<'w, 's> = Query<
    'w,
    's,
    (&'static AppCard, &'static mut Shape, &'static mut Transform),
    (Without<AppLink>, Without<AppCardLabel>, Without<AppIcon>),
>;
pub(crate) type AppLinkQuery<'w, 's> =
    Query<'w, 's, (&'static AppLink, &'static mut Shape), (Without<AppCard>, Without<AppIcon>)>;
pub(crate) type AppCardLabelQuery<'w, 's> = Query<
    'w,
    's,
    (
        &'static AppCardLabel,
        &'static mut Transform,
        &'static mut TextColor,
    ),
    (Without<AppCard>, Without<AppLink>, Without<AppIcon>),
>;
pub(crate) type AppIconQuery<'w, 's> = Query<
    'w,
    's,
    (&'static AppIcon, &'static mut Shape, &'static mut Transform),
    (Without<AppCard>, Without<AppLink>, Without<AppCardLabel>),
>;

pub(crate) type EventLogPanelQuery<'w, 's> = Query<
    'w,
    's,
    &'static mut Node,
    (
        With<EventLogPanel>,
        Without<EventLogToggleHint>,
        Without<ConsolePanel>,
    ),
>;
pub(crate) type EventLogHintQuery<'w, 's> = Query<
    'w,
    's,
    &'static mut Visibility,
    (
        With<EventLogToggleHint>,
        Without<EventLogPanel>,
        Without<ConsolePanel>,
    ),
>;

#[derive(SystemParam)]
pub(crate) struct FxParams<'w> {
    pub(crate) screen_flash: ResMut<'w, ScreenFlash>,
    pub(crate) replica_fx: ResMut<'w, ReplicaFxState>,
    pub(crate) app_fx: ResMut<'w, AppFxState>,
    pub(crate) track_pulse: ResMut<'w, TrackPulse>,
}

pub(crate) type ConsolePanelQuery<'w, 's> =
    Query<'w, 's, &'static mut Node, (With<ConsolePanel>, Without<EventLogPanel>)>;

pub(crate) type ConsoleContentQuery<'w, 's> = Query<
    'w,
    's,
    (Entity, Option<&'static Children>),
    (With<ConsoleContent>, Without<EventLogContent>),
>;
