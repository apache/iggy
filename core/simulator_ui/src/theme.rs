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

pub(crate) const BG_DARK: Color = Color::srgb_u8(0x07, 0x0c, 0x17);
pub(crate) const BG_PANEL: Color = Color::srgb_u8(0x0c, 0x12, 0x20);
pub(crate) const NEON_CYAN: Color = Color::srgb_u8(0x67, 0xe8, 0xf9);
pub(crate) const NEON_MAGENTA: Color = Color::srgb_u8(0xfa, 0x5e, 0x8a);
pub(crate) const NEON_YELLOW: Color = Color::srgb_u8(0xfe, 0xbc, 0x2e);
pub(crate) const IGGY_ORANGE: Color = Color::srgb_u8(0xff, 0x91, 0x03);
pub(crate) const COOL_WHITE: Color = Color::srgb_u8(0xff, 0xfa, 0xeb);
pub(crate) const DIM_GRAY: Color = Color::srgb_u8(0xaa, 0xaf, 0xb6);
pub(crate) const GRID_LINE: Color = Color::srgb_u8(0x11, 0x1d, 0x35);
pub(crate) const HUD_EDGE: Color = Color::srgb_u8(0x3d, 0x44, 0x50);
pub(crate) const HUD_ALERT: Color = Color::srgb_u8(0xff, 0x5f, 0x57);

pub(crate) const REPLICA_COUNT: u8 = 3;
pub(crate) const CIRCLE_RADIUS: f32 = 300.0;
pub(crate) const WORLD_CENTER_Y: f32 = 90.0;
pub(crate) const IGGY_SPRITE_SIZE: Vec2 = Vec2::new(210.0, 176.0);
pub(crate) const MSG_DURATION: f32 = 0.5;
pub(crate) const TRAIL_LIFETIME: f32 = 0.25;
pub(crate) const SCAN_LINE_COUNT: usize = 4;
pub(crate) const AMBIENT_PARTICLE_COUNT: usize = 60;
pub(crate) const COMMIT_RING_LIFETIME: f32 = 0.7;
pub(crate) const SCREEN_FLASH_DURATION: f32 = 0.25;
pub(crate) const SPEED_LINE_LIFETIME: f32 = 0.15;
pub(crate) const EVENT_LOG_MAX: usize = 200;
