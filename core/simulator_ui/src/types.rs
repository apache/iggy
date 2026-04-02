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

use bevy::prelude::Color;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

impl Status {
    pub fn from_str(s: &str) -> Self {
        match s {
            "view_change" => Self::ViewChange,
            "recovering" => Self::Recovering,
            _ => Self::Normal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Primary,
    Backup,
}

impl Role {
    pub fn from_str(s: &str) -> Self {
        match s {
            "primary" => Self::Primary,
            _ => Self::Backup,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Prepare,
    PrepareOk,
    StartViewChange,
    DoViewChange,
    StartView,
    ClientReply,
}

impl MessageType {
    pub fn color(self) -> Color {
        match self {
            Self::Prepare => Color::srgb_u8(95, 135, 253),
            Self::PrepareOk => Color::srgb_u8(20, 184, 166),
            Self::StartViewChange => Color::srgb_u8(250, 94, 138),
            Self::DoViewChange => Color::srgb_u8(168, 85, 247),
            Self::StartView => Color::srgb_u8(99, 102, 241),
            Self::ClientReply => Color::srgb_u8(255, 167, 3),
        }
    }

    pub fn from_action(action: &str) -> Option<Self> {
        match action {
            "send_prepare_ok" => Some(Self::PrepareOk),
            "send_start_view_change" => Some(Self::StartViewChange),
            "send_do_view_change" => Some(Self::DoViewChange),
            "send_start_view" => Some(Self::StartView),
            _ => None,
        }
    }
}

pub struct ReplicaState {
    pub id: u8,
    pub status: Status,
    pub role: Role,
    pub pipeline_depth: usize,
    pub alive: bool,
}
