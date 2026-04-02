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

//! Core types mirroring `consensus::observability` for WASM-compatible visualization.

use strum_macros::{AsRefStr, Display, EnumIter};

/// Replica status in the VSR protocol.
/// Mirrors `consensus::observability::Status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

/// Role of a replica in the current view.
/// Mirrors `consensus::observability::ReplicaRole`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum Role {
    Primary,
    Backup,
}

/// Type of VSR protocol message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum MessageType {
    ClientRequest,
    Prepare,
    PrepareOk,
    Commit,
    StartViewChange,
    DoViewChange,
    StartView,
    ClientReply,
}

impl MessageType {
    /// Brand color for this message type as (r, g, b).
    pub fn color_rgb(self) -> (u8, u8, u8) {
        match self {
            Self::ClientRequest => (0xff, 0xfa, 0xeb),   // cream
            Self::Prepare => (0x5f, 0x87, 0xfd),         // blue
            Self::PrepareOk => (0x14, 0xb8, 0xa6),       // teal
            Self::Commit => (0xff, 0x91, 0x03),           // orange
            Self::StartViewChange => (0xfa, 0x5e, 0x8a),  // pink
            Self::DoViewChange => (0xa8, 0x55, 0xf7),     // purple
            Self::StartView => (0x63, 0x66, 0xf1),        // indigo
            Self::ClientReply => (0xff, 0xa7, 0x03),      // orange
        }
    }

    /// Bevy Color for this message type.
    #[allow(dead_code)]
    pub fn color(self) -> bevy::color::Color {
        let (r, g, b) = self.color_rgb();
        bevy::color::Color::srgb_u8(r, g, b)
    }
}

/// A message in the VSR protocol.
#[derive(Debug, Clone)]
pub struct VsrMessage {
    pub msg_type: MessageType,
    pub from: u8,
    pub to: u8,
    pub view: u32,
    pub op: u64,
    pub commit: u64,
    pub id: u64,
}

/// Reason for initiating a view change.
/// Mirrors `consensus::observability::ViewChangeReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum ViewChangeReason {
    HeartbeatTimeout,
    ViewChangeTimeout,
    ReceivedStartViewChange,
    ReceivedDoViewChange,
}

/// Snapshot of a single replica's state for the UI.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ReplicaState {
    pub id: u8,
    pub status: Status,
    pub role: Role,
    pub view: u32,
    pub log_view: u32,
    pub commit: u64,
    pub op: u64,
    pub pipeline_depth: usize,
    pub alive: bool,
}
