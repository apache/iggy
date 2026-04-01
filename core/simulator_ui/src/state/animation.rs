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

//! In-flight message animation state and commit ring effects for the cluster
//! visualization. Tracks comet-style message arcs and expanding ring pulses
//! that fire when a replica commits an operation.

use crate::vsr::MessageType;

/// An in-flight message being animated between two replica positions.
pub struct InFlightMessage {
    pub id: u64,
    pub msg_type: MessageType,
    pub from_pos: egui::Pos2,
    pub to_pos: egui::Pos2,
    pub progress: f32,
    pub duration: f32,
}

impl InFlightMessage {
    /// Interpolate the current position along a quadratic bezier curve.
    ///
    /// The control point is offset perpendicular to the line between `from_pos`
    /// and `to_pos`, producing a visible arc rather than a straight line.
    pub fn pos(&self) -> egui::Pos2 {
        let t = self.progress.clamp(0.0, 1.0);

        let mid = egui::pos2(
            (self.from_pos.x + self.to_pos.x) * 0.5,
            (self.from_pos.y + self.to_pos.y) * 0.5,
        );

        // Perpendicular offset for the control point.
        let dx = self.to_pos.x - self.from_pos.x;
        let dy = self.to_pos.y - self.from_pos.y;
        let len = (dx * dx + dy * dy).sqrt().max(1.0);
        let perp_x = -dy / len;
        let perp_y = dx / len;
        let offset = 30.0;

        let control = egui::pos2(mid.x + perp_x * offset, mid.y + perp_y * offset);

        // Quadratic bezier: B(t) = (1-t)^2 * P0 + 2*(1-t)*t * C + t^2 * P1
        let inv = 1.0 - t;
        let x = inv * inv * self.from_pos.x + 2.0 * inv * t * control.x + t * t * self.to_pos.x;
        let y = inv * inv * self.from_pos.y + 2.0 * inv * t * control.y + t * t * self.to_pos.y;

        egui::pos2(x, y)
    }
}

/// An expanding translucent ring effect spawned at a replica when it commits.
#[allow(dead_code)]
pub struct CommitRing {
    pub pos: egui::Pos2,
    pub age: f32,
    pub max_age: f32,
}

/// Manages all active in-flight message animations and commit ring effects.
#[derive(Default)]
pub struct AnimationState {
    pub messages: Vec<InFlightMessage>,
    pub commit_rings: Vec<CommitRing>,
}

impl AnimationState {
    /// Add a new in-flight message animation.
    pub fn add(
        &mut self,
        id: u64,
        msg_type: MessageType,
        from: egui::Pos2,
        to: egui::Pos2,
        duration: f32,
    ) {
        self.messages.push(InFlightMessage {
            id,
            msg_type,
            from_pos: from,
            to_pos: to,
            progress: 0.0,
            duration,
        });
    }

    /// Spawn an expanding commit ring at the given position.
    pub fn add_commit_ring(&mut self, pos: egui::Pos2) {
        self.commit_rings.push(CommitRing {
            pos,
            age: 0.0,
            max_age: 0.6,
        });
    }

    /// Advance all animations by `dt` seconds. Completed animations are removed.
    pub fn advance(&mut self, dt: f32) {
        for msg in &mut self.messages {
            if msg.duration > 0.0 {
                msg.progress += dt / msg.duration;
            } else {
                msg.progress = 1.0;
            }
        }
        self.messages.retain(|m| m.progress < 1.0);

        for ring in &mut self.commit_rings {
            ring.age += dt;
        }
        self.commit_rings.retain(|r| r.age < r.max_age);
    }

    /// Returns true if there are any active animations.
    pub fn has_active(&self) -> bool {
        !self.messages.is_empty() || !self.commit_rings.is_empty()
    }

    /// Iterate over active in-flight messages.
    pub fn iter(&self) -> impl Iterator<Item = &InFlightMessage> {
        self.messages.iter()
    }
}
