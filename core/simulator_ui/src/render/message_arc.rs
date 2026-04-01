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

//! Draws an in-flight message as a clean comet with a 6-dot fading trail,
//! a 3-layer outer glow, and a solid 5px core. Elegant, not noisy.

use crate::state::animation::InFlightMessage;

/// Draw a message in flight as a clean comet with a 6-dot trail and 3-layer glow.
pub fn draw_message_arc(painter: &egui::Painter, msg: &InFlightMessage) {
    let color = msg.msg_type.color();
    let pos = msg.pos();

    // --- Fading trail: 6 dots behind the current position ---
    let trail_count = 6;
    for i in 1..=trail_count {
        let trail_t = (msg.progress - 0.025 * i as f32).clamp(0.0, 1.0);

        let trail_msg = InFlightMessage {
            id: msg.id,
            msg_type: msg.msg_type,
            from_pos: msg.from_pos,
            to_pos: msg.to_pos,
            progress: trail_t,
            duration: msg.duration,
        };
        let trail_pos = trail_msg.pos();

        let t_frac = (i as f32 - 1.0) / (trail_count - 1) as f32;
        let size = 3.5 - t_frac * 2.5; // 3.5 -> 1.0
        let alpha = (160.0 - t_frac * 140.0).max(20.0) as u8; // 160 -> 20

        let trail_color =
            egui::Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), alpha);
        painter.circle_filled(trail_pos, size.max(1.0), trail_color);
    }

    // --- Outer glow: 3 concentric circles with decreasing alpha ---
    let glow_layers: [(f32, u8); 3] = [(12.0, 25), (8.0, 55), (5.0, 100)];
    for (radius, alpha) in glow_layers {
        let glow_color =
            egui::Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), alpha);
        painter.circle_filled(pos, radius, glow_color);
    }

    // --- Core: solid 5px circle ---
    painter.circle_filled(pos, 5.0, color);
}
