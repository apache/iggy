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

//! Full-screen cluster view. The cluster is the game world -- clean, elegant,
//! with maximum negative space. Replicas orbit in a radial layout with minimal
//! concentric ring texture and simple link lines.

use std::f32::consts::PI;

use crate::render::message_arc::draw_message_arc;
use crate::render::replica_node::draw_replica_node;
use crate::state::animation::AnimationState;
use crate::vsr::{Network, ReplicaState};
use egui::Color32;

// -- Brand palette ------------------------------------------------------------

const BORDER: Color32 = Color32::from_rgb(0x3d, 0x44, 0x50);
const RED: Color32 = Color32::from_rgb(0xef, 0x44, 0x44);

/// Draw the full-screen cluster view. Returns the computed positions of each
/// replica so callers can map replica IDs to screen coordinates.
pub fn draw_cluster(
    ui: &mut egui::Ui,
    replicas: &[ReplicaState],
    animations: &AnimationState,
    network: &Network,
) -> Vec<egui::Pos2> {
    let full_rect = ui.available_rect_before_wrap();

    // Inset to avoid HUD overlap: 80px top, 70px bottom.
    let rect = egui::Rect::from_min_max(
        egui::pos2(full_rect.min.x, full_rect.min.y + 80.0),
        egui::pos2(full_rect.max.x, full_rect.max.y - 70.0),
    );

    let painter = ui.painter_at(full_rect);

    let center = rect.center();
    let min_dim = rect.width().min(rect.height());
    let radius = (min_dim * 0.35).max(160.0);
    let n = replicas.len();
    let time = ui.input(|i| i.time);

    // --- Background texture: 3 very subtle concentric circles ----------------
    for ring in 1..=3 {
        let r = radius * (ring as f32 / 3.0);
        painter.circle_stroke(
            center,
            r,
            egui::Stroke::new(
                0.4,
                Color32::from_rgba_unmultiplied(BORDER.r(), BORDER.g(), BORDER.b(), 12),
            ),
        );
    }

    // --- Position replicas in a circle, primary at top (-PI/2 offset) --------
    let positions: Vec<egui::Pos2> = (0..n)
        .map(|i| {
            let angle = 2.0 * PI * (i as f32) / (n as f32) - PI / 2.0;
            egui::pos2(
                center.x + radius * angle.cos(),
                center.y + radius * angle.sin(),
            )
        })
        .collect();

    // --- Link lines between all replica pairs --------------------------------
    for i in 0..n {
        for j in (i + 1)..n {
            let from = positions[i];
            let to = positions[j];
            let partitioned = network.is_partitioned(replicas[i].id, replicas[j].id);

            if partitioned {
                // Dashed red line (6px dash, 4px gap).
                let dx = to.x - from.x;
                let dy = to.y - from.y;
                let dist = (dx * dx + dy * dy).sqrt();
                if dist < 0.001 {
                    continue;
                }
                let dash_len = 6.0;
                let gap_len = 4.0;
                let step = dash_len + gap_len;
                let steps = (dist / step) as usize;
                let nx = dx / dist;
                let ny = dy / dist;
                let time_offset = (time * 30.0) as f32 % step;

                for s in 0..=steps {
                    let start_d = s as f32 * step + time_offset;
                    let end_d = (start_d + dash_len).min(dist);
                    if start_d >= dist {
                        break;
                    }
                    let p0 = egui::pos2(from.x + nx * start_d, from.y + ny * start_d);
                    let p1 = egui::pos2(from.x + nx * end_d, from.y + ny * end_d);
                    painter.line_segment([p0, p1], egui::Stroke::new(1.2, RED));
                }
            } else {
                // Healthy: single 0.8px line, very subtle.
                let core_color =
                    Color32::from_rgba_unmultiplied(BORDER.r(), BORDER.g(), BORDER.b(), 25);
                painter.line_segment([from, to], egui::Stroke::new(0.8, core_color));
            }
        }
    }

    // --- Draw replica nodes --------------------------------------------------
    for (i, replica) in replicas.iter().enumerate() {
        draw_replica_node(&painter, positions[i], replica, time);
    }

    // --- Draw in-flight message animations -----------------------------------
    for msg in animations.iter() {
        draw_message_arc(&painter, msg);
    }

    // Allocate the space so egui knows we used it.
    ui.allocate_rect(full_rect, egui::Sense::hover());

    positions
}
