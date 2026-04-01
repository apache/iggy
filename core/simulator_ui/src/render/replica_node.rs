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

//! Draws a single VSR replica node as a 150x180 card with a breathing glow
//! border, warm primary aura, role badge, labeled metrics, gradient pipeline
//! bar, and status indicator. Clean, commanding presence.

use crate::vsr::{ReplicaState, Role, Status};
use egui::Color32;

// -- Dimensions ---------------------------------------------------------------

const NODE_WIDTH: f32 = 150.0;
const NODE_HEIGHT: f32 = 180.0;
const CORNER_RADIUS: u8 = 14;

// -- Brand palette ------------------------------------------------------------

const CARD: Color32 = Color32::from_rgb(0x0e, 0x19, 0x30);
const ORANGE: Color32 = Color32::from_rgb(0xff, 0x91, 0x03);
const CREAM: Color32 = Color32::from_rgb(0xff, 0xfa, 0xeb);
const MUTED: Color32 = Color32::from_rgb(0x83, 0x8d, 0x95);
const BORDER: Color32 = Color32::from_rgb(0x3d, 0x44, 0x50);
const TEAL: Color32 = Color32::from_rgb(0x14, 0xb8, 0xa6);
const RED: Color32 = Color32::from_rgb(0xef, 0x44, 0x44);
const DEAD_BG: Color32 = Color32::from_rgb(0x1a, 0x1a, 0x1a);
const DEAD_BORDER: Color32 = Color32::from_rgb(0x44, 0x44, 0x44);
const SHADOW_COLOR: Color32 = Color32::from_rgba_premultiplied(0x00, 0x00, 0x00, 100);
const BAR_TRACK: Color32 = Color32::from_rgb(0x1a, 0x24, 0x3b);

// -- Helpers ------------------------------------------------------------------

/// Return the accent color for a given status (alive nodes only).
fn status_color(status: Status) -> Color32 {
    match status {
        Status::Normal => TEAL,
        Status::ViewChange => ORANGE,
        Status::Recovering => RED,
    }
}

/// Linearly interpolate between two colors by `t` in [0, 1].
fn lerp_color(a: Color32, b: Color32, t: f32) -> Color32 {
    let t = t.clamp(0.0, 1.0);
    let mix = |x: u8, y: u8| -> u8 { (x as f32 * (1.0 - t) + y as f32 * t) as u8 };
    Color32::from_rgb(mix(a.r(), b.r()), mix(a.g(), b.g()), mix(a.b(), b.b()))
}

/// Return a translucent version of a color.
fn with_alpha(c: Color32, a: u8) -> Color32 {
    Color32::from_rgba_premultiplied(
        (c.r() as u16 * a as u16 / 255) as u8,
        (c.g() as u16 * a as u16 / 255) as u8,
        (c.b() as u16 * a as u16 / 255) as u8,
        a,
    )
}

// -- Public entry point -------------------------------------------------------

/// Draw a single replica node centered at `center`.
///
/// The card is 150x180 pixels with rounded corners, a breathing glow border,
/// a warm orange aura for the Primary, role badge, labeled view/commit values,
/// a status label, and a gradient pipeline-depth bar at the bottom.
///
/// `time` is `ui.input(|i| i.time)` -- used for the breathing pulse effect.
pub fn draw_replica_node(
    painter: &egui::Painter,
    center: egui::Pos2,
    replica: &ReplicaState,
    time: f64,
) {
    let rect = egui::Rect::from_center_size(center, egui::vec2(NODE_WIDTH, NODE_HEIGHT));
    let rounding = egui::CornerRadius::same(CORNER_RADIUS);
    let dead = !replica.alive;

    let accent = if dead {
        DEAD_BORDER
    } else {
        status_color(replica.status)
    };

    // Breathing multiplier: gently pulses the glow border alpha.
    let breath = 0.85 + 0.15 * (time * 2.0).sin() as f32;

    // --- Primary glow aura: 6 concentric circles, warm orange ----------------
    if replica.role == Role::Primary && replica.alive {
        for i in (1..=6).rev() {
            let r = NODE_WIDTH * 0.5 + i as f32 * 8.0;
            let base_alpha = match i {
                1 => 10u8,
                2 => 8,
                3 => 6,
                4 => 5,
                5 => 4,
                _ => 3,
            };
            let alpha = (base_alpha as f32 * breath) as u8;
            painter.circle_filled(center, r, with_alpha(ORANGE, alpha));
        }
    }

    // --- Drop shadow ---------------------------------------------------------
    let shadow_offset = egui::vec2(2.0, 3.0);
    painter.rect(
        rect.translate(shadow_offset),
        rounding,
        SHADOW_COLOR,
        egui::Stroke::NONE,
        egui::StrokeKind::Outside,
    );

    // --- Outer glow border (breathing) ---------------------------------------
    if !dead {
        let glow_rect = rect.expand(2.5);
        let glow_alpha = (70.0 * breath) as u8;
        let stroke_alpha = (130.0 * breath) as u8;
        painter.rect(
            glow_rect,
            egui::CornerRadius::same(CORNER_RADIUS + 3),
            with_alpha(accent, glow_alpha),
            egui::Stroke::new(1.5, with_alpha(accent, stroke_alpha)),
            egui::StrokeKind::Outside,
        );
    }

    // --- Card background -----------------------------------------------------
    let bg = if dead { DEAD_BG } else { CARD };
    painter.rect(
        rect,
        rounding,
        bg,
        egui::Stroke::new(1.5, if dead { DEAD_BORDER } else { accent }),
        egui::StrokeKind::Outside,
    );

    // --- Replica ID (32px bold, the main identifier) -------------------------
    let id_y = rect.min.y + 30.0;
    let id_text = format!("R{}", replica.id);
    painter.text(
        egui::pos2(center.x, id_y),
        egui::Align2::CENTER_CENTER,
        &id_text,
        egui::FontId::new(32.0, egui::FontFamily::Proportional),
        if dead { MUTED } else { CREAM },
    );

    // --- Role badge (13px, right below ID) -----------------------------------
    let badge_y = rect.min.y + 52.0;
    let (badge_text, badge_color) = match replica.role {
        Role::Primary => ("PRIMARY", ORANGE),
        Role::Backup => ("BACKUP", MUTED),
    };
    painter.text(
        egui::pos2(center.x, badge_y),
        egui::Align2::CENTER_CENTER,
        badge_text,
        egui::FontId::new(13.0, egui::FontFamily::Proportional),
        if dead {
            with_alpha(MUTED, 120)
        } else {
            badge_color
        },
    );

    // --- Thin horizontal separator -------------------------------------------
    let sep_y = rect.min.y + 62.0;
    let sep_half_w = (NODE_WIDTH - 24.0) / 2.0;
    let sep_color = if dead {
        with_alpha(MUTED, 40)
    } else {
        with_alpha(BORDER, 80)
    };
    painter.line_segment(
        [
            egui::pos2(center.x - sep_half_w, sep_y),
            egui::pos2(center.x + sep_half_w, sep_y),
        ],
        egui::Stroke::new(1.0, sep_color),
    );

    // --- View and Commit as labeled values -----------------------------------
    let label_color = if dead { with_alpha(MUTED, 100) } else { MUTED };
    let value_color = if dead { with_alpha(CREAM, 100) } else { CREAM };

    // View
    let view_label_y = rect.min.y + 76.0;
    painter.text(
        egui::pos2(center.x, view_label_y),
        egui::Align2::CENTER_CENTER,
        "VIEW",
        egui::FontId::new(11.0, egui::FontFamily::Proportional),
        label_color,
    );
    let view_value_y = rect.min.y + 94.0;
    painter.text(
        egui::pos2(center.x, view_value_y),
        egui::Align2::CENTER_CENTER,
        replica.view.to_string(),
        egui::FontId::new(20.0, egui::FontFamily::Proportional),
        value_color,
    );

    // Commit
    let commit_label_y = rect.min.y + 112.0;
    painter.text(
        egui::pos2(center.x, commit_label_y),
        egui::Align2::CENTER_CENTER,
        "COMMIT",
        egui::FontId::new(11.0, egui::FontFamily::Proportional),
        label_color,
    );
    let commit_value_y = rect.min.y + 130.0;
    painter.text(
        egui::pos2(center.x, commit_value_y),
        egui::Align2::CENTER_CENTER,
        replica.commit.to_string(),
        egui::FontId::new(20.0, egui::FontFamily::Proportional),
        value_color,
    );

    // --- Status label (10px at bottom) ---------------------------------------
    let status_y = rect.min.y + 148.0;
    let status_text = if dead {
        "DEAD"
    } else {
        match replica.status {
            Status::Normal => "NORMAL",
            Status::ViewChange => "VIEW-CHANGE",
            Status::Recovering => "RECOVERING",
        }
    };
    let status_col = if dead { MUTED } else { accent };
    painter.text(
        egui::pos2(center.x, status_y),
        egui::Align2::CENTER_CENTER,
        status_text,
        egui::FontId::new(10.0, egui::FontFamily::Proportional),
        status_col,
    );

    // --- Pipeline depth bar (8px tall, at bottom) ----------------------------
    let bar_y = rect.max.y - 16.0;
    let bar_w = NODE_WIDTH - 20.0;
    let bar_h: f32 = 8.0;
    let bar_rounding_px = 4u8;
    let bar_rounding = egui::CornerRadius::same(bar_rounding_px);
    let bar_rect = egui::Rect::from_min_size(
        egui::pos2(center.x - bar_w / 2.0, bar_y - bar_h / 2.0),
        egui::vec2(bar_w, bar_h),
    );

    // Track
    painter.rect_filled(bar_rect, bar_rounding, BAR_TRACK);

    // Fill with teal-to-orange gradient (approximated with segments).
    let fill_fraction = (replica.pipeline_depth as f32 / 8.0).clamp(0.0, 1.0);
    if fill_fraction > 0.0 {
        let fill_w = bar_w * fill_fraction;
        let segments = (fill_w as u32).max(1);
        let seg_w = fill_w / segments as f32;
        for i in 0..segments {
            let t = i as f32 / segments.max(1) as f32;
            let color = if dead {
                with_alpha(MUTED, 100)
            } else {
                lerp_color(TEAL, ORANGE, t)
            };
            let x = bar_rect.min.x + i as f32 * seg_w;
            let seg_rect = egui::Rect::from_min_size(
                egui::pos2(x, bar_rect.min.y),
                egui::vec2(seg_w + 0.5, bar_h),
            );
            let seg_rect = seg_rect.intersect(bar_rect);
            let r = if i == 0 && i == segments - 1 {
                bar_rounding
            } else if i == 0 {
                egui::CornerRadius {
                    nw: bar_rounding_px,
                    sw: bar_rounding_px,
                    ..Default::default()
                }
            } else if i == segments - 1 {
                egui::CornerRadius {
                    ne: bar_rounding_px,
                    se: bar_rounding_px,
                    ..Default::default()
                }
            } else {
                egui::CornerRadius::ZERO
            };
            painter.rect_filled(seg_rect, r, color);
        }
    }
}
