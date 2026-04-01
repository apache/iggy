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

//! Draws a horizontal timeline bar showing recent simulation events as colored
//! glowing dots, with tick marks, a pulsing current-tick indicator, and a
//! scrolling 300-tick window.

use crate::vsr::SimEvent;

// -- Brand palette ------------------------------------------------------------

const CARD: egui::Color32 = egui::Color32::from_rgb(0x0e, 0x19, 0x30);
const ORANGE: egui::Color32 = egui::Color32::from_rgb(0xff, 0x91, 0x03);
const CREAM: egui::Color32 = egui::Color32::from_rgb(0xff, 0xfa, 0xeb);
const BORDER: egui::Color32 = egui::Color32::from_rgb(0x3d, 0x44, 0x50);
const TEAL: egui::Color32 = egui::Color32::from_rgb(0x14, 0xb8, 0xa6);
const PURPLE: egui::Color32 = egui::Color32::from_rgb(0xa8, 0x55, 0xf7);
const PINK: egui::Color32 = egui::Color32::from_rgb(0xfa, 0x5e, 0x8a);
const BLUE: egui::Color32 = egui::Color32::from_rgb(0x5f, 0x87, 0xfd);

/// Draw a horizontal event timeline with glowing event dots, tick ruler, and a
/// pulsing vertical line at the current tick.
pub fn draw_timeline(ui: &mut egui::Ui, events: &[SimEvent], current_tick: u64) {
    let timeline_height = 60.0;
    let (rect, _response) = ui.allocate_exact_size(
        egui::vec2(ui.available_width(), timeline_height),
        egui::Sense::hover(),
    );

    let painter = ui.painter_at(rect);

    // --- Background: dark card color rounded rect ---
    painter.rect_filled(rect, 6.0, CARD);

    // --- Layout constants ---
    let window = 300u64;
    let start_tick = current_tick.saturating_sub(window);
    let label_space = 100.0; // Reserve right side for "Tick: N" label.
    let bar_left = rect.left() + 8.0;
    let bar_right = rect.right() - label_space;
    let bar_width = bar_right - bar_left;
    let bar_y = rect.center().y;

    // Helper: map a tick to an x coordinate.
    let tick_to_x = |tick: u64| -> f32 {
        if window == 0 {
            return bar_left;
        }
        let t = (tick.saturating_sub(start_tick)) as f32 / window as f32;
        bar_left + t * bar_width
    };

    // --- Horizontal ruler line ---
    let ruler_color =
        egui::Color32::from_rgba_unmultiplied(BORDER.r(), BORDER.g(), BORDER.b(), 100);
    painter.line_segment(
        [egui::pos2(bar_left, bar_y), egui::pos2(bar_right, bar_y)],
        egui::Stroke::new(1.5, ruler_color),
    );

    // --- Tick marks every 10 ticks ---
    let first_mark = start_tick.div_ceil(10) * 10; // Round up to next multiple of 10.
    let mut mark_tick = first_mark;
    while mark_tick <= current_tick {
        let x = tick_to_x(mark_tick);
        let is_major = mark_tick.is_multiple_of(50);
        let mark_half_h = if is_major { 10.0 } else { 5.0 };
        let mark_alpha: u8 = if is_major { 80 } else { 40 };
        let mark_color =
            egui::Color32::from_rgba_unmultiplied(BORDER.r(), BORDER.g(), BORDER.b(), mark_alpha);

        painter.line_segment(
            [
                egui::pos2(x, bar_y - mark_half_h),
                egui::pos2(x, bar_y + mark_half_h),
            ],
            egui::Stroke::new(0.8, mark_color),
        );
        mark_tick += 10;
    }

    // --- Event dots with glow ---
    for event in events.iter().rev() {
        let tick = event.tick();
        if tick < start_tick {
            break;
        }
        if tick > current_tick {
            continue;
        }

        let x = tick_to_x(tick);
        let color = event_color(event);
        let dot_pos = egui::pos2(x, bar_y);

        // Outer glow layer.
        let glow = egui::Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 60);
        painter.circle_filled(dot_pos, 7.0, glow);

        // Core dot.
        painter.circle_filled(dot_pos, 4.0, color);
    }

    // --- Pulsing vertical line at current tick ---
    let cx = tick_to_x(current_tick);
    let time = ui.input(|i| i.time);
    let pulse_alpha = ((time * 3.0).sin() * 0.3 + 0.7) as f32; // Oscillates 0.4..1.0.
    let line_color = egui::Color32::from_rgba_unmultiplied(
        ORANGE.r(),
        ORANGE.g(),
        ORANGE.b(),
        (pulse_alpha * 200.0) as u8,
    );
    painter.line_segment(
        [
            egui::pos2(cx, rect.top() + 2.0),
            egui::pos2(cx, rect.bottom() - 2.0),
        ],
        egui::Stroke::new(2.0, line_color),
    );

    // --- "Tick: N" label in cream bold text on the right ---
    painter.text(
        egui::pos2(rect.right() - 8.0, bar_y),
        egui::Align2::RIGHT_CENTER,
        format!("Tick: {}", current_tick),
        egui::FontId::proportional(14.0),
        CREAM,
    );
}

/// Map a simulation event to its brand color for timeline display.
fn event_color(event: &SimEvent) -> egui::Color32 {
    match event {
        SimEvent::MessageSent { .. } => BLUE,
        SimEvent::ViewChangeStarted { .. } => PINK,
        SimEvent::OperationCommitted { .. } => ORANGE,
        SimEvent::PrimaryElected { .. } => PURPLE,
        SimEvent::PrepareAcked { .. } => TEAL,
        _ => egui::Color32::from_rgb(0x6b, 0x6b, 0x78), // muted gray
    }
}
