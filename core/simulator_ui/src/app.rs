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

//! Main application struct implementing `eframe::App` for the VSR simulator.
//!
//! Minimalist game-HUD design: the cluster view is 100% of the screen with
//! translucent overlay bars for controls and metrics.

use crate::brand::BrandAssets;
use crate::controls::fault_panel::{FaultAction, draw_fault_panel};
use crate::render::cluster_view::draw_cluster;
use crate::render::timeline::draw_timeline;
use crate::state::animation::AnimationState;
use crate::vsr::{SimEvent, VsrSimulator};
use egui::Color32;

// ---------------------------------------------------------------------------
// Brand palette
// ---------------------------------------------------------------------------

const BACKGROUND: Color32 = Color32::from_rgb(0x07, 0x0c, 0x17);
const CARD: Color32 = Color32::from_rgb(0x0e, 0x19, 0x30);
const ORANGE: Color32 = Color32::from_rgb(0xff, 0x91, 0x03);
const CREAM: Color32 = Color32::from_rgb(0xff, 0xfa, 0xeb);
const MUTED: Color32 = Color32::from_rgb(0x83, 0x8d, 0x95);
const BORDER: Color32 = Color32::from_rgb(0x3d, 0x44, 0x50);
const TEAL: Color32 = Color32::from_rgb(0x14, 0xb8, 0xa6);
const RED: Color32 = Color32::from_rgb(0xef, 0x44, 0x44);

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/// The main VSR simulator application.
pub struct SimulatorApp {
    brand: BrandAssets,
    simulator: VsrSimulator,
    animations: AnimationState,
    event_log: Vec<SimEvent>,
    replica_positions: Vec<egui::Pos2>,
    playing: bool,
    step_requested: bool,
    speed: f32,
    show_fault_panel: bool,
    total_commits: u64,
    ops_per_second: u64,
    commits_this_second: u64,
    last_second_tick: u64,
}

impl SimulatorApp {
    /// Create the application with a 3-replica simulator seeded at 42.
    pub fn new(cc: &eframe::CreationContext) -> Self {
        let brand = BrandAssets::load(&cc.egui_ctx);

        // Configure egui dark theme with brand colors.
        let mut visuals = egui::Visuals::dark();

        visuals.panel_fill = BACKGROUND;
        visuals.window_fill = CARD;
        visuals.extreme_bg_color = BACKGROUND;
        visuals.faint_bg_color = BACKGROUND;
        visuals.window_shadow = egui::Shadow::NONE;

        visuals.widgets.inactive.bg_fill = CARD;
        visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, CREAM);
        visuals.widgets.hovered.bg_fill = ORANGE.linear_multiply(0.2);
        visuals.widgets.hovered.fg_stroke = egui::Stroke::new(1.0, ORANGE);
        visuals.widgets.active.bg_fill = ORANGE.linear_multiply(0.4);
        visuals.widgets.active.fg_stroke = egui::Stroke::new(1.0, ORANGE);
        visuals.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0, CREAM);
        visuals.widgets.noninteractive.bg_fill = BACKGROUND;

        visuals.selection.bg_fill = ORANGE.linear_multiply(0.3);
        visuals.selection.stroke = egui::Stroke::new(1.0, ORANGE);
        visuals.override_text_color = Some(CREAM);

        cc.egui_ctx.set_visuals(visuals);

        cc.egui_ctx.style_mut(|s| {
            s.text_styles
                .insert(egui::TextStyle::Body, egui::FontId::proportional(14.0));
            s.text_styles
                .insert(egui::TextStyle::Button, egui::FontId::proportional(14.0));
            s.text_styles
                .insert(egui::TextStyle::Heading, egui::FontId::proportional(24.0));
            s.text_styles
                .insert(egui::TextStyle::Small, egui::FontId::proportional(11.0));
        });

        let replica_positions = vec![
            egui::pos2(300.0, 200.0),
            egui::pos2(500.0, 200.0),
            egui::pos2(400.0, 350.0),
        ];

        Self {
            brand,
            simulator: VsrSimulator::new(3, 42),
            animations: AnimationState::default(),
            event_log: Vec::new(),
            replica_positions,
            playing: false,
            step_requested: false,
            speed: 1.0,
            show_fault_panel: false,
            total_commits: 0,
            ops_per_second: 0,
            commits_this_second: 0,
            last_second_tick: 0,
        }
    }

    /// Run one simulation tick and process the resulting events.
    fn run_tick(&mut self) {
        let events = self.simulator.step();
        for event in &events {
            match event {
                SimEvent::MessageSent { message, .. } => {
                    let from_idx = message.from as usize;
                    let to_idx = message.to as usize;
                    if from_idx < self.replica_positions.len()
                        && to_idx < self.replica_positions.len()
                    {
                        let from_pos = self.replica_positions[from_idx];
                        let to_pos = self.replica_positions[to_idx];
                        let duration = 0.5 / self.speed;
                        self.animations.add(
                            message.id,
                            message.msg_type,
                            from_pos,
                            to_pos,
                            duration,
                        );
                    }
                }
                SimEvent::OperationCommitted { replica_id, .. } => {
                    self.total_commits += 1;
                    self.commits_this_second += 1;
                    let idx = *replica_id as usize;
                    if idx < self.replica_positions.len() {
                        self.animations.add_commit_ring(self.replica_positions[idx]);
                    }
                }
                _ => {}
            }
        }

        // Track ops/sec: every 60 ticks, snapshot.
        if self.simulator.tick >= self.last_second_tick + 60 {
            self.ops_per_second = self.commits_this_second;
            self.commits_this_second = 0;
            self.last_second_tick = self.simulator.tick;
        }

        self.event_log.extend(events);
    }

    /// Apply fault actions from the UI to the simulator.
    fn apply_faults(&mut self, actions: Vec<FaultAction>) {
        for action in actions {
            match action {
                FaultAction::Partition(a, b) => {
                    self.simulator.network.partition(a, b);
                }
                FaultAction::Heal(a, b) => {
                    self.simulator.network.heal(a, b);
                }
                FaultAction::Kill(id) => {
                    self.simulator.kill_replica(id);
                }
                FaultAction::Restart(id) => {
                    self.simulator.restart_replica(id);
                }
            }
        }
    }
}

impl eframe::App for SimulatorApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let dt = ctx.input(|i| i.stable_dt);

        // Advance simulation ticks.
        if self.playing {
            let ticks = ((self.speed * 60.0 * dt) as usize).clamp(1, 20);
            for _ in 0..ticks {
                self.run_tick();
            }
        }

        if self.step_requested {
            self.run_tick();
            self.step_requested = false;
        }

        // Advance animations.
        self.animations.advance(dt);

        // -----------------------------------------------------------------
        // Fullscreen central panel -- everything draws here
        // -----------------------------------------------------------------
        egui::CentralPanel::default()
            .frame(
                egui::Frame::central_panel(&ctx.style())
                    .fill(BACKGROUND)
                    .inner_margin(egui::Margin::same(0)),
            )
            .show(ctx, |ui| {
                let rect = ui.max_rect();
                let painter = ui.painter_at(rect);

                // 1. Cluster view (the hero, fullscreen)
                let replicas = self.simulator.replica_states();
                self.replica_positions =
                    draw_cluster(ui, &replicas, &self.animations, &self.simulator.network);

                // 2. Top HUD bar overlay (48px, translucent)
                let hud_rect = egui::Rect::from_min_size(rect.min, egui::vec2(rect.width(), 48.0));
                painter.rect_filled(
                    hud_rect,
                    0.0,
                    Color32::from_rgba_premultiplied(7, 12, 23, 200),
                );
                // Thin bottom edge line
                painter.line_segment(
                    [
                        egui::pos2(hud_rect.left(), hud_rect.bottom()),
                        egui::pos2(hud_rect.right(), hud_rect.bottom()),
                    ],
                    egui::Stroke::new(
                        0.5,
                        Color32::from_rgba_unmultiplied(BORDER.r(), BORDER.g(), BORDER.b(), 60),
                    ),
                );

                // Logo on left
                let logo_h = 32.0;
                let logo_aspect =
                    self.brand.logo.size()[0] as f32 / self.brand.logo.size()[1] as f32;
                let logo_w = logo_h * logo_aspect;
                let logo_rect = egui::Rect::from_min_size(
                    egui::pos2(rect.min.x + 12.0, hud_rect.center().y - logo_h / 2.0),
                    egui::vec2(logo_w, logo_h),
                );
                painter.image(
                    self.brand.logo.id(),
                    logo_rect,
                    egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)),
                    Color32::WHITE,
                );

                // Playback controls on the right side of the HUD bar (interactive)
                let controls_rect = egui::Rect::from_min_max(
                    egui::pos2(rect.max.x - 280.0, hud_rect.min.y + 4.0),
                    egui::pos2(rect.max.x - 8.0, hud_rect.max.y - 4.0),
                );
                ui.allocate_new_ui(egui::UiBuilder::new().max_rect(controls_rect), |ui| {
                    ui.horizontal_centered(|ui| {
                        ui.spacing_mut().item_spacing = egui::vec2(6.0, 0.0);

                        // Play / Pause button
                        let (play_icon, play_color) = if self.playing {
                            ("\u{23F8}", ORANGE)
                        } else {
                            ("\u{25B6}", CREAM)
                        };
                        let play_btn = egui::Button::new(
                            egui::RichText::new(play_icon)
                                .size(20.0)
                                .color(if self.playing { BACKGROUND } else { play_color }),
                        )
                        .fill(if self.playing {
                            ORANGE
                        } else {
                            MUTED.linear_multiply(0.2)
                        })
                        .corner_radius(4.0)
                        .min_size(egui::vec2(36.0, 32.0));
                        if ui.add(play_btn).clicked() {
                            self.playing = !self.playing;
                        }

                        // Step button
                        let step_btn = egui::Button::new(
                            egui::RichText::new("\u{23ED}").size(16.0).color(CREAM),
                        )
                        .fill(MUTED.linear_multiply(0.15))
                        .stroke(egui::Stroke::new(0.5, BORDER))
                        .corner_radius(4.0)
                        .min_size(egui::vec2(30.0, 32.0));
                        if ui.add(step_btn).clicked() {
                            self.step_requested = true;
                        }

                        // Speed label
                        ui.label(
                            egui::RichText::new(format!("\u{00D7}{:.1}", self.speed))
                                .size(14.0)
                                .color(MUTED),
                        );

                        // Speed slider
                        let slider = egui::Slider::new(&mut self.speed, 0.1..=10.0)
                            .logarithmic(true)
                            .show_value(false);
                        ui.add_sized([60.0, 18.0], slider);

                        // Send request button
                        let send_btn = egui::Button::new(
                            egui::RichText::new("\u{1F4E8}")
                                .size(14.0)
                                .color(BACKGROUND)
                                .strong(),
                        )
                        .fill(ORANGE)
                        .corner_radius(4.0)
                        .min_size(egui::vec2(34.0, 32.0));
                        if ui.add(send_btn).clicked() {
                            self.simulator.inject_client_request();
                            let new_events: Vec<SimEvent> = self.simulator.events
                                [self.event_log.len().min(self.simulator.events.len())..]
                                .to_vec();
                            for event in &new_events {
                                match event {
                                    SimEvent::MessageSent { message, .. } => {
                                        let from_idx = message.from as usize;
                                        let to_idx = message.to as usize;
                                        if from_idx < self.replica_positions.len()
                                            && to_idx < self.replica_positions.len()
                                        {
                                            let from_pos = self.replica_positions[from_idx];
                                            let to_pos = self.replica_positions[to_idx];
                                            self.animations.add(
                                                message.id,
                                                message.msg_type,
                                                from_pos,
                                                to_pos,
                                                0.5 / self.speed,
                                            );
                                        }
                                    }
                                    SimEvent::OperationCommitted { replica_id, .. } => {
                                        self.total_commits += 1;
                                        let idx = *replica_id as usize;
                                        if idx < self.replica_positions.len() {
                                            self.animations
                                                .add_commit_ring(self.replica_positions[idx]);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            self.event_log.extend(new_events);
                        }
                    });
                });

                // 3. Bottom HUD bar overlay (60px, translucent)
                let bottom_rect =
                    egui::Rect::from_min_max(egui::pos2(rect.min.x, rect.max.y - 60.0), rect.max);
                painter.rect_filled(
                    bottom_rect,
                    0.0,
                    Color32::from_rgba_premultiplied(7, 12, 23, 180),
                );
                // Thin top edge line
                painter.line_segment(
                    [
                        egui::pos2(bottom_rect.left(), bottom_rect.top()),
                        egui::pos2(bottom_rect.right(), bottom_rect.top()),
                    ],
                    egui::Stroke::new(
                        0.5,
                        Color32::from_rgba_unmultiplied(BORDER.r(), BORDER.g(), BORDER.b(), 60),
                    ),
                );

                // Timeline inside bottom HUD (left portion)
                let timeline_rect = egui::Rect::from_min_max(
                    egui::pos2(bottom_rect.min.x + 12.0, bottom_rect.min.y + 6.0),
                    egui::pos2(bottom_rect.max.x - 120.0, bottom_rect.max.y - 6.0),
                );
                let mut timeline_ui = ui.new_child(
                    egui::UiBuilder::new()
                        .max_rect(timeline_rect)
                        .layout(egui::Layout::left_to_right(egui::Align::Center)),
                );
                draw_timeline(&mut timeline_ui, &self.event_log, self.simulator.tick);

                // Tick counter on right side of bottom HUD
                painter.text(
                    egui::pos2(bottom_rect.max.x - 16.0, bottom_rect.center().y),
                    egui::Align2::RIGHT_CENTER,
                    format!("T {}", self.simulator.tick),
                    egui::FontId::new(24.0, egui::FontFamily::Proportional),
                    CREAM,
                );

                // 4. Top-right metrics overlay
                let metrics_rect = egui::Rect::from_min_size(
                    egui::pos2(rect.max.x - 148.0, 56.0),
                    egui::vec2(140.0, 108.0),
                );
                painter.rect_filled(
                    metrics_rect,
                    8.0,
                    Color32::from_rgba_premultiplied(14, 25, 48, 180),
                );
                painter.rect_stroke(
                    metrics_rect,
                    8.0,
                    egui::Stroke::new(
                        0.5,
                        Color32::from_rgba_unmultiplied(BORDER.r(), BORDER.g(), BORDER.b(), 40),
                    ),
                    egui::StrokeKind::Outside,
                );

                let mx = metrics_rect.max.x - 12.0;
                let mut my = metrics_rect.min.y + 16.0;

                // OPS/s
                painter.text(
                    egui::pos2(mx, my),
                    egui::Align2::RIGHT_CENTER,
                    "OPS/s",
                    egui::FontId::new(11.0, egui::FontFamily::Proportional),
                    MUTED,
                );
                my += 20.0;
                painter.text(
                    egui::pos2(mx, my),
                    egui::Align2::RIGHT_CENTER,
                    self.ops_per_second.to_string(),
                    egui::FontId::new(28.0, egui::FontFamily::Proportional),
                    ORANGE,
                );
                my += 24.0;

                // COMMITS
                painter.text(
                    egui::pos2(mx, my),
                    egui::Align2::RIGHT_CENTER,
                    "COMMITS",
                    egui::FontId::new(11.0, egui::FontFamily::Proportional),
                    MUTED,
                );
                my += 16.0;
                painter.text(
                    egui::pos2(mx, my),
                    egui::Align2::RIGHT_CENTER,
                    self.total_commits.to_string(),
                    egui::FontId::new(20.0, egui::FontFamily::Proportional),
                    TEAL,
                );

                // VIEW (derive from primary)
                let view_number = self
                    .simulator
                    .replica_states()
                    .iter()
                    .find(|r| r.role == crate::vsr::Role::Primary && r.alive)
                    .map(|r| r.view)
                    .unwrap_or(0);

                let view_rect = egui::Rect::from_min_size(
                    egui::pos2(metrics_rect.min.x, metrics_rect.max.y + 4.0),
                    egui::vec2(140.0, 36.0),
                );
                painter.rect_filled(
                    view_rect,
                    8.0,
                    Color32::from_rgba_premultiplied(14, 25, 48, 180),
                );
                painter.text(
                    egui::pos2(view_rect.max.x - 12.0, view_rect.min.y + 10.0),
                    egui::Align2::RIGHT_CENTER,
                    "VIEW",
                    egui::FontId::new(11.0, egui::FontFamily::Proportional),
                    MUTED,
                );
                painter.text(
                    egui::pos2(view_rect.max.x - 12.0, view_rect.min.y + 26.0),
                    egui::Align2::RIGHT_CENTER,
                    view_number.to_string(),
                    egui::FontId::new(20.0, egui::FontFamily::Proportional),
                    CREAM,
                );

                // 5. Fault toggle button (below metrics)
                let fault_btn_rect = egui::Rect::from_min_size(
                    egui::pos2(view_rect.min.x, view_rect.max.y + 4.0),
                    egui::vec2(140.0, 28.0),
                );
                ui.allocate_new_ui(egui::UiBuilder::new().max_rect(fault_btn_rect), |ui| {
                    let chaos_fill = if self.show_fault_panel {
                        RED.linear_multiply(0.5)
                    } else {
                        MUTED.linear_multiply(0.12)
                    };
                    let chaos_btn = egui::Button::new(
                        egui::RichText::new("\u{26A1} FAULTS")
                            .size(14.0)
                            .color(CREAM)
                            .strong(),
                    )
                    .fill(chaos_fill)
                    .corner_radius(6.0)
                    .min_size(egui::vec2(140.0, 28.0));
                    if ui.add(chaos_btn).clicked() {
                        self.show_fault_panel = !self.show_fault_panel;
                    }
                });
            });

        // -----------------------------------------------------------------
        // Fault injection window (toggled)
        // -----------------------------------------------------------------
        let mut fault_actions = Vec::new();
        if self.show_fault_panel {
            egui::Window::new("\u{26A1} Fault Injection")
                .anchor(egui::Align2::RIGHT_TOP, [-8.0, 240.0])
                .resizable(false)
                .collapsible(false)
                .default_width(260.0)
                .frame(
                    egui::Frame::window(&ctx.style())
                        .fill(CARD)
                        .inner_margin(egui::Margin::same(12))
                        .stroke(egui::Stroke::new(1.0, BORDER)),
                )
                .show(ctx, |ui| {
                    fault_actions = draw_fault_panel(
                        ui,
                        self.simulator.replicas.len() as u8,
                        &self.simulator.network,
                    );
                });
        }

        self.apply_faults(fault_actions);

        // Only repaint when there is something moving.
        if self.playing || self.animations.has_active() {
            ctx.request_repaint();
        }
    }
}
