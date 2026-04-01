// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Fault injection panel: kill/restart replicas and partition network links.

use egui::{Color32, RichText};
use strum_macros::{AsRefStr, Display, EnumIter};

const CREAM: Color32 = Color32::from_rgb(0xff, 0xfa, 0xeb);
const MUTED: Color32 = Color32::from_rgb(0x83, 0x8d, 0x95);
const TEAL: Color32 = Color32::from_rgb(0x14, 0xb8, 0xa6);
const RED: Color32 = Color32::from_rgb(0xef, 0x44, 0x44);
const BORDER: Color32 = Color32::from_rgb(0x3d, 0x44, 0x50);
const CARD: Color32 = Color32::from_rgb(0x0e, 0x19, 0x30);

/// Actions that can be triggered from the fault injection panel.
#[derive(Debug, Clone, PartialEq, Eq, Display, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum FaultAction {
    Partition(u8, u8),
    Heal(u8, u8),
    Kill(u8),
    Restart(u8),
}

/// Draw the fault injection panel. Returns a list of fault actions requested.
pub fn draw_fault_panel(
    ui: &mut egui::Ui,
    replica_count: u8,
    network: &crate::vsr::Network,
) -> Vec<FaultAction> {
    let mut actions = Vec::new();

    ui.spacing_mut().item_spacing = egui::vec2(8.0, 8.0);

    // Section header
    ui.label(RichText::new("FAULT INJECTION").size(12.0).color(MUTED));
    ui.add_space(8.0);

    // Replicas sub-header
    ui.label(RichText::new("REPLICAS").size(11.0).color(MUTED));

    // Per-replica kill/restart rows
    for i in 0..replica_count {
        let alive = network.is_alive(i);
        ui.horizontal(|ui| {
            ui.label(
                RichText::new(format!("R{i}"))
                    .size(15.0)
                    .color(CREAM)
                    .strong(),
            );
            ui.add_space(8.0);

            if alive {
                let kill_btn =
                    egui::Button::new(RichText::new("KILL").size(11.0).color(CREAM).strong())
                        .fill(RED.linear_multiply(0.8))
                        .corner_radius(3.0)
                        .min_size(egui::vec2(60.0, 26.0));

                if ui.add(kill_btn).clicked() {
                    actions.push(FaultAction::Kill(i));
                }
            } else {
                let restart_btn =
                    egui::Button::new(RichText::new("RESTART").size(11.0).color(CREAM).strong())
                        .fill(TEAL.linear_multiply(0.8))
                        .corner_radius(3.0)
                        .min_size(egui::vec2(60.0, 26.0));

                if ui.add(restart_btn).clicked() {
                    actions.push(FaultAction::Restart(i));
                }
            }
        });
    }

    ui.add_space(12.0);

    // Network partitions sub-header
    ui.label(RichText::new("NETWORK PARTITIONS").size(11.0).color(MUTED));

    // NxN matrix grid
    let cell_size = 24.0;
    egui::Grid::new("partition_matrix")
        .spacing([4.0, 4.0])
        .show(ui, |ui| {
            // Header row: empty corner + column headers
            ui.label(RichText::new("").size(11.0));
            for j in 0..replica_count {
                ui.label(RichText::new(format!("R{j}")).size(11.0).color(MUTED));
            }
            ui.end_row();

            // Data rows
            for i in 0..replica_count {
                // Row header
                ui.label(RichText::new(format!("R{i}")).size(11.0).color(MUTED));

                for j in 0..replica_count {
                    if i == j {
                        // Diagonal: dark CARD cell
                        let (rect, _) = ui.allocate_exact_size(
                            egui::vec2(cell_size, cell_size),
                            egui::Sense::hover(),
                        );
                        ui.painter().rect_filled(rect, 2.0, CARD);
                        ui.painter().rect_stroke(
                            rect,
                            2.0,
                            egui::Stroke::new(1.0, BORDER),
                            egui::epaint::StrokeKind::Inside,
                        );
                    } else {
                        let partitioned = network.is_partitioned(i, j);
                        let color = if partitioned { RED } else { TEAL };

                        let (rect, response) = ui.allocate_exact_size(
                            egui::vec2(cell_size, cell_size),
                            egui::Sense::click(),
                        );

                        let fill = if response.hovered() {
                            color.linear_multiply(0.7)
                        } else {
                            color
                        };
                        ui.painter().rect_filled(rect, 2.0, fill);
                        ui.painter().rect_stroke(
                            rect,
                            2.0,
                            egui::Stroke::new(1.0, BORDER),
                            egui::epaint::StrokeKind::Inside,
                        );

                        if response.clicked() {
                            if partitioned {
                                actions.push(FaultAction::Heal(i, j));
                            } else {
                                actions.push(FaultAction::Partition(i, j));
                            }
                        }
                    }
                }
                ui.end_row();
            }
        });

    actions
}
