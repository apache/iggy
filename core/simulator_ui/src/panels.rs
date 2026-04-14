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

use crate::components::*;
use crate::queries::*;
use crate::resources::*;
use crate::theme::*;

pub(crate) fn update_event_log_panel(
    mut commands: Commands,
    time: Res<Time>,
    mut event_log: ResMut<EventLog>,
    console: Res<GameConsole>,
    mut panel_query: EventLogPanelQuery,
    mut hint_query: EventLogHintQuery,
    content_query: Query<(Entity, Option<&Children>), With<EventLogContent>>,
) {
    let delta = time.delta_secs();

    let target = if event_log.visible { 1.0 } else { 0.0 };
    event_log.slide += (target - event_log.slide) * 8.0 * delta;
    event_log.slide = event_log.slide.clamp(0.0, 1.0);
    if (event_log.slide - target).abs() < 0.005 {
        event_log.slide = target;
    }

    let right_px = -520.0 * (1.0 - event_log.slide);
    for mut node in panel_query.iter_mut() {
        node.right = Val::Px(right_px);
    }

    for mut vis in hint_query.iter_mut() {
        *vis = if event_log.visible || console.open {
            Visibility::Hidden
        } else {
            Visibility::Inherited
        };
    }

    if event_log.slide < 0.01 {
        return;
    }

    if event_log.generation == event_log.last_rendered {
        return;
    }
    event_log.last_rendered = event_log.generation;

    for (content_entity, children) in content_query.iter() {
        if let Some(children) = children {
            for child in children.iter() {
                commands.entity(child).despawn();
            }
        }
        let max_visible = if event_log.show_details { 30 } else { 60 };
        let skip = event_log.entries.len().saturating_sub(max_visible);
        let visible_entries: Vec<_> = event_log.entries.iter().skip(skip).collect();

        commands.entity(content_entity).with_children(|parent| {
            if visible_entries.is_empty() {
                parent.spawn((
                    Text::new("-- no events yet (SPACE to start, R to inject) --"),
                    TextFont {
                        font_size: 11.0,
                        ..default()
                    },
                    TextColor(DIM_GRAY),
                ));
                return;
            }
            for entry in visible_entries.iter().rev() {
                parent
                    .spawn((
                        Node {
                            flex_direction: FlexDirection::Column,
                            padding: UiRect::axes(Val::Px(6.0), Val::Px(4.0)),
                            border: UiRect::left(Val::Px(3.0)),
                            margin: UiRect::bottom(Val::Px(2.0)),
                            ..default()
                        },
                        BackgroundColor(Color::srgba_u8(0x0c, 0x12, 0x20, 0xaa)),
                        BorderColor::all(entry.color.with_alpha(0.6)),
                    ))
                    .with_children(|card| {
                        card.spawn((
                            Text::new(format!("T{} {} {}", entry.tick, entry.icon, entry.headline)),
                            TextFont {
                                font_size: 14.0,
                                ..default()
                            },
                            TextColor(entry.color),
                        ));
                        if event_log.show_details {
                            card.spawn((
                                Text::new(&entry.detail),
                                TextFont {
                                    font_size: 12.0,
                                    ..default()
                                },
                                TextColor(COOL_WHITE.with_alpha(0.7)),
                                Node {
                                    margin: UiRect::top(Val::Px(3.0)),
                                    ..default()
                                },
                            ));
                        }
                    });
            }
        });
    }
}

pub(crate) fn update_game_console(
    mut commands: Commands,
    time: Res<Time>,
    mut console: ResMut<GameConsole>,
    mut panel_query: crate::queries::ConsolePanelQuery,
    content_query: crate::queries::ConsoleContentQuery,
) {
    let delta = time.delta_secs();

    let target = if console.open { 1.0 } else { 0.0 };
    console.slide += (target - console.slide) * 6.0 * delta;
    console.slide = console.slide.clamp(0.0, 1.0);
    if (console.slide - target).abs() < 0.005 {
        console.slide = target;
    }

    let top_pct = -50.0 * (1.0 - console.slide);
    for mut node in panel_query.iter_mut() {
        node.top = Val::Percent(top_pct);
    }

    if console.slide < 0.01 {
        return;
    }

    let current_generation = console
        .raw_generation
        .load(std::sync::atomic::Ordering::Relaxed);
    if current_generation == console.last_seen_generation {
        return;
    }
    console.last_seen_generation = current_generation;

    let lines = {
        let raw = console.raw_lines.lock().unwrap_or_else(|e| e.into_inner());
        raw.iter().rev().take(60).cloned().collect::<Vec<_>>()
    };

    for (content_entity, children) in content_query.iter() {
        if let Some(children) = children {
            for child in children.iter() {
                commands.entity(child).despawn();
            }
        }

        commands.entity(content_entity).with_children(|parent| {
            if lines.is_empty() {
                parent.spawn((
                    Text::new("-- waiting for events (SPACE to start, R to inject) --"),
                    TextFont {
                        font_size: 13.0,
                        ..default()
                    },
                    TextColor(DIM_GRAY),
                ));
                return;
            }

            for line in &lines {
                let color = if line.contains("Committed") {
                    IGGY_ORANGE
                } else if line.contains("ViewChange") || line.contains("start_view_change") {
                    NEON_MAGENTA
                } else if line.contains("PrimaryElected") || line.contains("send_start_view") {
                    NEON_YELLOW
                } else if line.contains("quorum_reached=true") {
                    NEON_CYAN
                } else if line.contains("PrepareAcked") {
                    Color::srgb_u8(20, 184, 166)
                } else if line.contains("PrepareQueued") {
                    Color::srgb_u8(95, 135, 253)
                } else if line.contains("ClientRequest") || line.contains("ClientReply") {
                    Color::srgb_u8(255, 167, 3)
                } else if line.contains("ControlMessage") {
                    Color::srgb_u8(168, 85, 247)
                } else {
                    DIM_GRAY
                };

                parent.spawn((
                    Text::new(line.as_str()),
                    TextFont {
                        font_size: 13.0,
                        ..default()
                    },
                    TextColor(color),
                ));
            }
        });
    }
}
