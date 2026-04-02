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

use std::f32::consts::PI;

use bevy::prelude::*;
use bevy_prototype_lyon::prelude::*;

use crate::components::*;
use crate::helpers::*;
use crate::resources::*;
use crate::theme::*;

pub(crate) fn setup_camera(mut commands: Commands) {
    commands.spawn(Camera2d);
}

pub(crate) fn setup_world(
    mut commands: Commands,
    positions: Res<ReplicaPositions>,
    asset_server: Res<AssetServer>,
) {
    let mascot_handle: Handle<Image> = asset_server.load("iggy.png");

    for offset in [-420.0, -280.0, -140.0, 0.0, 140.0, 280.0, 420.0] {
        let rising = shapes::Line(
            Vec2::new(-700.0, -520.0 + offset),
            Vec2::new(700.0, 880.0 + offset),
        );
        commands.spawn((
            build_stroke(&rising, GRID_LINE.with_alpha(0.22), 0.75),
            Transform::from_xyz(0.0, -90.0, -0.19),
        ));
        let falling = shapes::Line(
            Vec2::new(-700.0, 880.0 - offset),
            Vec2::new(700.0, -520.0 - offset),
        );
        commands.spawn((
            build_stroke(&falling, GRID_LINE.with_alpha(0.16), 0.75),
            Transform::from_xyz(0.0, -90.0, -0.19),
        ));
    }

    for (size, alpha, depth) in [(640.0, 0.10, -0.14), (760.0, 0.04, -0.13)] {
        commands.spawn((
            build_stroke(
                &rectangle_shape(size, size),
                GRID_LINE.with_alpha(alpha),
                1.0,
            ),
            Transform {
                translation: Vec3::new(0.0, WORLD_CENTER_Y, depth),
                rotation: Quat::from_rotation_z(PI / 4.0),
                ..default()
            },
        ));
    }

    for scan_idx in 0..SCAN_LINE_COUNT {
        let y_pos = -500.0 + (scan_idx as f32) * (1000.0 / SCAN_LINE_COUNT as f32);
        let line = shapes::Line(Vec2::new(-700.0, 0.0), Vec2::new(700.0, 0.0));
        commands.spawn((
            ScanLine {
                speed: 30.0 + (scan_idx as f32) * 15.0,
            },
            build_stroke(&line, NEON_CYAN.with_alpha(0.03), 1.0),
            Transform::from_xyz(0.0, y_pos, -0.05),
        ));
    }

    commands.spawn((
        TrackRing,
        build_stroke(
            &circle_shape(CIRCLE_RADIUS),
            GRID_LINE.with_alpha(0.35),
            0.9,
        ),
        Transform::from_xyz(0.0, WORLD_CENTER_Y, 0.0),
    ));
    commands.spawn((
        build_stroke(
            &circle_shape(CIRCLE_RADIUS - 18.0),
            GRID_LINE.with_alpha(0.12),
            0.5,
        ),
        Transform::from_xyz(0.0, WORLD_CENTER_Y, -0.01),
    ));
    commands.spawn((
        build_stroke(
            &circle_shape(CIRCLE_RADIUS + 18.0),
            GRID_LINE.with_alpha(0.12),
            0.5,
        ),
        Transform::from_xyz(0.0, WORLD_CENTER_Y, -0.01),
    ));

    for particle_idx in 0..AMBIENT_PARTICLE_COUNT {
        let phase = particle_idx as f32 / AMBIENT_PARTICLE_COUNT as f32;
        let x_pos = (phase * 17.3 + 0.5).fract() * 1600.0 - 800.0;
        let y_pos = (phase * 23.7 + 0.2).fract() * 1100.0 - 550.0;
        let size = 0.8 + (phase * 7.1).fract() * 1.8;
        let alpha = 0.04 + (phase * 13.3).fract() * 0.10;
        let color = match particle_idx % 5 {
            0 => NEON_CYAN,
            1 => IGGY_ORANGE,
            _ => COOL_WHITE,
        };

        commands.spawn((
            AmbientParticle {
                velocity: Vec2::new(
                    ((phase * 31.1).fract() - 0.5) * 12.0,
                    8.0 + (phase * 11.7).fract() * 18.0,
                ),
                drift_phase: phase * PI * 2.0,
                drift_speed: 0.4 + (phase * 5.3).fract() * 0.8,
                base_alpha: alpha,
            },
            build_fill(&circle_shape(size), color.with_alpha(alpha)),
            Transform::from_xyz(x_pos, y_pos, -0.08),
        ));
    }

    commands.spawn((
        ScreenFlashOverlay,
        Sprite::from_color(NEON_CYAN.with_alpha(0.0), Vec2::new(1600.0, 1100.0)),
        Transform::from_xyz(0.0, 0.0, 90.0),
    ));

    for replica_id in 0..REPLICA_COUNT {
        let position = positions.0[replica_id as usize];
        let is_primary = replica_id == 0;
        let accent = if is_primary { IGGY_ORANGE } else { NEON_CYAN };
        let facing_right = position.x < 0.0;

        commands.spawn((
            ReplicaShadow { id: replica_id },
            Sprite::from_color(BG_DARK.with_alpha(0.45), Vec2::new(118.0, 24.0)),
            Transform::from_xyz(position.x, position.y - 84.0, 0.35),
        ));

        commands.spawn((
            ReplicaGlow { id: replica_id },
            build_fill(&circle_shape(74.0), accent.with_alpha(0.12)),
            Transform::from_xyz(position.x, position.y - 6.0, 0.5),
        ));

        commands.spawn((
            ReplicaNeonOutline { id: replica_id },
            build_stroke(&circle_shape(82.0), accent.with_alpha(0.35), 2.0),
            Transform::from_xyz(position.x, position.y - 2.0, 0.8),
        ));

        commands.spawn((
            ReplicaBody { id: replica_id },
            Sprite {
                image: mascot_handle.clone(),
                rect: Some(iggy_sprite_rect()),
                custom_size: Some(IGGY_SPRITE_SIZE),
                flip_x: !facing_right,
                ..default()
            },
            Transform::from_xyz(position.x, position.y + 8.0, 1.6),
        ));

        for (layer, glyph, size) in [(0u8, "z", 34.0), (1u8, "z", 28.0), (2u8, "z", 22.0)] {
            commands.spawn((
                ReplicaSleepText {
                    id: replica_id,
                    layer,
                },
                Text2d::new(glyph),
                TextFont {
                    font_size: size,
                    ..default()
                },
                TextColor(COOL_WHITE.with_alpha(0.9)),
                Transform::from_xyz(position.x + 60.0, position.y + 88.0, 1.8),
                Visibility::Hidden,
            ));
        }

        commands.spawn((
            ReplicaIdText { id: replica_id },
            Text2d::new(format!("R{replica_id}")),
            TextFont {
                font_size: 28.0,
                ..default()
            },
            TextColor(COOL_WHITE),
            Transform::from_xyz(position.x, position.y - 112.0, 3.0),
        ));

        let label = if is_primary {
            "LEAD DOG / running"
        } else {
            "PACK DOG / running"
        };
        let role_color = if is_primary { IGGY_ORANGE } else { DIM_GRAY };
        commands.spawn((
            ReplicaRoleText { id: replica_id },
            Text2d::new(label),
            TextFont {
                font_size: 11.0,
                ..default()
            },
            TextColor(role_color),
            Transform::from_xyz(position.x, position.y - 130.0, 3.0),
        ));

        commands.spawn((
            ReplicaCalloutText { id: replica_id },
            Text2d::new(""),
            TextFont {
                font_size: 17.0,
                ..default()
            },
            TextColor(COOL_WHITE.with_alpha(0.0)),
            Transform::from_xyz(position.x, position.y + 118.0, 3.2),
            Visibility::Hidden,
        ));
    }

    for from_id in 0..REPLICA_COUNT {
        for to_id in (from_id + 1)..REPLICA_COUNT {
            let from_pos = positions.0[from_id as usize];
            let to_pos = positions.0[to_id as usize];
            let bend = if (from_id + to_id) % 2 == 0 {
                56.0
            } else {
                -56.0
            };
            let lane = replica_link_path(from_pos, to_pos, bend);

            commands.spawn((
                LinkLine { from_id, to_id },
                build_path_stroke(&lane, GRID_LINE.with_alpha(0.65), 1.4),
                Transform::from_xyz(0.0, 0.0, 0.05),
            ));
        }
    }

    let apps = [
        (
            0usize,
            "BALL THROWER",
            AppKind::Producer,
            Vec2::new(-560.0, 330.0),
            0u8,
            IGGY_ORANGE,
            true,
        ),
        (
            1usize,
            "BALL FETCHER",
            AppKind::Consumer,
            Vec2::new(560.0, 330.0),
            1u8,
            NEON_CYAN,
            false,
        ),
        (
            2usize,
            "TREAT DISPENSER",
            AppKind::Producer,
            Vec2::new(-560.0, 70.0),
            2u8,
            NEON_YELLOW,
            true,
        ),
        (
            3usize,
            "TREAT GOBBLER",
            AppKind::Consumer,
            Vec2::new(560.0, 70.0),
            0u8,
            COOL_WHITE,
            false,
        ),
    ];

    for (id, title, kind, anchor, target_replica, accent, inbound) in apps {
        let target = positions.0[target_replica as usize];
        let lane = app_link_path(anchor, target, inbound);
        commands.spawn((
            AppLink {
                id,
                anchor,
                target_replica,
                accent,
                inbound,
            },
            build_path_stroke(&lane, accent.with_alpha(0.18), 1.5),
            Transform::from_xyz(0.0, 0.0, 0.03),
        ));

        commands.spawn((
            AppCard { id, anchor, accent },
            build_fill(&circle_shape(50.0), accent.with_alpha(0.10)),
            Transform::from_xyz(anchor.x, anchor.y, 1.2),
        ));

        commands.spawn((
            AppIcon {
                id,
                anchor,
                accent,
                kind,
            },
            build_path_stroke(&app_icon_path(kind), accent.with_alpha(0.95), 2.4),
            Transform::from_xyz(anchor.x, anchor.y + 4.0, 1.35),
        ));

        commands.spawn((
            AppCardLabel { id, anchor },
            Text2d::new(title),
            TextFont {
                font_size: 13.0,
                ..default()
            },
            TextColor(COOL_WHITE),
            Transform::from_xyz(anchor.x, anchor.y - 64.0, 1.3),
        ));
    }
}

pub(crate) fn setup_hud(mut commands: Commands, asset_server: Res<AssetServer>) {
    let sygnet_handle: Handle<Image> =
        asset_server.load("logo/4x/iggy-apache-sygnet-color-darkbg@4x.png");
    let wordmark_handle: Handle<Image> =
        asset_server.load("logo/4x/iggy-apache-logo-wo-sygnet-light@4x.png");

    commands
        .spawn(Node {
            width: Val::Percent(100.0),
            height: Val::Percent(100.0),
            ..default()
        })
        .with_children(|root| {
            root.spawn((
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(0.0),
                    right: Val::Px(0.0),
                    top: Val::Px(0.0),
                    height: Val::Px(56.0),
                    padding: UiRect::axes(Val::Px(20.0), Val::Px(8.0)),
                    justify_content: JustifyContent::SpaceBetween,
                    align_items: AlignItems::Center,
                    ..default()
                },
                BackgroundColor(Color::srgba_u8(0x07, 0x0c, 0x17, 0xe8)),
            ))
            .with_children(|bar| {
                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(24.0),
                    ..default()
                })
                .with_children(|left| {
                    left.spawn((
                        Text::new("iggy::pack"),
                        TextFont {
                            font_size: 16.0,
                            ..default()
                        },
                        TextColor(IGGY_ORANGE),
                    ));
                    left.spawn(Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Baseline,
                        column_gap: Val::Px(6.0),
                        ..default()
                    })
                    .with_children(|stat| {
                        stat.spawn((
                            Text::new("TREATS"),
                            TextFont {
                                font_size: 9.0,
                                ..default()
                            },
                            TextColor(DIM_GRAY),
                        ));
                        stat.spawn((
                            HudCommitsText,
                            Text::new("0"),
                            TextFont {
                                font_size: 20.0,
                                ..default()
                            },
                            TextColor(NEON_CYAN),
                        ));
                    });
                    left.spawn(Node {
                        flex_direction: FlexDirection::Row,
                        align_items: AlignItems::Baseline,
                        column_gap: Val::Px(6.0),
                        ..default()
                    })
                    .with_children(|stat| {
                        stat.spawn((
                            Text::new("LAPS/S"),
                            TextFont {
                                font_size: 9.0,
                                ..default()
                            },
                            TextColor(DIM_GRAY),
                        ));
                        stat.spawn((
                            HudOpsText,
                            Text::new("0"),
                            TextFont {
                                font_size: 20.0,
                                ..default()
                            },
                            TextColor(IGGY_ORANGE),
                        ));
                    });
                });

                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(6.0),
                    ..default()
                })
                .with_children(|logo| {
                    logo.spawn((
                        ImageNode::new(sygnet_handle),
                        Node {
                            width: Val::Px(38.0),
                            height: Val::Px(29.0),
                            ..default()
                        },
                    ));
                    logo.spawn((
                        ImageNode::new(wordmark_handle),
                        Node {
                            width: Val::Px(78.0),
                            height: Val::Px(25.0),
                            ..default()
                        },
                    ));
                });

                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    align_items: AlignItems::Center,
                    column_gap: Val::Px(16.0),
                    ..default()
                })
                .with_children(|right| {
                    right.spawn((
                        HudTickText,
                        Text::new("T 0"),
                        TextFont {
                            font_size: 18.0,
                            ..default()
                        },
                        TextColor(COOL_WHITE),
                    ));
                    right
                        .spawn(Node {
                            flex_direction: FlexDirection::Column,
                            align_items: AlignItems::End,
                            ..default()
                        })
                        .with_children(|col| {
                            col.spawn((
                                HudStateText,
                                Text::new("RACING"),
                                TextFont {
                                    font_size: 16.0,
                                    ..default()
                                },
                                TextColor(NEON_CYAN),
                            ));
                            col.spawn((
                                HudSpeedText,
                                Text::new("x1.0"),
                                TextFont {
                                    font_size: 16.0,
                                    ..default()
                                },
                                TextColor(COOL_WHITE),
                            ));
                        });
                });
            });

            root.spawn((
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(0.0),
                    right: Val::Px(0.0),
                    bottom: Val::Px(0.0),
                    padding: UiRect::axes(Val::Px(24.0), Val::Px(14.0)),
                    justify_content: JustifyContent::SpaceBetween,
                    align_items: AlignItems::Center,
                    border: UiRect::top(Val::Px(1.0)),
                    ..default()
                },
                BackgroundColor(Color::srgba_u8(0x07, 0x0c, 0x17, 0xe8)),
                BorderColor::all(HUD_EDGE),
            ))
            .with_children(|bar| {
                bar.spawn(Node {
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(2.0),
                    ..default()
                })
                .with_children(|col| {
                    col.spawn((
                        Text::new("APACHE IGGY SIMULATOR"),
                        TextFont {
                            font_size: 16.0,
                            ..default()
                        },
                        TextColor(COOL_WHITE),
                    ));
                    col.spawn((
                        Text::new("Italian greyhounds racing to consensus"),
                        TextFont {
                            font_size: 12.0,
                            ..default()
                        },
                        TextColor(DIM_GRAY),
                    ));
                });

                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    column_gap: Val::Px(12.0),
                    flex_wrap: FlexWrap::Wrap,
                    ..default()
                })
                .with_children(|keys| {
                    for (key, action, color) in [
                        ("SPACE", "race/rest", NEON_CYAN),
                        ("R", "throw ball", IGGY_ORANGE),
                        ("K", "trip dog", HUD_ALERT),
                        ("P", "partition", NEON_MAGENTA),
                        ("H", "heal all", NEON_CYAN),
                        ("UP/DN", "speed", NEON_YELLOW),
                        ("E", "event log", NEON_YELLOW),
                        ("D", "details", DIM_GRAY),
                        ("~/T", "console", NEON_CYAN),
                    ] {
                        keys.spawn(Node {
                            flex_direction: FlexDirection::Row,
                            align_items: AlignItems::Center,
                            column_gap: Val::Px(4.0),
                            ..default()
                        })
                        .with_children(|pair| {
                            pair.spawn((
                                Node {
                                    padding: UiRect::axes(Val::Px(8.0), Val::Px(4.0)),
                                    border: UiRect::all(Val::Px(1.0)),
                                    ..default()
                                },
                                BackgroundColor(BG_PANEL),
                                BorderColor::all(color.with_alpha(0.5)),
                            ))
                            .with_children(|badge| {
                                badge.spawn((
                                    Text::new(key),
                                    TextFont {
                                        font_size: 13.0,
                                        ..default()
                                    },
                                    TextColor(color),
                                ));
                            });
                            pair.spawn((
                                Text::new(action),
                                TextFont {
                                    font_size: 12.0,
                                    ..default()
                                },
                                TextColor(COOL_WHITE.with_alpha(0.7)),
                            ));
                        });
                    }
                });

                bar.spawn(Node {
                    flex_direction: FlexDirection::Row,
                    column_gap: Val::Px(8.0),
                    ..default()
                })
                .with_children(|chips| {
                    for (label, color) in
                        [("3 GREYHOUNDS", IGGY_ORANGE), ("FAULT READY", HUD_ALERT)]
                    {
                        chips
                            .spawn((
                                Node {
                                    padding: UiRect::axes(Val::Px(8.0), Val::Px(4.0)),
                                    border: UiRect::all(Val::Px(1.0)),
                                    ..default()
                                },
                                BackgroundColor(BG_PANEL.with_alpha(0.9)),
                                BorderColor::all(color.with_alpha(0.4)),
                            ))
                            .with_children(|chip| {
                                chip.spawn((
                                    Text::new(label),
                                    TextFont {
                                        font_size: 12.0,
                                        ..default()
                                    },
                                    TextColor(color),
                                ));
                            });
                    }
                });
            });

            root.spawn((
                PauseOverlay,
                Node {
                    position_type: PositionType::Absolute,
                    width: Val::Percent(100.0),
                    height: Val::Percent(100.0),
                    justify_content: JustifyContent::Center,
                    align_items: AlignItems::Center,
                    flex_direction: FlexDirection::Column,
                    row_gap: Val::Px(8.0),
                    ..default()
                },
                Visibility::Hidden,
            ))
            .with_children(|col| {
                col.spawn((
                    PauseMainText,
                    Text::new("RESTING"),
                    TextFont {
                        font_size: 52.0,
                        ..default()
                    },
                    TextColor(NEON_YELLOW),
                ));
                col.spawn((
                    PauseSubtext,
                    Text::new("press SPACE to unleash"),
                    TextFont {
                        font_size: 16.0,
                        ..default()
                    },
                    TextColor(DIM_GRAY),
                ));
            });

            root.spawn((
                EventLogPanel,
                Node {
                    position_type: PositionType::Absolute,
                    right: Val::Px(-520.0),
                    top: Val::Px(58.0),
                    bottom: Val::Px(50.0),
                    width: Val::Px(500.0),
                    flex_direction: FlexDirection::Column,
                    padding: UiRect::all(Val::Px(12.0)),
                    border: UiRect::left(Val::Px(2.0)),
                    overflow: Overflow::scroll_y(),
                    ..default()
                },
                BackgroundColor(Color::srgba_u8(0x07, 0x0c, 0x17, 0xf2)),
                BorderColor::all(IGGY_ORANGE.with_alpha(0.3)),
            ))
            .with_children(|panel| {
                panel.spawn((
                    Text::new("PACK ACTIVITY LOG"),
                    TextFont {
                        font_size: 16.0,
                        ..default()
                    },
                    TextColor(IGGY_ORANGE),
                    Node {
                        margin: UiRect::bottom(Val::Px(4.0)),
                        ..default()
                    },
                ));
                panel.spawn((
                    Text::new("[D] toggle details  [E] close"),
                    TextFont {
                        font_size: 9.0,
                        ..default()
                    },
                    TextColor(DIM_GRAY),
                    Node {
                        margin: UiRect::bottom(Val::Px(8.0)),
                        ..default()
                    },
                ));
                panel.spawn((
                    Node {
                        width: Val::Percent(100.0),
                        height: Val::Px(1.0),
                        margin: UiRect::bottom(Val::Px(6.0)),
                        ..default()
                    },
                    BackgroundColor(IGGY_ORANGE.with_alpha(0.25)),
                ));
                panel.spawn((
                    EventLogContent,
                    Node {
                        flex_direction: FlexDirection::ColumnReverse,
                        flex_grow: 1.0,
                        overflow: Overflow::scroll_y(),
                        ..default()
                    },
                ));
            });

            root.spawn((
                ConsolePanel,
                Node {
                    position_type: PositionType::Absolute,
                    left: Val::Px(0.0),
                    right: Val::Px(0.0),
                    top: Val::Percent(-50.0),
                    height: Val::Percent(50.0),
                    flex_direction: FlexDirection::Column,
                    padding: UiRect::new(Val::Px(16.0), Val::Px(16.0), Val::Px(10.0), Val::Px(8.0)),
                    overflow: Overflow::scroll_y(),
                    ..default()
                },
                BackgroundColor(Color::srgba_u8(0x02, 0x05, 0x0c, 0xee)),
            ))
            .with_children(|panel| {
                panel.spawn((
                    ConsoleContent,
                    Node {
                        flex_direction: FlexDirection::ColumnReverse,
                        flex_grow: 1.0,
                        overflow: Overflow::scroll_y(),
                        ..default()
                    },
                ));
                panel.spawn((
                    Node {
                        width: Val::Percent(100.0),
                        height: Val::Px(1.0),
                        margin: UiRect::axes(Val::Px(0.0), Val::Px(4.0)),
                        ..default()
                    },
                    BackgroundColor(IGGY_ORANGE.with_alpha(0.35)),
                ));
                panel
                    .spawn(Node {
                        width: Val::Percent(100.0),
                        justify_content: JustifyContent::SpaceBetween,
                        align_items: AlignItems::Center,
                        ..default()
                    })
                    .with_children(|row| {
                        row.spawn((
                            Text::new("iggy.sim >"),
                            TextFont {
                                font_size: 11.0,
                                ..default()
                            },
                            TextColor(IGGY_ORANGE),
                        ));
                        row.spawn((
                            Text::new("~ or T to close"),
                            TextFont {
                                font_size: 9.0,
                                ..default()
                            },
                            TextColor(DIM_GRAY),
                        ));
                    });
            });

            root.spawn((
                EventLogToggleHint,
                Node {
                    position_type: PositionType::Absolute,
                    right: Val::Px(16.0),
                    bottom: Val::Px(54.0),
                    flex_direction: FlexDirection::Row,
                    column_gap: Val::Px(14.0),
                    ..default()
                },
            ))
            .with_children(|hint| {
                hint.spawn((
                    Text::new("[E] event log   [~ / T] console"),
                    TextFont {
                        font_size: 10.0,
                        ..default()
                    },
                    TextColor(DIM_GRAY),
                ));
            });
        });
}
