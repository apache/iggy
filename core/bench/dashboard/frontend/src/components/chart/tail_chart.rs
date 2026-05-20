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

use crate::format::format_ms;
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::group_metrics_summary::BenchmarkGroupMetricsSummary;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct TailChartProps {
    pub benchmark: BenchmarkReportLight,
    #[prop_or(None)]
    pub on_details: Option<Callback<()>>,
    #[prop_or_default]
    pub compact: bool,
}

#[function_component(TailChart)]
pub fn tail_chart(props: &TailChartProps) -> Html {
    let hovered = use_state(|| None::<usize>);

    let Some(summary) = props.benchmark.group_metrics.first().map(|m| &m.summary) else {
        return html! {};
    };

    let points = percentile_points(summary);
    let max_latency = points
        .iter()
        .map(|(_, _, latency)| *latency)
        .fold(0.0_f64, f64::max);
    if max_latency <= 0.0 {
        return html! {};
    }
    let projected: Vec<Projected> = points
        .iter()
        .map(|(label, percentile, latency)| {
            let (screen_x, screen_y) = project_percentile(*percentile, *latency, max_latency);
            Projected {
                x: screen_x,
                y: screen_y,
                latency_ms: *latency,
                label,
            }
        })
        .collect();

    let line_path = polyline(&projected);
    let area_path = closed_polyline(&projected);
    let details_button = props.on_details.as_ref().map(|details_callback| {
        let details_callback = details_callback.clone();
        html! {
            <button
                type="button"
                class="tail-chart-details-btn"
                onclick={Callback::from(move |_| details_callback.emit(()))}
            >
                {"View details"}
                <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
                     fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <line x1="5" y1="12" x2="19" y2="12" />
                    <polyline points="12 5 19 12 12 19" />
                </svg>
            </button>
        }
    });

    let wrap_classes = classes!(
        "tail-chart-wrap",
        props.compact.then_some("tail-chart-wrap-compact")
    );

    html! {
        <div class={wrap_classes}>
            <div class="tail-chart-header">
                <div class="tail-chart-legend">
                    <span class="tail-chart-legend-dot" />
                    <span class="tail-chart-legend-label">{"Latency percentiles"}</span>
                    <span class="tail-chart-legend-hint">{"hover for details"}</span>
                </div>
                <div class="tail-chart-header-right">
                    <span class="tail-chart-subject" title={props.benchmark.params.pretty_name.clone()}>
                        { props.benchmark.params.pretty_name.clone() }
                    </span>
                    { details_button.unwrap_or_default() }
                </div>
            </div>
            <svg
                class="tail-chart"
                viewBox={format!("0 0 {} {}", CHART_W as i32, CHART_H as i32)}
                preserveAspectRatio="xMidYMid meet"
                onmouseleave={ {
                    let hovered = hovered.clone();
                    Callback::from(move |_| hovered.set(None))
                } }
            >
                <defs>
                    <linearGradient id="tail-area-grad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stop-color="#ff9103" stop-opacity="0.32" />
                        <stop offset="100%" stop-color="#ff9103" stop-opacity="0" />
                    </linearGradient>
                    <filter id="tail-line-glow" x="-10%" y="-50%" width="120%" height="200%">
                        <feGaussianBlur stdDeviation="2.5" result="blur" />
                        <feMerge>
                            <feMergeNode in="blur" />
                            <feMergeNode in="SourceGraphic" />
                        </feMerge>
                    </filter>
                    <filter id="tail-dot-glow" x="-80%" y="-80%" width="260%" height="260%">
                        <feGaussianBlur stdDeviation="4" result="blur" />
                        <feMerge>
                            <feMergeNode in="blur" />
                            <feMergeNode in="SourceGraphic" />
                        </feMerge>
                    </filter>
                </defs>
                { render_gridlines(max_latency) }
                <path class="tail-chart-area" d={area_path} fill="url(#tail-area-grad)" />
                <path
                    class="tail-chart-line"
                    d={line_path}
                    fill="none"
                    stroke="#ff9103"
                    stroke-width="2.5"
                    stroke-linejoin="round"
                    stroke-linecap="round"
                    filter="url(#tail-line-glow)"
                />
                { render_crosshair(hovered.as_ref().and_then(|i| projected.get(*i))) }
                { render_points(&projected, *hovered, hovered.clone()) }
                { render_axis_labels(&projected, *hovered) }
                { render_tooltip(hovered.as_ref().and_then(|i| projected.get(*i))) }
            </svg>
        </div>
    }
}

const CHART_W: f64 = 1000.0;
const CHART_H: f64 = 340.0;
const MARGIN_L: f64 = 56.0;
const MARGIN_R: f64 = 24.0;
const MARGIN_T: f64 = 24.0;
const MARGIN_B: f64 = 44.0;

const PERCENTILE_POINTS: [(&str, f64); 6] = [
    ("P50", 50.0),
    ("P90", 90.0),
    ("P95", 95.0),
    ("P99", 99.0),
    ("P99.9", 99.9),
    ("P99.99", 99.99),
];

struct Projected {
    x: f64,
    y: f64,
    latency_ms: f64,
    label: &'static str,
}

fn render_gridlines(max_latency: f64) -> Html {
    let steps = 4;
    let items: Vec<_> = (0..=steps)
        .map(|index| {
            let fraction = index as f64 / steps as f64;
            let y = CHART_H - MARGIN_B - fraction * (CHART_H - MARGIN_T - MARGIN_B);
            let value = fraction * max_latency;
            html! {
                <g class="tail-chart-grid">
                    <line x1={MARGIN_L.to_string()} y1={y.to_string()}
                          x2={(CHART_W - MARGIN_R).to_string()} y2={y.to_string()} />
                    <text x={(MARGIN_L - 10.0).to_string()} y={(y + 3.5).to_string()} text-anchor="end">
                        {format_ms(value)}
                    </text>
                </g>
            }
        })
        .collect();
    html! {
        <>
            {for items}
            <text
                class="tail-chart-axis-unit"
                x={(MARGIN_L - 10.0).to_string()}
                y={(MARGIN_T - 8.0).to_string()}
                text-anchor="end"
            >{"ms"}</text>
        </>
    }
}

fn render_axis_labels(projected: &[Projected], hovered: Option<usize>) -> Html {
    html! {
        <g class="tail-chart-axis-labels">
            { for projected.iter().enumerate().map(|(index, point)| {
                let is_active = Some(index) == hovered;
                html! {
                    <text
                        class={if is_active { "tail-chart-axis-label-active" } else { "" }}
                        x={point.x.to_string()}
                        y={(CHART_H - MARGIN_B + 22.0).to_string()}
                        text-anchor="middle"
                    >
                        {point.label}
                    </text>
                }
            })}
        </g>
    }
}

fn render_points(
    projected: &[Projected],
    hovered: Option<usize>,
    hovered_handle: UseStateHandle<Option<usize>>,
) -> Html {
    html! {
        <g>
            { for projected.iter().enumerate().map(|(index, point)| {
                let is_active = Some(index) == hovered;
                let on_enter = {
                    let hovered_handle = hovered_handle.clone();
                    Callback::from(move |_| hovered_handle.set(Some(index)))
                };
                html! {
                    <g
                        class={classes!("tail-chart-point", is_active.then_some("active"))}
                        style={format!("--stagger: {index}")}
                        onmouseenter={on_enter.clone()}
                        onmouseover={on_enter}
                    >
                        <circle
                            class="tail-chart-point-outer"
                            cx={point.x.to_string()}
                            cy={point.y.to_string()}
                            r="22"
                            fill="transparent"
                            pointer-events="all"
                        />
                        {
                            if is_active {
                                html! {
                                    <circle
                                        class="tail-chart-point-halo"
                                        cx={point.x.to_string()}
                                        cy={point.y.to_string()}
                                        r="9"
                                        fill="none"
                                        stroke="#ff9103"
                                        stroke-width="1.2"
                                        stroke-opacity="0.5"
                                    />
                                }
                            } else {
                                html! {}
                            }
                        }
                        <circle
                            class="tail-chart-point-dot"
                            cx={point.x.to_string()}
                            cy={point.y.to_string()}
                            r={if is_active { "4.5" } else { "3" }}
                            filter={if is_active { "url(#tail-dot-glow)" } else { "" }}
                        />
                    </g>
                }
            })}
        </g>
    }
}

fn render_crosshair(hovered: Option<&Projected>) -> Html {
    let Some(point) = hovered else {
        return html! {};
    };
    html! {
        <g class="tail-chart-crosshair">
            <line
                x1={point.x.to_string()}
                y1={MARGIN_T.to_string()}
                x2={point.x.to_string()}
                y2={(CHART_H - MARGIN_B).to_string()}
            />
            <line
                x1={MARGIN_L.to_string()}
                y1={point.y.to_string()}
                x2={(CHART_W - MARGIN_R).to_string()}
                y2={point.y.to_string()}
            />
        </g>
    }
}

fn render_tooltip(hovered: Option<&Projected>) -> Html {
    let Some(point) = hovered else {
        return html! {};
    };
    let box_width = 110.0;
    let box_height = 46.0;
    let gap = 12.0;

    let mut box_x = point.x - box_width / 2.0;
    let mut box_y = point.y - box_height - gap;
    if box_x < MARGIN_L {
        box_x = MARGIN_L;
    }
    if box_x + box_width > CHART_W - MARGIN_R {
        box_x = CHART_W - MARGIN_R - box_width;
    }
    if box_y < MARGIN_T {
        box_y = point.y + gap;
    }
    let center_x = box_x + box_width / 2.0;

    html! {
        <g class="tail-chart-tooltip" pointer-events="none">
            <rect
                x={box_x.to_string()}
                y={box_y.to_string()}
                width={box_width.to_string()}
                height={box_height.to_string()}
                rx="8"
                ry="8"
                class="tail-chart-tooltip-bg"
            />
            <text
                x={center_x.to_string()}
                y={(box_y + 18.0).to_string()}
                text-anchor="middle"
                class="tail-chart-tooltip-label"
            >
                {point.label}
            </text>
            <text
                x={center_x.to_string()}
                y={(box_y + 34.0).to_string()}
                text-anchor="middle"
                class="tail-chart-tooltip-value"
            >
                {format!("{} ms", format_ms(point.latency_ms))}
            </text>
        </g>
    }
}

fn polyline(projected: &[Projected]) -> String {
    projected
        .iter()
        .enumerate()
        .map(|(index, point)| {
            let command = if index == 0 { 'M' } else { 'L' };
            format!("{command}{:.1},{:.1}", point.x, point.y)
        })
        .collect()
}

fn closed_polyline(projected: &[Projected]) -> String {
    let baseline_y = CHART_H - MARGIN_B;
    match (projected.first(), projected.last()) {
        (Some(first), Some(last)) => {
            format!(
                "M{:.1},{baseline_y:.1} {} L{:.1},{baseline_y:.1} Z",
                first.x,
                polyline(projected),
                last.x
            )
        }
        _ => String::new(),
    }
}

fn percentile_points(summary: &BenchmarkGroupMetricsSummary) -> Vec<(&'static str, f64, f64)> {
    let values = [
        summary.average_p50_latency_ms,
        summary.average_p90_latency_ms,
        summary.average_p95_latency_ms,
        summary.average_p99_latency_ms,
        summary.average_p999_latency_ms,
        summary.average_p9999_latency_ms,
    ];
    PERCENTILE_POINTS
        .iter()
        .zip(values.iter())
        .map(|((label, percentile), latency)| (*label, *percentile, *latency))
        .collect()
}

fn project_percentile(percentile: f64, latency_ms: f64, max_latency: f64) -> (f64, f64) {
    let tail_fraction = (1.0 - percentile / 100.0).max(1e-5);
    let x_log = -tail_fraction.log10();
    let x_min = -(1.0 - 0.50_f64).log10();
    let x_max = -(1.0 - 0.9999_f64).log10();
    let span = x_max - x_min;
    let normalized = if span > 0.0 {
        ((x_log - x_min) / span).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let screen_x = MARGIN_L + normalized * (CHART_W - MARGIN_L - MARGIN_R);

    let y_fraction = if max_latency > 0.0 {
        (latency_ms / max_latency).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let screen_y = CHART_H - MARGIN_B - y_fraction * (CHART_H - MARGIN_T - MARGIN_B);
    (screen_x, screen_y)
}
