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

use crate::state::ui::{MetricField, MetricRange, ParamField, ParamRange, UiAction, use_ui};
use yew::prelude::*;

#[function_component(ParamFiltersPanel)]
pub fn param_filters_panel() -> Html {
    let ui = use_ui();
    let is_open = use_state(|| false);

    let toggle = {
        let is_open = is_open.clone();
        Callback::from(move |_| is_open.set(!*is_open))
    };

    let on_clear = {
        let ui = ui.clone();
        Callback::from(move |e: MouseEvent| {
            e.stop_propagation();
            ui.dispatch(UiAction::ClearParamFilters);
        })
    };

    let filters = ui.param_filters.clone();
    let active = filters.active_count();
    let chevron = if *is_open { "v" } else { ">" };

    let title = if active > 0 {
        format!("Filters ({active} active)")
    } else {
        "Filters".to_string()
    };

    html! {
        <div class="param-filters-container">
            <button type="button" class="param-filters-header" onclick={toggle}>
                <span class="param-filters-chevron">{chevron}</span>
                <span class={classes!("param-filters-title", (active > 0).then_some("has-active"))}>
                    {title}
                </span>
            </button>
            if *is_open {
                <div class="param-filters-body">
                    <ParamFilterRow label="Producers" field={ParamField::Producers}
                        range={filters.producers.clone()} />
                    <ParamFilterRow label="Consumers" field={ParamField::Consumers}
                        range={filters.consumers.clone()} />
                    <ParamFilterRow label="Streams" field={ParamField::Streams}
                        range={filters.streams.clone()} />
                    <ParamFilterRow label="Partitions" field={ParamField::Partitions}
                        range={filters.partitions.clone()} />
                    <ParamFilterRow label="Cons. Groups" field={ParamField::ConsumerGroups}
                        range={filters.consumer_groups.clone()} />
                    <MetricFilterRow label="Tput (MB/s)" field={MetricField::ThroughputMbS}
                        range={filters.throughput_mb_s.clone()} step="1" />
                    <MetricFilterRow label="P99 (ms)" field={MetricField::P99LatencyMs}
                        range={filters.p99_latency_ms.clone()} step="0.01" />
                    if active > 0 {
                        <button type="button" class="param-filters-clear" onclick={on_clear}>
                            {"Clear all"}
                        </button>
                    }
                </div>
            }
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct ParamFilterRowProps {
    label: AttrValue,
    field: ParamField,
    range: ParamRange,
}

#[function_component(ParamFilterRow)]
fn param_filter_row(props: &ParamFilterRowProps) -> Html {
    let ui = use_ui();
    let field = props.field;
    let range = props.range.clone();

    let on_from = {
        let ui = ui.clone();
        let range = range.clone();
        Callback::from(move |e: InputEvent| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            let next = ParamRange {
                from: parse_u32(&input.value()),
                to: range.to,
            };
            ui.dispatch(UiAction::SetParamRange(field, next));
        })
    };

    let on_to = {
        let ui = ui.clone();
        let range = range.clone();
        Callback::from(move |e: InputEvent| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            let next = ParamRange {
                from: range.from,
                to: parse_u32(&input.value()),
            };
            ui.dispatch(UiAction::SetParamRange(field, next));
        })
    };

    let from_value = range.from.map(|v| v.to_string()).unwrap_or_default();
    let to_value = range.to.map(|v| v.to_string()).unwrap_or_default();

    html! {
        <div class="param-filter-row">
            <label class="param-filter-label">{&props.label}</label>
            <input type="number" min="0" step="1" class="param-filter-input"
                placeholder="min" value={from_value} oninput={on_from} />
            <span class="param-filter-sep">{"-"}</span>
            <input type="number" min="0" step="1" class="param-filter-input"
                placeholder="max" value={to_value} oninput={on_to} />
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct MetricFilterRowProps {
    label: AttrValue,
    field: MetricField,
    range: MetricRange,
    step: AttrValue,
}

#[function_component(MetricFilterRow)]
fn metric_filter_row(props: &MetricFilterRowProps) -> Html {
    let ui = use_ui();
    let field = props.field;
    let range = props.range.clone();

    let on_from = {
        let ui = ui.clone();
        let range = range.clone();
        Callback::from(move |e: InputEvent| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            let next = MetricRange {
                from: parse_f64(&input.value()),
                to: range.to,
            };
            ui.dispatch(UiAction::SetMetricRange(field, next));
        })
    };

    let on_to = {
        let ui = ui.clone();
        let range = range.clone();
        Callback::from(move |e: InputEvent| {
            let input: web_sys::HtmlInputElement = e.target_unchecked_into();
            let next = MetricRange {
                from: range.from,
                to: parse_f64(&input.value()),
            };
            ui.dispatch(UiAction::SetMetricRange(field, next));
        })
    };

    let from_value = range.from.map(format_f64).unwrap_or_default();
    let to_value = range.to.map(format_f64).unwrap_or_default();

    html! {
        <div class="param-filter-row">
            <label class="param-filter-label">{&props.label}</label>
            <input type="number" min="0" step={props.step.clone()} class="param-filter-input"
                placeholder="min" value={from_value} oninput={on_from} />
            <span class="param-filter-sep">{"-"}</span>
            <input type="number" min="0" step={props.step.clone()} class="param-filter-input"
                placeholder="max" value={to_value} oninput={on_to} />
        </div>
    }
}

fn parse_u32(raw: &str) -> Option<u32> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        trimmed.parse().ok()
    }
}

fn parse_f64(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse().ok().filter(|v: &f64| v.is_finite())
}

fn format_f64(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{v:.0}")
    } else {
        format!("{v}")
    }
}
