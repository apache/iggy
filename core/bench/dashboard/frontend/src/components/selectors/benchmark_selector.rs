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

use crate::{
    components::selectors::benchmark_kind_selector::BenchmarkKindSelector,
    state::benchmark::{BenchmarkAction, use_benchmark},
};
use bench_report::benchmark_kind::BenchmarkKind;
use std::collections::HashSet;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct BenchmarkSelectorProps {
    pub kind: BenchmarkKind,
}

#[function_component(BenchmarkSelector)]
pub fn benchmark_selector(props: &BenchmarkSelectorProps) -> Html {
    let benchmark_ctx = use_benchmark();
    let selected_kind = benchmark_ctx.state.selected_kind;

    let available_kinds: HashSet<_> = benchmark_ctx
        .state
        .entries
        .keys()
        .filter(|k| match props.kind {
            BenchmarkKind::PinnedProducer
            | BenchmarkKind::PinnedConsumer
            | BenchmarkKind::PinnedProducerAndConsumer => {
                matches!(
                    k,
                    BenchmarkKind::PinnedProducer
                        | BenchmarkKind::PinnedConsumer
                        | BenchmarkKind::PinnedProducerAndConsumer
                )
            }
            BenchmarkKind::BalancedProducer
            | BenchmarkKind::BalancedConsumerGroup
            | BenchmarkKind::BalancedProducerAndConsumerGroup => {
                matches!(
                    k,
                    BenchmarkKind::BalancedProducer
                        | BenchmarkKind::BalancedConsumerGroup
                        | BenchmarkKind::BalancedProducerAndConsumerGroup
                )
            }
            BenchmarkKind::EndToEndProducingConsumer
            | BenchmarkKind::EndToEndProducingConsumerGroup => {
                matches!(
                    k,
                    BenchmarkKind::EndToEndProducingConsumer
                        | BenchmarkKind::EndToEndProducingConsumerGroup
                )
            }
        })
        .cloned()
        .collect();

    let empty_vec = Vec::new();
    let current_benchmarks = benchmark_ctx
        .state
        .entries
        .get(&selected_kind)
        .unwrap_or(&empty_vec);

    let on_benchmark_select = {
        let dispatch = benchmark_ctx.dispatch.clone();
        let entries = benchmark_ctx.state.entries.clone();
        Callback::from(move |pretty_name: String| {
            let selected_benchmark = entries.get(&selected_kind).and_then(|benchmarks| {
                benchmarks
                    .iter()
                    .find(|b| b.params.pretty_name == pretty_name)
            });
            dispatch.emit(BenchmarkAction::SelectBenchmark(Box::new(
                selected_benchmark.cloned(),
            )));
        })
    };

    let on_kind_select = {
        let dispatch = benchmark_ctx.dispatch.clone();
        Callback::from(move |kind: BenchmarkKind| {
            dispatch.emit(BenchmarkAction::SelectBenchmarkKind(kind));
        })
    };

    let current_value = benchmark_ctx
        .state
        .selected_benchmark
        .as_ref()
        .map(|b| b.params.pretty_name.clone())
        .unwrap_or_default();

    html! {
        <div class="benchmark-select">
            <BenchmarkKindSelector
                selected_kind={selected_kind}
                on_kind_select={on_kind_select}
                available_kinds={available_kinds}
            />

            <div class="benchmark-list">
                {current_benchmarks.iter().map(|benchmark| {
                    let pretty_name = benchmark.params.pretty_name.clone();
                    let is_active = pretty_name == current_value;
                    let on_click = {
                        let on_benchmark_select = on_benchmark_select.clone();
                        let pretty_name = pretty_name.clone();
                        Callback::from(move |_| {
                            on_benchmark_select.emit(pretty_name.clone());
                        })
                    };
                    let pretty_name = pretty_name.split("(").next().unwrap().to_string();

                    html! {
                        <div
                            class={classes!(
                                "benchmark-list-item",
                                is_active.then_some("active")
                            )}
                            onclick={on_click}
                        >
                            <div class="benchmark-list-item-content">
                                <div class="benchmark-list-item-title">
                                    <span class="benchmark-list-item-dot" />
                                    {pretty_name}
                                </div>

                                <div class="benchmark-list-item-details">
                                    {if let Some(remark) = benchmark.params.remark.as_deref() {
                                        if !remark.is_empty() {
                                            let truncated_remark = if remark.len() > 30 {
                                                format!("{}..", &remark[0..28])
                                            } else {
                                                remark.to_string()
                                            };
                                            html! {
                                                <div class="benchmark-list-item-subtitle">
                                                    <span class="benchmark-list-item-label">{"Remark:"}</span>
                                                    <span>{truncated_remark}</span>
                                                </div>
                                            }
                                        } else {
                                            html! {}
                                        }
                                    } else {
                                        html! {}
                                    }}
                                </div>
                            </div>
                        </div>
                    }
                }).collect::<Html>()}
            </div>
        </div>
    }
}
