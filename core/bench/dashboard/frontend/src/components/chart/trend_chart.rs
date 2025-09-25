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

use crate::api::fetch_benchmark_trend;
use crate::components::chart::plot_trend::create_chart;
use crate::components::chart::{PlotConfig, PlotType, dispose_chart};
use crate::components::selectors::measurement_type_selector::MeasurementType;
use crate::state::hardware::use_hardware;
use charming::Echarts;
use gloo::console::log;
use yew::platform::spawn_local;
use yew::prelude::*;
use yew_hooks::use_size;

#[derive(Properties, PartialEq)]
#[allow(dead_code)]
pub struct TrendChartProps {
    pub params_identifier: String,
    pub measurement_type: MeasurementType,
    pub is_dark: bool,
}

#[function_component(TrendChart)]
pub fn trend_chart(props: &TrendChartProps) -> Html {
    let hardware_ctx = use_hardware();
    let chart_data = use_state(Vec::new);
    let chart_node = use_node_ref();
    let chart_size = use_size(chart_node.clone());
    let echarts = use_state(|| None::<Echarts>);

    {
        let params_identifier = props.params_identifier.clone();
        let hardware = hardware_ctx.state.selected_hardware.clone();
        let chart_data = chart_data.clone();

        use_effect_with(
            (params_identifier, hardware),
            move |(params_identifier, hardware)| {
                let params_identifier = params_identifier.clone();
                let hardware = hardware.clone();
                spawn_local(async move {
                    if let Some(hardware) = hardware {
                        match fetch_benchmark_trend(&hardware, &params_identifier).await {
                            Ok(data) => {
                                chart_data.set(data);
                            }
                            Err(e) => {
                                log!(format!("Error fetching trend data: {}", e));
                            }
                        }
                    }
                });
                Box::new(|| ())
            },
        );
    }

    {
        let data = (*chart_data).clone();
        let measurement_type = props.measurement_type.clone();
        let is_dark = props.is_dark;
        let echarts: UseStateHandle<Option<Echarts>> = echarts.clone();

        use_effect_with(
            (data, measurement_type, is_dark, chart_size),
            move |(data, measurement_type, is_dark, size)| {
                if !data.is_empty() {
                    let plot_type = match measurement_type {
                        MeasurementType::Latency => PlotType::Latency,
                        MeasurementType::Throughput => PlotType::Throughput,
                    };

                    let (width, height) = *size;
                    let config = PlotConfig {
                        width,
                        height,
                        is_dark: *is_dark,
                        element_id: "trend-chart".to_string(),
                    };

                    // Dispose existing chart if any
                    if echarts.is_some() {
                        dispose_chart("trend-chart");
                    }

                    // Render new chart
                    match create_chart(&config, data, &plot_type) {
                        Ok(new_e) => echarts.set(Some(new_e)),
                        Err(e) => log!(format!("Error rendering chart: {}", e)),
                    }
                } else {
                    log!(format!("No data to render chart"));
                }

                || ()
            },
        );
    }

    html! {
        <div ref={chart_node} id="trend-chart" style="width: calc(100% - 40px); height: calc(100% - 20px); margin: 10px 20px;"></div>
    }
}
