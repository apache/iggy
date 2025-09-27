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

use std::fmt::Display;
use std::str::FromStr;
use yew::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub enum MeasurementType {
    Latency,
    Throughput,
}

impl Display for MeasurementType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeasurementType::Latency => write!(f, "Latency"),
            MeasurementType::Throughput => write!(f, "Throughput"),
        }
    }
}

impl FromStr for MeasurementType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Latency" => Ok(MeasurementType::Latency),
            "Throughput" => Ok(MeasurementType::Throughput),
            _ => Err(()),
        }
    }
}

#[derive(Properties, PartialEq)]
#[allow(dead_code)]
pub struct MeasurementTypeSelectorProps {
    pub selected_measurement: MeasurementType,
    pub on_measurement_select: Callback<MeasurementType>,
}

#[function_component(MeasurementTypeSelector)]
pub fn measurement_type_selector(props: &MeasurementTypeSelectorProps) -> Html {
    let is_latency = matches!(props.selected_measurement, MeasurementType::Latency);

    html! {
        <div class="view-mode-container">
            <h3>{"Measurements"}</h3>
            <div class="segmented-control">
                <button
                    class={if is_latency { "segment active" } else { "segment" }}
                    onclick={props.on_measurement_select.reform(|_| MeasurementType::Latency)}
                >
                    {"Latency"}
                </button>
                <button
                    class={if !is_latency { "segment active" } else { "segment" }}
                    onclick={props.on_measurement_select.reform(|_| MeasurementType::Throughput)}
                >
                    {"Throughput"}
                </button>
            </div>
        </div>
    }
}
