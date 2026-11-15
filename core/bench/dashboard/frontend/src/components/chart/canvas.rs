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

//! The frame that draws a charming chart in the page's theme. The pages build their own options and
//! hand the chart to [`ChartCanvas`].

use std::rc::Rc;

use charming::theme::Theme;
use charming::{Chart, WasmRenderer};
use gloo::console::error;
use web_sys::Element;
use yew::prelude::*;

use super::{dispose_chart_at, reset_zoom_at};
use crate::hooks::use_size;

/// A chart that compares by its allocation, because it can hold thousands of points per line.
/// Holding the `Rc` keeps the allocation alive, so a new chart never takes its address.
#[derive(Clone)]
pub struct SharedChart(Rc<Chart>);

impl SharedChart {
    pub fn new(chart: Chart) -> Self {
        Self(Rc::new(chart))
    }
}

impl PartialEq for SharedChart {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Properties, PartialEq)]
pub struct ChartCanvasProps {
    /// Unique on the page.
    pub id: AttrValue,
    /// Nothing is drawn while `None`, for example while the data loads.
    pub chart: Option<SharedChart>,
    /// Over the chart, for example the loading state.
    #[prop_or_default]
    pub children: Html,
    /// Added to the frame, for example to change its height.
    #[prop_or_default]
    pub class: Classes,
}

/// Draws the chart at its frame's size and in the page's theme, and redraws on each change. A double
/// click zooms a chart with a zoom slider out to the whole run.
#[function_component(ChartCanvas)]
pub fn chart_canvas(props: &ChartCanvasProps) -> Html {
    let is_dark = use_context::<(bool, Callback<()>)>().is_some_and(|(is_dark, _)| is_dark);
    let frame = use_node_ref();
    let canvas = use_node_ref();
    let size = use_size(frame.clone());
    {
        let (id, canvas) = (props.id.clone(), canvas.clone());
        use_effect_with(
            (props.chart.clone(), size, is_dark),
            move |(chart, (width, height), is_dark)| {
                let element = canvas.cast::<Element>();
                if let (Some(SharedChart(chart)), Some(_)) = (chart, &element)
                    && *width > 0
                    && *height > 0
                {
                    render(&id, chart, (*width, *height), *is_dark);
                }
                move || {
                    if let Some(element) = element {
                        dispose_chart_at(&element);
                    }
                }
            },
        );
    }
    let ondblclick = {
        let canvas = canvas.clone();
        Callback::from(move |_: MouseEvent| {
            if let Some(element) = canvas.cast::<Element>() {
                reset_zoom_at(&element);
            }
        })
    };
    html! {
        <div class={classes!("run-chart", props.class.clone())} ref={frame}>
            <div id={props.id.clone()} class="run-chart-canvas" ref={canvas} {ondblclick}></div>
            <div class="run-chart-status">{ props.children.clone() }</div>
        </div>
    }
}

fn render(canvas_id: &str, chart: &Chart, (width, height): (u32, u32), is_dark: bool) {
    let theme = if is_dark { Theme::Dark } else { Theme::Default };
    if let Err(render_error) = WasmRenderer::new(width, height)
        .theme(theme)
        .render(canvas_id, chart)
    {
        error!(format!(
            "Couldn't draw the {canvas_id} chart: {render_error}"
        ));
    }
}
