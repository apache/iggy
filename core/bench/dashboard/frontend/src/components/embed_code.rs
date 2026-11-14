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

use yew::prelude::*;

use crate::components::command_block::CommandBlock;
use crate::config::get_api_base_url;

/// The chart of a run that an embed shows.
#[derive(Clone, Debug, PartialEq)]
pub enum MeasurementType {
    Latency,
    Throughput,
}

#[derive(Clone, PartialEq)]
enum EmbedTab {
    Iframe,
    Image,
}

#[derive(Clone, PartialEq)]
enum ImageFormat {
    Markdown,
    Html,
}

#[derive(Properties, PartialEq)]
pub struct EmbedCodeProps {
    pub uuid: String,
    pub measurement_type: MeasurementType,
    pub is_dark: bool,
}

/// The code that puts a run's chart on another page.
#[function_component(EmbedCode)]
pub fn embed_code(props: &EmbedCodeProps) -> Html {
    let active_tab = use_state(|| EmbedTab::Iframe);
    let image_format = use_state(|| ImageFormat::Markdown);

    let chart_type = match &props.measurement_type {
        MeasurementType::Latency => "latency",
        MeasurementType::Throughput => "throughput",
    };

    let theme = if props.is_dark { "dark" } else { "light" };
    let base_url = get_api_base_url();
    let embed_url = format!(
        "{base_url}/embed/{uuid}?type={chart_type}&theme={theme}",
        uuid = props.uuid,
    );
    let benchmark_url = format!("{base_url}/benchmarks/{uuid}", uuid = props.uuid);
    let png_url = format!(
        "{base_url}/embed/{uuid}/chart.png?type={chart_type}&theme={theme}&width=1600&height=1200&legend=true",
        uuid = props.uuid,
    );

    let iframe_snippet =
        format!(r#"<iframe src="{embed_url}" width="800" height="600" frameborder="0"></iframe>"#);

    let markdown_snippet = format!("[![Iggy Benchmark]({png_url})]({benchmark_url})");
    let html_image_snippet = format!(
        r#"<a href="{benchmark_url}"><img src="{png_url}" alt="Iggy Benchmark" width="800"></a>"#
    );

    let snippet = match *active_tab {
        EmbedTab::Iframe => iframe_snippet,
        EmbedTab::Image => match *image_format {
            ImageFormat::Markdown => markdown_snippet,
            ImageFormat::Html => html_image_snippet,
        },
    };

    let on_iframe_tab = {
        let active_tab = active_tab.clone();
        Callback::from(move |_| active_tab.set(EmbedTab::Iframe))
    };

    let on_image_tab = {
        let active_tab = active_tab.clone();
        Callback::from(move |_| active_tab.set(EmbedTab::Image))
    };

    let on_markdown_format = {
        let image_format = image_format.clone();
        Callback::from(move |_| image_format.set(ImageFormat::Markdown))
    };

    let on_html_format = {
        let image_format = image_format.clone();
        Callback::from(move |_| image_format.set(ImageFormat::Html))
    };

    let image = *active_tab == EmbedTab::Image;
    html! {
        <div class="embed-code">
            <div class="explore-kinds" role="group" aria-label="Embed as">
                { choice("Iframe", !image, on_iframe_tab) }
                { choice("Image", image, on_image_tab) }
            </div>
            if image {
                <div class="explore-kinds" role="group" aria-label="Image format">
                    { choice("Markdown", *image_format == ImageFormat::Markdown, on_markdown_format) }
                    { choice("HTML", *image_format == ImageFormat::Html, on_html_format) }
                </div>
            }
            <CommandBlock command={snippet} />
            if image {
                <a class="run-action" href={png_url} target="_blank" rel="noopener">
                    {"Download PNG"}
                </a>
                <p class="card-note">{"Themes: dark, light, dark_nobg, light_nobg"}</p>
            }
        </div>
    }
}

/// An embed option, styled like Explore's kind chips.
fn choice(label: &'static str, pressed: bool, onclick: Callback<MouseEvent>) -> Html {
    html! {
        <button type="button" class="explore-kind" aria-pressed={pressed.to_string()} {onclick}>
            { label }
        </button>
    }
}
