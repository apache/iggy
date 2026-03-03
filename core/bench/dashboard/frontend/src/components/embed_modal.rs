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

use crate::components::selectors::measurement_type_selector::MeasurementType;
use crate::config::get_api_base_url;
use gloo::timers::callback::Timeout;
use web_sys::window;
use yew::prelude::*;

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
pub struct EmbedModalProps {
    pub uuid: String,
    pub measurement_type: MeasurementType,
    pub is_dark: bool,
}

#[function_component(EmbedModal)]
pub fn embed_modal(props: &EmbedModalProps) -> Html {
    let active_tab = use_state(|| EmbedTab::Iframe);
    let image_format = use_state(|| ImageFormat::Markdown);
    let notification_visible = use_state(|| false);

    let chart_type = match &props.measurement_type {
        MeasurementType::Latency => "latency",
        MeasurementType::Throughput => "throughput",
        MeasurementType::Distribution => "distribution",
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
        EmbedTab::Iframe => iframe_snippet.clone(),
        EmbedTab::Image => match *image_format {
            ImageFormat::Markdown => markdown_snippet.clone(),
            ImageFormat::Html => html_image_snippet.clone(),
        },
    };

    let on_copy = {
        let snippet = snippet.clone();
        let notification_visible = notification_visible.clone();
        Callback::from(move |_| {
            if let Some(window) = window() {
                let clipboard = window.navigator().clipboard();
                let _ = clipboard.write_text(&snippet);
                notification_visible.set(true);

                let notification_visible = notification_visible.clone();
                let timeout = Timeout::new(1_000, move || {
                    notification_visible.set(false);
                });
                timeout.forget();
            }
        })
    };

    let on_download = {
        let png_url = png_url.clone();
        Callback::from(move |_| {
            if let Some(window) = window() {
                let _ = window.open_with_url_and_target(&png_url, "_blank");
            }
        })
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

    html! {
        <div class="embed-modal">
            <div class="tooltip-section">
                <h4>{"Embed Chart"}</h4>
                <div class="embed-tabs">
                    <button
                        class={classes!("embed-tab", (*active_tab == EmbedTab::Iframe).then_some("active"))}
                        onclick={on_iframe_tab}
                    >
                        {"Iframe"}
                    </button>
                    <button
                        class={classes!("embed-tab", (*active_tab == EmbedTab::Image).then_some("active"))}
                        onclick={on_image_tab}
                    >
                        {"Image"}
                    </button>
                </div>
                {
                    if *active_tab == EmbedTab::Image {
                        html! {
                            <div class="embed-format-toggle">
                                <button
                                    class={classes!("embed-format-btn", (*image_format == ImageFormat::Markdown).then_some("active"))}
                                    onclick={on_markdown_format}
                                >
                                    {"Markdown"}
                                </button>
                                <button
                                    class={classes!("embed-format-btn", (*image_format == ImageFormat::Html).then_some("active"))}
                                    onclick={on_html_format}
                                >
                                    {"HTML"}
                                </button>
                            </div>
                        }
                    } else {
                        html! {}
                    }
                }
                <div class="tooltip-content">
                    <p class="command-row">
                        <span class="command-text embed-snippet">{&snippet}</span>
                        <span class="copy-button-container">
                            <button onclick={on_copy} class="copy-button">{"Copy"}</button>
                            <span class={classes!(
                                "copy-notification",
                                (*notification_visible).then_some("visible")
                            )}>
                                {"Copied!"}
                            </span>
                        </span>
                    </p>
                    {
                        if *active_tab == EmbedTab::Image {
                            html! {
                                <>
                                    <p class="embed-download-row">
                                        <button onclick={on_download} class="copy-button embed-download-button">
                                            {"Download PNG"}
                                        </button>
                                    </p>
                                    <p class="embed-theme-hint">
                                        {"Themes: dark, light, dark_nobg, light_nobg"}
                                    </p>
                                </>
                            }
                        } else {
                            html! {}
                        }
                    }
                </div>
            </div>
        </div>
    }
}
